package main

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	batchConcurrency = 6  // workers per batch
	batchJobTTL      = 24 * time.Hour
)

type batchJob struct {
	ID         string             `json:"id"`
	Created    time.Time          `json:"created"`
	LastAccess time.Time          `json:"last_access"`
	Total      int                `json:"total"`
	Done       int                `json:"done"`
	Found      int                `json:"found"`
	Missing    int                `json:"missing"`
	Status     string             `json:"status"` // running | finished | error
	Items      []batchItem        `json:"items"`
	mu         sync.Mutex         `json:"-"`
	subs       map[chan batchEvt]struct{}
	subsMu     sync.Mutex
}

type batchItem struct {
	Input  string `json:"input"`            // exact user input string
	ISBN   string `json:"isbn"`             // normalised
	Found  bool   `json:"found"`
	Source string `json:"source,omitempty"`
	File   string `json:"file,omitempty"`   // basename in cache dir
	Error  string `json:"error,omitempty"`
}

type batchEvt struct {
	Done    int    `json:"done"`
	Total   int    `json:"total"`
	Found   int    `json:"found"`
	Missing int    `json:"missing"`
	Current string `json:"current,omitempty"`
	Source  string `json:"source,omitempty"`
	Status  string `json:"status,omitempty"`
}

type batchManager struct {
	dir   string
	cache *diskCache
	jobs  sync.Map // jobID -> *batchJob
}

func newBatchManager(cacheDir string, cache *diskCache) *batchManager {
	d := filepath.Join(cacheDir, "jobs")
	_ = os.MkdirAll(d, 0o755)
	bm := &batchManager{dir: d, cache: cache}
	bm.loadPersisted()
	go bm.startCleaner()
	return bm
}

// loadPersisted rehydrates jobs from meta.json files written by previous server
// runs so the UI's batch list survives restarts. Running jobs are downgraded to
// "error" since their workers are gone.
func (bm *batchManager) loadPersisted() {
	entries, err := os.ReadDir(bm.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		meta := filepath.Join(bm.dir, e.Name(), "meta.json")
		b, err := os.ReadFile(meta)
		if err != nil {
			continue
		}
		var j batchJob
		if err := json.Unmarshal(b, &j); err != nil {
			continue
		}
		if j.Status == "running" {
			j.Status = "error"
		}
		j.subs = map[chan batchEvt]struct{}{}
		bm.jobs.Store(j.ID, &j)
	}
}

func (bm *batchManager) jobDir(id string) string { return filepath.Join(bm.dir, id) }

func (bm *batchManager) startCleaner() {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for range t.C {
		bm.sweepExpired()
	}
}

func (bm *batchManager) sweepExpired() {
	cutoff := time.Now().Add(-batchJobTTL)
	entries, err := os.ReadDir(bm.dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			path := filepath.Join(bm.dir, e.Name())
			_ = os.RemoveAll(path)
			bm.jobs.Delete(e.Name())
		}
	}
}

func newJobID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (bm *batchManager) startJob(inputs []string) *batchJob {
	id := newJobID()
	_ = os.MkdirAll(bm.jobDir(id), 0o755)

	items := make([]batchItem, 0, len(inputs))
	for _, raw := range inputs {
		normalised := normalizeISBN(raw)
		items = append(items, batchItem{Input: raw, ISBN: normalised})
	}
	job := &batchJob{
		ID:         id,
		Created:    time.Now(),
		LastAccess: time.Now(),
		Total:      len(items),
		Status:     "running",
		Items:      items,
		subs:       map[chan batchEvt]struct{}{},
	}
	bm.jobs.Store(id, job)
	bm.persist(job)
	go bm.run(job)
	return job
}

func (bm *batchManager) get(id string) *batchJob {
	v, ok := bm.jobs.Load(id)
	if !ok {
		return nil
	}
	j := v.(*batchJob)
	j.mu.Lock()
	j.LastAccess = time.Now()
	j.mu.Unlock()
	return j
}

func (bm *batchManager) persist(j *batchJob) {
	j.mu.Lock()
	snapshot := *j
	j.mu.Unlock()
	snapshot.subs = nil
	b, _ := json.MarshalIndent(snapshot, "", "  ")
	_ = os.WriteFile(filepath.Join(bm.jobDir(j.ID), "meta.json"), b, 0o644)
}

func (bm *batchManager) publish(j *batchJob, evt batchEvt) {
	j.subsMu.Lock()
	defer j.subsMu.Unlock()
	for ch := range j.subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (bm *batchManager) subscribe(j *batchJob) chan batchEvt {
	ch := make(chan batchEvt, 64)
	j.subsMu.Lock()
	j.subs[ch] = struct{}{}
	j.subsMu.Unlock()
	return ch
}

func (bm *batchManager) unsubscribe(j *batchJob, ch chan batchEvt) {
	j.subsMu.Lock()
	delete(j.subs, ch)
	j.subsMu.Unlock()
	close(ch)
}

func (bm *batchManager) run(job *batchJob) {
	ctx := context.Background()
	idxCh := make(chan int, len(job.Items))
	for i := range job.Items {
		idxCh <- i
	}
	close(idxCh)

	var wg sync.WaitGroup
	for w := 0; w < batchConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idxCh {
				bm.processItem(ctx, job, i)
			}
		}()
	}
	wg.Wait()

	job.mu.Lock()
	job.Status = "finished"
	job.mu.Unlock()
	bm.persist(job)
	bm.publish(job, batchEvt{Done: job.Done, Total: job.Total, Found: job.Found, Missing: job.Missing, Status: "finished"})
}

func (bm *batchManager) processItem(ctx context.Context, job *batchJob, i int) {
	job.mu.Lock()
	it := job.Items[i]
	job.mu.Unlock()

	if it.ISBN == "" {
		job.mu.Lock()
		job.Items[i].Error = "invalid isbn"
		job.Done++
		job.Missing++
		job.mu.Unlock()
		bm.publish(job, batchEvt{Done: job.Done, Total: job.Total, Found: job.Found, Missing: job.Missing, Current: it.Input, Source: "invalid"})
		return
	}

	var (
		img      []byte
		mime     string
		src      string
		skipFetch bool
	)
	if bm.cache != nil {
		if hit, ok := bm.cache.get(it.ISBN); ok {
			if hit.Found {
				if data, err := base64.StdEncoding.DecodeString(hit.ImageB64); err == nil && len(data) > 0 {
					img = data
					mime = hit.Mime
					src = hit.Source
					skipFetch = true
				}
			} else {
				skipFetch = true // negative cache: trust the miss until TTL expires
			}
		}
	}

	if !skipFetch {
		// 50s per item: enough for the full 5-phase pipeline (race 6s + meta wait
		// 8s + islamicFR race 9s + 4× meta-aware @ 7s + goodreads 6s ≈ 57s worst
		// case, but most resolve via phase 1 in under 4s).
		itemCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
		img, mime, src = tryCover(itemCtx, it.ISBN)
		cancel()
		if bm.cache != nil {
			resp := coverResp{Found: img != nil, ISBN: it.ISBN}
			if img != nil {
				resp.Source = src
				resp.Mime = mime
				resp.ImageB64 = base64.StdEncoding.EncodeToString(img)
			}
			bm.cache.put(it.ISBN, resp)
		}
	}

	job.mu.Lock()
	job.Done++
	if img != nil {
		ext := mimeExt(mime)
		fname := safeFilename(it.Input) + ext
		_ = os.WriteFile(filepath.Join(bm.jobDir(job.ID), fname), img, 0o644)
		job.Items[i].Found = true
		job.Items[i].Source = src
		job.Items[i].File = fname
		job.Found++
	} else {
		job.Items[i].Error = "no cover"
		job.Missing++
	}
	doneSnap := job.Done
	foundSnap := job.Found
	missSnap := job.Missing
	totalSnap := job.Total
	job.mu.Unlock()

	bm.publish(job, batchEvt{Done: doneSnap, Total: totalSnap, Found: foundSnap, Missing: missSnap, Current: it.Input, Source: src})

	if doneSnap%5 == 0 || doneSnap == totalSnap {
		bm.persist(job)
	}
}

func mimeExt(mime string) string {
	switch mime {
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/gif":
		return ".gif"
	default:
		return ".jpg"
	}
}

func safeFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z',
			r == '-', r == '_', r == '.', r == 'X':
			b.WriteRune(r)
		default:
			// Strip any other character (slashes, dots that could traverse, etc.)
		}
	}
	out := b.String()
	if out == "" {
		out = "unknown"
	}
	if len(out) > 80 {
		out = out[:80]
	}
	return out
}

// ---------- HTTP handlers ----------

func handleBatchStart(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Auth") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var body struct {
			ISBNs []string `json:"isbns"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4*1024*1024)).Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if len(body.ISBNs) == 0 {
			http.Error(w, "empty isbns", http.StatusBadRequest)
			return
		}
		if len(body.ISBNs) > 5000 {
			http.Error(w, "too many isbns (max 5000)", http.StatusBadRequest)
			return
		}
		job := bm.startJob(body.ISBNs)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"job_id": job.ID,
			"total":  job.Total,
		})
	}
}

func handleBatchStatus(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("X-Auth") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/batch/")
		id = strings.TrimSuffix(id, "/status")
		j := bm.get(id)
		if j == nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		j.mu.Lock()
		snap := struct {
			ID      string  `json:"id"`
			Total   int     `json:"total"`
			Done    int     `json:"done"`
			Found   int     `json:"found"`
			Missing int     `json:"missing"`
			Status  string  `json:"status"`
			Created time.Time `json:"created"`
			Items   []batchItem `json:"items"`
		}{j.ID, j.Total, j.Done, j.Found, j.Missing, j.Status, j.Created, j.Items}
		j.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	}
}

func handleBatchEvents(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// SSE auth via query string (EventSource can't set headers).
		token := r.URL.Query().Get("auth")
		if token == "" {
			token = r.Header.Get("X-Auth")
		}
		if token != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/batch/")
		id = strings.TrimSuffix(id, "/events")
		j := bm.get(id)
		if j == nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		ch := bm.subscribe(j)
		defer bm.unsubscribe(j, ch)

		// Initial snapshot
		j.mu.Lock()
		init := batchEvt{Done: j.Done, Total: j.Total, Found: j.Found, Missing: j.Missing, Status: j.Status}
		j.mu.Unlock()
		writeSSE(w, init)
		flusher.Flush()
		if init.Status == "finished" {
			return
		}

		ping := time.NewTicker(15 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				writeSSE(w, evt)
				flusher.Flush()
				if evt.Status == "finished" {
					return
				}
			case <-ping.C:
				_, _ = io.WriteString(w, ": ping\n\n")
				flusher.Flush()
			}
		}
	}
}

func writeSSE(w io.Writer, evt batchEvt) {
	b, _ := json.Marshal(evt)
	name := "progress"
	if evt.Status == "finished" {
		name = "finished"
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
}

func handleBatchZip(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Browsers can't set headers on a download anchor. Allow auth via ?auth= for the ZIP path.
		token := r.URL.Query().Get("auth")
		if token == "" {
			token = r.Header.Get("X-Auth")
		}
		if token != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/batch/")
		id = strings.TrimSuffix(id, "/zip")
		j := bm.get(id)
		if j == nil {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		j.mu.Lock()
		if j.Status != "finished" {
			j.mu.Unlock()
			http.Error(w, "job still running", http.StatusConflict)
			return
		}
		items := append([]batchItem(nil), j.Items...)
		j.mu.Unlock()

		// Optional split: ?part=N&perPart=K (1-indexed parts). When perPart is 0
		// or unset, the response contains every item in a single ZIP.
		perPart, _ := strconv.Atoi(r.URL.Query().Get("perPart"))
		part, _ := strconv.Atoi(r.URL.Query().Get("part"))
		if perPart < 0 {
			perPart = 0
		}
		totalParts := 1
		if perPart > 0 {
			totalParts = (len(items) + perPart - 1) / perPart
			if totalParts == 0 {
				totalParts = 1
			}
			if part < 1 {
				part = 1
			}
			if part > totalParts {
				http.Error(w, "part out of range", http.StatusBadRequest)
				return
			}
		}
		start, end := 0, len(items)
		if perPart > 0 {
			start = (part - 1) * perPart
			end = start + perPart
			if end > len(items) {
				end = len(items)
			}
		}
		slice := items[start:end]

		filename := fmt.Sprintf("covers-%s.zip", id[:8])
		if perPart > 0 {
			filename = fmt.Sprintf("covers-%s-%dof%d.zip", id[:8], part, totalParts)
		}
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
		zw := zip.NewWriter(w)
		defer zw.Close()

		var notFound []string
		for _, it := range slice {
			if !it.Found || it.File == "" {
				notFound = append(notFound, it.Input)
				continue
			}
			data, err := os.ReadFile(filepath.Join(bm.jobDir(id), it.File))
			if err != nil {
				continue
			}
			// Filename uses the EXACT user input (preserved with its dashes/spaces stripped to safe chars only).
			ext := filepath.Ext(it.File)
			entryName := safeFilename(it.Input) + ext
			fw, err := zw.Create(entryName)
			if err != nil {
				continue
			}
			_, _ = fw.Write(data)
		}
		if len(notFound) > 0 {
			fw, _ := zw.Create("_not_found.txt")
			_, _ = fw.Write([]byte(strings.Join(notFound, "\n") + "\n"))
		}
		reportPayload := map[string]any{
			"id": id, "total": len(slice),
			"found":   len(slice) - len(notFound),
			"missing": len(notFound),
			"items":   slice,
		}
		if perPart > 0 {
			reportPayload["part"] = part
			reportPayload["of"] = totalParts
			reportPayload["per_part"] = perPart
		}
		report, _ := json.MarshalIndent(reportPayload, "", "  ")
		fw, _ := zw.Create("_report.json")
		_, _ = fw.Write(report)
	}
}

// jobImageBytes sums the size of every cover file written for a job.
func (bm *batchManager) jobImageBytes(id string) int64 {
	var total int64
	dir := bm.jobDir(id)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "meta.json" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

func handleBatchList(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("X-Auth") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		type entry struct {
			ID         string    `json:"id"`
			Created    time.Time `json:"created"`
			Total      int       `json:"total"`
			Done       int       `json:"done"`
			Found      int       `json:"found"`
			Missing    int       `json:"missing"`
			Status     string    `json:"status"`
			SizeBytes  int64     `json:"size_bytes"`
			FirstInput string    `json:"first_input,omitempty"`
			LastInput  string    `json:"last_input,omitempty"`
		}
		var out []entry
		bm.jobs.Range(func(_, v any) bool {
			j := v.(*batchJob)
			j.mu.Lock()
			e := entry{
				ID: j.ID, Created: j.Created, Total: j.Total, Done: j.Done,
				Found: j.Found, Missing: j.Missing, Status: j.Status,
				SizeBytes: bm.jobImageBytes(j.ID),
			}
			if len(j.Items) > 0 {
				e.FirstInput = j.Items[0].Input
				e.LastInput = j.Items[len(j.Items)-1].Input
			}
			j.mu.Unlock()
			out = append(out, e)
			return true
		})
		sort.Slice(out, func(i, k int) bool { return out[i].Created.After(out[k].Created) })
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"batches": out})
	}
}

func handleBatchDelete(bm *batchManager, authToken, allowedOrigin string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, allowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Header.Get("X-Auth") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/batch/")
		id = strings.TrimSuffix(id, "/delete")
		if id == "" || strings.ContainsAny(id, "/\\") {
			http.Error(w, "bad id", http.StatusBadRequest)
			return
		}
		bm.jobs.Delete(id)
		_ = os.RemoveAll(bm.jobDir(id))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}
}
