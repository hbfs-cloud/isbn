package main

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type config struct {
	Addr          string
	AuthToken     string
	AllowedOrigin string
	CacheDir      string
	CacheTTL      time.Duration
	NegTTL        time.Duration
}

func loadConfig() config {
	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	tok := os.Getenv("AUTH_TOKEN")
	if tok == "" {
		log.Fatal("AUTH_TOKEN env var is required (no default)")
	}
	return config{
		Addr:          get("ADDR", ":8080"),
		AuthToken:     tok,
		AllowedOrigin: get("ALLOWED_ORIGIN", "https://hbfs-cloud.github.io"),
		CacheDir:      get("CACHE_DIR", "/cache"),
		CacheTTL:      30 * 24 * time.Hour,
		NegTTL:        7 * 24 * time.Hour,
	}
}

type coverResp struct {
	Found    bool   `json:"found"`
	ISBN     string `json:"isbn"`
	Source   string `json:"source,omitempty"`
	Mime     string `json:"mime,omitempty"`
	ImageB64 string `json:"image_b64,omitempty"`
	Cached   bool   `json:"cached,omitempty"`
}

type coverReq struct {
	ISBN string `json:"isbn"`
}

type ipLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
}

func (l *ipLimiter) get(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lim, ok := l.limiters[ip]; ok {
		return lim
	}
	lim := rate.NewLimiter(rate.Limit(20), 100)
	l.limiters[ip] = lim
	return lim
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.Index(xff, ","); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if host == "" {
		host = r.RemoteAddr
	}
	return host
}

func main() {
	cfg := loadConfig()
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		log.Fatalf("cache dir: %v", err)
	}
	cache := newDiskCache(cfg.CacheDir, cfg.CacheTTL, cfg.NegTTL)
	go cache.startPruner(24 * time.Hour)

	bm := newBatchManager(cfg.CacheDir, cache)

	limiter := &ipLimiter{limiters: map[string]*rate.Limiter{}}

	mux := http.NewServeMux()
	mux.HandleFunc("/batch", handleBatchStart(bm, cfg.AuthToken, cfg.AllowedOrigin))
	mux.HandleFunc("/batches", handleBatchList(bm, cfg.AuthToken, cfg.AllowedOrigin))
	mux.HandleFunc("/batch/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/events"):
			handleBatchEvents(bm, cfg.AuthToken, cfg.AllowedOrigin)(w, r)
		case strings.HasSuffix(path, "/zip"):
			handleBatchZip(bm, cfg.AuthToken, cfg.AllowedOrigin)(w, r)
		case strings.HasSuffix(path, "/status"):
			handleBatchStatus(bm, cfg.AuthToken, cfg.AllowedOrigin)(w, r)
		case strings.HasSuffix(path, "/delete") || r.Method == http.MethodDelete:
			handleBatchDelete(bm, cfg.AuthToken, cfg.AllowedOrigin)(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, cfg.AllowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		got := r.Header.Get("X-Auth")
		if subtle.ConstantTimeCompare([]byte(got), []byte(cfg.AuthToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	mux.HandleFunc("/cache/wipe", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, cfg.AllowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		got := r.Header.Get("X-Auth")
		if subtle.ConstantTimeCompare([]byte(got), []byte(cfg.AuthToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		n, bytes, err := cache.wipe()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok": true, "removed": n, "bytes": bytes,
		})
	})

	mux.HandleFunc("/debug/source", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, cfg.AllowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		got := r.Header.Get("X-Auth")
		if subtle.ConstantTimeCompare([]byte(got), []byte(cfg.AuthToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		isbn := normalizeISBN(r.URL.Query().Get("isbn"))
		name := r.URL.Query().Get("source")
		if isbn == "" || name == "" {
			http.Error(w, "isbn+source required", http.StatusBadRequest)
			return
		}
		src, ok := allSources[name]
		if !ok {
			http.Error(w, "unknown source", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		var meta *bookMeta
		// Always fetch meta for /debug — sources that use it (islamic-FR, *-meta)
		// will benefit; pure-ISBN sources ignore it. Adds ~300-800ms per probe.
		meta = fetchMetadata(ctx, isbn)
		t0 := time.Now()
		img, mime, err := src(ctx, isbn, meta)
		elapsed := time.Since(t0)
		resp := map[string]any{
			"source":     name,
			"isbn":       isbn,
			"elapsed_ms": elapsed.Milliseconds(),
		}
		if err != nil {
			resp["error"] = err.Error()
		}
		if img != nil {
			resp["found"] = true
			resp["mime"] = mime
			resp["size"] = len(img)
			resp["image_b64"] = base64.StdEncoding.EncodeToString(img)
		} else {
			resp["found"] = false
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/cover", func(w http.ResponseWriter, r *http.Request) {
		setCORS(w, cfg.AllowedOrigin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ip := clientIP(r)
		if !limiter.get(ip).Allow() {
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
		got := r.Header.Get("X-Auth")
		if subtle.ConstantTimeCompare([]byte(got), []byte(cfg.AuthToken)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req coverReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		isbn := normalizeISBN(req.ISBN)
		if isbn == "" {
			http.Error(w, "invalid isbn", http.StatusBadRequest)
			return
		}

		if hit, ok := cache.get(isbn); ok {
			writeJSON(w, hit, true)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 50*time.Second)
		defer cancel()
		img, mime, src := tryCover(ctx, isbn)
		if img == nil {
			resp := coverResp{Found: false, ISBN: isbn}
			cache.put(isbn, resp)
			writeJSON(w, resp, false)
			return
		}
		resp := coverResp{
			Found:    true,
			ISBN:     isbn,
			Source:   src,
			Mime:     mime,
			ImageB64: base64.StdEncoding.EncodeToString(img),
		}
		cache.put(isbn, resp)
		writeJSON(w, resp, false)
	})

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// 0 = no write timeout. Required so /batch/{id}/events (SSE) and
		// /batch/{id}/zip can stream for the full duration of large jobs.
		// Per-request timeouts come from context.WithTimeout in handlers.
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}
	log.Printf("isbn-proxy listening on %s (origin=%s)", cfg.Addr, cfg.AllowedOrigin)
	log.Fatal(srv.ListenAndServe())
}

func setCORS(w http.ResponseWriter, origin string) {
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Auth, Cache-Control")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

func writeJSON(w http.ResponseWriter, resp coverResp, cached bool) {
	resp.Cached = cached
	w.Header().Set("Content-Type", "application/json")
	if !resp.Found {
		w.WriteHeader(http.StatusNotFound)
	}
	_ = json.NewEncoder(w).Encode(resp)
}
