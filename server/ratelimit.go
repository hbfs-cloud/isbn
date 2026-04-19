package main

import (
	"context"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Per-host concurrency cap. Tweak via hostConcurrency below if a source needs
// a different ceiling. Default 2 keeps us under the radar of typical
// shared-hosting WAFs while still letting a batch make progress.
const defaultHostConcurrency = 2

var hostConcurrency = map[string]int{
	"backapi.leonardo-service.com": 4, // Albouraq REST API tolerates more
	"www.googleapis.com":           8,
	"openlibrary.org":              8,
	"covers.openlibrary.org":       8,
	"m.media-amazon.com":           4,
}

type hostSems struct {
	mu   sync.Mutex
	sems map[string]chan struct{}
}

var sems = &hostSems{sems: map[string]chan struct{}{}}

func (h *hostSems) get(host string) chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.sems[host]; ok {
		return s
	}
	cap := defaultHostConcurrency
	if v, ok := hostConcurrency[host]; ok {
		cap = v
	}
	s := make(chan struct{}, cap)
	h.sems[host] = s
	return s
}

// acquireHost blocks until a slot for `host` is available or ctx is cancelled.
// The returned release function MUST be called.
func acquireHost(ctx context.Context, rawURL string) (func(), error) {
	host := hostOf(rawURL)
	if host == "" {
		return func() {}, nil
	}
	sem := sems.get(host)
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// sleepJitter sleeps a random amount in [minMs, maxMs]. Returns early if ctx
// is cancelled. Used to space requests to the same host.
func sleepJitter(ctx context.Context, minMs, maxMs int) {
	if maxMs <= minMs {
		return
	}
	d := time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

var uaPool = []string{
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:130.0) Gecko/20100101 Firefox/130.0",
}

var uaCounter uint64

func pickUA() string {
	i := atomic.AddUint64(&uaCounter, 1)
	return uaPool[int(i)%len(uaPool)]
}
