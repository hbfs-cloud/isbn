package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type diskCache struct {
	dir    string
	posTTL time.Duration
	negTTL time.Duration
	mu     sync.Mutex
}

type cacheEntry struct {
	Resp coverResp `json:"resp"`
	TS   int64     `json:"ts"`
}

func newDiskCache(dir string, posTTL, negTTL time.Duration) *diskCache {
	return &diskCache{dir: dir, posTTL: posTTL, negTTL: negTTL}
}

func (c *diskCache) keyPath(isbn string) string {
	h := sha256.Sum256([]byte(isbn))
	name := hex.EncodeToString(h[:])
	return filepath.Join(c.dir, name[:2], name+".json")
}

func (c *diskCache) get(isbn string) (coverResp, bool) {
	p := c.keyPath(isbn)
	b, err := os.ReadFile(p)
	if err != nil {
		return coverResp{}, false
	}
	var e cacheEntry
	if err := json.Unmarshal(b, &e); err != nil {
		return coverResp{}, false
	}
	age := time.Since(time.Unix(e.TS, 0))
	ttl := c.posTTL
	if !e.Resp.Found {
		ttl = c.negTTL
	}
	if age > ttl {
		_ = os.Remove(p)
		return coverResp{}, false
	}
	_ = os.Chtimes(p, time.Now(), time.Now())
	return e.Resp, true
}

func (c *diskCache) put(isbn string, resp coverResp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.keyPath(isbn)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		log.Printf("cache mkdir: %v", err)
		return
	}
	b, err := json.Marshal(cacheEntry{Resp: resp, TS: time.Now().Unix()})
	if err != nil {
		return
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		log.Printf("cache write: %v", err)
		return
	}
	_ = os.Rename(tmp, p)
}

func (c *diskCache) startPruner(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		c.prune()
	}
}

// wipe deletes every cache entry on disk. The /jobs subdirectory is preserved
// so existing batches stay browsable.
func (c *diskCache) wipe() (int, int64, error) {
	var n int
	var bytes int64
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if e.Name() == "jobs" {
			continue
		}
		sub := filepath.Join(c.dir, e.Name())
		_ = filepath.Walk(sub, func(_ string, info os.FileInfo, _ error) error {
			if info != nil && !info.IsDir() {
				n++
				bytes += info.Size()
			}
			return nil
		})
		_ = os.RemoveAll(sub)
	}
	return n, bytes, nil
}

func (c *diskCache) prune() {
	now := time.Now()
	_ = filepath.Walk(c.dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		var e cacheEntry
		if err := json.Unmarshal(b, &e); err != nil {
			_ = os.Remove(p)
			return nil
		}
		ttl := c.posTTL
		if !e.Resp.Found {
			ttl = c.negTTL
		}
		if now.Sub(time.Unix(e.TS, 0)) > ttl {
			_ = os.Remove(p)
		}
		return nil
	})
}
