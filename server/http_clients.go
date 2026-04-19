package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/RomainMichau/CycleTLS/cycletls"
	"github.com/RomainMichau/cloudscraper_go/cloudscraper"
	_ "golang.org/x/image/webp"
)

const desktopUA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

var stdClient = &http.Client{
	Timeout: 12 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 8 {
			return errors.New("too many redirects")
		}
		return nil
	},
}

func httpGet(ctx context.Context, url string, headers map[string]string) ([]byte, int, string, error) {
	release, err := acquireHost(ctx, url)
	if err != nil {
		return nil, 0, "", err
	}
	defer release()
	sleepJitter(ctx, 50, 200)

	body, code, mime, err := httpGetOnce(ctx, url, headers)
	// One quick retry on 429/503 with backoff.
	if err == nil && (code == 429 || code == 503) {
		sleepJitter(ctx, 600, 1400)
		body, code, mime, err = httpGetOnce(ctx, url, headers)
	}
	return body, code, mime, err
}

func httpGetOnce(ctx context.Context, url string, headers map[string]string) ([]byte, int, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, 0, "", err
	}
	if _, ok := headers["User-Agent"]; !ok {
		req.Header.Set("User-Agent", pickUA())
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9,fr;q=0.8")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := stdClient.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 12*1024*1024))
	if err != nil {
		return nil, resp.StatusCode, "", err
	}
	return body, resp.StatusCode, resp.Header.Get("Content-Type"), nil
}

var (
	csOnce   sync.Once
	csClient *cloudscraper.CloudScrapper
	csErr    error
)

func cloudflareClient() (*cloudscraper.CloudScrapper, error) {
	csOnce.Do(func() {
		csClient, csErr = cloudscraper.Init(false, true)
	})
	return csClient, csErr
}

func cloudflareGet(ctx context.Context, url string, headers map[string]string) ([]byte, int, string, error) {
	release, err := acquireHost(ctx, url)
	if err != nil {
		return nil, 0, "", err
	}
	defer release()
	sleepJitter(ctx, 80, 300)

	cs, err := cloudflareClient()
	if err != nil {
		return nil, 0, "", fmt.Errorf("cloudscraper init: %w", err)
	}
	if headers == nil {
		headers = map[string]string{}
	}
	if _, ok := headers["User-Agent"]; !ok {
		headers["User-Agent"] = pickUA()
	}
	if _, ok := headers["Accept"]; !ok {
		headers["Accept"] = "*/*"
	}
	opts := cycletls.Options{
		Headers:    headers,
		Timeout:    20,
		ForceHTTP1: true,
	}

	type result struct {
		body []byte
		code int
		mime string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := cs.Do(url, opts, "GET")
		if err != nil {
			ch <- result{err: err}
			return
		}
		mime := ""
		if v, ok := resp.Headers["Content-Type"]; ok {
			mime = v
		}
		ch <- result{body: []byte(resp.Body), code: resp.Status, mime: mime}
	}()

	select {
	case <-ctx.Done():
		return nil, 0, "", ctx.Err()
	case r := <-ch:
		return r.body, r.code, r.mime, r.err
	}
}

func looksLikeImage(body []byte, mime string) bool {
	if len(body) < 200 {
		return false
	}
	if mime != "" && (containsCI(mime, "image/") || containsCI(mime, "octet-stream")) {
		return len(body) >= 1024
	}
	if len(body) >= 8 {
		switch {
		case body[0] == 0xFF && body[1] == 0xD8:
			return true // JPEG
		case body[0] == 0x89 && body[1] == 'P' && body[2] == 'N' && body[3] == 'G':
			return true // PNG
		case body[0] == 'G' && body[1] == 'I' && body[2] == 'F':
			return true // GIF
		case body[0] == 'R' && body[1] == 'I' && body[2] == 'F' && body[3] == 'F':
			return true // WEBP
		}
	}
	return false
}

func detectMime(body []byte, fallback string) string {
	if len(body) >= 4 {
		switch {
		case body[0] == 0xFF && body[1] == 0xD8:
			return "image/jpeg"
		case body[0] == 0x89 && body[1] == 'P':
			return "image/png"
		case body[0] == 'G' && body[1] == 'I' && body[2] == 'F':
			return "image/gif"
		case body[0] == 'R' && body[1] == 'I' && body[2] == 'F' && body[3] == 'F':
			return "image/webp"
		}
	}
	if fallback != "" {
		return fallback
	}
	return "image/jpeg"
}

var stockPhotoDomains = []string{
	"canva.com", "freepik.com", "shutterstock.com", "dreamstime.com",
	"123rf.com", "vecteezy.com", "istockphoto.com", "gettyimages.com",
	"alamy.com", "pexels.com", "unsplash.com", "pixabay.com",
	"depositphotos.com", "stock.adobe.com", "adobestock.com",
	"vectorstock.com", "fotolia.com", "bigstockphoto.com", "rf123.com",
	"wallpaperaccess.com", "wallpapercave.com", "wallhere.com",
	"pinimg.com", "pinterest.", "redbubble.com", "etsy.com",
	"placeit.net", "envato.", "behance.net", "dribbble.com",
	"123freevectors.com", "vexels.com", "rawpixel.com",
}

func isStockPhotoURL(u string) bool {
	low := strings.ToLower(u)
	for _, d := range stockPhotoDomains {
		if strings.Contains(low, d) {
			return true
		}
	}
	return false
}

func looksLikeBookCover(img []byte, srcURL string) bool {
	if isStockPhotoURL(srcURL) {
		return false
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(img))
	if err != nil {
		return false
	}
	w, h := cfg.Width, cfg.Height
	if w < 80 || h < 120 {
		return false
	}
	if w > 4000 || h > 6000 {
		return false
	}
	ratio := float64(w) / float64(h)
	return ratio >= 0.45 && ratio <= 0.95
}

func containsCI(s, sub string) bool {
	if len(s) < len(sub) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		ok := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if a >= 'A' && a <= 'Z' {
				a += 32
			}
			if b >= 'A' && b <= 'Z' {
				b += 32
			}
			if a != b {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
