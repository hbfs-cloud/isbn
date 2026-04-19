package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

type sourceFn func(ctx context.Context, isbn string, meta *bookMeta) (image []byte, mime string, err error)

var allSources = map[string]sourceFn{
	"openlibrary-13":  olSource(13),
	"openlibrary-10":  olSource(10),
	"google-books":    googleBooksSource,
	"amazon-com":      amazonSource("01"),
	"amazon-uk":       amazonSource("02"),
	"amazon-de":       amazonSource("03"),
	"amazon-fr":       amazonSource("08"),
	"amazon-jp":       amazonSource("09"),
	"amazon-ca":       amazonSource("11"),
	"amazon-mainline": amazonSource("MAIN"),
	"bnf-sru":         bnfSource,
	"goodreads":       goodreadsSource,
	"goodreads-meta":  goodreadsMetaSource,
	"bing-images":     bingSource(false),
	"bing-meta":       bingSource(true),
	"yandex-images":   yandexSource(false),
	"yandex-meta":     yandexSource(true),
	"ddg-images":      ddgSource(false),
	"ddg-meta":        ddgSource(true),
	"maison-ennour":      islamicBookSiteSource("maison-ennour", "https://www.maisondennour.com/?s={ISBN}&post_type=product"),
	"tawhid":             islamicBookSiteSource("tawhid", "https://www.edition-tawhid.com/?s={ISBN}&post_type=product"),
	"sana":               islamicBookSiteSource("sana", "https://www.librairie-sana.com/fr/recherche?controller=search&s={ISBN}"),
	"albayyinah":         islamicBookSiteSource("albayyinah", "https://albayyinah.fr/recherche?controller=search&s={ISBN}"),
	"sifatusafwa":        islamicBookSiteSource("sifatusafwa", "https://sifatusafwa.com/fr/recherche?controller=search&s={ISBN}"),
	"librairie-musulmane": islamicBookSiteSource("librairie-musulmane", "https://www.la-librairie-musulmane.com/?s={ISBN}&post_type=product"),
	"al-imen":            islamicBookSiteSource("al-imen", "https://fr.al-imen.com/search?q={ISBN}&type=product"),
	"maktaba-alqalam":    islamicBookSiteSource("maktaba-alqalam", "https://maktaba-alqalam.com/?s={ISBN}&post_type=product"),
	"alhidayah":          islamicBookSiteSource("alhidayah", "https://alhidayah.fr/recherche?controller=search&s={ISBN}"),
	"alfil-maktaba":      islamicBookSiteSource("alfil-maktaba", "https://alfil-maktaba.fr/search?q={ISBN}&type=product"),
	"librairie-al-imam":  islamicBookSiteSource("librairie-al-imam", "https://librairiealimam.fr/recherche?controller=search&s={ISBN}"),
	"maktaba-abou-imran": islamicBookSiteSource("maktaba-abou-imran", "https://maktaba-abou-imran.com/?s={ISBN}&post_type=product"),
	"albouraq":           albouraqSource,
}

type source struct {
	name string
	run  sourceFn
}

type bookMeta struct {
	Title   string
	Authors []string
}

func tryCover(ctx context.Context, isbn string) ([]byte, string, string) {
	metaCh := make(chan *bookMeta, 1)
	go func() {
		// Title/author lookup gets a generous 12s budget. Both Google Books and
		// OpenLibrary occasionally take 4-7s when contended; cutting it short
		// strands phase 3 (meta-aware scrape) and forces a not-found.
		mctx, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		metaCh <- fetchMetadata(mctx, isbn)
	}()

	cheap := []source{
		{"openlibrary-13", olSource(13)},
		{"openlibrary-10", olSource(10)},
		{"google-books", googleBooksSource},
		{"amazon-com", amazonSource("01")},
		{"amazon-uk", amazonSource("02")},
		{"amazon-de", amazonSource("03")},
		{"amazon-fr", amazonSource("08")},
		{"amazon-jp", amazonSource("09")},
		{"amazon-ca", amazonSource("11")},
		{"amazon-mainline", amazonSource("MAIN")},
		{"bnf-sru", bnfSource},
	}

	if img, mime, src := raceSources(ctx, cheap, isbn, nil, 6*time.Second); img != nil {
		<-metaCh
		return img, mime, src
	}

	var meta *bookMeta
	select {
	case meta = <-metaCh:
	case <-time.After(8 * time.Second):
	}

	islamicFR := []source{
		{"maison-ennour", allSources["maison-ennour"]},
		{"tawhid", allSources["tawhid"]},
		{"sana", allSources["sana"]},
		{"albayyinah", allSources["albayyinah"]},
		{"sifatusafwa", allSources["sifatusafwa"]},
		{"librairie-musulmane", allSources["librairie-musulmane"]},
		{"al-imen", allSources["al-imen"]},
		{"maktaba-alqalam", allSources["maktaba-alqalam"]},
		{"alhidayah", allSources["alhidayah"]},
		{"alfil-maktaba", allSources["alfil-maktaba"]},
		{"librairie-al-imam", allSources["librairie-al-imam"]},
		{"maktaba-abou-imran", allSources["maktaba-abou-imran"]},
		{"albouraq", allSources["albouraq"]},
	}
	if img, mime, src := raceSources(ctx, islamicFR, isbn, meta, 12*time.Second); img != nil {
		return img, mime, src
	}

	hasMeta := meta != nil && meta.Title != ""
	// 979-prefix ISBNs are Amazon KDP exclusives: no ISBN-10, no Google Books,
	// no OpenLibrary entry. Meta will always be nil for them, but a raw bing/
	// yandex/ddg search by the ISBN-13 reliably returns the Amazon cover, so
	// we let phases 3 and 4 run without meta in that case.
	is979 := strings.HasPrefix(isbn, "979")
	if hasMeta || is979 {
		if hasMeta {
			log.Printf("[%s] meta=%q authors=%v → meta-aware scrape", isbn, meta.Title, meta.Authors)
		} else {
			log.Printf("[%s] no meta but 979-prefix → raw-ISBN scrape", isbn)
		}
		metaScrape := []source{
			{"bing-meta", bingSource(true)},
			{"yandex-meta", yandexSource(true)},
			{"ddg-meta", ddgSource(true)},
			{"goodreads-meta", goodreadsMetaSource},
		}
		for _, s := range metaScrape {
			if ctx.Err() != nil {
				return nil, "", ""
			}
			sctx, cancel := context.WithTimeout(ctx, 7*time.Second)
			img, mime, err := s.run(sctx, isbn, meta)
			cancel()
			if err == nil && img != nil {
				return img, mime, s.name
			}
		}
	}

	// goodreads validates by matching the ISBN inside the resolved book page URL
	// so it stays safe to query even without metadata. Bing/Yandex/DDG image
	// search only become trustworthy once we can score results against the real
	// title/authors — without that signal they regularly return covers from
	// completely unrelated books, so skip them when meta is unavailable
	// (except for 979 ISBNs, where the Amazon cover is canonical).
	rawScrape := []source{{"goodreads", goodreadsSource}}
	if hasMeta || is979 {
		rawScrape = append(rawScrape,
			source{"bing-images", bingSource(false)},
			source{"yandex-images", yandexSource(false)},
			source{"ddg-images", ddgSource(false)},
		)
	}
	for _, s := range rawScrape {
		if ctx.Err() != nil {
			return nil, "", ""
		}
		sctx, cancel := context.WithTimeout(ctx, 6*time.Second)
		img, mime, err := s.run(sctx, isbn, nil)
		cancel()
		if err == nil && img != nil {
			return img, mime, s.name
		}
	}
	return nil, "", ""
}

func raceSources(ctx context.Context, sources []source, isbn string, meta *bookMeta, budget time.Duration) ([]byte, string, string) {
	rctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	type winResult struct {
		img  []byte
		mime string
		name string
	}
	winCh := make(chan winResult, len(sources))
	var wg sync.WaitGroup
	for _, s := range sources {
		wg.Add(1)
		go func(s source) {
			defer wg.Done()
			img, mime, err := s.run(rctx, isbn, meta)
			if err == nil && img != nil {
				select {
				case winCh <- winResult{img, mime, s.name}:
				default:
				}
			}
		}(s)
	}
	go func() { wg.Wait(); close(winCh) }()

	for {
		select {
		case <-rctx.Done():
			return nil, "", ""
		case r, ok := <-winCh:
			if !ok {
				return nil, "", ""
			}
			return r.img, r.mime, r.name
		}
	}
}

// ---------- OpenLibrary ----------
func olSource(form int) sourceFn {
	return func(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
		i10, i13 := bothISBNForms(isbn)
		key := i13
		if form == 10 && i10 != "" {
			key = i10
		}
		if key == "" {
			return nil, "", fmt.Errorf("no isbn")
		}
		u := fmt.Sprintf("https://covers.openlibrary.org/b/isbn/%s-L.jpg?default=false", key)
		body, code, mime, err := httpGet(ctx, u, nil)
		if err != nil {
			return nil, "", err
		}
		if code != 200 || !looksLikeImage(body, mime) {
			return nil, "", fmt.Errorf("ol miss code=%d", code)
		}
		return body, detectMime(body, mime), nil
	}
}

// ---------- Google Books ----------
type gbVolume struct {
	Items []struct {
		VolumeInfo struct {
			Title       string   `json:"title"`
			Authors     []string `json:"authors"`
			ImageLinks  map[string]string `json:"imageLinks"`
		} `json:"volumeInfo"`
	} `json:"items"`
}

func googleBooksSource(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
	api := fmt.Sprintf("https://www.googleapis.com/books/v1/volumes?q=isbn:%s&maxResults=1", isbn)
	body, code, _, err := httpGet(ctx, api, nil)
	if err != nil || code != 200 {
		return nil, "", fmt.Errorf("gb api code=%d err=%v", code, err)
	}
	var v gbVolume
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, "", err
	}
	if len(v.Items) == 0 {
		return nil, "", fmt.Errorf("gb empty")
	}
	links := v.Items[0].VolumeInfo.ImageLinks
	for _, k := range []string{"extraLarge", "large", "medium", "thumbnail", "smallThumbnail"} {
		u, ok := links[k]
		if !ok || u == "" {
			continue
		}
		u = strings.Replace(u, "http://", "https://", 1)
		u = strings.Replace(u, "&edge=curl", "", 1)
		// Force highest-res rendering Google will give us. zoom=0 returns the
		// largest available size; fife=w1200 hints at a target width.
		u = strings.Replace(u, "&zoom=1", "&zoom=0", 1)
		u = strings.Replace(u, "&zoom=5", "&zoom=0", 1)
		if !strings.Contains(u, "fife=") {
			u += "&fife=w1200-h1800"
		}
		img, c, mime, err := httpGet(ctx, u, nil)
		if err != nil || c != 200 || !looksLikeImage(img, mime) {
			continue
		}
		// Reject square thumbnails (Google occasionally serves a 128x128
		// placeholder for obscure editions) and other non-book aspect ratios.
		if !looksLikeBookCover(img, u) {
			continue
		}
		return img, detectMime(img, mime), nil
	}
	return nil, "", fmt.Errorf("gb no image")
}

// ---------- Amazon ----------
func amazonSource(region string) sourceFn {
	return func(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
		i10, _ := bothISBNForms(isbn)
		if i10 == "" {
			return nil, "", fmt.Errorf("no isbn10")
		}
		hosts := []string{"m.media-amazon.com", "images-na.ssl-images-amazon.com", "images-eu.ssl-images-amazon.com"}
		var lastErr error
		for _, host := range hosts {
			var u string
			if region == "MAIN" {
				u = fmt.Sprintf("https://%s/images/P/%s.LZZZZZZZ.jpg", host, i10)
			} else {
				u = fmt.Sprintf("https://%s/images/P/%s.%s.LZZZZZZZ.jpg", host, i10, region)
			}
			body, code, mime, err := httpGet(ctx, u, map[string]string{"Referer": "https://www.amazon.com/"})
			if err != nil {
				lastErr = err
				continue
			}
			if code == 200 && looksLikeImage(body, mime) && len(body) > 1500 {
				return body, detectMime(body, mime), nil
			}
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("amazon no-image")
		}
		return nil, "", lastErr
	}
}

// ---------- BNF ----------
var bnfArkRe = regexp.MustCompile(`ark:/12148/[a-zA-Z0-9]+`)

func bnfSource(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
	q := fmt.Sprintf(`bib.isbn any "%s"`, isbn)
	api := "http://catalogue.bnf.fr/api/SRU?version=1.2&operation=searchRetrieve&query=" +
		url.QueryEscape(q) + "&recordSchema=unimarcXchange&maximumRecords=1"
	body, code, _, err := httpGet(ctx, api, nil)
	if err != nil || code != 200 {
		return nil, "", fmt.Errorf("bnf code=%d err=%v", code, err)
	}
	m := bnfArkRe.FindString(string(body))
	if m == "" {
		return nil, "", fmt.Errorf("bnf no ark")
	}
	ark := strings.TrimPrefix(m, "ark:/12148/")
	for _, sz := range []string{"f1.highres", "f1.medres", "f1.lowres"} {
		u := fmt.Sprintf("https://gallica.bnf.fr/ark:/12148/%s/%s", ark, sz)
		img, c, mime, e := httpGet(ctx, u, nil)
		if e == nil && c == 200 && looksLikeImage(img, mime) {
			return img, detectMime(img, mime), nil
		}
	}
	thumb := fmt.Sprintf("https://catalogue.bnf.fr/couverture?&appName=NE&idArk=ark:/12148/%s&couverture=1", ark)
	img, c, mime, e := httpGet(ctx, thumb, nil)
	if e == nil && c == 200 && looksLikeImage(img, mime) {
		return img, detectMime(img, mime), nil
	}
	return nil, "", fmt.Errorf("bnf no cover")
}

// ---------- Goodreads ----------
var ogImageRe = regexp.MustCompile(`<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']`)
var grImgRe = regexp.MustCompile(`<img[^>]+id=["']coverImage["'][^>]+src=["']([^"']+)["']`)

func goodreadsSource(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
	searchURL := "https://www.goodreads.com/search?q=" + url.QueryEscape(isbn)
	html, code, _, err := cloudflareGet(ctx, searchURL, map[string]string{
		"Accept": "text/html,application/xhtml+xml",
	})
	if err != nil || code >= 400 {
		return nil, "", fmt.Errorf("gr search code=%d err=%v", code, err)
	}
	bookURL := extractGoodreadsBookURL(string(html))
	if bookURL == "" {
		return nil, "", fmt.Errorf("gr no book link")
	}
	page, code, _, err := cloudflareGet(ctx, bookURL, map[string]string{
		"Accept": "text/html",
	})
	if err != nil || code >= 400 {
		return nil, "", fmt.Errorf("gr page code=%d err=%v", code, err)
	}
	imgURL := ""
	if m := grImgRe.FindStringSubmatch(string(page)); len(m) > 1 {
		imgURL = m[1]
	} else if m := ogImageRe.FindStringSubmatch(string(page)); len(m) > 1 {
		imgURL = m[1]
	}
	if imgURL == "" {
		return nil, "", fmt.Errorf("gr no image meta")
	}
	imgURL = strings.ReplaceAll(imgURL, "&amp;", "&")
	img, c, mime, err := httpGet(ctx, imgURL, map[string]string{"Referer": "https://www.goodreads.com/"})
	if err != nil || c != 200 || !looksLikeImage(img, mime) {
		return nil, "", fmt.Errorf("gr img code=%d err=%v", c, err)
	}
	return img, detectMime(img, mime), nil
}

var grBookHrefRe = regexp.MustCompile(`href=["'](/book/show/[^"']+)["']`)

func extractGoodreadsBookURL(html string) string {
	m := grBookHrefRe.FindStringSubmatch(html)
	if len(m) < 2 {
		return ""
	}
	href := m[1]
	if strings.Contains(href, "?") {
		href = href[:strings.Index(href, "?")]
	}
	return "https://www.goodreads.com" + href
}

func goodreadsMetaSource(ctx context.Context, isbn string, meta *bookMeta) ([]byte, string, error) {
	if meta == nil {
		return nil, "", fmt.Errorf("no meta")
	}
	q := strings.TrimSpace(meta.Title + " " + strings.Join(meta.Authors, " "))
	if q == "" {
		return nil, "", fmt.Errorf("no query")
	}
	searchURL := "https://www.goodreads.com/search?q=" + url.QueryEscape(q)
	html, code, _, err := cloudflareGet(ctx, searchURL, nil)
	if err != nil || code >= 400 {
		return nil, "", fmt.Errorf("gr-meta search code=%d", code)
	}
	bookURL := extractGoodreadsBookURL(string(html))
	if bookURL == "" {
		return nil, "", fmt.Errorf("gr-meta no link")
	}
	page, code, _, err := cloudflareGet(ctx, bookURL, nil)
	if err != nil || code >= 400 {
		return nil, "", fmt.Errorf("gr-meta page code=%d", code)
	}
	imgURL := ""
	if m := grImgRe.FindStringSubmatch(string(page)); len(m) > 1 {
		imgURL = m[1]
	} else if m := ogImageRe.FindStringSubmatch(string(page)); len(m) > 1 {
		imgURL = m[1]
	}
	if imgURL == "" {
		return nil, "", fmt.Errorf("gr-meta no img")
	}
	imgURL = strings.ReplaceAll(imgURL, "&amp;", "&")
	img, c, mime, err := httpGet(ctx, imgURL, map[string]string{"Referer": "https://www.goodreads.com/"})
	if err != nil || c != 200 || !looksLikeImage(img, mime) {
		return nil, "", fmt.Errorf("gr-meta img code=%d", c)
	}
	return img, detectMime(img, mime), nil
}

// ---------- French Islamic Booksellers ----------
var anyImgRe = regexp.MustCompile(`<img[^>]+src=["']([^"']+\.(?:jpe?g|png|webp|JPG|PNG|WEBP))[^"']*["']`)
var prestaProductLinkRe = regexp.MustCompile(`<a[^>]+href=["'](https?://[^"']+)["'][^>]+class=["'][^"']*(?:thumbnail|product-link|product_img_link)`)
var wooProductLinkRe = regexp.MustCompile(`<a[^>]+href=["'](https?://[^"']+/(?:produit|product)/[^"']+)["']`)
var shopifyProductLinkRe = regexp.MustCompile(`<a[^>]+href=["'](https?://[^"']+/products/[^"']+)["']`)
var shopifyRelProductLinkRe = regexp.MustCompile(`<a[^>]+href=["'](/products/[^"']+)["']`)

const maxProductCandidates = 8

// islamicBookSiteSource tries 2 strategies: ISBN search (rare; Albouraq's API
// indexes ISBN, but most WooCommerce/Prestashop sites do not), then a
// title-based search using the supplied book metadata. Each candidate product
// page is fetched and its HTML is checked for the ISBN before its cover is
// returned, so we never accept an unrelated "you might also like" suggestion.
func islamicBookSiteSource(name, urlTpl string) sourceFn {
	return func(ctx context.Context, isbn string, meta *bookMeta) ([]byte, string, error) {
		i10, i13 := bothISBNForms(isbn)

		// Try the input form first, then the alternate form. Most bookstores
		// index a book under one canonical ISBN — if you searched the wrong
		// form, the lookup misses even when the book is in stock. The early
		// exit in the loop below means we only pay this cost on real misses.
		queries := []string{isbn}
		for _, alt := range []string{i13, i10} {
			if alt != "" && alt != isbn {
				queries = append(queries, alt)
				break
			}
		}
		if meta != nil && meta.Title != "" {
			q := meta.Title
			if len(meta.Authors) > 0 {
				q = q + " " + meta.Authors[0]
			}
			queries = append(queries, sanitizeQuery(q))
		}

		baseHost := baseHostOf(urlTpl)

		// Some Prestashop themes (e.g. albayyinah) ship search HTML that
		// CycleTLS-driven cloudflareGet doesn't always hydrate. Fall back to
		// the plain httpGet client when the bypassed call yields no candidates.
		fetch := func(u string, h map[string]string) (string, int, error) {
			b, c, _, err := cloudflareGet(ctx, u, h)
			cf := len(extractProductLinksFor(string(b), baseHost))
			if err == nil && c < 400 && len(b) > 0 && cf > 0 {
				return string(b), c, nil
			}
			b2, c2, _, err2 := httpGet(ctx, u, h)
			pl := len(extractProductLinksFor(string(b2), baseHost))
			thumbHits := strings.Count(string(b2), "product-thumbnail")
			log.Printf("[%s] fetch %s cf=(%d/%d/%v cands=%d) http=(%d/%d/%v cands=%d thumb=%d)",
				name, u, c, len(b), err, cf, c2, len(b2), err2, pl, thumbHits)
			if err2 == nil && c2 < 400 && pl > 0 {
				return string(b2), c2, nil
			}
			if err == nil {
				return string(b), c, nil
			}
			if err2 == nil {
				return string(b2), c2, nil
			}
			return "", c2, err2
		}

		for _, q := range queries {
			searchURL := strings.ReplaceAll(urlTpl, "{ISBN}", url.QueryEscape(q))
			body, code, err := fetch(searchURL, map[string]string{
				"Accept": "text/html,application/xhtml+xml",
			})
			if err != nil || code >= 400 {
				continue
			}
			if hasNoResultsMarker(body) {
				continue
			}
			candidates := extractProductLinksFor(body, baseHost)
			candidates = sortCandidatesByISBN(candidates, isbn, i10, i13)
			log.Printf("[%s] search q=%q size=%d candidates=%d hasIsbn=%v",
				name, q, len(body), len(candidates), strings.Contains(body, isbn))
			for i, productURL := range candidates {
				if i >= maxProductCandidates {
					break
				}
				pageStr, c, err := fetch(productURL, map[string]string{
					"Referer": searchURL,
					"Accept":  "text/html",
				})
				if err != nil || c >= 400 {
					continue
				}
				if !pageContainsISBN(pageStr, isbn, i10, i13) {
					continue
				}
				if img, mime, ok := pickCoverFromHTML(ctx, pageStr, productURL); ok {
					return img, mime, nil
				}
			}
		}
		return nil, "", fmt.Errorf("%s no match", name)
	}
}

func sanitizeQuery(s string) string {
	out := []rune{}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == ' ', r == '-':
			out = append(out, ' ')
		case r > 127:
			out = append(out, r)
		}
	}
	cleaned := strings.Join(strings.Fields(string(out)), " ")
	words := strings.Fields(cleaned)
	if len(words) > 6 {
		words = words[:6]
	}
	return strings.Join(words, " ")
}

func extractProductLinks(body string) []string {
	return extractProductLinksFor(body, "")
}

// extractProductLinksFor collects all candidate product URLs from a search
// results page (Prestashop, WooCommerce, Shopify). When baseHost is non-empty,
// same-host relative Shopify links (/products/...) are promoted to absolute.
func extractProductLinksFor(body, baseHost string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(u string) {
		if u == "" || seen[u] {
			return
		}
		seen[u] = true
		out = append(out, u)
	}
	for _, m := range prestaProductLinkRe.FindAllStringSubmatch(body, -1) {
		add(m[1])
	}
	for _, m := range wooProductLinkRe.FindAllStringSubmatch(body, -1) {
		add(m[1])
	}
	for _, m := range shopifyProductLinkRe.FindAllStringSubmatch(body, -1) {
		add(m[1])
	}
	if baseHost != "" {
		for _, m := range shopifyRelProductLinkRe.FindAllStringSubmatch(body, -1) {
			add(baseHost + m[1])
		}
	}
	return out
}

// sortCandidatesByISBN puts URLs whose path contains the ISBN (in any form)
// at the front, preserving relative order. Most bookstore CMSes embed the
// ISBN in the product slug, so matching URLs are almost always the right book.
func sortCandidatesByISBN(cands []string, isbn, i10, i13 string) []string {
	if len(cands) <= 1 {
		return cands
	}
	needles := []string{}
	for _, k := range []string{isbn, i10, i13} {
		if k != "" {
			needles = append(needles, k)
		}
	}
	if len(needles) == 0 {
		return cands
	}
	var pri, rest []string
	for _, c := range cands {
		match := false
		for _, n := range needles {
			if strings.Contains(c, n) {
				match = true
				break
			}
		}
		if match {
			pri = append(pri, c)
		} else {
			rest = append(rest, c)
		}
	}
	return append(pri, rest...)
}

// baseHostOf returns "scheme://host" for a URL, or "" if parsing fails. Used
// to absolutize Shopify relative product links on the originating host.
func baseHostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

func pageContainsISBN(html, isbn, i10, i13 string) bool {
	for _, k := range []string{isbn, i10, i13} {
		if k != "" && strings.Contains(html, k) {
			return true
		}
	}
	return false
}

func hasNoResultsMarker(html string) bool {
	low := strings.ToLower(html)
	for _, marker := range []string{
		"search-no-results",
		"woocommerce-no-products-found",
		"no-products-found",
		"product-listing-empty",
		"#js-product-list-no-results",
		"id=\"js-product-list-no-results\"",
		"aucun résultat",
		"aucun produit",
		"no products were found",
		"recherche : 0 résultat",
	} {
		if strings.Contains(low, strings.ToLower(marker)) {
			return true
		}
	}
	return false
}

// ---------- Albouraq (JSON API) ----------
type albouraqResp struct {
	Total int `json:"total"`
	Data  []struct {
		CodeBarre    string `json:"_code_barre"`
		Designation  string `json:"designation"`
		ArticleImage []struct {
			Link string `json:"link"`
		} `json:"articleimage"`
	} `json:"data"`
}

func albouraqSource(ctx context.Context, isbn string, _ *bookMeta) ([]byte, string, error) {
	api := "https://backapi.leonardo-service.com/api/sofiadis/albouraq/articles?page=1&ecom_type=albouraq&smart_search=" + isbn
	body, code, _, err := httpGet(ctx, api, map[string]string{
		"Origin":  "https://www.albouraq.fr",
		"Referer": "https://www.albouraq.fr/",
		"Accept":  "application/json",
	})
	if err != nil || code != 200 {
		return nil, "", fmt.Errorf("albouraq api code=%d err=%v", code, err)
	}
	var r albouraqResp
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, "", err
	}
	for _, d := range r.Data {
		if d.CodeBarre != isbn {
			continue
		}
		for _, im := range d.ArticleImage {
			if im.Link == "" {
				continue
			}
			img, c, mime, err := httpGet(ctx, im.Link, map[string]string{"Referer": "https://www.albouraq.fr/"})
			if err == nil && c == 200 && looksLikeImage(img, mime) && looksLikeBookCover(img, im.Link) {
				return img, detectMime(img, mime), nil
			}
		}
	}
	return nil, "", fmt.Errorf("albouraq no cover")
}

func pickCoverFromHTML(ctx context.Context, html, referer string) ([]byte, string, bool) {
	tryURL := func(u string) ([]byte, string, bool) {
		u = strings.ReplaceAll(u, "&amp;", "&")
		if !strings.HasPrefix(u, "http") {
			return nil, "", false
		}
		if isStockPhotoURL(u) {
			return nil, "", false
		}
		low := strings.ToLower(u)
		for _, bad := range []string{"logo", "favicon", "/icons/", "icon-", "banner", "sprite", "loader", "placeholder", "no_image", "noimage"} {
			if strings.Contains(low, bad) {
				return nil, "", false
			}
		}
		img, c, mime, err := httpGet(ctx, u, map[string]string{"Referer": referer})
		if err != nil || c != 200 {
			return nil, "", false
		}
		if !looksLikeImage(img, mime) || !looksLikeBookCover(img, u) {
			return nil, "", false
		}
		return img, detectMime(img, mime), true
	}

	if m := ogImageRe.FindStringSubmatch(html); len(m) > 1 {
		if img, mime, ok := tryURL(m[1]); ok {
			return img, mime, true
		}
	}
	for _, m := range anyImgRe.FindAllStringSubmatch(html, 16) {
		if img, mime, ok := tryURL(m[1]); ok {
			return img, mime, true
		}
	}
	return nil, "", false
}

// ---------- Bing Images ----------
var bingMurlRe = regexp.MustCompile(`murl&quot;:&quot;([^&]+)&quot;`)
var bingMurlRe2 = regexp.MustCompile(`"murl":"([^"]+)"`)

func bingSource(useMeta bool) sourceFn {
	return func(ctx context.Context, isbn string, meta *bookMeta) ([]byte, string, error) {
		var q string
		if useMeta && meta != nil {
			q = meta.Title + " " + strings.Join(meta.Authors, " ") + " book cover"
		} else {
			q = isbn + " book cover"
		}
		searchURL := "https://www.bing.com/images/search?q=" + url.QueryEscape(q) + "&form=HDRSC2&first=1"
		html, code, _, err := cloudflareGet(ctx, searchURL, map[string]string{
			"Accept": "text/html",
		})
		if err != nil || code >= 400 {
			return nil, "", fmt.Errorf("bing code=%d err=%v", code, err)
		}
		body := string(html)
		var urls []string
		for _, m := range bingMurlRe.FindAllStringSubmatch(body, 8) {
			urls = append(urls, m[1])
		}
		for _, m := range bingMurlRe2.FindAllStringSubmatch(body, 8) {
			urls = append(urls, m[1])
		}
		for _, u := range urls {
			u = strings.ReplaceAll(u, `\u0026`, "&")
			u = strings.ReplaceAll(u, `\/`, "/")
			if isStockPhotoURL(u) {
				continue
			}
			img, c, mime, err := httpGet(ctx, u, map[string]string{"Referer": "https://www.bing.com/"})
			if err == nil && c == 200 && looksLikeImage(img, mime) && len(img) > 4000 && looksLikeBookCover(img, u) {
				return img, detectMime(img, mime), nil
			}
		}
		return nil, "", fmt.Errorf("bing no valid cover")
	}
}

// ---------- Yandex Images ----------
var yandexImgRe = regexp.MustCompile(`"img_href":"([^"]+)"`)
var yandexOriginRe = regexp.MustCompile(`"origin":\{"url":"([^"]+)"`)

func yandexSource(useMeta bool) sourceFn {
	return func(ctx context.Context, isbn string, meta *bookMeta) ([]byte, string, error) {
		var q string
		if useMeta && meta != nil {
			q = meta.Title + " " + strings.Join(meta.Authors, " ")
		} else {
			q = isbn + " book cover"
		}
		searchURL := "https://yandex.com/images/search?text=" + url.QueryEscape(q) + "&isize=large"
		html, code, _, err := cloudflareGet(ctx, searchURL, map[string]string{
			"Accept": "text/html",
		})
		if err != nil || code >= 400 {
			return nil, "", fmt.Errorf("yandex code=%d err=%v", code, err)
		}
		body := string(html)
		var urls []string
		for _, m := range yandexImgRe.FindAllStringSubmatch(body, 6) {
			urls = append(urls, m[1])
		}
		for _, m := range yandexOriginRe.FindAllStringSubmatch(body, 6) {
			urls = append(urls, m[1])
		}
		for _, u := range urls {
			u = strings.ReplaceAll(u, `\u0026`, "&")
			u = strings.ReplaceAll(u, `\/`, "/")
			if isStockPhotoURL(u) {
				continue
			}
			img, c, mime, err := httpGet(ctx, u, map[string]string{"Referer": "https://yandex.com/"})
			if err == nil && c == 200 && looksLikeImage(img, mime) && len(img) > 4000 && looksLikeBookCover(img, u) {
				return img, detectMime(img, mime), nil
			}
		}
		return nil, "", fmt.Errorf("yandex no valid cover")
	}
}

// ---------- DuckDuckGo Images ----------
var ddgVqdRe = regexp.MustCompile(`vqd=['"]?(\d-\d+(?:-\d+)?)['"]?`)

type ddgImgResp struct {
	Results []struct {
		Image string `json:"image"`
	} `json:"results"`
}

func ddgSource(useMeta bool) sourceFn {
	return func(ctx context.Context, isbn string, meta *bookMeta) ([]byte, string, error) {
		var q string
		if useMeta && meta != nil {
			q = meta.Title + " " + strings.Join(meta.Authors, " ") + " book cover"
		} else {
			q = isbn + " book cover"
		}
		landing := "https://duckduckgo.com/?q=" + url.QueryEscape(q) + "&iar=images&iax=images&ia=images"
		html, code, _, err := cloudflareGet(ctx, landing, nil)
		if err != nil || code >= 400 {
			return nil, "", fmt.Errorf("ddg landing code=%d", code)
		}
		m := ddgVqdRe.FindStringSubmatch(string(html))
		if len(m) < 2 {
			return nil, "", fmt.Errorf("ddg no vqd")
		}
		vqd := m[1]
		api := "https://duckduckgo.com/i.js?l=us-en&o=json&q=" + url.QueryEscape(q) + "&vqd=" + vqd + "&f=,,,&p=1"
		body, code, _, err := cloudflareGet(ctx, api, map[string]string{
			"Referer": landing,
			"Accept":  "application/json",
		})
		if err != nil || code != 200 {
			return nil, "", fmt.Errorf("ddg api code=%d", code)
		}
		var d ddgImgResp
		if err := json.Unmarshal(body, &d); err != nil {
			return nil, "", err
		}
		for i, r := range d.Results {
			if i >= 6 {
				break
			}
			if isStockPhotoURL(r.Image) {
				continue
			}
			img, c, mime, err := httpGet(ctx, r.Image, nil)
			if err == nil && c == 200 && looksLikeImage(img, mime) && len(img) > 4000 && looksLikeBookCover(img, r.Image) {
				return img, detectMime(img, mime), nil
			}
		}
		return nil, "", fmt.Errorf("ddg no valid cover")
	}
}

// ---------- Metadata (title+author) ----------
func fetchMetadata(ctx context.Context, isbn string) *bookMeta {
	api := fmt.Sprintf("https://www.googleapis.com/books/v1/volumes?q=isbn:%s&maxResults=1", isbn)
	body, code, _, err := httpGet(ctx, api, nil)
	if err == nil && code == 200 {
		var v gbVolume
		if json.Unmarshal(body, &v) == nil && len(v.Items) > 0 {
			vi := v.Items[0].VolumeInfo
			if vi.Title != "" {
				return &bookMeta{Title: vi.Title, Authors: vi.Authors}
			}
		}
	}
	api = fmt.Sprintf("https://openlibrary.org/api/books?bibkeys=ISBN:%s&jscmd=details&format=json", isbn)
	body, code, _, err = httpGet(ctx, api, nil)
	if err != nil || code != 200 {
		return nil
	}
	var raw map[string]struct {
		Details struct {
			Title   string `json:"title"`
			Authors []struct {
				Name string `json:"name"`
			} `json:"authors"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &raw) != nil {
		return nil
	}
	for _, v := range raw {
		if v.Details.Title == "" {
			continue
		}
		var auths []string
		for _, a := range v.Details.Authors {
			auths = append(auths, a.Name)
		}
		return &bookMeta{Title: v.Details.Title, Authors: auths}
	}
	return nil
}
