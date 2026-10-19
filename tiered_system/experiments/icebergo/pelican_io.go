package main

// An iceberg-go `io.IO` for Pelican/NRDStor over HTTPS.
//
// iceberg-go ships IO for local files, memory and gocloud blob stores --
// nothing for a plain authenticated HTTPS origin, so Pelican is unaddressable
// out of the box. This is the Go counterpart of experiments/pelican_fileio.py
// and exists for the same reason: without it the experiment would report a
// missing import rather than testing whether the v3 write path works on this
// storage.
//
// It registers itself for the "https" scheme via io.Register, which is the
// supported extension point -- no forking required.
//
// Pelican specifics this has to respect, all measured in SUMMARY.md §17-19:
//   - No append. A writer buffers locally and PUTs once on Close.
//   - No compare-and-swap. Irrelevant here (single writer), but it is why
//     the SQLite catalog still holds the pointer.
//   - ~150 ms per request whatever the size, so request count is the cost.
//   - Tokens may be sent as an Authorization header (§19); the ?authz=
//     form is a redirect workaround, not a requirement.

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	iceio "github.com/apache/iceberg-go/io"
)

// requestCounts records how much traffic a commit actually costs, so the
// Pelican run can be compared against the local one and against exp19's
// 9-requests-per-commit figure for pyiceberg.
type requestCounts struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *requestCounts) add(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[method]++
}

func (c *requestCounts) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.n {
		out[k] = v
	}
	return out
}

func (c *requestCounts) total() int {
	t := 0
	for _, v := range c.snapshot() {
		t += v
	}
	return t
}

var counts = &requestCounts{}

// PelicanIO implements iceberg-go's IO over the NRDStor origin.
type PelicanIO struct {
	key    *ecdsa.PrivateKey
	prefix string // token scope root, e.g. /cold-storage-test-icebergo
	client *http.Client
}

func NewPelicanIO(key *ecdsa.PrivateKey, prefix string) *PelicanIO {
	return &PelicanIO{
		key:    key,
		prefix: prefix,
		client: &http.Client{Timeout: 300 * time.Second},
	}
}

// pathOf turns whatever iceberg hands us -- a full https:// URL or a bare
// path -- into a namespace-relative path like /cold-storage-test-icebergo/x.
func (p *PelicanIO) pathOf(name string) (string, error) {
	if strings.HasPrefix(name, "https://") {
		u, err := url.Parse(name)
		if err != nil {
			return "", err
		}
		return strings.TrimPrefix(u.Path, "/sage"), nil
	}
	if !strings.HasPrefix(name, "/") {
		return "/" + name, nil
	}
	return name, nil
}

func (p *PelicanIO) do(method, name string, body []byte) (*http.Response, error) {
	path, err := p.pathOf(name)
	if err != nil {
		return nil, err
	}
	// Scope to the whole test prefix, not the single object: Iceberg reads
	// metadata JSON and Avro manifests alongside the data file, and a
	// per-object scope returns 403 in a way that looks like a capability
	// limit but is really a scoping mistake (exp19 §4b).
	tok, err := mint(p.key, fmt.Sprintf("read:%s write:%s", p.prefix, p.prefix), 900*time.Second)
	if err != nil {
		return nil, err
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, rootURL+path, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	counts.add(method)
	return p.client.Do(req)
}

// --- IO ---------------------------------------------------------------

func (p *PelicanIO) Open(name string) (iceio.File, error) {
	b, err := p.ReadFile(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &pelicanFile{name: name, Reader: bytes.NewReader(b), size: int64(len(b))}, nil
}

func (p *PelicanIO) Remove(name string) error {
	resp, err := p.do(http.MethodDelete, name, nil)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		return &fs.PathError{Op: "remove", Path: name,
			Err: fmt.Errorf("HTTP %d", resp.StatusCode)}
	}
	return nil
}

// PropFind lists the immediate children of a collection, excluding the
// collection's own href (which XRootD includes in its 207 response -- the
// off-by-one behind the 70,015-vs-70,016 discrepancy in §19).
func (p *PelicanIO) PropFind(name string) (int, []string, error) {
	path, err := p.pathOf(name)
	if err != nil {
		return 0, nil, err
	}
	tok, err := mint(p.key, fmt.Sprintf("read:%s write:%s", p.prefix, p.prefix), 900*time.Second)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequest("PROPFIND", rootURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Depth", "1")
	counts.add("PROPFIND")
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 207 {
		return resp.StatusCode, nil, nil
	}
	var kids []string
	for _, m := range hrefRE.FindAllSubmatch(body, -1) {
		h := string(m[1])
		if u, err := url.Parse(h); err == nil && u.Path != "" {
			h = u.Path
		}
		h = strings.TrimPrefix(h, "/sage")
		if strings.TrimRight(h, "/") != strings.TrimRight(path, "/") {
			kids = append(kids, h)
		}
	}
	return resp.StatusCode, kids, nil
}

var hrefRE = regexp.MustCompile(`<[dD]:href>([^<]+)</[dD]:href>`)

// RemoveAll deletes a prefix recursively. Pelican returns 409 on DELETE of a
// non-empty collection, so children must go first -- the plain Remove is not
// enough for cleanup, which is how this was found.
func (p *PelicanIO) RemoveAll(name string) (int, error) {
	st, kids, err := p.PropFind(name)
	if err != nil {
		return 0, err
	}
	if st == http.StatusNotFound {
		return 0, nil
	}
	if st != 207 {
		return 0, fmt.Errorf("PROPFIND %s: HTTP %d", name, st)
	}
	n := 0
	for _, k := range kids {
		subSt, subKids, err := p.PropFind(k)
		if err != nil {
			return n, err
		}
		if subSt == 207 && len(subKids) > 0 {
			sub, err := p.RemoveAll(k)
			n += sub
			if err != nil {
				return n, err
			}
			continue
		}
		if err := p.Remove(k); err != nil {
			return n, err
		}
		n++
	}
	if err := p.Remove(name); err != nil {
		return n, err
	}
	return n, nil
}

// --- ReadFileIO -------------------------------------------------------

func (p *PelicanIO) ReadFile(name string) ([]byte, error) {
	resp, err := p.do(http.MethodGet, name, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fs.ErrNotExist
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", name, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return io.ReadAll(resp.Body)
}

// --- WriteFileIO ------------------------------------------------------

func (p *PelicanIO) WriteFile(name string, b []byte) error {
	resp, err := p.do(http.MethodPut, name, b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("PUT %s: HTTP %d", name, resp.StatusCode)
	}
	return nil
}

func (p *PelicanIO) Create(name string) (iceio.FileWriter, error) {
	return &pelicanWriter{io: p, name: name}, nil
}

// --- file handle ------------------------------------------------------

// pelicanFile buffers the whole object. Parquet seeks backwards to find its
// footer, and Pelican charges ~150 ms per request, so streaming with range
// requests would be slower here, not faster, for the metadata-sized objects
// Iceberg reads. (Large data files are a different trade-off; see §17.)
type pelicanFile struct {
	name string
	*bytes.Reader
	size int64
}

func (f *pelicanFile) Stat() (fs.FileInfo, error) {
	return pelicanStat{name: f.name, size: f.size}, nil
}
func (f *pelicanFile) Close() error { return nil }

type pelicanStat struct {
	name string
	size int64
}

func (s pelicanStat) Name() string       { return s.name }
func (s pelicanStat) Size() int64        { return s.size }
func (s pelicanStat) Mode() fs.FileMode  { return 0o644 }
func (s pelicanStat) ModTime() time.Time { return time.Time{} }
func (s pelicanStat) IsDir() bool        { return false }
func (s pelicanStat) Sys() any           { return nil }

// pelicanWriter buffers locally and PUTs once on Close, because Pelican has
// no append: a second PUT replaces rather than extends.
type pelicanWriter struct {
	io   *PelicanIO
	name string
	buf  bytes.Buffer
}

func (w *pelicanWriter) Write(p []byte) (int, error)         { return w.buf.Write(p) }
func (w *pelicanWriter) ReadFrom(r io.Reader) (int64, error) { return w.buf.ReadFrom(r) }
func (w *pelicanWriter) Close() error {
	return w.io.WriteFile(w.name, w.buf.Bytes())
}

// Compile-time proof we satisfy the interfaces iceberg-go looks for.
var (
	_ iceio.IO          = (*PelicanIO)(nil)
	_ iceio.ReadFileIO  = (*PelicanIO)(nil)
	_ iceio.WriteFileIO = (*PelicanIO)(nil)
	_ iceio.File        = (*pelicanFile)(nil)
	_ iceio.FileWriter  = (*pelicanWriter)(nil)
)
