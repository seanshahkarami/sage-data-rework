//go:build pelican

// §32: does the filesystem catalog actually WORK on Pelican with ONE writer?
//
// §24 asked "can a database-free commit be made SAFE here" and answered no,
// because the origin has no compare-and-swap. §31 swept every conditional
// primitive and explained why (XRootD 5.9.7: no ETag, no Last-Modified,
// DAV: 1, 2xx even for nonsense methods). Both probed raw HTTP. Neither
// ever built a table with the hadoop catalog -- §24 constructed one past the
// `allow-unsafe-commits` guard and then went straight to curl-level probes.
//
// That leaves the question a single-writer deployment actually cares about
// unanswered, and SAGE has exactly one writer. The hadoop catalog's unsafety
// is a lost-update race between CONCURRENT committers; with one writer that
// race cannot occur. So the honest question is not "is it safe under
// concurrency" (no, settled) but "does it work at all, and what exactly
// breaks when it doesn't".
//
// The project's own standard applies here: §19 wrote ~230 lines of
// pelican_fileio.py and §21 wrote pelican_io.go rather than accept a FileIO
// gap as final. So this does not stop at "PelicanIO doesn't implement the
// interface" -- it implements the eight methods HadoopCatalogFS demands and
// finds out which ones the ORIGIN cannot back.
//
// Of those eight, seven are honest. RenameNoReplace is the one that cannot
// be: it is defined as "rename, but fail if the destination exists", and
// §31 established the origin offers no way to express that. The adapter
// below implements it as a check-then-move, which is NOT atomic, and says
// so loudly. That is the whole point -- the gap is a TOCTOU window, and
// this measures what sits inside it.
//
// Writes under /sage/cold-storage-test-fscat1/, its own prefix, removed after.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

const FS1PREFIX = "/cold-storage-test-fscat1"

// NOTE: every method below scopes its token to h.prefix, the prefix the
// embedded PelicanIO was constructed with -- NOT to a package-level const.
// An earlier draft hardcoded FS1PREFIX, which worked only because this file
// was the sole user; §33 reuses the adapter under a different prefix and the
// mismatch surfaced as "namespace does not exist", a confusing way for a
// token-scoping mistake to present (exp19 §4b hit the same class of bug).
//
// hadoopFS adapts PelicanIO to hadoop.HadoopCatalogFS, which demands eight
// interfaces: ListableIO, ReadFileIO, WriteFileIO, StatIO, RenameIO,
// RenameNoReplaceIO, RemoveAllIO, MkdirAllIO. PelicanIO already supplies
// Open/ReadFile/Create/WriteFile/Remove; the rest are here.
type hadoopFS struct {
	*PelicanIO
	// unsafeRenames counts RenameNoReplace calls that could not be made
	// atomic. Every one is a window in which a concurrent writer could
	// clobber a committed metadata file.
	unsafeRenames int
}

// Rename: plain MOVE. §17 measured this as atomic on NRDStor, and §31
// re-confirmed it works even though it is absent from the Allow list.
func (h *hadoopFS) Rename(oldpath, newpath string) error {
	src, err := h.pathOf(oldpath)
	if err != nil {
		return err
	}
	dst, err := h.pathOf(newpath)
	if err != nil {
		return err
	}
	resp, err := h.doMove(src, dst, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("MOVE %s -> %s: HTTP %d", src, dst, resp.StatusCode)
	}
	return nil
}

// RenameNoReplace is the method the whole filesystem catalog rests on, and
// the one this origin cannot support.
//
// Contract: move oldpath to newpath, returning fs.ErrExist if newpath is
// already taken. The catalog relies on that refusal being ATOMIC -- it is
// how two racing committers are serialized.
//
// §31 established there is no way to ask this origin for that. `Overwrite: F`
// is not parsed; there is no ETag for If-Match; the WebDAV If: header is
// ignored. So the only available implementation is check-then-move, which
// has a TOCTOU window between the check and the move. It satisfies the
// INTERFACE while silently failing the GUARANTEE -- exactly the shape of bug
// §24 warned about ("the worst shape for a bug, since testing would not
// surface it"), so it is counted and reported rather than hidden.
func (h *hadoopFS) RenameNoReplace(oldpath, newpath string) error {
	dst, err := h.pathOf(newpath)
	if err != nil {
		return err
	}
	// The check half of check-then-move.
	if _, err := h.Stat(newpath); err == nil {
		return fmt.Errorf("rename %s: %w", dst, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	h.unsafeRenames++
	return h.Rename(oldpath, newpath)
}

func (h *hadoopFS) doMove(src, dst string, overwrite bool) (*http.Response, error) {
	tok, err := mint(h.key, fmt.Sprintf("read:%s write:%s", h.prefix, h.prefix), 900*time.Second)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("MOVE", rootURL+src, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Destination", rootURL+dst)
	if !overwrite {
		req.Header.Set("Overwrite", "F") // ignored here; see §31
	}
	counts.add("MOVE")
	return h.client.Do(req)
}

var statTrace func(string, string)

func (h *hadoopFS) Stat(path string) (fs.FileInfo, error) {
	p, err := h.pathOf(path)
	if err != nil {
		return nil, err
	}
	tok, err := mint(h.key, fmt.Sprintf("read:%s write:%s", h.prefix, h.prefix), 900*time.Second)
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest(http.MethodHead, rootURL+p, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	counts.add("HEAD")
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		// A collection has no HEAD representation on this origin but does
		// answer PROPFIND with 207. Without this, every namespace looks
		// missing and CreateTable fails with "namespace does not exist".
		if statTrace != nil {
			statTrace(path, fmt.Sprintf("HEAD %d (notfound branch), resolved=%q", resp.StatusCode, p))
		}
		if st, _, perr := h.PropFind(p); perr == nil && st == 207 {
			nm := strings.TrimRight(p, "/")
			return statInfo{name: nm[strings.LastIndex(nm, "/")+1:], dir: true}, nil
		}
		return nil, fmt.Errorf("stat %s: %w", p, fs.ErrNotExist)
	}
	if resp.StatusCode >= 300 {
		if statTrace != nil {
			statTrace(path, fmt.Sprintf("HTTP %d -> error", resp.StatusCode))
		}
		return nil, fmt.Errorf("stat %s: HTTP %d", p, resp.StatusCode)
	}
	// A collection also answers HEAD with 200 on this origin, so the status
	// code alone cannot distinguish file from directory -- an earlier draft
	// assumed it could and reported every namespace as a file, which the
	// catalog rejected as "namespace does not exist". PROPFIND is what
	// actually distinguishes them: a collection returns 207 with a
	// self-href ending in "/", a file does not.
	name := p[strings.LastIndex(strings.TrimRight(p, "/"), "/")+1:]
	isDir, derr := h.isCollection(p)
	if statTrace != nil {
		statTrace(path, fmt.Sprintf("HEAD %d, isCollection=%v err=%v -> resolved=%q dir=%v", resp.StatusCode, isDir, derr, p, isDir))
	}
	if derr == nil && isDir {
		return statInfo{name: name, dir: true}, nil
	}
	return statInfo{name: name, size: resp.ContentLength, dir: false}, nil
}

// isCollection asks PROPFIND with Depth: 0 whether this path is a WebDAV
// collection, by checking whether its own href carries a trailing slash.
func (h *hadoopFS) isCollection(p string) (bool, error) {
	tok, err := mint(h.key, fmt.Sprintf("read:%s write:%s", h.prefix, h.prefix), 900*time.Second)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest("PROPFIND", rootURL+p, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Depth", "0")
	counts.add("PROPFIND")
	resp, err := h.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 207 {
		return false, nil
	}
	body, _ := io.ReadAll(resp.Body)
	// This origin does NOT slash-terminate the self href -- an earlier draft
	// keyed on that and classified every collection as a file. It does emit
	// both standard discriminators, so use them:
	//     <lp1:resourcetype><D:collection/></lp1:resourcetype>
	//     <lp1:iscollection>1</lp1:iscollection>
	b := string(body)
	return strings.Contains(b, "<D:collection") ||
		strings.Contains(b, ":collection/>") ||
		iscollRE.MatchString(b), nil
}

// statInfo exists because pelicanStat hardcodes IsDir() == false, and the
// hadoop catalog decides whether a namespace exists via Stat().IsDir().
// pelicanStat is shared with exp19/exp21, so this shadows it locally rather
// than changing their behaviour.
type statInfo struct {
	name string
	size int64
	dir  bool
}

func (s statInfo) Name() string { return s.name }
func (s statInfo) Size() int64  { return s.size }
func (s statInfo) Mode() fs.FileMode {
	if s.dir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}
func (s statInfo) ModTime() time.Time { return time.Time{} }
func (s statInfo) IsDir() bool        { return s.dir }
func (s statInfo) Sys() any           { return nil }

// MkdirAll creates a collection with MKCOL, which §31 measured as working
// (201) on this origin. An earlier draft returned nil on the theory that PUT
// creates parents implicitly -- that was wrong: the hadoop catalog creates a
// namespace as a DIRECTORY and then checks Stat().IsDir(), so a no-op here
// surfaced as "namespace does not exist: sage" on CreateTable.
func (h *hadoopFS) MkdirAll(path string) error {
	p, err := h.pathOf(path)
	if err != nil {
		return err
	}
	segs := strings.Split(strings.Trim(strings.TrimPrefix(p, h.prefix), "/"), "/")
	cur := h.prefix
	for _, seg := range segs {
		if seg == "" {
			continue
		}
		cur += "/" + seg
		tok, err := mint(h.key, fmt.Sprintf("read:%s write:%s", h.prefix, h.prefix), 900*time.Second)
		if err != nil {
			return err
		}
		req, err := http.NewRequest("MKCOL", rootURL+cur, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		counts.add("MKCOL")
		resp, err := h.client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		// 405/409 mean it already exists, which MkdirAll treats as success.
		if resp.StatusCode >= 300 && resp.StatusCode != 405 && resp.StatusCode != 409 {
			return fmt.Errorf("MKCOL %s: HTTP %d", cur, resp.StatusCode)
		}
	}
	return nil
}

func (h *hadoopFS) RemoveAll(name string) error {
	_, err := h.PelicanIO.RemoveAll(name)
	return err
}

// WalkDir over PROPFIND. Depth:1 per level, recursing into collections.
func (h *hadoopFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	rp, err := h.pathOf(root)
	if err != nil {
		return err
	}
	var walk func(string) error
	walk = func(p string) error {
		st, kids, err := h.PropFind(p)
		if err != nil {
			return err
		}
		if st == http.StatusNotFound {
			return nil
		}
		for _, k := range kids {
			isDir := strings.HasSuffix(k, "/")
			full := rootURL + k
			if isDir {
				if err := walk(strings.TrimRight(k, "/")); err != nil {
					return err
				}
				continue
			}
			if err := fn(full, pelicanDirEntry{name: k[strings.LastIndex(strings.TrimRight(k, "/"), "/")+1:]}, nil); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(rp)
}

type pelicanDirEntry struct{ name string }

func (d pelicanDirEntry) Name() string               { return d.name }
func (d pelicanDirEntry) IsDir() bool                { return false }
func (d pelicanDirEntry) Type() fs.FileMode          { return 0 }
func (d pelicanDirEntry) Info() (fs.FileInfo, error) { return pelicanStat{name: d.name}, nil }

var iscollRE = regexp.MustCompile(`(?i)<[^>]*iscollection>\s*1\s*<`)

var _ hadoop.HadoopCatalogFS = (*hadoopFS)(nil)

// iceio.Register is process-global and last-call-wins. Three tests in this
// package each need their own adapter (different prefixes), so they install
// into activeFS instead of each calling Register. Registering once, here,
// keeps `go test -run` with multiple tests working; without it whichever
// test registered last silently served the others' requests under the wrong
// token scope, and the losers failed with no output at all.
var activeFS *hadoopFS

func init() {
	iceio.Register("https", func(_ context.Context, _ *url.URL, _ map[string]string) (iceio.IO, error) {
		if activeFS == nil {
			return nil, fmt.Errorf("no hadoopFS installed; call useFS(t, fs) first")
		}
		return activeFS, nil
	})
}

// useFS installs fs for the duration of one test and restores the previous
// value afterwards, so tests do not leak into each other.
func useFS(t *testing.T, fs *hadoopFS) {
	t.Helper()
	prev := activeFS
	activeFS = fs
	t.Cleanup(func() { activeFS = prev })
}

func TestFilesystemCatalogSingleWriter(t *testing.T) {
	ctx := context.Background()
	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Skipf("no Pelican key: %v", err)
	}

	hfs := &hadoopFS{PelicanIO: NewPelicanIO(key, FS1PREFIX)}
	useFS(t, hfs)

	warehouse := rootURL + FS1PREFIX
	t.Cleanup(func() {
		n, _ := hfs.PelicanIO.RemoveAll(FS1PREFIX)
		st, _, _ := hfs.PropFind(FS1PREFIX)
		t.Logf("cleanup: %d objects removed, PROPFIND -> %d", n, st)
	})

	mkcat := func() (*hadoop.Catalog, error) {
		return hadoop.NewCatalog("sage", warehouse,
			iceberg.Properties{"allow-unsafe-commits": "true"})
	}

	// statTrace is left wired but silent: it was essential in finding that a
	// collection answers HEAD with 200 here (so the status code alone cannot
	// tell a namespace from a file), and the next person debugging this will
	// want it. Uncomment to re-enable.
	// statTrace = func(in, out string) { t.Logf("    stat(%q): %s", in, out) }

	cat, err := mkcat()
	if err != nil {
		t.Fatalf("[FAIL] Q0: catalog construction: %v", err)
	}
	t.Log("[PASS] Q0: hadoop catalog constructed against a Pelican warehouse")

	ns := table.Identifier{"sage"}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("[FAIL] CreateNamespace: %v", err)
	}

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "node", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64},
	)
	ident := table.Identifier{"sage", "obs"}

	// ---- Q1: create a table --------------------------------------------
	tbl, err := cat.CreateTable(ctx, ident, sch)
	if err != nil {
		t.Fatalf("[FAIL] Q1: CreateTable on Pelican: %v", err)
	}
	t.Logf("[PASS] Q1: table created, metadata at %s", tbl.MetadataLocation())

	// ---- Q2: version-hint.text, the database-free pointer ---------------
	hintPath := FS1PREFIX + "/sage/obs/metadata/version-hint.text"
	hint, err := hfs.ReadFile(hintPath)
	if err != nil {
		t.Errorf("[FAIL] Q2: version-hint.text unreadable: %v", err)
	} else {
		t.Logf("[PASS] Q2: version-hint.text on Pelican, contents %q", strings.TrimSpace(string(hint)))
	}
	before := strings.TrimSpace(string(hint))

	// ---- Q4: a second sequential commit must advance the pointer --------
	tbl2, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("[FAIL] Q4: LoadTable: %v", err)
	}
	txn := tbl2.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{"sage.commit": "two"}); err != nil {
		t.Fatalf("[FAIL] Q4: SetProperties: %v", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Errorf("[FAIL] Q4: second commit: %v", err)
	} else {
		h2, _ := hfs.ReadFile(hintPath)
		after := strings.TrimSpace(string(h2))
		if after != before && after != "" {
			t.Logf("[PASS] Q4: pointer advanced %q -> %q", before, after)
		} else {
			t.Errorf("[FAIL] Q4: pointer did not advance (%q -> %q)", before, after)
		}
	}

	// ---- Q5: the pointer names a file that exists -----------------------
	hNow, _ := hfs.ReadFile(hintPath)
	v := strings.TrimSpace(string(hNow))
	if v != "" {
		mpath := fmt.Sprintf("%s/sage/obs/metadata/v%s.metadata.json", FS1PREFIX, v)
		if _, err := hfs.Stat(mpath); err == nil {
			t.Logf("[PASS] Q5: version-hint.text -> v%s.metadata.json exists", v)
		} else {
			t.Errorf("[FAIL] Q5: pointer names v%s.metadata.json: %v", v, err)
		}
	}

	// ---- Q3: a fresh catalog, no shared state, loads the table ----------
	// The reader-with-only-a-URL case: no database anywhere in the picture.
	cat2, err := mkcat()
	if err != nil {
		t.Fatalf("[FAIL] Q3: second catalog: %v", err)
	}
	loaded, err := cat2.LoadTable(ctx, ident)
	if err != nil {
		t.Errorf("[FAIL] Q3: independent catalog could not load the table: %v", err)
	} else {
		t.Logf("[PASS] Q3: independent catalog loaded it from the URL alone, %d fields, metadata %s",
			len(loaded.Schema().Fields()), loaded.MetadataLocation())
	}

	// ---- the caveat, quantified -----------------------------------------
	t.Logf("[ -- ] RenameNoReplace calls that could NOT be made atomic: %d", hfs.unsafeRenames)
	t.Logf("[ -- ] each is a TOCTOU window; harmless with one writer, a silent " +
		"lost update with two (§18 measured 3/3, §31 explains why no header closes it)")
	t.Logf("[ -- ] requests: %v", counts.snapshot())
}
