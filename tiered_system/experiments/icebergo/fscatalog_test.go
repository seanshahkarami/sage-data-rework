//go:build pelican

package main

// exp24: can Iceberg run on Pelican with NO database at all?
//
// §23 found that iceberg-go ships a filesystem ("hadoop") catalog that keeps
// the current-version pointer on storage as version-hint.text and needs no
// database. That was measured on local disk. If it also holds on NRDStor,
// then the last objection to Iceberg in plans/4_catalog_recommendation.md --
// "its commit requires a stateful service" -- disappears entirely, and the
// comparison against the custom catalog changes shape.
//
// So it is worth testing rather than assuming, in both directions: the
// finding is interesting if it works AND if it does not.
//
// Two questions, deliberately separated:
//
//   1. What does iceberg-go itself say about a non-local warehouse? This
//      needs no network -- the check is in the constructor.
//   2. Does the storage provide the one primitive the catalog's commit
//      protocol depends on? The protocol is in commitMetadataFile:
//      RenameNoReplace(temp -> claim) and RenameNoReplace(claim -> vN), and
//      it is safe ONLY because a rename onto an existing path FAILS. If the
//      storage performs that rename instead of refusing, the protocol does
//      not degrade gracefully -- it silently publishes over a committed
//      version.
//
// §17 measured `MOVE` with `Overwrite: F` returning 201 and overwriting, so
// question 2 has an expected answer. It is re-tested here against the exact
// primitive the catalog calls, because §17 tested the pointer-swap use (where
// overwriting is what you WANT) and this is the opposite use (where
// overwriting is the failure).
//
// Run with:  go test -v -tags pelican -run TestFilesystemCatalog ./...
//
// Safety: everything is written under FSPREFIX, a directory used by this test
// alone and distinct from exp21's, and removed afterwards. /sage/node-data and
// the sage-storage-loader paths are never touched.

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
)

// Its own prefix, distinct from exp21's /cold-storage-test-icebergo.
const FSPREFIX = "/cold-storage-test-fscat"

// moveNoReplace issues the request the filesystem catalog's commit protocol
// depends on: move src onto dst, but ONLY if dst does not already exist.
//
// `Overwrite: F` is the WebDAV way to say that (RFC 4918 §10.6). The catalog
// expects fs.ErrExist back when the destination is taken; what matters here
// is whether the origin enforces it or just does the move.
func moveNoReplace(p *PelicanIO, src, dst string) (int, error) {
	tok, err := mint(p.key, fmt.Sprintf("read:%s write:%s", p.prefix, p.prefix), 900*time.Second)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("MOVE", rootURL+src, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Destination", rootURL+dst)
	req.Header.Set("Overwrite", "F")
	counts.add("MOVE")
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

func TestFilesystemCatalogOnPelican(t *testing.T) {
	// --- Question 1: what does iceberg-go allow? No network needed. ------
	//
	// The guard is in NewCatalog, before any filesystem is loaded, so this
	// runs without credentials and without touching the origin.

	_, errRefuse := hadoop.NewCatalog("sage",
		"https://nrdstor.nationalresearchplatform.org:8443/sage/whatever",
		iceberg.Properties{})
	pass(t, "iceberg-go REFUSES a filesystem catalog on remote storage by default",
		errRefuse != nil, fmt.Sprintf("%v", errRefuse))

	// The refusal is not incidental -- upstream names the exact failure mode,
	// which is the same one exp18 measured for the custom catalog on Pelican.
	pass(t, "...and the stated reason is non-atomic commits losing data",
		errRefuse != nil && strings.Contains(errRefuse.Error(), "not atomic") &&
			strings.Contains(errRefuse.Error(), "lose data"),
		"upstream requires an explicit `allow-unsafe-commits` opt-in")

	// With the opt-in it gets further -- the refusal is a policy guard, not a
	// capability limit. It then fails on the IO scheme instead, which is the
	// §21 FileIO gap and not what is being tested here.
	_, errOptIn := hadoop.NewCatalog("sage",
		"https://nrdstor.nationalresearchplatform.org:8443/sage/whatever",
		iceberg.Properties{"allow-unsafe-commits": "true"})
	pass(t, "the guard is policy, not capability: opting in gets past it",
		errOptIn == nil || !strings.Contains(fmt.Sprintf("%v", errOptIn), "allow-unsafe-commits"),
		fmt.Sprintf("%v", errOptIn))

	// --- Question 2: does the storage back the protocol up? --------------
	//
	// Upstream's warning is about object stores generally. Whether it applies
	// to THIS origin is a measurement, and NRDStor is posix-backed, so it is
	// genuinely open: MOVE is atomic here (§17), which is more than S3 gives.

	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Fatalf("issuer key: %v", err)
	}
	pio := NewPelicanIO(key, FSPREFIX)

	_ = pio.Remove(FSPREFIX + "/src")
	_ = pio.Remove(FSPREFIX + "/dst")

	// Stand in for the catalog's two files: a freshly written candidate and
	// an already-committed version that must not be clobbered.
	if err := pio.WriteFile(FSPREFIX+"/src", []byte("NEW uncommitted metadata")); err != nil {
		t.Fatalf("cannot continue without write access: %v", err)
	}
	if err := pio.WriteFile(FSPREFIX+"/dst", []byte("COMMITTED v2 metadata")); err != nil {
		t.Fatalf("setup dst: %v", err)
	}

	st, err := moveNoReplace(pio, FSPREFIX+"/src", FSPREFIX+"/dst")
	if err != nil {
		t.Fatalf("MOVE: %v", err)
	}

	// 412 Precondition Failed would be the correct answer; the catalog maps a
	// refusal to fs.ErrExist and reports a conflict. Measured: 201.
	//
	// The checks below assert what the origin ACTUALLY does, so the test
	// passes by recording the truth rather than failing. The finding is the
	// negative one, and it is stated in the names.
	refused := st == http.StatusPreconditionFailed || st == http.StatusForbidden
	pass(t, "MOVE with `Overwrite: F` is IGNORED -- no refusal, the move happens",
		!refused, fmt.Sprintf("HTTP %d (412 would be a refusal; this is a success)", st))

	// The status code could mislead on its own, so read the destination back.
	// This is the check that actually decides it: if the committed bytes are
	// gone, a committed metadata version was destroyed.
	after, errRead := pio.ReadFile(FSPREFIX + "/dst")
	survived := errRead == nil && string(after) == "COMMITTED v2 metadata"
	pass(t, "the already-committed version was DESTROYED by the move",
		!survived, fmt.Sprintf("dst held \"COMMITTED v2 metadata\", now holds %q", string(after)))

	// Control. If MOVE did not work at all, "the destination survived" would
	// pass for the wrong reason, exactly like the vacuous tag-expiry pass in
	// §22. A move to a FREE path must succeed for the result above to mean
	// anything.
	_ = pio.Remove(FSPREFIX + "/src2")
	_ = pio.Remove(FSPREFIX + "/free")
	if err := pio.WriteFile(FSPREFIX+"/src2", []byte("movable")); err != nil {
		t.Fatalf("setup src2: %v", err)
	}
	stFree, err := moveNoReplace(pio, FSPREFIX+"/src2", FSPREFIX+"/free")
	if err != nil {
		t.Fatalf("control MOVE: %v", err)
	}
	moved, errFree := pio.ReadFile(FSPREFIX + "/free")
	pass(t, "control: the same MOVE to a FREE path SUCCEEDS",
		stFree < 300 && errFree == nil && string(moved) == "movable",
		fmt.Sprintf("HTTP %d, free now %q", stFree, string(moved)))

	// Same question for the other way a writer could claim a version: a plain
	// PUT guarded by If-None-Match. If either primitive held, a database-free
	// commit could be built even without the stock catalog.
	_ = pio.Remove(FSPREFIX + "/put")
	if err := pio.WriteFile(FSPREFIX+"/put", []byte("COMMITTED")); err != nil {
		t.Fatalf("setup put: %v", err)
	}
	tok, _ := mint(key, fmt.Sprintf("read:%s write:%s", FSPREFIX, FSPREFIX), 900*time.Second)
	req, _ := http.NewRequest(http.MethodPut, rootURL+FSPREFIX+"/put",
		strings.NewReader("OVERWRITTEN"))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("If-None-Match", "*")
	counts.add("PUT")
	resp, err := pio.client.Do(req)
	if err != nil {
		t.Fatalf("conditional PUT: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	putAfter, _ := pio.ReadFile(FSPREFIX + "/put")
	pass(t, "PUT with `If-None-Match: *` is IGNORED too -- no second primitive",
		string(putAfter) != "COMMITTED",
		fmt.Sprintf("HTTP %d, content now %q", resp.StatusCode, string(putAfter)))

	// Both primitives behave the same way, which is the point: the gap is the
	// origin's, not one header's. Neither MOVE nor PUT can express "only if
	// absent", so no database-free commit protocol can be made safe here --
	// not the stock filesystem catalog, and not a hand-rolled replacement.

	t.Logf("\nrequests: %v (total %d)", counts.snapshot(), counts.total())

	t.Cleanup(func() {
		n, err := pio.RemoveAll(FSPREFIX)
		if err != nil {
			t.Errorf("CLEANUP FAILED for %s after %d objects: %v", FSPREFIX, n, err)

			return
		}
		stc, _, _ := pio.PropFind(FSPREFIX)
		pass(t, "cleanup: test prefix removed and verified gone",
			stc == 404, fmt.Sprintf("%d objects removed, PROPFIND now HTTP %d", n, stc))
	})
}
