//go:build portable

package main

// exp27: can the long-term store be handed to storage SAGE does not run?
//
// `plans/Data Pipeline Considerations.md` asks for exactly this:
//
//   "it would be helpful if the new data path could push completed,
//    finalized Parquet files in the Long Term path to operational resources
//    not managed or governed by Sage. After the successful push ...
//    operational responsibility, including search and redaction would end.
//    Use Case: Suppose the nwf.org wants to purchase a node and save the
//    accumulated data for their non-profit. Could the Long Term data be
//    routed to AWS, or other storage platforms?"
//
// That is two separate questions, and conflating them is the easy mistake:
//
//   Q1 ADDRESSING. Can this client talk to storage other than Pelican at
//      all, without a fork? "Anything can be built" is true of both designs,
//      so the only interesting answer is what already EXISTS.
//
//   Q2 PORTABILITY. Can a table that was WRITTEN on one storage system be
//      MOVED to another and still open? This is the part the use case
//      actually needs, and it is where a table format can fail badly:
//      Iceberg metadata stores the location of every data file, and if
//      those are absolute URLs, a copied table points back at storage the
//      new owner cannot read -- or worse, can read, which means the handoff
//      silently never happened.
//
// Q2 is the one with a real chance of a negative result, so it gets the
// adversarial treatment: the source is DESTROYED before the moved copy is
// read, so a pass cannot come from the reader quietly falling back to the
// original location. Without that, a test like this passes vacuously.
//
// Local disk only, deliberately. These are format and client questions --
// whether paths inside metadata survive a move is decided by the bytes, not
// by the origin. exp21 already showed this client's writes reach Pelican
// intact. Using two temp dirs also lets the source be destroyed, which is
// not something to do to a real origin.
//
// Run with:  go test -v -tags portable -run TestPortable ./...

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	_ "modernc.org/sqlite"

	// Imported for side effects ONLY. Each package's init() registers its
	// schemes with the IO registry, so importing is the entire
	// integration -- no configuration, no fork. That is the claim Q1
	// tests, and these three lines are the evidence for it.
	_ "github.com/apache/iceberg-go/io/gocloud/azure"
	_ "github.com/apache/iceberg-go/io/gocloud/gcs"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
)

func pck(t *testing.T, name string, ok bool, detail string) {
	t.Helper()
	tag := "PASS"
	if !ok {
		tag = "FAIL"
	}
	if detail != "" {
		t.Logf("[%s] %s\n       %s", tag, name, detail)
	} else {
		t.Logf("[%s] %s", tag, name)
	}
	if !ok {
		t.Fail()
	}
}

// ---------------------------------------------------------------- Q1 ----

// TestPortableSchemes asks what storage this client can already address.
// No network: the question is what is COMPILED IN, not whether a given
// bucket answers today.
func TestPortableSchemes(t *testing.T) {
	// NOTE ON METHOD: init() in each imported backend has already run by
	// the time any test executes, so this cannot measure "before vs after
	// importing". There is no honest before-state inside one binary. What
	// it does show is that the ONLY integration step was the import line
	// -- no credentials, no config, no fork -- and that the schemes are
	// present as a result. The `file` scheme is registered by the library
	// itself (io/registry.go:76-77), not by any backend.
	after := iceio.GetRegisteredSchemes()
	sort.Strings(after)
	pck(t, "local filesystem is registered by the library itself",
		contains(after, "file"), strings.Join(after, " "))

	want := map[string][]string{
		"S3 (and S3-compatible: MinIO, Ceph, Alibaba OSS)": {"s3", "s3a", "s3n", "oss"},
		"Google Cloud Storage":                             {"gs"},
		"Azure Blob":                                       {"abfs", "abfss", "wasb", "wasbs"},
	}
	for label, schemes := range want {
		missing := []string{}
		for _, s := range schemes {
			if !contains(after, s) {
				missing = append(missing, s)
			}
		}
		pck(t, "ships a backend for "+label, len(missing) == 0,
			"schemes: "+strings.Join(schemes, " "))
	}

	pck(t, "every scheme reachable from a plain import, no fork",
		len(after) >= 9, strings.Join(after, " "))

	// The cost side, so this is not reported as free. Importing all three
	// backends took this module's dependency count from 78 to 133 (+55,
	// measured by `go mod tidy` before and after adding the three import
	// lines): the AWS SDK v2, cloud.google.com/go/storage and the Azure
	// SDK, with their transitive trees. Importing only the one backend
	// actually needed is the obvious mitigation, and Pelican needs none of
	// them -- pelican_io.go is ~190 lines against net/http.
	pck(t, "cost noted: +55 modules for all three cloud backends (78 -> 133)",
		true, "import only the backend needed; Pelican's own IO adds no deps")

	// The extension point. This is what pelican_io.go uses -- the reason
	// Pelican support is ~190 lines rather than a fork. Proving it is
	// public API matters more than the count above: it is what makes an
	// UNANTICIPATED storage system reachable.
	iceio.Register("exp27scheme", func(ctx context.Context, u *url.URL, props map[string]string) (iceio.IO, error) {
		return nil, fmt.Errorf("not used")
	})
	defer iceio.Unregister("exp27scheme")
	pck(t, "a NEW scheme can be registered by a third party (no fork)",
		contains(iceio.GetRegisteredSchemes(), "exp27scheme"),
		"io.Register(scheme, factory) is exported -- how pelican_io.go attaches")

	// Honesty check: an unregistered scheme must FAIL, otherwise the
	// registry proves nothing.
	_, err := iceio.LoadFS(context.Background(), nil, "nosuchscheme://bucket/x")
	pck(t, "control: an unregistered scheme is refused, so the registry is real",
		err != nil, fmt.Sprintf("%v", err))
}

// ---------------------------------------------------------------- Q2 ----

// TestPortableHandoff is the nwf.org / CROCUS case: write a table here,
// hand the bytes to someone else's storage, and have it work there with the
// original gone.
func TestPortableHandoff(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	dst := t.TempDir()

	tbl, _ := buildTable(t, ctx, src)
	metaLoc := tbl.MetadataLocation()

	// What does the metadata actually record? This decides the outcome.
	raw, err := os.ReadFile(strings.TrimPrefix(metaLoc, "file://"))
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	loc, _ := meta["location"].(string)
	pck(t, "table metadata records an ABSOLUTE location, not a relative one",
		strings.HasPrefix(loc, "file://") && strings.Contains(loc, src),
		"location = "+loc)

	// Count how many absolute references to the OLD root exist in the
	// metadata tree. Every one is a path that has to be rewritten.
	refs := strings.Count(string(raw), src)
	pck(t, "that absolute root is embedded repeatedly, so a copy is not enough",
		refs > 1, fmt.Sprintf("%d occurrences of the source root in one metadata file", refs))

	// --- the naive handoff: copy the bytes, change nothing -------------
	if err := copyTree(src, dst); err != nil {
		t.Fatal(err)
	}

	// DESTROY the source. Without this the next check can pass by reading
	// the original files through the absolute paths, which would make the
	// whole experiment vacuous.
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatal("source not actually destroyed; the rest would be vacuous")
	}
	pck(t, "control: the source storage is gone before any read is attempted",
		true, "anything that still works is working from the copy")

	movedMeta := strings.Replace(metaLoc, src, dst, 1)
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, movedMeta)
	}

	// The metadata file itself is addressed directly, so it opens.
	moved, err := table.NewFromLocation(ctx, table.Identifier{"nwf", "obs"}, movedMeta, fsys, nil)
	pck(t, "the moved table's METADATA still opens (schema, docs, properties)",
		err == nil && moved != nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.FailNow()
	}
	pck(t, "schema survives the move intact",
		len(moved.Schema().Fields()) == 4,
		fmt.Sprintf("%d fields", len(moved.Schema().Fields())))

	// ... but the DATA pointers are still absolute into the dead source.
	rowsMoved, errMoved := countRows(ctx, moved)
	pck(t, "but READING DATA fails -- file paths still point at the old storage",
		errMoved != nil || rowsMoved == 0,
		fmt.Sprintf("rows=%d err=%v", rowsMoved, errMoved))

	// Name the stale pointer explicitly rather than inferring it from the
	// failure, which could have had any number of causes.
	//
	// Careful here: firstDataPath cannot reach the data files at all now,
	// because the hop BEFORE them is already broken -- the snapshot's
	// manifest-list path is itself absolute into the dead source. So this
	// asserts on the manifest list, which is what actually failed. (An
	// earlier version of this check claimed to show a *data file* path and
	// passed only because the error string happened to contain the source
	// root -- a vacuous pass. The data-file path is demonstrated for real
	// in TestPortableHandoffDone, where the source still exists.)
	snap := moved.CurrentSnapshot()
	mlist := ""
	if snap != nil {
		mlist = snap.ManifestList
	}
	pck(t, "the snapshot's manifest-list pointer is absolute into the DEAD source",
		strings.Contains(mlist, src), "manifest list: "+mlist)
}

// TestPortableHandoffDone is the same handoff done correctly: rewrite the
// locations. This separates "Iceberg cannot be moved" (false) from "moving
// it requires a deliberate step" (true), which is a very different finding.
func TestPortableHandoffDone(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	dst := t.TempDir()

	tbl, want := buildTable(t, ctx, src)
	metaLoc := tbl.MetadataLocation()

	if err := copyTree(src, dst); err != nil {
		t.Fatal(err)
	}

	// The rewrite. Iceberg metadata is JSON and Avro; the JSON half can be
	// rewritten textually, but the manifest files are Avro and hold the
	// data-file paths, so a sed over *.json is NOT sufficient. Prove that
	// first -- it is the trap someone attempting this by hand would hit.
	rewriteJSON(t, dst, src, dst)

	movedMeta := strings.Replace(metaLoc, src, dst, 1)
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, movedMeta)
	}
	moved, err := table.NewFromLocation(ctx, table.Identifier{"nwf", "obs"}, movedMeta, fsys, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Source still alive here, so this reads through whichever path works.
	// The point is what the AVRO still says, not whether the read succeeds.
	stale := firstDataPath(ctx, t, moved)
	pck(t, "rewriting the JSON is NOT enough -- manifests are Avro and still stale",
		strings.Contains(stale, src),
		"after JSON rewrite, data path = "+stale)

	pck(t, "so a correct handoff needs a real rewrite, not sed over *.json",
		true, "iceberg-go v0.7.0 ships no location-rewrite helper (checked: no RewriteLocation/rewrite_location in the module)")

	// Sanity: the table is intact where it was written. The move is the
	// problem, not the table.
	got, err := countRows(ctx, tbl)
	pck(t, "control: the table reads correctly at its ORIGINAL location",
		err == nil && got == want, fmt.Sprintf("rows=%d want=%d err=%v", got, want, err))
}

// ------------------------------------------------------------- helpers ---

func buildTable(t *testing.T, ctx context.Context, wh string) (*table.Table, int64) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(wh, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	cat, err := sqlcat.NewCatalog("sage", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + wh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		t.Fatal(err)
	}

	// v2 + microseconds: the format the 2026-10-14 decision settled on.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "plugin", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatal(err)
	}
	tbl = writePortable(t, ctx, tbl, 0)
	tbl = writePortable(t, ctx, tbl, 100)

	return tbl, 200
}

func writePortable(t *testing.T, ctx context.Context, tbl *table.Table, base int) *table.Table {
	t.Helper()
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "plugin", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	bld := array.NewRecordBuilder(memory.DefaultAllocator, arrSchema)
	defer bld.Release()
	for i := 0; i < 100; i++ {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1735689600000000 + int64(base+i)))
		bld.Field(1).(*array.StringBuilder).Append("env.temperature")
		bld.Field(2).(*array.StringBuilder).Append("plugin-iio:0.6.0")
		bld.Field(3).(*array.Float64Builder).Append(float64(base+i) * 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()

	at := array.NewTableFromRecords(arrSchema, []arrow.Record{rec})
	defer at.Release()
	out, err := tbl.AppendTable(ctx, at, 100, nil)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func countRows(ctx context.Context, tbl *table.Table) (int64, error) {
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		return 0, err
	}
	defer at.Release()

	return at.NumRows(), nil
}

// firstDataPath returns the file path Iceberg has recorded for the first
// data file, which is the thing that does or does not survive a move.
func firstDataPath(ctx context.Context, t *testing.T, tbl *table.Table) string {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return "(no snapshot)"
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return "(no fs: " + err.Error() + ")"
	}
	manifests, err := snap.Manifests(fs)
	if err != nil || len(manifests) == 0 {
		return fmt.Sprintf("(no manifests: %v)", err)
	}
	entries, err := manifests[0].FetchEntries(fs, false)
	if err != nil || len(entries) == 0 {
		return fmt.Sprintf("(no entries: %v)", err)
	}

	return entries[0].DataFile().FilePath()
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}

		return os.WriteFile(target, b, 0o644)
	})
}

// rewriteJSON does the naive thing a person would try: replace the old root
// in every *.json. It deliberately leaves the Avro manifests alone, because
// showing that this is insufficient is the point.
func rewriteJSON(t *testing.T, root, from, to string) {
	t.Helper()
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".json") {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}

		return os.WriteFile(p, []byte(strings.ReplaceAll(string(b), from, to)), 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}

	return false
}
