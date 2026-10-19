//go:build reader

package main

// exp23: the NDP question -- what can a READ-ONLY third party do with an
// Iceberg table it did not write?
//
// NDP is an ecosystem and enabler, not a data host: it would read SAGE data,
// never write it. That makes the reader path, not the commit path, the one
// that decides whether Iceberg helps here. Three things have to be true, and
// none of them were covered by exp19-exp22:
//
//  1. A reader must NOT need the writer's catalog. Every Iceberg experiment
//     so far has gone through the SQLite catalog, which is the writer's
//     private component. If a reader needs it, Iceberg is a liability for
//     NDP rather than a help -- it would mean handing out database access.
//
//  2. The table must be SELF-DESCRIBING. The claim under test is that
//     Iceberg tells a consumer the structure of the data, where today they
//     must open Parquet files and infer it.
//
//  3. A reader must be able to discover structure WITHOUT scanning data,
//     because scanning is what costs money on Pelican (~150 ms/request).
//
// Local disk: these are format questions. exp21 already proved this client's
// bytes reach Pelican intact, and the reader path is the same code.
//
// Run with:  go test -v -tags reader -run TestReader ./...

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	_ "modernc.org/sqlite"
)

func rck(t *testing.T, name string, ok bool, detail string) {
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

// TestReaderWithoutCatalog is the NDP shape: a consumer who has the storage
// URL and nothing else.
func TestReaderWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()

	// --- the WRITER's side: a realistic archive-shaped table -----------
	db, err := sql.Open("sqlite", filepath.Join(wh, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cat, err := sqlcat.NewCatalog("sage", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + wh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		t.Fatal(err)
	}

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}, Required: false,
			Doc: "observation time, nanosecond precision"},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false,
			Doc: "measurement name, e.g. env.temperature"},
		iceberg.NestedField{ID: 3, Name: "plugin", Type: iceberg.PrimitiveTypes.String, Required: false,
			Doc: "plugin that produced the record"},
		iceberg.NestedField{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false,
			Doc: "measured value"},
	)

	ident := table.Identifier{"sage", "obs"}
	tbl, err := cat.CreateTable(ctx, ident, sc,
		catalog.WithProperties(iceberg.Properties{
			"format-version": "3",
			// Table properties are free-form and travel with the table.
			// This is where dataset-level documentation would live.
			"sage.description": "SAGE sensor observations, archive export",
			"sage.contact":     "sage-support@example.org",
			"sage.license":     "CC-BY-4.0",
		}))
	if err != nil {
		t.Fatal(err)
	}

	tbl = writeObs(t, ctx, tbl, 0)
	tbl = writeObs(t, ctx, tbl, 100)
	metaLoc := tbl.MetadataLocation()

	// --- the READER's side: NDP ---------------------------------------
	// Everything below this line uses ONLY metaLoc -- a storage path. No
	// catalog handle, no database, no credentials beyond read. `nil` is
	// passed where the catalog would go.
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, metaLoc)
	}

	ro, err := table.NewFromLocation(ctx, table.Identifier{"ndp", "obs"}, metaLoc, fsys, nil)
	rck(t, "a reader can open the table with NO catalog (nil), just a URL",
		err == nil && ro != nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.FailNow()
	}

	// 1. Structure, without reading a single data file.
	fields := ro.Schema().Fields()
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, fmt.Sprintf("%s:%s", f.Name, f.Type))
	}
	rck(t, "reader sees the full schema with types, without opening any Parquet",
		len(fields) == 4, strings.Join(names, ", "))

	// Column documentation travels with the table -- this is the part a
	// bare directory of Parquet cannot carry at all.
	docs := 0
	for _, f := range fields {
		if f.Doc != "" {
			docs++
		}
	}
	rck(t, "per-column documentation survives to the reader", docs == 4,
		fmt.Sprintf("%d of 4 fields carry a doc string; time -> %q", docs, fields[0].Doc))

	// Dataset-level metadata (licence, contact, description).
	props := ro.Properties()
	rck(t, "table properties carry dataset-level metadata (licence, contact)",
		props["sage.license"] == "CC-BY-4.0" && props["sage.contact"] != "",
		fmt.Sprintf("license=%q description=%q", props["sage.license"], props["sage.description"]))

	// 2. Row counts and file sizes WITHOUT scanning. This is the question
	// that decides whether a catalogue listing is cheap: a portal showing
	// "N rows, M files, X bytes" must not have to read the data to do it.
	snap := ro.CurrentSnapshot()
	var rows, files, bytes string
	if snap != nil && snap.Summary != nil {
		rows = snap.Summary.Properties["total-records"]
		files = snap.Summary.Properties["total-data-files"]
		bytes = snap.Summary.Properties["total-files-size"]
	}
	rck(t, "row count, file count and size available from metadata alone",
		rows == "200" && files == "2" && bytes != "",
		fmt.Sprintf("rows=%s files=%s bytes=%s", rows, files, bytes))

	// 3. History. A reader can see how the dataset evolved -- the
	// provenance a directory tree cannot answer.
	rck(t, "reader can enumerate the table's history",
		len(ro.Metadata().Snapshots()) == 2,
		fmt.Sprintf("%d snapshots", len(ro.Metadata().Snapshots())))

	// 4. And it can actually read the data, still with no catalog.
	scanned, err := ro.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("catalog-free scan: %v", err)
	}
	defer scanned.Release()
	rck(t, "reader can SCAN the data with no catalog", scanned.NumRows() == 200,
		fmt.Sprintf("got %d rows", scanned.NumRows()))

	// 5. Predicate pushdown works for a reader too -- so NDP users get the
	// pruning, not just the writer.
	sub, err := ro.Scan(table.WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("plugin"), "plugin-A"))).ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("filtered scan: %v", err)
	}
	defer sub.Release()
	rck(t, "a read-only consumer gets predicate pushdown", sub.NumRows() < 200 && sub.NumRows() > 0,
		fmt.Sprintf("%d of 200 rows match plugin-A", sub.NumRows()))

	// --- the catch, stated as a check ---------------------------------
	// A catalog-free reader is pinned to the metadata file it was given.
	// It cannot discover that the writer has since committed a newer one;
	// Refresh needs the catalog it does not have. This is the real cost of
	// the NDP shape and it should fail loudly here rather than be a
	// footnote.
	tbl = writeObs(t, ctx, tbl, 200)
	again, err := ro.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("rescan: %v", err)
	}
	defer again.Release()
	rck(t, "CATCH: a URL-pinned reader does NOT see later commits (still 200, not 300)",
		again.NumRows() == 200,
		fmt.Sprintf("reader sees %d rows; writer has since committed more", again.NumRows()))

	// Refresh goes straight to t.cat.LoadTable, so with a nil catalog it
	// panics rather than returning an error. Recover and report it either
	// way: the point is that freshness is unavailable, not how it fails.
	errRefresh := func() (e error) {
		defer func() {
			if r := recover(); r != nil {
				e = fmt.Errorf("panic: %v", r)
			}
		}()
		return ro.Refresh(ctx)
	}()
	rck(t, "CATCH: Refresh without a catalog fails (so freshness needs a pointer)",
		errRefresh != nil, fmt.Sprintf("%v", errRefresh))
}

// TestVersionHintPointer: the resolution to the catch above. Iceberg's
// filesystem catalog keeps the "which metadata file is current" pointer as a
// plain text file NEXT TO the data, so a reader needs no database -- just one
// more GET. This uses the REAL hadoop catalog rather than hand-writing the
// file, because the question is whether iceberg-go *produces* this layout,
// not whether the convention exists on paper.
func TestVersionHintPointer(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()

	cat, err := hadoop.NewCatalog("sage", wh, iceberg.Properties{})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		t.Fatal(err)
	}

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "3"}))
	rck(t, "a catalog with NO database can create the table (filesystem catalog)",
		err == nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.FailNow()
	}

	// Commit something, so the pointer has to move.
	if _, err := writeTwo(ctx, tbl); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Whatever the warehouse layout, find the pointer the client wrote.
	var hintPath string
	filepath.Walk(wh, func(p string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() && strings.HasSuffix(p, "version-hint.text") {
			hintPath = p
		}
		return nil
	})
	rck(t, "iceberg-go WRITES a version-hint.text pointer on storage", hintPath != "",
		hintPath)
	if hintPath == "" {
		t.FailNow()
	}

	raw, err := os.ReadFile(hintPath)
	rck(t, "the pointer is tiny: one extra GET, not a database connection",
		err == nil && len(raw) < 16,
		fmt.Sprintf("version-hint.text = %q (%d bytes)", strings.TrimSpace(string(raw)), len(raw)))

	// The payoff: a reader resolves the CURRENT table from storage alone --
	// read the hint, then read the metadata file it names. No catalog.
	ver := strings.TrimSpace(string(raw))
	metaDir := filepath.Dir(hintPath)
	curMeta := filepath.Join(metaDir, "v"+ver+".metadata.json")
	if _, err := os.Stat(curMeta); err != nil {
		// Some versions name it differently; fall back to a glob so the
		// check tests resolution rather than a filename guess.
		matches, _ := filepath.Glob(filepath.Join(metaDir, "v"+ver+".metadata.json*"))
		if len(matches) > 0 {
			curMeta = matches[0]
		}
	}
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, curMeta)
	}
	ro, err := table.NewFromLocation(ctx, table.Identifier{"ndp", "obs"}, curMeta, fsys, nil)
	rck(t, "a reader resolves the CURRENT table from storage alone (hint -> metadata)",
		err == nil && ro != nil, fmt.Sprintf("resolved %s: %v", filepath.Base(curMeta), err))
	if err != nil {
		return
	}
	got, err := ro.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer got.Release()
	rck(t, "and reads the data the pointer names, with no catalog anywhere",
		got.NumRows() == 2, fmt.Sprintf("%d rows", got.NumRows()))
}

// TestInteropFixtures writes two tables for the Python/DuckDB interop check
// in ndp_interop.py: one v3 + timestamptz_ns (what the archive needs) and
// one v2 + timestamptz (microseconds, the conventional choice). Having both
// is what makes the interop result diagnostic rather than just a failure --
// it separates "DuckDB cannot do v3" from "DuckDB cannot do nanoseconds".
//
// ICEBERGO_FIXTURES=/path writes them there; otherwise this is skipped.
func TestInteropFixtures(t *testing.T) {
	dir := os.Getenv("ICEBERGO_FIXTURES")
	if dir == "" {
		t.Skip("set ICEBERGO_FIXTURES=<dir> to write interop fixtures")
	}
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		version string
		typ     iceberg.Type
		unit    arrow.TimeUnit
	}{
		{"v3_ns", "3", iceberg.TimestampTzNsType{}, arrow.Nanosecond},
		{"v2_us", "2", iceberg.PrimitiveTypes.TimestampTz, arrow.Microsecond},
	} {
		wh := filepath.Join(dir, tc.name)
		if err := os.MkdirAll(wh, 0o755); err != nil {
			t.Fatal(err)
		}
		cat, err := hadoop.NewCatalog("sage", wh, iceberg.Properties{})
		if err != nil {
			t.Fatal(err)
		}
		if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
			t.Fatal(err)
		}
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "time", Type: tc.typ, Required: false},
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		)
		tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
			catalog.WithProperties(iceberg.Properties{"format-version": tc.version}))
		if err != nil {
			t.Fatalf("%s: create: %v", tc.name, err)
		}

		sch := arrow.NewSchema([]arrow.Field{
			{Name: "time", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: "UTC"}, Nullable: true},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil)
		bld := array.NewRecordBuilder(memory.DefaultAllocator, sch)
		base := int64(1735689606915629791)
		if tc.unit == arrow.Microsecond {
			base /= 1000
		}
		for i := 0; i < 10; i++ {
			bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(base + int64(i)))
			bld.Field(1).(*array.Float64Builder).Append(float64(i) + 0.5)
		}
		rec := bld.NewRecord()
		at := array.NewTableFromRecords(sch, []arrow.Record{rec})
		if _, err := tbl.AppendTable(ctx, at, 10, nil); err != nil {
			t.Fatalf("%s: append: %v", tc.name, err)
		}
		at.Release()
		rec.Release()
		bld.Release()
		t.Logf("wrote %s fixture at %s", tc.name, wh)
	}
}

// writeTwo appends two rows; used where the content does not matter, only
// that a commit happened and the pointer had to move.
func writeTwo(ctx context.Context, tbl *table.Table) (*table.Table, error) {
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer bld.Release()
	for i := 0; i < 2; i++ {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1735689606915629791 + int64(i)))
		bld.Field(1).(*array.Float64Builder).Append(float64(i) + 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	at := array.NewTableFromRecords(sch, []arrow.Record{rec})
	defer at.Release()

	return tbl.AppendTable(ctx, at, 2, nil)
}

// writeObs appends 100 rows, alternating between two plugins so a filtered
// read has something to prune.
func writeObs(t *testing.T, ctx context.Context, tbl *table.Table, base int) *table.Table {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "plugin", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer bld.Release()
	for i := 0; i < 100; i++ {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1735689606915629791 + int64(base+i)))
		bld.Field(1).(*array.StringBuilder).Append("env.temperature")
		if (base+i)%2 == 0 {
			bld.Field(2).(*array.StringBuilder).Append("plugin-A")
		} else {
			bld.Field(2).(*array.StringBuilder).Append("plugin-B")
		}
		bld.Field(3).(*array.Float64Builder).Append(float64(base+i) + 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	at := array.NewTableFromRecords(sch, []arrow.Record{rec})
	defer at.Release()

	out, err := tbl.AppendTable(ctx, at, 100, nil)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return out
}
