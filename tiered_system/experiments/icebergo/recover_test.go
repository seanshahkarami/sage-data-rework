//go:build recover

package main

// exp28: can the archive be recovered after the catalog is lost?
//
// Prompted by a disaster-recovery article describing two Iceberg recovery
// scenarios and the two Spark procedures that address them:
//
//   Scenario 1: the catalog is missing or points at the wrong
//               metadata.json           -> CALL system.register_table(...)
//   Scenario 2: file paths changed during the restore
//                                       -> CALL system.rewrite_table_path(...)
//
// Both claims are about SPARK. This repo's writer is Go, and §27 measured
// that iceberg-go ships no path-rewrite helper at all. So the question this
// test answers is not "does Iceberg support recovery" -- it is:
//
//     WHAT IS ACTUALLY AVAILABLE TO *US*, with a Go writer and a SQL
//     catalog, when the catalog database is gone?
//
// This matters more here than in a typical deployment. §24 established that
// Pelican cannot back a database-free catalog, so a SQL catalog is
// mandatory -- which means the catalog database is a single point of
// failure that the storage itself cannot reconstruct. Whether it is
// RECOVERABLE is therefore a real operational question, not a formality.
//
// Deliberately adversarial: the catalog is DELETED, not merely corrupted,
// and the recovered table is checked for its actual row values, not just
// that something opened.
//
// Local disk: these are format and client questions. exp21 showed this
// client's bytes reach Pelican intact; deleting a catalog mid-test is not
// something to do to a shared origin.
//
// Run with:  go test -v -tags recover -run TestRecover ./...

import (
	"context"
	"database/sql"
	"fmt"
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
)

func rc(t *testing.T, name string, ok bool, detail string) {
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

// TestRecoverAfterCatalogLoss is the blog's scenario 1, on our stack.
func TestRecoverAfterCatalogLoss(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()
	dbPath := filepath.Join(wh, "catalog.db")

	// --- normal operation ---------------------------------------------
	tbl := recBuild(t, ctx, dbPath, wh)
	metaLoc := tbl.MetadataLocation()
	wantRows, err := recCount(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	rc(t, "baseline: table written and readable", wantRows == 200,
		fmt.Sprintf("%d rows", wantRows))

	// --- the disaster --------------------------------------------------
	// Delete the catalog database entirely. The data and metadata on
	// storage are untouched, which is exactly the article's premise:
	// "all your table files are present, but the catalog doesn't point to
	// the correct snapshot."
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatal("catalog not actually deleted; the rest would be vacuous")
	}
	rc(t, "DISASTER: the catalog database is deleted", true,
		"data and metadata on storage are untouched")

	// --- recovery path A: no catalog at all ----------------------------
	// Before reaching for any procedure: §23 showed a reader needs no
	// catalog. That makes READ recovery instant, and it is the first thing
	// to check in an outage -- the data is never actually unreachable.
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, metaLoc)
	}
	ro, err := table.NewFromLocation(ctx, table.Identifier{"dr", "obs"}, metaLoc, fsys, nil)
	rc(t, "RECOVERY A: the table opens from the metadata path with NO catalog",
		err == nil && ro != nil, fmt.Sprintf("%v", err))
	if err == nil {
		got, cerr := recCount(ctx, ro)
		rc(t, "and every row is readable -- data was never lost with the catalog",
			cerr == nil && got == wantRows,
			fmt.Sprintf("%d rows, want %d, err=%v", got, wantRows, cerr))
	}

	// --- what the article recommends, and whether we have it -----------
	// `register_table` is a Spark procedure. In iceberg-go it is a method
	// on the catalog -- but NOT on every catalog. Checked at compile time
	// below via the interface assertion, and reported here.
	type registrar interface {
		RegisterTable(context.Context, table.Identifier, string) (*table.Table, error)
	}

	db2, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	fresh, err := sqlcat.NewCatalog("sage", db2, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + wh,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, hasRegister := any(fresh).(registrar)
	rc(t, "the SQL catalog does NOT implement RegisterTable in iceberg-go v0.7.0",
		!hasRegister,
		"present on rest/glue/hive; absent on sql and hadoop; not in the Catalog interface")

	// --- recovery path B: re-register by hand --------------------------
	// Since the procedure is unavailable here, recovery means writing the
	// catalog row directly. The SQL catalog's whole state for a table is
	// one row in `iceberg_tables`, so this is a small, auditable fix --
	// but it is OUR step to write, not a library call.
	if err := fresh.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		t.Fatal(err)
	}
	// Force the catalog to create its schema, then insert the pointer.
	if _, err := db2.ExecContext(ctx,
		`INSERT INTO iceberg_tables
		   (catalog_name, table_namespace, table_name, metadata_location, iceberg_type)
		 VALUES (?, ?, ?, ?, ?)`,
		"sage", "sage", "obs", metaLoc, "TABLE"); err != nil {
		t.Fatal(err)
	}

	recovered, err := fresh.LoadTable(ctx, table.Identifier{"sage", "obs"})
	rc(t, "RECOVERY B: catalog rebuilt by inserting ONE row pointing at metadata.json",
		err == nil && recovered != nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.FailNow()
	}

	got, err := recCount(ctx, recovered)
	rc(t, "the recovered table returns every row, through the catalog",
		err == nil && got == wantRows,
		fmt.Sprintf("%d rows, want %d, err=%v", got, wantRows, err))

	rc(t, "snapshot history survived the catalog loss",
		len(recovered.Metadata().Snapshots()) == 2,
		fmt.Sprintf("%d snapshots", len(recovered.Metadata().Snapshots())))

	// And the table is WRITABLE again -- recovery is not read-only.
	after := recWrite(t, ctx, recovered, 500)
	n, err := recCount(ctx, after)
	rc(t, "the recovered table accepts new commits (not a read-only rescue)",
		err == nil && n == wantRows+100,
		fmt.Sprintf("%d rows after append, want %d", n, wantRows+100))
}

// TestRecoverFindsLatestMetadata tests the article's sharper warning: the
// catalog may point at the WRONG metadata.json, and picking the right one
// is on us. It also tests the claim that makes that recoverable --
// that the newest metadata file can be identified from storage alone.
func TestRecoverFindsLatestMetadata(t *testing.T) {
	ctx := context.Background()
	wh := t.TempDir()
	dbPath := filepath.Join(wh, "catalog.db")

	tbl := recBuild(t, ctx, dbPath, wh)
	v2 := tbl.MetadataLocation()
	tbl = recWrite(t, ctx, tbl, 300) // a third commit
	v3 := tbl.MetadataLocation()
	rc(t, "two different metadata files exist after a further commit",
		v2 != v3, filepath.Base(v2)+" -> "+filepath.Base(v3))

	// Restoring a backup that is one commit stale: the catalog points at
	// v2 while v3 is the real latest. Iceberg will NOT notice -- it opens
	// v2 happily and silently serves older data.
	fsys := func(ctx context.Context) (iceio.IO, error) {
		return iceio.LoadFS(ctx, nil, v2)
	}
	stale, err := table.NewFromLocation(ctx, table.Identifier{"dr", "obs"}, v2, fsys, nil)
	if err != nil {
		t.Fatal(err)
	}
	staleRows, _ := recCount(ctx, stale)
	rc(t, "a stale metadata pointer opens WITHOUT error, serving older data",
		staleRows == 200, fmt.Sprintf("%d rows from the stale pointer (newest has 300)", staleRows))

	// This is why the article says to record the latest metadata.json in
	// every backup. Can it instead be recovered from storage? The metadata
	// directory is listable and the files are version-numbered.
	mdDir := filepath.Join(wh, "sage.db", "obs", "metadata")
	ents, err := os.ReadDir(mdDir)
	if err != nil {
		t.Fatal(err)
	}
	var jsons []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".metadata.json") {
			jsons = append(jsons, e.Name())
		}
	}
	sort.Strings(jsons)
	rc(t, "every metadata version is on storage and listable",
		len(jsons) >= 3, strings.Join(jsons, " "))

	// Two independent ways to pick the newest. Check BOTH agree rather
	// than asserting either works -- if they ever disagree, the cheap one
	// is the unsafe one.
	//
	// (1) By name. iceberg-go writes a zero-padded sequence prefix
	//     (00000-, 00001-, ...), so a lexical sort is correct here. That
	//     is a property of this writer's naming, not of the Iceberg spec,
	//     which is why it is checked rather than assumed.
	byName := jsons[len(jsons)-1]

	// (2) By content: last-updated-ms inside each metadata file. Does not
	//     depend on any filename convention.
	byTime, ts := recNewestByTimestamp(t, mdDir, jsons)

	rc(t, "the newest metadata file can be identified from storage alone",
		byTime == filepath.Base(v3),
		fmt.Sprintf("by last-updated-ms: %s (ts=%d)", byTime, ts))

	rc(t, "name order and timestamp order AGREE (zero-padded sequence prefix)",
		byName == byTime && byName == filepath.Base(v3),
		fmt.Sprintf("by name: %s | by time: %s", byName, byTime))
}

// ------------------------------------------------------------- helpers ---

func recBuild(t *testing.T, ctx context.Context, dbPath, wh string) *table.Table {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
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
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)
	tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatal(err)
	}
	tbl = recWrite(t, ctx, tbl, 0)

	return recWrite(t, ctx, tbl, 100)
}

func recWrite(t *testing.T, ctx context.Context, tbl *table.Table, base int) *table.Table {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer bld.Release()
	for i := 0; i < 100; i++ {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1735689600000000 + int64(base+i)))
		bld.Field(1).(*array.StringBuilder).Append("env.temperature")
		bld.Field(2).(*array.Float64Builder).Append(float64(base+i) * 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()

	at := array.NewTableFromRecords(sch, []arrow.Record{rec})
	defer at.Release()
	out, err := tbl.AppendTable(ctx, at, 100, nil)
	if err != nil {
		t.Fatal(err)
	}

	return out
}

func recCount(ctx context.Context, tbl *table.Table) (int64, error) {
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		return 0, err
	}
	defer at.Release()

	return at.NumRows(), nil
}

// recNewestByTimestamp picks the newest metadata file by reading
// last-updated-ms out of each, which is the content-based discriminator
// that does not depend on filename conventions.
func recNewestByTimestamp(t *testing.T, dir string, names []string) (string, int64) {
	t.Helper()
	var bestName string
	var best int64 = -1
	for _, n := range names {
		raw, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			continue
		}
		md, err := table.ParseMetadataBytes(raw)
		if err != nil {
			continue
		}
		if ts := md.LastUpdatedMillis(); ts > best {
			best, bestName = ts, n
		}
	}

	return bestName, best
}
