//go:build pelican

package main

// exp21: does the Go v3 + nanosecond path work on real Pelican/NRDStor?
//
// exp20 proved iceberg-go writes format-version 3 with timestamptz_ns
// bit-exact, but on local disk. exp19 proved Iceberg runs on Pelican, but
// through pyiceberg, which cannot write v3 at all. This closes the one
// remaining gap: v3 + nanoseconds + the real origin, together.
//
// Run with:  go test -v -tags pelican -run TestPelican ./...
// The build tag keeps the network out of the default `go test`.
//
// Safety, per the standing constraints on this work:
//   - everything is written under PREFIX, a directory created for this
//     test alone, and deleted afterwards;
//   - /sage/node-data and the sage-storage-loader paths are never touched;
//   - tokens are scoped to PREFIX and live 15 minutes;
//   - no token or key material is written to disk.

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// A directory of its own, distinct from exp17's /cold-storage-test and
// exp19's /cold-storage-test-iceberg, so a failure here cannot disturb
// either of those and cleanup can be verified independently.
const PREFIX = "/cold-storage-test-icebergo"

func pass(t *testing.T, name string, ok bool, detail string) {
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

func TestPelicanV3Nanoseconds(t *testing.T) {
	ctx := context.Background()

	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Fatalf("issuer key: %v", err)
	}
	pio := NewPelicanIO(key, PREFIX)

	// The extension point: iceberg-go has no https IO, so supply one.
	iceio.Register("https", func(_ context.Context, _ *url.URL, _ map[string]string) (iceio.IO, error) {
		return pio, nil
	})

	// Fresh prefix. If a previous run died mid-way, start clean.
	_ = pio.Remove(PREFIX + "/warehouse")

	// 0. Does Pelican accept a Go-minted SciToken at all? Everything below
	// depends on it, and a signature-encoding bug would otherwise surface
	// as a confusing Iceberg error several layers up.
	probe := PREFIX + "/warehouse/_tokencheck"
	errTok := pio.WriteFile(probe, []byte("go-minted token ok"))
	pass(t, "Pelican accepts a Go-minted SciToken (ES256, raw R||S)",
		errTok == nil, fmt.Sprintf("%v", errTok))
	if errTok != nil {
		t.Fatalf("cannot continue without a valid token")
	}
	got, err := pio.ReadFile(probe)
	pass(t, "read-after-write on Pelican", err == nil && string(got) == "go-minted token ok",
		fmt.Sprintf("%q", string(got)))
	_ = pio.Remove(probe)

	// The catalog pointer stays in SQLite, locally. §19 measured why it
	// cannot live on Pelican: Content-Range on PUT is silently ignored and
	// PATCH returns 501, so a mutable row has nowhere to live there.
	tmp, err := os.MkdirTemp("", "icebergo-pelican-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmp)

	db, err := sql.Open("sqlite", filepath.Join(tmp, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	warehouse := rootURL + PREFIX + "/warehouse"
	cat, err := sqlcat.NewCatalog("icebergo", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": warehouse,
	})
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

	before := counts.total()
	t0 := time.Now()
	tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "3"}),
		catalog.WithLocation(warehouse+"/sage/obs"))
	createDur := time.Since(t0)
	pass(t, "v3 table with timestamptz_ns CREATED on Pelican", err == nil,
		fmt.Sprintf("%v", err))
	if err != nil {
		t.Fatalf("cannot continue")
	}
	pass(t, "metadata reports format-version 3", tbl.Metadata().Version() == 3,
		fmt.Sprintf("version=%d, %d requests, %s", tbl.Metadata().Version(),
			counts.total()-before, createDur.Round(time.Millisecond)))

	// Append real archive timestamps, every one with a non-zero remainder.
	if err := loadSamples(samplePath()); err != nil {
		t.Fatalf("samples: %v", err)
	}
	t.Logf("appending %d real archive timestamps", len(samples))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()
	tb := bld.Field(0).(*array.TimestampBuilder)
	vb := bld.Field(1).(*array.Float64Builder)
	for i, ns := range samples {
		tb.Append(arrow.Timestamp(ns))
		vb.Append(float64(i) + 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	atbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer atbl.Release()

	beforeAppend := counts.total()
	t1 := time.Now()
	tbl2, err := tbl.AppendTable(ctx, atbl, int64(len(samples)), nil)
	appendDur := time.Since(t1)
	pass(t, "APPEND of nanosecond rows to Pelican", err == nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.Fatalf("cannot continue")
	}
	t.Logf("       append: %d requests, %s", counts.total()-beforeAppend,
		appendDur.Round(time.Millisecond))

	// Read back THROUGH Pelican and compare exactly. This is the check that
	// catches a silent downcast; everything above would pass without it.
	beforeScan := counts.total()
	t2 := time.Now()
	scan, err := tbl2.Scan().ToArrowTable(ctx)
	scanDur := time.Since(t2)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer scan.Release()

	pass(t, "row count round-trips over Pelican",
		scan.NumRows() == int64(len(samples)),
		fmt.Sprintf("got %d want %d, %d requests, %s", scan.NumRows(), len(samples),
			counts.total()-beforeScan, scanDur.Round(time.Millisecond)))

	var roundTripped []int64
	for _, c := range scan.Column(0).Data().Chunks() {
		ts := c.(*array.Timestamp)
		for i := 0; i < ts.Len(); i++ {
			roundTripped = append(roundTripped, int64(ts.Value(i)))
		}
	}
	exact := len(roundTripped) == len(samples)
	for i := range roundTripped {
		if i < len(samples) && roundTripped[i] != samples[i] {
			exact = false
		}
	}
	detail := ""
	if len(roundTripped) > 0 {
		detail = fmt.Sprintf("want[0]=%d got[0]=%d", samples[0], roundTripped[0])
	}
	pass(t, "nanosecond values survive EXACTLY over Pelican", exact, detail)

	truncated := len(roundTripped) > 0
	for i := range roundTripped {
		if roundTripped[i] != (samples[i]/1000)*1000 {
			truncated = false
		}
	}
	pass(t, "values NOT silently rounded to microseconds", !truncated, "")

	// What actually landed on the origin: read the metadata JSON straight
	// back over HTTPS rather than trusting the in-memory view.
	loc := tbl2.Metadata().Location()
	pass(t, "table location is on Pelican", strings.HasPrefix(loc, "https://"), loc)

	raw, err := pio.ReadFile(tbl2.MetadataLocation())
	onDiskV3 := err == nil && containsAny(string(raw), `"format-version" : 3`, `"format-version":3`)
	pass(t, "on-origin metadata JSON says format-version 3", onDiskV3,
		firstMatch(string(raw), `"format-version"`))
	pass(t, "on-origin metadata JSON declares timestamptz_ns",
		err == nil && strings.Contains(string(raw), "timestamptz_ns"),
		firstMatch(string(raw), "timestamptz_ns"))

	// Pelican rejects DELETE on a non-empty collection. Recorded as a check
	// rather than silently handled in cleanup: anyone porting catalog
	// maintenance (snapshot expiry, orphan removal) will hit it, and the
	// flat Remove that looks correct leaves the prefix behind.
	errFlat := pio.Remove(PREFIX + "/warehouse")
	pass(t, "DELETE on a non-empty collection is REFUSED (409, needs recursion)",
		errFlat != nil, fmt.Sprintf("%v", errFlat))

	t.Logf("\nformat-version: %d", tbl2.Metadata().Version())
	t.Logf("column type:    %s", tbl2.Schema().Field(0).Type)
	t.Logf("metadata:       %s", tbl2.MetadataLocation())
	t.Logf("requests:       %v (total %d)", counts.snapshot(), counts.total())

	// Cleanup: remove everything this test wrote, then prove it is gone.
	// Only PREFIX is touched -- never /node-data, never the loader paths.
	t.Cleanup(func() {
		if os.Getenv("ICEBERGO_KEEP") != "" {
			t.Logf("ICEBERGO_KEEP set; leaving %s in place", PREFIX)
			return
		}
		// RemoveAll, not Remove: Pelican returns 409 on DELETE of a
		// non-empty collection, so a flat delete leaves the prefix behind.
		n, err := pio.RemoveAll(PREFIX)
		if err != nil {
			t.Errorf("CLEANUP FAILED for %s after %d objects: %v", PREFIX, n, err)
			return
		}
		st, _, _ := pio.PropFind(PREFIX)
		pass(t, "cleanup: test prefix removed and verified gone",
			st == 404, fmt.Sprintf("%d objects removed, PROPFIND now HTTP %d", n, st))
	})
}

func samplePath() string {
	if p := os.Getenv("ICEBERGO_SAMPLES"); p != "" {
		return p
	}
	return "testdata/real_ns.json"
}

// containsAny tolerates either JSON spacing style the writer may emit.
func containsAny(h string, alts ...string) bool {
	for _, a := range alts {
		if strings.Contains(h, a) {
			return true
		}
	}
	return false
}

// firstMatch returns a short window around the first occurrence, so a
// failure shows what the origin actually returned instead of nothing.
func firstMatch(h, n string) string {
	i := strings.Index(h, n)
	if i < 0 {
		return "(not found)"
	}
	end := i + 60
	if end > len(h) {
		end = len(h)
	}
	return h[i:end]
}
