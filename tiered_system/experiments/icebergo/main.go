// Does iceberg-go write format-version 3 with nanosecond timestamps, where
// pyiceberg 0.12.0 cannot?
//
// pyiceberg has the ns types but refuses to serialize v3 metadata at all
// (table/metadata.py:588 raises NotImplementedError unconditionally,
// apache/iceberg-python#1551). That is a client gap, not a spec gap. This
// asks whether the Go client -- the one the exporter would actually use --
// has the same gap.
//
// The test that matters is the ROUND TRIP, not table creation. A table can
// be created with a timestamptz_ns column and still truncate on write; that
// is exactly the failure mode we are trying to detect. So every value here
// carries a non-zero nanosecond remainder, and the check is exact equality
// after reading back. 1735689606915629791 is a real value from the archive
// (the one pyarrow names when it refuses the microsecond cast).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	_ "modernc.org/sqlite"
)

// Timestamps with a non-zero ns remainder, so a silent downcast to
// microseconds cannot pass this test. Defaults are illustrative; pass a
// JSON array of int64 as argv[1] to run against real archive values
// (experiments/README.md shows how they are extracted).
var samples = []int64{
	1735689606915629791,
	1735689606915629123,
	1735689607000000001,
	1735689608123456789,
	1735689609999999999,
}

// loadSamples replaces the defaults with real archive timestamps when a
// file is given. Every loaded value is asserted to carry a remainder --
// a file of microsecond-aligned values would make the round-trip check
// vacuous, which is the one way this test could pass while meaning nothing.
func loadSamples(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var vals []int64
	if err := json.Unmarshal(raw, &vals); err != nil {
		return err
	}
	if len(vals) == 0 {
		return fmt.Errorf("%s holds no timestamps", path)
	}
	for _, v := range vals {
		if v%1000 == 0 {
			return fmt.Errorf("%d is microsecond-aligned; it cannot detect truncation", v)
		}
	}
	samples = vals
	return nil
}

var fails int

func check(name string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		fails++
	}
	fmt.Printf("[%s] %s\n", status, name)
	if detail != "" {
		fmt.Printf("       %s\n", detail)
	}
}

func main() {
	ctx := context.Background()

	source := "built-in samples"
	if len(os.Args) > 1 {
		if err := loadSamples(os.Args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "cannot load %s: %v\n", os.Args[1], err)
			os.Exit(2)
		}
		source = fmt.Sprintf("%d real archive timestamps from %s", len(samples), os.Args[1])
	}

	wh, err := os.MkdirTemp("", "icebergo-v3-*")
	if err != nil {
		panic(err)
	}
	// ICEBERGO_KEEP=1 leaves the warehouse behind so the metadata JSON can
	// be inspected directly -- the in-memory Version() could in principle
	// disagree with what was serialized.
	if os.Getenv("ICEBERGO_KEEP") == "" {
		defer os.RemoveAll(wh)
	}

	fmt.Printf("iceberg-go v3 + timestamptz_ns probe\ninput:     %s\nwarehouse: %s\n\n", source, wh)

	db, err := sql.Open("sqlite", filepath.Join(wh, "catalog.db"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cat, err := sqlcat.NewCatalog("probe", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + wh,
	})
	if err != nil {
		panic(err)
	}
	if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		panic(err)
	}

	// timestamptz_ns is a v3-only type: minimum_format_version() == 3.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	// 1. Does v2 reject it? If this SUCCEEDS the whole premise is wrong --
	// it would mean the type is not actually v3-gated here.
	_, errV2 := cat.CreateTable(ctx, table.Identifier{"sage", "obs_v2"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	check("v2 REJECTS timestamptz_ns (type really is v3-gated)",
		errV2 != nil, fmt.Sprintf("%v", errV2))

	// 2. The question pyiceberg answers with NotImplementedError.
	tbl, err := cat.CreateTable(ctx, table.Identifier{"sage", "obs"}, sc,
		catalog.WithProperties(iceberg.Properties{"format-version": "3"}))
	check("v3 table CREATED with a timestamptz_ns column", err == nil,
		fmt.Sprintf("%v", err))
	if err != nil {
		fmt.Printf("\n%d check(s) failed\n", fails)
		os.Exit(1)
	}
	check("metadata reports format-version 3", tbl.Metadata().Version() == 3,
		fmt.Sprintf("version = %d", tbl.Metadata().Version()))

	// 3. Append. Creating a table only proves the metadata serializes;
	// writing is where a truncating client would lose the remainder.
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

	tbl2, err := tbl.AppendTable(ctx, atbl, int64(len(samples)), nil)
	check("APPEND of nanosecond rows succeeded", err == nil, fmt.Sprintf("%v", err))
	if err != nil {
		fmt.Printf("\n%d check(s) failed\n", fails)
		os.Exit(1)
	}

	// 4. Read back and compare exactly. This is the check that catches a
	// silent truncation -- a client that downcast to microseconds would
	// have passed everything above.
	scan, err := tbl2.Scan().ToArrowTable(ctx)
	if err != nil {
		panic(err)
	}
	defer scan.Release()

	check("row count round-trips", scan.NumRows() == int64(len(samples)),
		fmt.Sprintf("got %d, want %d", scan.NumRows(), len(samples)))

	got := make([]int64, 0, len(samples))
	col := scan.Column(0).Data()
	for _, c := range col.Chunks() {
		ts := c.(*array.Timestamp)
		for i := 0; i < ts.Len(); i++ {
			got = append(got, int64(ts.Value(i)))
		}
	}

	exact := len(got) == len(samples)
	for i := range got {
		if i < len(samples) && got[i] != samples[i] {
			exact = false
		}
	}
	detail := fmt.Sprintf("want[0]=%d got[0]=%d", samples[0], got[0])
	if !exact {
		detail += "  <-- PRECISION LOST"
	}
	check("nanosecond values survive EXACTLY (no truncation)", exact, detail)

	// A microsecond downcast would zero the last three digits. Name that
	// failure mode explicitly so a future reader sees it was tested for.
	trunc := true
	for i := range got {
		if got[i] != (samples[i]/1000)*1000 {
			trunc = false
		}
	}
	check("values were NOT silently rounded to microseconds", !trunc, "")

	// 5. What actually landed on disk.
	fmt.Printf("\nformat-version: %d\n", tbl2.Metadata().Version())
	fmt.Printf("column type:    %s\n", tbl2.Schema().Field(0).Type)
	show := got
	if len(show) > 5 {
		show = show[:5]
	}
	fmt.Printf("round-tripped:  %v (first %d of %d)\n", show, len(show), len(got))

	fmt.Printf("\n%d check(s) failed\n", fails)
	if fails > 0 {
		os.Exit(1)
	}
}
