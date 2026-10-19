//go:build evolve

package main

// exp22: two claims that came up when weighing Iceberg, neither previously
// measured.
//
//  1. "With a custom catalog the schema has to be right from the start; with
//     Iceberg we can ship a beta schema and change it later." Is that true of
//     iceberg-go v0.7.0, and -- the part that actually matters -- do files
//     written under the OLD schema still read correctly afterwards? A schema
//     change that silently invalidates existing data would be worse than no
//     schema change at all.
//
//  2. "Does Iceberg help with DOIs / data organization?" The citable part of
//     a DOI is a fixed, retrievable version of a dataset. Iceberg's answer is
//     a TAG: a named, immutable pointer to one snapshot. So: can a tag be
//     created, does it survive later writes, and does reading it return the
//     data as of the tag rather than the data as of now?
//
// Local disk only -- these are format questions, not storage questions, and
// exp21 already settled that this client's writes work on Pelican. No network.
//
// Run with:  go test -v -tags evolve -run TestSchemaEvolution ./...
//            go test -v -tags evolve -run TestSnapshotTag ./...

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	_ "modernc.org/sqlite"
)

func ck(t *testing.T, name string, ok bool, detail string) {
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

// newCat builds a throwaway local warehouse and a v3 table shaped like a
// first-cut ("beta") version of the archive schema: time + value only.
func newCat(t *testing.T) (context.Context, *sqlcat.Catalog, *table.Table, table.Identifier) {
	t.Helper()
	ctx := context.Background()

	wh := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(wh, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	cat, err := sqlcat.NewCatalog("evolve", db, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + wh,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		t.Fatal(err)
	}

	beta := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	ident := table.Identifier{"sage", "obs"}
	tbl, err := cat.CreateTable(ctx, ident, beta,
		catalog.WithProperties(iceberg.Properties{"format-version": "3"}))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, cat, tbl, ident
}

// appendBeta writes rows under the two-column schema.
func appendBeta(t *testing.T, ctx context.Context, tbl *table.Table, times []int64) *table.Table {
	t.Helper()
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer bld.Release()
	for i, ns := range times {
		bld.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ns))
		bld.Field(1).(*array.Float64Builder).Append(float64(i) + 0.5)
	}
	rec := bld.NewRecord()
	defer rec.Release()
	at := array.NewTableFromRecords(sch, []arrow.Record{rec})
	defer at.Release()

	out, err := tbl.AppendTable(ctx, at, int64(len(times)), nil)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return out
}

// TestSchemaEvolution: can a shipped schema be changed, and do the rows
// written before the change still read back correctly?
func TestSchemaEvolution(t *testing.T) {
	ctx, cat, tbl, ident := newCat(t)

	// Two rows under the beta schema, with real-looking ns remainders.
	old := []int64{1735689606915629791, 1735689608123456789}
	tbl = appendBeta(t, ctx, tbl, old)
	ck(t, "beta schema (time, value) written", tbl.CurrentSnapshot() != nil, "")

	// The change a "we got the schema wrong" moment would need: add the
	// columns that were forgotten, rename one, widen another's type.
	txn := tbl.NewTransaction()
	us := txn.UpdateSchema(true, false)
	us.AddColumn([]string{"name"}, iceberg.PrimitiveTypes.String, "measurement name", false, nil)
	us.AddColumn([]string{"plugin"}, iceberg.PrimitiveTypes.String, "plugin that produced it", false, nil)
	us.RenameColumn([]string{"value"}, "reading")
	errEvolve := us.Commit()
	ck(t, "ADD two columns + RENAME one, on a published table", errEvolve == nil,
		fmt.Sprintf("%v", errEvolve))
	if errEvolve != nil {
		t.FailNow()
	}
	tbl2, err := txn.Commit(ctx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Type widening (float -> double is the classic allowed promotion;
	// int -> long likewise). Narrowing must be refused -- that is the
	// guardrail that makes evolution safe rather than merely possible.
	txn2 := tbl2.NewTransaction()
	us2 := txn2.UpdateSchema(true, false)
	us2.UpdateColumn([]string{"reading"}, table.ColumnUpdate{
		FieldType: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Float32, Valid: true},
	})
	errNarrow := us2.Commit()
	ck(t, "NARROWING double -> float is REFUSED (evolution is checked, not blind)",
		errNarrow != nil, fmt.Sprintf("%v", errNarrow))

	// The check that matters: rows written under the OLD schema, read back
	// through the NEW one. A client that got this wrong would drop them or
	// error; the spec says the added columns read as null and the renamed
	// column keeps its data, because resolution is by field ID, not name.
	scan, err := tbl2.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("scan after evolution: %v", err)
	}
	defer scan.Release()

	ck(t, "rows written under the OLD schema are still readable",
		scan.NumRows() == int64(len(old)),
		fmt.Sprintf("got %d rows, want %d", scan.NumRows(), len(old)))

	names := []string{}
	for _, f := range scan.Schema().Fields() {
		names = append(names, f.Name)
	}
	ck(t, "new columns appear in the scanned schema", len(names) == 4,
		fmt.Sprintf("columns: %v", names))

	// The renamed column must still carry its ORIGINAL values. If Iceberg
	// resolved by name rather than field ID, this is where old data would
	// come back null.
	idx := -1
	for i, n := range names {
		if n == "reading" {
			idx = i
		}
	}
	renamedOK := false
	if idx >= 0 {
		var vals []float64
		for _, c := range scan.Column(idx).Data().Chunks() {
			a := c.(*array.Float64)
			for i := 0; i < a.Len(); i++ {
				if a.IsValid(i) {
					vals = append(vals, a.Value(i))
				}
			}
		}
		renamedOK = len(vals) == len(old) && vals[0] == 0.5
		ck(t, "RENAMED column still returns pre-rename data (resolution is by field ID)",
			renamedOK, fmt.Sprintf("values=%v", vals))
	} else {
		ck(t, "RENAMED column still returns pre-rename data (resolution is by field ID)",
			false, "column 'reading' not found")
	}

	// Timestamps must be untouched by all of this.
	var ts []int64
	for _, c := range scan.Column(0).Data().Chunks() {
		a := c.(*array.Timestamp)
		for i := 0; i < a.Len(); i++ {
			ts = append(ts, int64(a.Value(i)))
		}
	}
	exact := len(ts) == len(old)
	for i := range ts {
		if i < len(old) && ts[i] != old[i] {
			exact = false
		}
	}
	ck(t, "nanosecond values unchanged across the schema change", exact,
		fmt.Sprintf("%v", ts))

	// Schema history is retained, so "which schema was this file written
	// under?" stays answerable -- the provenance half of the DOI question.
	reloaded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	ck(t, "both schema versions retained in metadata",
		len(reloaded.Metadata().Schemas()) >= 2,
		fmt.Sprintf("%d schemas, current id=%d",
			len(reloaded.Metadata().Schemas()), reloaded.Schema().ID))
}

// TestSnapshotTag: the DOI-shaped question. Can one version be named,
// does the name survive later writes, and does reading it return the data
// as of then rather than as of now?
func TestSnapshotTag(t *testing.T) {
	ctx, cat, tbl, ident := newCat(t)

	v1 := []int64{1735689606915629791, 1735689608123456789}
	tbl = appendBeta(t, ctx, tbl, v1)
	tagged := tbl.CurrentSnapshot().SnapshotID

	// Name this snapshot -- the thing a DOI would resolve to.
	// v0.7.0 has no CreateTag helper; the update goes through the catalog.
	_, _, err := cat.CommitTable(ctx, ident, nil, []table.Update{
		table.NewSetSnapshotRefUpdate("doi-v1", tagged, table.TagRef, -1, -1, -1),
	})
	ck(t, "TAG created naming one snapshot ('doi-v1')", err == nil, fmt.Sprintf("%v", err))
	if err != nil {
		t.FailNow()
	}

	// Keep publishing. A citation is only worth anything if later writes
	// cannot move it.
	tbl2, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	tbl2 = appendBeta(t, ctx, tbl2, []int64{1735689609999999999, 1735689610111111111, 1735689611222222222})
	ck(t, "three more rows appended after tagging",
		tbl2.CurrentSnapshot().SnapshotID != tagged, "")

	reloaded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}

	snap := reloaded.SnapshotByName("doi-v1")
	ck(t, "tag survives later writes and still resolves", snap != nil &&
		snap.SnapshotID == tagged, fmt.Sprintf("tag -> %v, original %d", snap, tagged))

	// Current state: all 5 rows.
	now, err := reloaded.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer now.Release()
	ck(t, "HEAD shows every row written so far", now.NumRows() == 5,
		fmt.Sprintf("got %d, want 5", now.NumRows()))

	// The tagged version: must still be the 2 rows as of the tag.
	asOf, err := reloaded.Scan(table.WithSnapshotID(tagged)).ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("scan at tag: %v", err)
	}
	defer asOf.Release()
	ck(t, "reading the TAG returns the data as of then, not as of now",
		asOf.NumRows() == int64(len(v1)),
		fmt.Sprintf("got %d, want %d", asOf.NumRows(), len(v1)))

	// And it is the same rows, not merely the same count.
	var got []int64
	for _, c := range asOf.Column(0).Data().Chunks() {
		a := c.(*array.Timestamp)
		for i := 0; i < a.Len(); i++ {
			got = append(got, int64(a.Value(i)))
		}
	}
	same := len(got) == len(v1)
	for i := range got {
		if i < len(v1) && got[i] != v1[i] {
			same = false
		}
	}
	ck(t, "the tagged rows are byte-identical to what was published", same,
		fmt.Sprintf("%v", got))

	// A tag is metadata, not a copy: the citation costs no duplicated data.
	ck(t, "tag is a pointer, not a copy (no extra data files)",
		len(reloaded.Metadata().Snapshots()) == 2,
		fmt.Sprintf("%d snapshots, %d refs",
			len(reloaded.Metadata().Snapshots()), countRefs(reloaded)))

	// The question that decides whether a tag is safe to mint a DOI against:
	// does routine snapshot expiry -- the maintenance job every Iceberg table
	// eventually runs to stop metadata growing without bound -- delete the
	// tagged version out from under the citation? Expire as aggressively as
	// the API allows: keep one snapshot, nothing older than zero duration.
	//
	// A control is needed first. With only a tagged snapshot and HEAD, expiry
	// has nothing it is permitted to drop, so "the tag survived" would pass
	// even if tags were not protected at all. Add a third snapshot that is
	// neither tagged nor current: if THAT one is still present afterwards,
	// expiry did nothing and the result below means nothing.
	reloaded = appendBeta(t, ctx, reloaded, []int64{1735689612333333333})
	doomed := reloaded.CurrentSnapshot().SnapshotID
	reloaded = appendBeta(t, ctx, reloaded, []int64{1735689613444444444})
	ck(t, "control: an untagged, non-HEAD snapshot exists to be expired",
		len(reloaded.Metadata().Snapshots()) == 4 && doomed != reloaded.CurrentSnapshot().SnapshotID,
		fmt.Sprintf("%d snapshots, doomed=%d, head=%d",
			len(reloaded.Metadata().Snapshots()), doomed, reloaded.CurrentSnapshot().SnapshotID))

	txn := reloaded.NewTransaction()
	if err := txn.ExpireSnapshots(table.WithRetainLast(1), table.WithOlderThan(0)); err != nil {
		t.Fatalf("expire: %v", err)
	}
	expired, err := txn.Commit(ctx)
	if err != nil {
		t.Fatalf("expire commit: %v", err)
	}

	// First: did expiry actually do anything? Without this the next check
	// is vacuous.
	stillDoomed := expired.Metadata().SnapshotByID(doomed) != nil
	ck(t, "control: the untagged snapshot WAS expired (so expiry really ran)",
		!stillDoomed, fmt.Sprintf("%d snapshots left of 4", len(expired.Metadata().Snapshots())))

	survived := expired.SnapshotByName("doi-v1")
	ck(t, "tagged snapshot SURVIVES aggressive expiry (retainLast=1, olderThan=0)",
		survived != nil && survived.SnapshotID == tagged,
		fmt.Sprintf("%d snapshots left; tag resolves=%v", len(expired.Metadata().Snapshots()),
			survived != nil))

	// Surviving in metadata is not enough -- the data files it references
	// must still be readable, or the citation resolves to an error.
	if survived != nil {
		stillThere, err := expired.Scan(table.WithSnapshotID(tagged)).ToArrowTable(ctx)
		if err != nil {
			ck(t, "the tagged version is still READABLE after expiry", false, fmt.Sprintf("%v", err))
		} else {
			defer stillThere.Release()
			ck(t, "the tagged version is still READABLE after expiry",
				stillThere.NumRows() == int64(len(v1)),
				fmt.Sprintf("got %d rows, want %d", stillThere.NumRows(), len(v1)))
		}
	}

	if os.Getenv("ICEBERGO_KEEP") != "" {
		t.Logf("metadata: %s", reloaded.MetadataLocation())
	}
}

func countRefs(t *table.Table) int {
	n := 0
	for range t.Metadata().Refs() {
		n++
	}
	return n
}
