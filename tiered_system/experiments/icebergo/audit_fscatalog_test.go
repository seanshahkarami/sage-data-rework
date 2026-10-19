//go:build pelican

package main

// Independent audit: real data, fresh reader, exact row identities, and a
// missing/corrupt version hint. Every request uses a unique, scoped prefix.
// Run only this test: go test -tags pelican -run '^TestAuditFilesystemCatalog$' -v .
import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

func TestAuditFilesystemCatalog(t *testing.T) {
	ctx := context.Background()
	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Fatal(err)
	}
	pre := fmt.Sprintf("/cold-storage-audit-%d", time.Now().UnixNano())
	hfs := &hadoopFS{PelicanIO: NewPelicanIO(key, pre)}
	hfs.client.Timeout = 30 * time.Second
	useFS(t, hfs)
	t.Logf("isolated storage: %s%s", rootURL, pre)
	// Check before creating anything, and never clean an existing namespace.
	st, _, err := hfs.PropFind(pre)
	if err != nil || st != 404 {
		t.Fatalf("fresh prefix: status=%d err=%v", st, err)
	}
	t.Cleanup(func() {
		n, err := hfs.PelicanIO.RemoveAll(pre)
		st, _, checkErr := hfs.PropFind(pre)
		if err != nil || checkErr != nil || st != 404 {
			t.Errorf("cleanup %s: removed=%d status=%d errors=%v/%v", pre, n, st, err, checkErr)
		} else {
			t.Logf("cleanup verified: removed=%d, PROPFIND=404", n)
		}
	})
	warehouse := rootURL + pre
	if _, err := hadoop.NewCatalog("audit", warehouse, nil); err == nil {
		t.Fatal("expected remote unsafe-commit guard")
	}
	newCatalog := func() *hadoop.Catalog {
		cat, err := hadoop.NewCatalog("audit", warehouse, iceberg.Properties{"allow-unsafe-commits": "true"})
		if err != nil {
			t.Fatal(err)
		}
		return cat
	}
	cat := newCatalog()
	if err := cat.CreateNamespace(ctx, table.Identifier{"audit"}, nil); err != nil {
		t.Fatal(err)
	}
	ident := table.Identifier{"audit", "observations"}
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "producer", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "seq", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	if _, err := cat.CreateTable(ctx, ident, schema); err != nil {
		t.Fatal(err)
	}
	hint := pre + "/audit/observations/metadata/version-hint.text"
	checkHint := func(want string) {
		b, err := hfs.ReadFile(hint)
		if err != nil || strings.TrimSpace(string(b)) != want {
			t.Fatalf("hint: got %q err=%v want=%q", b, err, want)
		}
	}
	checkHint("1")
	const producers, rows = 4, 250
	paths, errs := make([]string, producers), make([]error, producers)
	var wg sync.WaitGroup
	for producer := range producers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sc := arrow.NewSchema([]arrow.Field{
				{Name: "producer", Type: arrow.PrimitiveTypes.Int64},
				{Name: "seq", Type: arrow.PrimitiveTypes.Int64}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
			defer b.Release()
			for seq := range rows {
				b.Field(0).(*array.Int64Builder).Append(int64(id))
				b.Field(1).(*array.Int64Builder).Append(int64(seq))
			}
			rec := b.NewRecordBatch()
			defer rec.Release()
			paths[id] = fmt.Sprintf("%s%s/data/producer-%d.parquet", rootURL, pre, id)
			w, err := hfs.Create(paths[id])
			if err != nil {
				errs[id] = err
				return
			}
			fw, err := pqarrow.NewFileWriter(sc, w, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
			if err != nil {
				errs[id] = err
				return
			}
			if err := fw.Write(rec); err != nil {
				errs[id] = err
				return
			}
			if err := fw.Close(); err != nil {
				errs[id] = err
				return
			}
			errs[id] = w.Close()
		}(producer)
	}
	wg.Wait()
	for id, err := range errs {
		if err != nil {
			t.Fatalf("producer %d: %v", id, err)
		}
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	txn := tbl.NewTransaction()
	if err := txn.AddFiles(ctx, paths, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	checkHint("2")
	checkRows := func(label string) {
		// Recreate catalog and reload metadata instead of reading writer state.
		loaded, err := newCatalog().LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		at, err := loaded.Scan().ToArrowTable(ctx)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		defer at.Release()
		r := array.NewTableReader(at, 100)
		defer r.Release()
		seen := map[[2]int64]int{}
		for r.Next() {
			rec := r.RecordBatch()
			p, s := rec.Column(0).(*array.Int64), rec.Column(1).(*array.Int64)
			for i := 0; i < p.Len(); i++ {
				seen[[2]int64{p.Value(i), s.Value(i)}]++
			}
		}
		if err := r.Err(); err != nil {
			t.Fatal(err)
		}
		if len(seen) != producers*rows {
			t.Fatalf("%s: %d unique rows", label, len(seen))
		}
		for p := range producers {
			for seq := range rows {
				if seen[[2]int64{int64(p), int64(seq)}] != 1 {
					t.Fatalf("%s: missing/duplicate %d/%d", label, p, seq)
				}
			}
		}
		t.Logf("PASS %s: all 1000 exact row identities, independent catalog", label)
	}
	checkRows("parallel producers / one committer")
	for _, content := range []string{"1", "garbage"} {
		if err := hfs.WriteFile(hint, []byte(content)); err != nil {
			t.Fatal(err)
		}
		checkRows("hint=" + content)
	}
	if err := hfs.Remove(hint); err != nil {
		t.Fatal(err)
	}
	checkRows("hint missing")
	// Commit after a missing hint, then verify its exact new version.
	tbl, err = newCatalog().LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}
	txn = tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{"audit": "third-commit"}); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	checkHint("3")
	checkRows("commit after missing hint")
	// Test the origin primitive independently, without claiming this is an
	// end-to-end concurrent Iceberg transaction test.
	src, dst := pre+"/move-source", pre+"/move-destination"
	if err := hfs.WriteFile(src, []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := hfs.WriteFile(dst, []byte("old")); err != nil {
		t.Fatal(err)
	}
	resp, err := hfs.doMove(src, dst, false) // Overwrite: F
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	body, err := hfs.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("origin MOVE Overwrite:F: HTTP=%d destination=%q", resp.StatusCode, body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && string(body) == "new" {
		t.Log("CONFIRMED: origin ignores no-replace condition; concurrent commits remain unsafe")
	} else {
		t.Errorf("origin behavior changed: review concurrency conclusion")
	}
}
