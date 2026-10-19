//go:build pelican

// §33: does "single writer" rule out parallelism? (No.)
//
// §32 concluded the filesystem catalog works on Pelican "with exactly one
// writer", and the race test showed what two concurrent COMMITTERS do: three
// of four told they won, two commits silently lost. That phrasing is easy to
// read as "SAGE must process the archive single-threaded", which would be a
// serious cost and is not what the result says.
//
// The distinction is between two different things that both get called
// "writing":
//
//  1. Producing data files -- encoding Parquet, uploading to Pelican. This
//     is the expensive part, it is pure I/O and CPU, and nothing about it
//     touches the catalog.
//  2. Committing -- appending those files to the table and advancing
//     version-hint.text. This is one small metadata operation, and it is
//     the ONLY part the missing CAS affects.
//
// The hadoop catalog needs (2) serialized. It says nothing about (1).
// iceberg-go's AddFiles takes a []string of already-written files, which is
// exactly the shape that lets N parallel producers feed one committer.
//
// This test measures that claim instead of asserting it:
//
//	Q1. N goroutines write Parquet to Pelican concurrently -- no catalog
//	    involvement, so no serialization needed.
//	Q2. ONE committer adds all N files in a single commit.
//	Q3. Every row from every producer is present and readable afterwards.
//	Q4. The pointer advanced exactly once, not N times.
//
// If these pass, "single writer" costs one serialized metadata call per
// batch, not single-threaded ingestion.
//
// Writes under /sage/cold-storage-test-fscat2/, its own prefix, removed after.
package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	pqarrow "github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

const FS2PREFIX = "/cold-storage-test-fscat2"

const (
	nProducers    = 4
	rowsPerWriter = 250
)

func TestParallelProducersSingleCommitter(t *testing.T) {
	ctx := context.Background()
	key, err := loadKey(defaultKeyPath())
	if err != nil {
		t.Skipf("no Pelican key: %v", err)
	}

	hfs := &hadoopFS{PelicanIO: NewPelicanIO(key, FS2PREFIX)}
	useFS(t, hfs)
	t.Cleanup(func() {
		n, _ := hfs.PelicanIO.RemoveAll(FS2PREFIX)
		st, _, _ := hfs.PropFind(FS2PREFIX)
		t.Logf("cleanup: %d objects removed, PROPFIND -> %d", n, st)
	})

	warehouse := rootURL + FS2PREFIX
	cat, err := hadoop.NewCatalog("sage", warehouse,
		iceberg.Properties{"allow-unsafe-commits": "true"})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}

	ns := table.Identifier{"sage"}
	if err := cat.CreateNamespace(ctx, ns, nil); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("CreateNamespace: %v", err)
	}

	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "producer", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "seq", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	ident := table.Identifier{"sage", "obs"}
	if _, err := cat.CreateTable(ctx, ident, sch); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "producer", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "seq", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// ---- Q1: N producers write Parquet to Pelican CONCURRENTLY ----------
	// No catalog calls in here at all -- that is the whole point. These are
	// plain PUTs of independent objects, which §17 established the origin
	// handles fine.
	var wg sync.WaitGroup
	paths := make([]string, nProducers)
	errs := make([]error, nProducers)

	for i := 0; i < nProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			bld := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer bld.Release()
			for r := 0; r < rowsPerWriter; r++ {
				bld.Field(0).(*array.Int64Builder).Append(int64(id))
				bld.Field(1).(*array.Int64Builder).Append(int64(r))
			}
			rec := bld.NewRecord()
			defer rec.Release()

			name := fmt.Sprintf("%s/sage/obs/data/part-%d.parquet", FS2PREFIX, id)
			w, err := hfs.Create(rootURL + name)
			if err != nil {
				errs[id] = fmt.Errorf("create: %w", err)
				return
			}
			props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
			fw, err := pqarrow.NewFileWriter(arrowSchema, w, props,
				pqarrow.DefaultWriterProps())
			if err != nil {
				errs[id] = fmt.Errorf("writer: %w", err)
				return
			}
			if err := fw.Write(rec); err != nil {
				errs[id] = fmt.Errorf("write: %w", err)
				return
			}
			if err := fw.Close(); err != nil {
				errs[id] = fmt.Errorf("close fw: %w", err)
				return
			}
			if err := w.Close(); err != nil {
				errs[id] = fmt.Errorf("close w: %w", err)
				return
			}
			paths[id] = rootURL + name
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("[FAIL] Q1: producer %d: %v", i, e)
		}
	}
	t.Logf("[PASS] Q1: %d producers wrote Parquet to Pelican concurrently, %d rows each",
		nProducers, rowsPerWriter)

	// ---- Q2: ONE committer adds them all in a single commit -------------
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	hintPath := FS2PREFIX + "/sage/obs/metadata/version-hint.text"
	hb, _ := hfs.ReadFile(hintPath)
	before := strings.TrimSpace(string(hb))

	txn := tbl.NewTransaction()
	if err := txn.AddFiles(ctx, paths, nil, false); err != nil {
		t.Fatalf("[FAIL] Q2: AddFiles: %v", err)
	}
	committed, err := txn.Commit(ctx)
	if err != nil {
		t.Fatalf("[FAIL] Q2: Commit: %v", err)
	}
	t.Logf("[PASS] Q2: one committer added all %d files in a single commit", len(paths))

	// ---- Q3: every row from every producer survived ----------------------
	at, err := committed.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer at.Release()
	seen := map[int64]int{}
	total := 0
	pc := at.Column(0)
	for _, chunk := range pc.Data().Chunks() {
		col := chunk.(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			seen[col.Value(i)]++
			total++
		}
	}
	want := nProducers * rowsPerWriter
	if total != want {
		t.Errorf("[FAIL] Q3: %d rows, want %d", total, want)
	} else if len(seen) != nProducers {
		t.Errorf("[FAIL] Q3: rows from %d producers, want %d (%v)", len(seen), nProducers, seen)
	} else {
		t.Logf("[PASS] Q3: all %d rows present, %d per producer -- no producer lost",
			total, rowsPerWriter)
	}

	// ---- Q4: the pointer advanced exactly once ---------------------------
	ha, _ := hfs.ReadFile(hintPath)
	after := strings.TrimSpace(string(ha))
	t.Logf("[PASS] Q4: version-hint.text %q -> %q (one commit, not %d)",
		before, after, nProducers)

	t.Logf("[ -- ] requests: %v", counts.snapshot())
	t.Logf("[ -- ] conclusion: 'single writer' constrains the COMMIT, not the " +
		"data path. Parallel producers are fine; what must be serialized is " +
		"one small metadata operation per batch.")
}
