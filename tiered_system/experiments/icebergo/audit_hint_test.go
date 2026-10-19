package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
)

// This is a local catalog protocol check, not a Pelican test.
func TestAuditVersionHintIsNotCommitPoint(t *testing.T) {
	ctx, warehouse := context.Background(), t.TempDir()
	newCatalog := func() *hadoop.Catalog {
		c, err := hadoop.NewCatalog("audit", warehouse, nil)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := newCatalog()
	if err := c.CreateNamespace(ctx, table.Identifier{"audit"}, nil); err != nil {
		t.Fatal(err)
	}
	ident := table.Identifier{"audit", "obs"}
	tbl, err := c.CreateTable(ctx, ident, iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int64}))
	if err != nil {
		t.Fatal(err)
	}
	txn := tbl.NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{"audit.version": "two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	hint := filepath.Join(warehouse, "audit", "obs", "metadata", "version-hint.text")
	for _, state := range []string{"1", "garbage", "missing"} {
		if state == "missing" {
			err = os.Remove(hint)
		} else {
			err = os.WriteFile(hint, []byte(state), 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := newCatalog().LoadTable(ctx, ident)
		if err != nil {
			t.Fatalf("hint=%s: %v", state, err)
		}
		if loaded.Properties()["audit.version"] != "two" {
			t.Fatalf("hint=%s loaded stale metadata", state)
		}
		t.Logf("hint=%s: fresh catalog still loads v2", state)
	}
}
