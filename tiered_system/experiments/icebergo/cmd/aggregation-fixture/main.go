// Command aggregation-fixture registers prewritten tiny local Parquet files in
// an isolated Iceberg v3 SQL catalog. It performs no network IO or cleanup.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/iceberg-go"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type input struct {
	Root          string `json:"root"`
	ValueEncoding string `json:"value_encoding"`
	Tables        []struct {
		Name  string   `json:"name"`
		Files []string `json:"files"`
	} `json:"tables"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("supply the generated fixture-input.json")
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		return err
	}
	var in input
	if err = json.Unmarshal(raw, &in); err != nil {
		return err
	}
	if in.ValueEncoding != "" && in.ValueEncoding != "variant-uint64-decimal-v1" {
		return fmt.Errorf("unknown experimental value encoding")
	}
	root, err := filepath.Abs(in.Root)
	if err != nil {
		return err
	}
	for _, part := range strings.Split(filepath.ToSlash(root), "/") {
		if part == "node-data" {
			return fmt.Errorf("/node-data forbidden")
		}
	}
	// Root must be new. Existing Parquet inputs live in the parent preparation.
	if err = os.Mkdir(root, 0700); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", filepath.Join(root, "catalog.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	warehouse := "file://" + filepath.ToSlash(root)
	cat, err := sqlcat.NewCatalog("exp88", db, sqlcat.SQLite, iceberg.Properties{"warehouse": warehouse})
	if err != nil {
		return err
	}
	if err = cat.CreateNamespace(ctx, table.Identifier{"sage"}, nil); err != nil {
		return err
	}
	fields := []iceberg.NestedField{{ID: 1, Name: "time", Type: iceberg.TimestampTzNsType{}}, {ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String}}
	for i, name := range []string{"plugin", "vsn", "node", "host", "task", "job", "zone"} {
		fields = append(fields, iceberg.NestedField{ID: i + 3, Name: name, Type: iceberg.PrimitiveTypes.String})
	}
	fields = append(fields, iceberg.NestedField{ID: 10, Name: "meta", Type: &iceberg.MapType{KeyID: 12, KeyType: iceberg.PrimitiveTypes.String, ValueID: 13, ValueType: iceberg.PrimitiveTypes.String}}, iceberg.NestedField{ID: 11, Name: "value", Type: iceberg.VariantType{}})
	schema := iceberg.NewSchema(0, fields...)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "day_time", Transform: iceberg.DayTransform{}})
	unsorted := table.UnsortedSortOrder
	output := map[string]string{}
	for _, item := range in.Tables {
		if item.Name != "full" && item.Name != "cold" {
			return fmt.Errorf("unexpected table name")
		}
		ident := table.Identifier{"sage", item.Name}
		location := warehouse + "/sage/" + item.Name
		_, _, err = cat.CommitTable(ctx, ident, []table.Requirement{table.AssertCreate()}, []table.Update{
			table.NewUpgradeFormatVersionUpdate(3), table.NewAssignUUIDUpdate(uuid.New()),
			table.NewAddSchemaUpdate(schema), table.NewSetCurrentSchemaUpdate(-1),
			table.NewAddPartitionSpecUpdate(&spec, true), table.NewSetDefaultSpecUpdate(-1),
			table.NewAddSortOrderUpdate(&unsorted), table.NewSetDefaultSortOrderUpdate(-1),
			table.NewSetLocationUpdate(location), table.NewSetPropertiesUpdate(iceberg.Properties{"sage.layout": "plugin-vsn-time", "sage.layout.version": "bounded-plugin-v1", "sage.batch.order": "time", "sage.row-group.rows": "262144", "write.metadata.delete-after-commit.enabled": "false"}),
		})
		if err != nil {
			return err
		}
		if in.ValueEncoding != "" {
			_, _, err = cat.CommitTable(ctx, ident, nil, []table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{"sage.value.encoding": in.ValueEncoding}),
			})
			if err != nil {
				return err
			}
		}
		t, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return err
		}
		tx := t.NewTransaction()
		for _, file := range item.Files {
			path, err := filepath.EvalSymlinks(file)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(filepath.Dir(root), path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				return fmt.Errorf("input outside private preparation")
			}
			if _, err = os.Stat(path); err != nil {
				return err
			}
			if err = tx.AddFiles(ctx, []string{"file://" + filepath.ToSlash(path)}, nil, false, table.WithAddFilesConcurrency(1)); err != nil {
				return err
			}
		}
		t, err = tx.Commit(ctx)
		if err != nil {
			return err
		}
		if t.Metadata().Version() != 3 || !t.Schema().Equals(schema) {
			return fmt.Errorf("fixture schema/version differs")
		}
		output[item.Name] = t.MetadataLocation()
	}
	encoded, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(root, "tables.json")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(encoded, '\n'))
	closed := f.Close()
	if err != nil {
		return err
	}
	if closed != nil {
		return closed
	}
	fmt.Println(path)
	return nil
}
