// layoutpack preserves native Parquet VARIANT while placing bounded plugin-pure
// row groups in a preselected, sorted file batch. No JSON conversion.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

const fileRows int64 = 10_000_000

func localPath(location string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "" && u.Scheme != "file") || (u.Host != "" && u.Host != "localhost") {
		return "", fmt.Errorf("expected isolated local file")
	}
	p, err := filepath.Abs(u.Path)
	if err != nil {
		return "", err
	}
	for _, part := range strings.Split(p, string(filepath.Separator)) {
		if part == "node-data" {
			return "", fmt.Errorf("/node-data is not a layout staging location")
		}
	}
	return p, nil
}

type key struct {
	plugin, vsn         string
	pluginNull, vsnNull bool
	stamp               int64
}

func cmpString(a string, an bool, b string, bn bool) int {
	if an && bn {
		return 0
	}
	if an != bn {
		if an {
			return 1
		}
		return -1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func samePlugin(a, b key) bool { return cmpString(a.plugin, a.pluginNull, b.plugin, b.pluginNull) == 0 }
func less(a, b key) bool {
	if c := cmpString(a.plugin, a.pluginNull, b.plugin, b.pluginNull); c != 0 {
		return c < 0
	}
	if c := cmpString(a.vsn, a.vsnNull, b.vsn, b.vsnNull); c != 0 {
		return c < 0
	}
	return a.stamp < b.stamp
}

func pack(ctx context.Context, input, output string, rowGroupRows int64) (err error) {
	if rowGroupRows < 100_000 || rowGroupRows > 1_000_000 {
		return fmt.Errorf("row-group-rows must be between 100000 and 1000000")
	}
	input, err = localPath(input)
	if err != nil {
		return err
	}
	output, err = localPath(output)
	if err != nil {
		return err
	}
	reader, err := file.OpenParquetFile(input, false)
	if err != nil {
		return err
	}
	defer reader.Close()
	if reader.NumRows() < 1 || reader.NumRows() > fileRows {
		return fmt.Errorf("input must contain 1..%d rows", fileRows)
	}
	ar, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 65536}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	rr, err := ar.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	defer rr.Release()
	schema := rr.Schema()
	values := schema.FieldIndices("value")
	if len(values) != 1 {
		return fmt.Errorf("missing value VARIANT")
	}
	value := schema.Field(values[0])
	id, ok := value.Metadata.GetValue("PARQUET:field_id")
	ext, isExt := value.Type.(arrow.ExtensionType)
	if !ok || id != "11" || !isExt || ext.ExtensionName() != "arrow.parquet.variant" {
		return fmt.Errorf("expected value VARIANT with field ID 11")
	}
	p, v, t := schema.FieldIndices("plugin"), schema.FieldIndices("vsn"), schema.FieldIndices("time")
	if len(p) != 1 || len(v) != 1 || len(t) != 1 {
		return fmt.Errorf("missing layout keys")
	}
	if err = os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer func() {
		out.Close()
		if err != nil {
			os.Remove(output)
		}
	}()
	sorting := make([]parquet.SortingColumn, 0, 3)
	for _, name := range []string{"plugin", "vsn", "time"} {
		index := reader.MetaData().Schema.ColumnIndexByName(name)
		if index < 0 {
			return fmt.Errorf("missing Parquet sort column %s", name)
		}
		sorting = append(sorting, parquet.SortingColumn{ColumnIdx: int32(index), Descending: false, NullsFirst: false})
	}
	fw, err := pqarrow.NewFileWriter(schema, out, parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(rowGroupRows), parquet.WithCompression(compress.Codecs.Zstd), parquet.WithCompressionLevel(3), parquet.WithSortingColumns(sorting)), pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer func() {
		if fw != nil {
			fw.Close()
		}
	}()
	var previous key
	seen := false
	for rr.Next() {
		rec := rr.RecordBatch()
		plugins, pok := rec.Column(p[0]).(*array.String)
		vsns, vok := rec.Column(v[0]).(*array.String)
		stamps, tok := rec.Column(t[0]).(*array.Timestamp)
		if !pok || !vok || !tok {
			return fmt.Errorf("unexpected layout key types")
		}
		ts := stamps.DataType().(*arrow.TimestampType)
		if ts.Unit != arrow.Nanosecond || ts.TimeZone != "UTC" {
			return fmt.Errorf("time must retain UTC nanoseconds")
		}
		start := int64(0)
		for i := 0; i < int(rec.NumRows()); i++ {
			if stamps.IsNull(i) {
				return fmt.Errorf("null time")
			}
			current := key{plugin: plugins.Value(i), pluginNull: plugins.IsNull(i), vsn: vsns.Value(i), vsnNull: vsns.IsNull(i), stamp: int64(stamps.Value(i))}
			if seen && less(current, previous) {
				return fmt.Errorf("input is not sorted by plugin/vsn/time")
			}
			if !seen || !samePlugin(current, previous) {
				if int64(i) > start {
					slice := rec.NewSlice(start, int64(i))
					err = fw.WriteBuffered(slice)
					slice.Release()
					if err != nil {
						return err
					}
				}
				if err = fw.NewBufferedRowGroupChecked(); err != nil {
					return err
				}
				start = int64(i)
			}
			previous = current
			seen = true
		}
		if start < rec.NumRows() {
			slice := rec.NewSlice(start, rec.NumRows())
			err = fw.WriteBuffered(slice)
			slice.Release()
			if err != nil {
				return err
			}
		}
	}
	if err = rr.Err(); err != nil {
		return err
	}
	err = fw.Close()
	fw = nil
	return err
}

func commit(ctx context.Context, endpoint, namespace, expected, list string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		return fmt.Errorf("use an isolated localhost REST catalog")
	}
	raw, err := os.ReadFile(list)
	if err != nil {
		return err
	}
	var paths []string
	if err = json.Unmarshal(raw, &paths); err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("empty file list")
	}
	cat, err := rest.NewCatalog(ctx, "layout", endpoint, rest.WithWarehouseLocation("warehouse"))
	if err != nil {
		return err
	}
	tbl, err := cat.LoadTable(ctx, table.Identifier{namespace, "obs"})
	if err != nil {
		return err
	}
	if tbl.MetadataLocation() != expected {
		return fmt.Errorf("catalog head changed before AddFiles")
	}
	if tbl.Metadata().Version() != 3 {
		return fmt.Errorf("expected Iceberg v3")
	}
	root, err := localPath(tbl.Location())
	if err != nil {
		return err
	}
	for _, path := range paths {
		local, err := localPath(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, local)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("data file is outside isolated table root")
		}
	}
	txn := tbl.NewTransaction()
	if err = txn.AddFiles(ctx, paths, nil, false, table.WithAddFilesConcurrency(1)); err != nil {
		return err
	}
	_, err = txn.Commit(ctx)
	return err
}

func main() {
	mode := flag.String("mode", "pack", "pack or commit")
	input := flag.String("input", "", "sorted native Parquet batch")
	output := flag.String("output", "", "new Parquet file")
	rowGroupRows := flag.Int64("row-group-rows", 262144, "maximum rows per plugin-pure row group (100000..1000000)")
	endpoint := flag.String("endpoint", "", "local REST endpoint")
	namespace := flag.String("namespace", "sage", "namespace")
	head := flag.String("head", "", "expected authoritative head")
	files := flag.String("files", "", "JSON file list")
	flag.Parse()
	ctx := context.Background()
	var err error
	switch *mode {
	case "pack":
		err = pack(ctx, *input, *output, *rowGroupRows)
	case "commit":
		err = commit(ctx, *endpoint, *namespace, *head, *files)
	default:
		err = fmt.Errorf("unknown mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
