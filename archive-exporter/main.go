package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	lineprotocol "github.com/influxdata/line-protocol"
	parquet "github.com/parquet-go/parquet-go"
)

type Observation struct {
	Time int64  `parquet:"time,timestamp(nanosecond)"`
	Name string `parquet:"name,dict"`

	// promoted tags; optional so a missing tag is NULL rather than ""
	// (safe: Influx tags can't have empty values)
	Plugin string `parquet:"plugin,optional,dict"`
	VSN    string `parquet:"vsn,optional,dict"`
	Node   string `parquet:"node,optional,dict"`
	Host   string `parquet:"host,optional,dict"`
	Task   string `parquet:"task,optional,dict"`
	Job    string `parquet:"job,optional,dict"`
	Zone   string `parquet:"zone,optional,dict"`

	// catch-all for every other tag
	Meta map[string]string `parquet:"meta,optional"`

	// exactly one is non-nil
	ValueFloat  *float64 `parquet:"value_float,optional"`
	ValueInt    *int64   `parquet:"value_int,optional"`
	ValueString *string  `parquet:"value_str,optional"`
}

func writeBatch(batch []Observation, batchNum int) error {
	filename := fmt.Sprintf("archive/part-%d.parquet", batchNum)
	tempname := filename + ".tmp"
	f, _ := os.Create(tempname)
	w := parquet.NewGenericWriter[Observation](f,
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := w.Write(batch); err != nil {
		return fmt.Errorf("failed to write parquet data: %w", err)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	os.Rename(tempname, filename)
	return nil
}

func main() {
	log.Printf("starting export...")

	cmd := exec.Command(
		"influxd",
		"inspect",
		"export-lp",
		"--bucket-id", "b3a4e89ad74c5acc",
		"--engine-path", "/media/local/pvc-6da578ef-e9bc-47fc-9f64-cfe30a24ff5e_shared_influxdb-data-beehive-influxdb-0/engine",
		"--start", "2025-01-01T00:00:00Z",
		"--end", "2025-01-02T00:00:00Z",
		"--output-path", "-",
	)

	r, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("failed to create pipe to influxd export-lp: %s", err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to start influxd export-lp: %s", err)
	}

	parser := lineprotocol.NewStreamParser(r)

	batch := make([]Observation, 0, 10_000_000)
	batchNum := 0

	for {
		point, err := parser.Next()
		if err != nil {
			break
		}

		var obs Observation

		obs.Time = point.Time().UnixMicro()
		obs.Name = point.Name()
		obs.Meta = map[string]string{}

		// set tags
		for _, tag := range point.TagList() {
			switch tag.Key {
			case "host":
				obs.Host = tag.Value
			case "node":
				obs.Node = tag.Value
			case "vsn":
				obs.VSN = tag.Value
			case "zone":
				obs.Zone = tag.Value
			case "task":
				obs.Task = tag.Value
			case "plugin":
				obs.Plugin = tag.Value
			case "job":
				obs.Job = tag.Value
			// collect remaining tags under generic meta map
			default:
				obs.Meta[tag.Key] = tag.Value
			}
		}

		if obs.Plugin == "" {
			// skip system data for now
			continue
		}

		value := point.FieldList()[0].Value

		switch v := value.(type) {
		case float64:
			obs.ValueFloat = &v
		case int64:
			obs.ValueInt = &v
		case string:
			obs.ValueString = &v
		default:
			continue
		}

		if len(batch) >= 10_000_000 {
			log.Printf("flushing batch %d...", batchNum)
			writeBatch(batch, batchNum)
			log.Printf("done flushing batch %d...", batchNum)
			batch = batch[:0]
			batchNum++
		}

		batch = append(batch, obs)
	}

	// flush final batch
	log.Printf("flushing final batch %d...", batchNum)
	writeBatch(batch, batchNum)
	log.Printf("done flushing final batch %d...", batchNum)

	log.Printf("finished export!")
}
