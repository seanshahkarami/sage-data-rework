package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	lineprotocol "github.com/influxdata/line-protocol"
	parquet "github.com/parquet-go/parquet-go"
)

type Observation struct {
	Time int64  `parquet:"time,timestamp(nanosecond),delta"`
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

func sortBatch(batch []Observation) {
	slices.SortFunc(batch, func(a, b Observation) int {
		return cmp.Or(
			cmp.Compare(a.Plugin, b.Plugin),
			cmp.Compare(a.VSN, b.VSN),
			cmp.Compare(a.Time, b.Time),
		)
	})
}

func writeBatch(date string, batch []Observation, batchNum int) error {
	sortBatch(batch)
	filename := fmt.Sprintf("work/date=%s/data_%d.parquet", date, batchNum)
	tempname := filename + ".tmp"
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return fmt.Errorf("failed to create parquet data directory: %w", err)
	}
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

func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir(), nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err // some other problem, e.g. permission denied
}

func exportDate(date string) error {
	taskStartTime := time.Now()

	log.Printf("starting export for %s...", date)

	workDir := fmt.Sprintf("work/date=%s", date)
	if err := os.RemoveAll(workDir); err != nil {
		return fmt.Errorf("failed to remove existing work dir %s", workDir)
	}

	ok, err := dirExists(fmt.Sprintf("archive/date=%s", date))
	if ok {
		log.Printf("export for %s already exists. skipping!", date)
		return nil
	}
	if err != nil {
		return fmt.Errorf("archive directory check failed: %w", err)
	}

	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return fmt.Errorf("failed to parse export date: %w", err)
	}

	startTime := parsedDate.Format("2006-01-02") + "T00:00:00Z"
	endTime := parsedDate.AddDate(0, 0, 1).Format("2006-01-02") + "T00:00:00Z"

	log.Printf("exporting lp data for %s - %s", startTime, endTime)

	// NOTE Based on my observation, exports shouldn't take longer than 15-30 minutes. We just put an hour upper bound to be safe.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
		"influxd",
		"inspect",
		"export-lp",
		"--bucket-id", "b3a4e89ad74c5acc",
		"--engine-path", "/media/local/pvc-6da578ef-e9bc-47fc-9f64-cfe30a24ff5e_shared_influxdb-data-beehive-influxdb-0/engine",
		"--start", startTime,
		"--end", endTime,
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
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("influxd export-lp failed: %w", err)
		}

		var obs Observation

		obs.Time = point.Time().UnixNano()
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
			if err := writeBatch(date, batch, batchNum); err != nil {
				return fmt.Errorf("failed to flush batch %d: %w", batchNum, err)
			}
			log.Printf("done flushing batch %d...", batchNum)
			batch = batch[:0]
			batchNum++
		}

		batch = append(batch, obs)
	}

	// flush final batch
	log.Printf("flushing final batch %d...", batchNum)
	if err := writeBatch(date, batch, batchNum); err != nil {
		return fmt.Errorf("failed to flush final batch %d: %w", batchNum, err)
	}
	log.Printf("done flushing final batch %d...", batchNum)

	// atomic replace directory
	if err := os.MkdirAll("archive", 0o755); err != nil {
		return fmt.Errorf("failed to create archive dir: %w", err)
	}
	if err := os.Rename(fmt.Sprintf("work/date=%s/", date), fmt.Sprintf("archive/date=%s/", date)); err != nil {
		return fmt.Errorf("failed to move work dir to archive: %w", err)
	}

	taskDuration := time.Since(taskStartTime)
	log.Printf("finished export for %s in %s", date, taskDuration)
	return nil
}

func main() {
	dates := os.Args[1:]
	for _, date := range dates {
		if err := exportDate(date); err != nil {
			log.Fatalf("error during export %s: %s", date, err)
		}
	}
}
