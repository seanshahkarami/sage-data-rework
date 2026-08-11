package main

import (
	"log"
	"os"
	"os/exec"
	"strings"

	lineprotocol "github.com/influxdata/line-protocol"
	"github.com/parquet-go/parquet-go"
)

type Measurement struct {
	Time        int64  `parquet:"time,timestamp(microsecond),delta"`
	Measurement string `parquet:"measurement,dict,zstd"` // wxt.env.temp
	// tags
	Host    string `parquet:"host,dict,zstd"`    // 000048b02d35a87e.ws-nxcore
	Node    string `parquet:"node,dict,zstd"`    // 000048b02d35a87e
	VSN     string `parquet:"vsn,dict,zstd"`     // W08E
	Zone    string `parquet:"zone,dict,zstd"`    // core
	Task    string `parquet:"task,dict,zstd"`    // waggle-wxt536
	Plugin  string `parquet:"plugin,dict,zstd"`  // registry.sagecontinuum.org/...
	Sensor  string `parquet:"sensor,dict,zstd"`  // vaisala-wxt536
	Units   string `parquet:"units,dict,zstd"`   // degree Celsius
	Missing string `parquet:"missing,dict,zstd"` // -9999.9
	Job     string `parquet:"job,dict,zstd"`
	// field
	Value float64 `parquet:"value,split,zstd"`
}

var queryMeasurements = []string{
	"wxt.env.humidity",
	"wxt.env.pressure",
	"wxt.env.temp",
	// "wxt.hail.accumulation",
	// "wxt.heater.status",
	// "wxt.heater.temp",
	// "wxt.heater.volt",
	// "wxt.rain.accumulation",
	// "wxt.voltage.supply",
	// "wxt.wind.direction",
	// "wxt.wind.speed",
}

func main() {
	log.Printf("starting...")

	cmd := exec.Command(
		"influxd",
		"inspect",
		"export-lp",
		"--bucket-id", "b3a4e89ad74c5acc",
		"--engine-path", "/media/local/pvc-6da578ef-e9bc-47fc-9f64-cfe30a24ff5e_shared_influxdb-data-beehive-influxdb-0/engine",
		"--start", "2025-01-01T00:00:00Z",
		"--end", "2025-01-02T00:00:00Z",
		"--measurement", strings.Join(queryMeasurements, ","),
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

	f, _ := os.Create("wxt_2025-01-01.parquet.tmp")
	w := parquet.NewGenericWriter[Measurement](f,
		parquet.Compression(&parquet.Zstd),
	)

	batch := make([]Measurement, 0, 1_000_000)

	for {
		point, err := parser.Next()
		if err != nil {
			break
		}

		var measurement Measurement

		measurement.Time = point.Time().UnixMicro()
		measurement.Measurement = point.Name()

		// set tags
		for _, tag := range point.TagList() {
			switch tag.Key {
			case "host":
				measurement.Host = tag.Value
			case "node":
				measurement.Node = tag.Value
			case "vsn":
				measurement.VSN = tag.Value
			case "zone":
				measurement.Zone = tag.Value
			case "task":
				measurement.Task = tag.Value
			case "plugin":
				measurement.Plugin = tag.Value
			case "sensor":
				measurement.Sensor = tag.Value
			case "units":
				measurement.Units = tag.Value
			case "missing":
				measurement.Missing = tag.Value
			case "job":
				measurement.Job = tag.Value
			default:
				log.Fatalf("unknown tag %s", tag.Key)
			}
		}

		measurement.Value = point.FieldList()[0].Value.(float64)

		batch = append(batch, measurement)

		if len(batch) == cap(batch) {
			if _, err := w.Write(batch); err != nil {
				log.Fatalf("error: %s", err)
			}
			if err := w.Flush(); err != nil {
				log.Fatalf("flush: %s", err)
			}
			batch = batch[:0]
			log.Printf("wrote batch")
		}
	}

	if _, err := w.Write(batch); err != nil {
		log.Fatalf("write final batch: %s", err)
	}

	if err := cmd.Wait(); err != nil {
		log.Fatalf("influxd export-lp failed: %s", err)
	}

	if err := w.Close(); err != nil {
		log.Fatalf("close parquet writer: %s", err)
	}

	if err := f.Close(); err != nil {
		log.Fatalf("close file: %s", err)
	}

	if err := os.Rename("wxt_2025-01-01.parquet.tmp", "wxt_2025-01-01.parquet"); err != nil {
		log.Fatalf("rename file: %s", err)
	}

	log.Printf("finishing export")
}
