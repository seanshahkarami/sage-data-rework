package main

import (
	"log"
	"os"

	lineprotocol "github.com/influxdata/line-protocol"
	"github.com/parquet-go/parquet-go"
)

// wxt.env.temp,host=000048b02d35a87e.ws-nxcore,missing=-9999.9,node=000048b02d35a87e,plugin=registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14,sensor=vaisala-wxt536,task=waggle-wxt536,units=degree\ Celsius,vsn=W08E,zone=core value=2.2 1735689600028420265
// wxt.env.temp,host=000048b02d35a87e.ws-nxcore,missing=-9999.9,node=000048b02d35a87e,plugin=registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14,sensor=vaisala-wxt536,task=waggle-wxt536,units=degree\ Celsius,vsn=W08E,zone=core value=2.2 1735689600108255428
// wxt.env.temp,host=000048b02d35a87e.ws-nxcore,missing=-9999.9,node=000048b02d35a87e,plugin=registry.sagecontinuum.org/jrobrien/waggle-wxt536:0.24.11.14,sensor=vaisala-wxt536,task=waggle-wxt536,units=degree\ Celsius,vsn=W08E,zone=core value=2.2 1735689600188331907

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

func main() {
	parser := lineprotocol.NewStreamParser(os.Stdin)

	f, _ := os.Create("out.parquet")
	w := parquet.NewGenericWriter[Measurement](f,
		parquet.Compression(&parquet.Zstd),
	)

	batch := make([]Measurement, 0, 1_000_000)
	var measurement Measurement

	for {
		point, err := parser.Next()
		if err != nil {
			break
		}

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
			batch = batch[:0]
			log.Printf("wrote batch")
		}
	}

	w.Write(batch)
	w.Close()
}
