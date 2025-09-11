package main

import (
	"encoding/json"
	"os"

	lineprotocol "github.com/influxdata/line-protocol"
)

type Measurement struct {
	Name      string            `json:"measurement"`
	VSN       string            `json:"vsn"`
	Host      string            `json:"host"`
	Plugin    string            `json:"plugin"`
	Meta      map[string]string `json:"meta"`
	Value     any               `json:"value"`
	Timestamp int64             `json:"ts"`
}

func main() {
	parser := lineprotocol.NewStreamParser(os.Stdin)
	writer := json.NewEncoder(os.Stdout)

	for {
		point, err := parser.Next()
		if err != nil {
			break
		}

		var measurement Measurement

		measurement.Name = point.Name()
		measurement.Timestamp = point.Time().UnixNano()

		measurement.Meta = map[string]string{}

		for _, tag := range point.TagList() {
			switch tag.Key {
			case "vsn":
				measurement.VSN = tag.Value
			case "plugin":
				measurement.Plugin = tag.Value
			case "host":
				measurement.Host = tag.Value
			default:
				measurement.Meta[tag.Key] = tag.Value
			}
		}

		// We're assuming there's always a single value of the form value=
		measurement.Value = point.FieldList()[0].Value

		writer.Encode(&measurement)
	}
}
