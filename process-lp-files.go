package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Measurement represents a single data point
type Measurement struct {
	VSN         string
	Host        string
	Plugin      *string
	Measurement string
	Value       interface{}
	Meta        map[string]string
	Timestamp   time.Time
}

// UnmarshalJSON implements custom JSON unmarshaling for Measurement
func (m *Measurement) UnmarshalJSON(data []byte) error {
	// Create a temporary struct with the same fields but with int64 timestamp
	type tempMeasurement struct {
		VSN         string            `json:"vsn"`
		Host        string            `json:"host"`
		Plugin      *string           `json:"plugin"`
		Measurement string            `json:"measurement"`
		Value       interface{}       `json:"value"`
		Meta        map[string]string `json:"meta"`
		Timestamp   int64             `json:"ts"`
	}

	var temp tempMeasurement
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// Copy all fields
	m.VSN = temp.VSN
	m.Host = temp.Host
	m.Plugin = temp.Plugin
	m.Measurement = temp.Measurement
	m.Value = temp.Value
	m.Meta = temp.Meta

	// Convert timestamp from nanoseconds to time.Time
	m.Timestamp = time.Unix(0, temp.Timestamp).UTC()

	return nil
}

// GroupKey represents the grouping key for measurements
type GroupKey struct {
	Plugin      string
	Measurement string
	Date        time.Time
}

// ParquetRecord represents a record for Parquet writing
type ParquetRecord struct {
	VSN       string            `parquet:"vsn,optional"`
	Host      string            `parquet:"host,optional"`
	Timestamp int64             `parquet:"timestamp,delta"`
	Value     string            `parquet:"value,optional"`
	Meta      map[string]string `parquet:"meta,optional"`
}

func groupByPluginAndMeasurement(point Measurement) GroupKey {
	plugin := "system"
	if point.Plugin != nil {
		plugin = *point.Plugin
	}

	return GroupKey{
		Plugin:      plugin,
		Measurement: point.Measurement,
		Date:        point.Timestamp,
	}
}

func writeTableToParquetFile(records []ParquetRecord, path string) error {
	// Create temporary file
	tempPath := path + ".tmp"

	// Create file
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create Parquet writer
	writer := parquet.NewWriter(file, parquet.Compression(&parquet.Gzip))
	defer writer.Close()

	// Write records
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	// Close writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Rename temp file to final file
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

func sortRecords(records []Measurement) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].VSN != records[j].VSN {
			return records[i].VSN < records[j].VSN
		}
		if records[i].Host != records[j].Host {
			return records[i].Host < records[j].Host
		}
		return records[i].Timestamp.Before(records[j].Timestamp)
	})
}

func urlEncodePlugin(plugin string) string {
	return strings.ReplaceAll(plugin, "/", "%2F")
}

func getFileID(baseDir string) (int, error) {
	files, err := filepath.Glob(filepath.Join(baseDir, "*.parquet"))
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	// Current batch tracking
	var currentGroup GroupKey
	var currentBatch []Measurement
	const batchSize = 500_000

	for scanner.Scan() {
		var point Measurement
		if err := json.Unmarshal(scanner.Bytes(), &point); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
			continue
		}

		key := groupByPluginAndMeasurement(point)

		// Check if we need to start a new group or if current group is full
		if len(currentBatch) == 0 || !groupKeysEqual(currentGroup, key) || len(currentBatch) >= batchSize {
			// Write out current batch if it exists
			if len(currentBatch) > 0 {
				if err := writeBatch(currentGroup, currentBatch); err != nil {
					fmt.Fprintf(os.Stderr, "Error writing batch: %v\n", err)
				}
			}

			// Start new group
			currentGroup = key
			currentBatch = make([]Measurement, 0, batchSize)
		}

		currentBatch = append(currentBatch, point)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}

	// Write final batch
	if len(currentBatch) > 0 {
		if err := writeBatch(currentGroup, currentBatch); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing final batch: %v\n", err)
		}
	}
}

func groupKeysEqual(a, b GroupKey) bool {
	return a.Plugin == b.Plugin && a.Measurement == b.Measurement && a.Date.Equal(b.Date)
}

func writeBatch(groupKey GroupKey, points []Measurement) error {
	fmt.Printf("processing group %s, %s, %s with %d measurements\n",
		groupKey.Plugin, groupKey.Measurement, groupKey.Date.Format("2006-01-02"), len(points))
	return nil

	// Sort records
	sortRecords(points)

	fmt.Printf("writing group %s, %s\n", groupKey.Plugin, groupKey.Measurement)

	// Convert to Parquet records
	var records []ParquetRecord
	for _, point := range points {
		record := ParquetRecord{
			VSN:       point.VSN,
			Host:      point.Host,
			Timestamp: point.Timestamp.UnixNano(),
			Value:     fmt.Sprintf("%v", point.Value),
			Meta:      point.Meta,
		}
		records = append(records, record)
	}

	fmt.Println("---")
	fmt.Printf("Schema: VSN, Host, Timestamp, Value, Meta\n")
	fmt.Println()

	// Create directory structure
	urlEncodedPlugin := urlEncodePlugin(groupKey.Plugin)
	baseDir := filepath.Join("go-data",
		fmt.Sprintf("plugin=%s", urlEncodedPlugin),
		fmt.Sprintf("measurement=%s", groupKey.Measurement),
		fmt.Sprintf("date=%s", groupKey.Date.Format("2006-01-02")))

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("error creating directory %s: %w", baseDir, err)
	}

	// Get file ID
	fileID, err := getFileID(baseDir)
	if err != nil {
		return fmt.Errorf("error getting file ID: %w", err)
	}

	// Create file path
	fileName := fmt.Sprintf("%04d.parquet", fileID)
	filePath := filepath.Join(baseDir, fileName)

	fmt.Printf("started writing %s with %d measurements\n", filePath, len(records))

	// Write to Parquet
	if err := writeTableToParquetFile(records, filePath); err != nil {
		return fmt.Errorf("error writing parquet file: %w", err)
	}

	fmt.Printf("finished writing %s\n", filePath)
	return nil
}
