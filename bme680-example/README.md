# BME680 Archive Example

## Intro

This is an example of how we might archive BME680 data as Parquet files.

You will need to pip install sage-data-client and download duckdb to run these.

In order to run the example, you do:

```
# query bme680 data and store in inputs/ as csvs
python3 get_data.py

# convert csv files into parquet files
./convert-csv-to-parquet

# can run some example queries against all outputs
./run-example-queries
```

## Specific things to explore

A couple things I'm looking into...

First, I'm interested in the per-file structure and types in the Parquets.

```
$ parquet meta outputs/2026-01-01.parquet 

File path:  outputs/2026-01-01.parquet
Created by: DuckDB version v1.5.2 (build 8a5851971f)
Properties:
  sensor: BME680
   units: {time:UTC,t:degC,p:hPa,rh:percent}
Schema:
message duckdb_schema {
  optional int64 time (TIMESTAMP(MICROS,true));
  optional binary vsn (STRING);
  optional double t;
  optional double p;
  optional double rh;
}


Row group 0:  count: 114841  7.11 B records  start: 4  total(compressed): 797.572 kB total(uncompressed):1012.337 kB 
--------------------------------------------------------------------------------
      type      encodings count     avg size   nulls   min / max
time  INT64     Z RRD     114841    2.75 B     0       "2026-01-01T00:00:00.90110..." / "2026-01-01T23:59:59.75960..."
vsn   BINARY    Z RRR     114841    0.05 B     0       "W01C" / "W0AA"
t     DOUBLE    Z RRR     114841    1.32 B     0       "-133.4" / "34.43"
p     DOUBLE    Z RRR     114841    1.87 B     0       "1501.0" / "113122.0"
rh    DOUBLE    Z RRR     114841    1.12 B     0       "-0.0" / "100.0"
```

In addition to compressing much, much better (~800KB vs 7MB):

```
# du -h inputs/2026-01-01.csv
7.1M	inputs/2026-01-01.csv
```

This gives a good feel how data is actually organized, typed, and how much space each field typically takes.
