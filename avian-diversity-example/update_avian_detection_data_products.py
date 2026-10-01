import sage_data_client
import os
import os.path
import datetime
import pandas as pd
import duckdb


dates = pd.date_range("2023-02-15", "today", tz="utc").date

for d in reversed(dates):
    filename = f"daily-exports/{d}.parquet"
    tempname = f"daily-exports/{d}.parquet.tmp"

    if os.path.exists(filename):
        print(f"data file {filename} already exists! skipping!")
        continue

    print(f"querying data for {d}...") 
    df = sage_data_client.query(
        start=d,
        end=d+datetime.timedelta(days=1),
        filter={
            "name": "env.detection.avian..*",
            "plugin": "registry.sagecontinuum.org/dariodematties/avian-diversity-monitoring:.*",
        }
    )

    if len(df) == 0:
        print(f"no data for {d}")
        continue

    df["time"] = df["timestamp"].dt.as_unit("ms")
    df["scientific_name"] = df["name"].str.removeprefix("env.detection.avian.")
    df["confidence"] = pd.to_numeric(df["value"]).round(3)
    df["vsn"] = df["meta.vsn"]
    df["app"] = df["meta.plugin"]
    df.sort_values("time", inplace=True)

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df[["time", "vsn", "app", "scientific_name", "confidence"]].to_parquet(
        tempname,
        engine="pyarrow",
        compression="zstd",
        index=False,
        use_dictionary=["vsn", "app", "scientific_name"],
        column_encoding={
            "time": "DELTA_BINARY_PACKED",
        },
    )

    os.rename(tempname, filename)

# TODO Let's see if we can build this into safe set of workflow while extracting common bits we end up using over and over.

print("creating final rollup")
duckdb.sql("""
COPY (
    SELECT * FROM 'daily-exports/*.parquet' ORDER BY vsn, time
) TO 'avian-detections.parquet' (
    FORMAT parquet,
    COMPRESSION zstd,
    COMPRESSION_LEVEL 9,
    PARQUET_VERSION V2
);
""")
