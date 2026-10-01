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
    donefile = f"daily-exports/.{d}-done"

    if os.path.exists(filename):
        print(f"data file {filename} already exists! skipping!")
        continue

    if os.path.exists(donefile):
        print(f"done file {donefile} already exists! skipping!")
        continue

    print(f"querying data for {d}...") 
    df = sage_data_client.query(
        start=d,
        end=d+datetime.timedelta(days=1),
        filter={
            "name": "upload",
            "plugin": ".*imagesampler.*",
            "camera": ".*",
        }
    )

    if len(df) == 0:
        print(f"no data for {d}")
        with open(donefile, "w"):
            pass
        continue

    def infer_camera(s: str):
        if "imagesampler-bottom" in s:
            return "bottom_camera"
        if "imagesampler-top" in s:
            return "top_camera"
        if "imagesampler-left" in s:
            return "left_camera"
        if "imagesampler-right" in s:
            return "right_camera"
        return None

    df["time"] = df["timestamp"].dt.as_unit("ms")
    try:
        df["camera"] = df["meta.camera"]
    except KeyError:
        df["camera"] = None
        df["camera"] = df["value"].apply(infer_camera)

    df["camera"] = df["camera"].astype(str)

    df["url"] = df["value"]
    df["vsn"] = df["meta.vsn"]
    df["app"] = df["meta.plugin"]
    df.sort_values("time", inplace=True)

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df[["time", "camera", "url", "vsn", "app"]].to_parquet(
        tempname,
        engine="pyarrow",
        compression="zstd",
        index=False,
        use_dictionary=["camera", "vsn", "app"],
        column_encoding={
            "time": "DELTA_BINARY_PACKED",
        },
    )

    os.rename(tempname, filename)

# # TODO Let's see if we can build this into safe set of workflow while extracting common bits we end up using over and over.

print("creating final rollup")
duckdb.sql("""
COPY (
    SELECT * REPLACE (camera::VARCHAR as camera) FROM read_parquet('daily-exports/*.parquet', union_by_name = true) ORDER BY time, camera
) TO 'image-sampler.parquet' (
    FORMAT parquet,
    COMPRESSION zstd,
    COMPRESSION_LEVEL 9,
    PARQUET_VERSION V2
);
""")
