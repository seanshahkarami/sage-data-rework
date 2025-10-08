#!/usr/bin/env python3
import argparse
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as parquet
from itertools import groupby, batched
import json
import sys
import os
from datetime import datetime, timezone


def groupby_plugin_and_measurement(point):
    ts = datetime.fromtimestamp(point["ts"] / 1000000000, tz=timezone.utc)
    return (point["plugin"] or "system", point["measurement"], ts.date())


def write_table_to_parquet_file(table: pa.Table, path: os.PathLike):
    temp_path = path.with_name(path.name + ".tmp")
    writer = parquet.ParquetWriter(
        temp_path,
        schema=table.schema,
        version="2.6",
        write_statistics=True,
        data_page_version="2.0",
        compression="zstd",
        use_dictionary=["vsn", "host"],
    )
    writer.write_table(table)
    temp_path.rename(path)


def main(output_dir: Path):
    points = map(json.loads, sys.stdin)

    for group, grouped_points in groupby(points, key=groupby_plugin_and_measurement):
        for batch_num, batched_points in enumerate(batched(grouped_points, 500_000)):
            (plugin, measurement, date) = group

            print(f"processing group {plugin}, {measurement}, {date} batch {batch_num}")

            timestamp_col = [point["ts"] for point in batched_points]
            vsn_col = [point["vsn"] for point in batched_points]
            host_col = [point["host"] for point in batched_points]
            value_col = [point["value"] for point in batched_points]
            meta_col = [point["meta"] for point in batched_points]

            print(f"writing group {plugin}, {measurement} batch {batch_num}")

            # schema = pa.schema(
            #     [
            #         pa.field("vsn", pa.string(), nullable=False),
            #         pa.field("host", pa.string(), nullable=False),
            #         pa.field("timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
            #         pa.field("meta", pa.map_(pa.string(), pa.string()), nullable=False),
            #     ],
            #     metadata={
            #         "vsn": "Node name.",
            #         "host": "Internal compute host name.",
            #         "timestamp": "Timestamp of measurement in nano-seconds UTC.",
            #         "meta": "Additional system and user defined metadata.",
            #     },
            # )

            table = pa.table(
                {
                    "vsn": vsn_col,
                    "host": host_col,
                    "timestamp": pa.array(
                        timestamp_col, type=pa.timestamp("ns", tz="UTC")
                    ),
                    "value": value_col,
                    "meta": meta_col,
                },
                # schema=schema,
            )

            # hmm... we're already gonna have different schemas per file anyway... purely based on the datatype..
            # and... in generally, we can't *really* even combine these thing with different units...
            # so, should we just expand the meta items???

            table = table.sort_by(
                [
                    ("vsn", "ascending"),
                    ("host", "ascending"),
                    ("timestamp", "ascending"),
                ]
            )

            print("---")
            print(table.schema)
            print()

            urlencoded_plugin = plugin.replace("/", "%2F")

            base_dir = Path(
                output_dir,
                f"plugin={urlencoded_plugin}",
                f"measurement={measurement}",
                f"date={date}",
            )
            base_dir.mkdir(parents=True, exist_ok=True)

            # file id is just the number of current files in the base dir.
            file_id = len(list(base_dir.glob("*.parquet")))

            writer_path = Path(base_dir, f"{file_id}.parquet")
            print(f"started writing {writer_path} with {len(table)} measurements")
            write_table_to_parquet_file(table, writer_path)
            print(f"finished writing {writer_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="data",
        type=Path,
        help="output directory for parquet files",
    )
    args = parser.parse_args()

    main(
        output_dir=args.output_dir,
    )
