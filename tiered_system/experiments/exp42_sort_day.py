#!/usr/bin/env python
"""
Experiment 42: write one cold day the way seed_cold.stage_day does, twice.

The first file is the writer before sort_by_time: each archive shard becomes
row groups that still span the day. The second is that file after
seed_cold.sort_by_time, which is the current ROW_GROUP_ROWS layout.

Nothing is uploaded. exp43 cuts the sorted file into larger groups, and
exp44 measures what a query fetches.

Usage: python -I exp42_sort_day.py <outdir> [--day 2025-03-01]
"""
import argparse
import glob
import os
import shutil
import sys
import time

import pyarrow.parquet as pq

_TOOLS = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "..", "demo", "tools")
sys.path.insert(0, os.path.abspath(_TOOLS))

import seed_cold as sc  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("outdir")
    ap.add_argument("--day", default="2025-03-01")
    ap.add_argument("--batch-rows", type=int, default=2_000_000)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    unsorted = os.path.join(args.outdir, "unsorted.parquet")
    sorted_path = os.path.join(args.outdir, "sorted.parquet")
    files = sorted(glob.glob(f"{sc.ARCHIVE}/date={args.day}/*.parquet"))
    if not files:
        raise SystemExit(f"no archive files for {args.day} under {sc.ARCHIVE}")
    print(f"archive files: {len(files)}", flush=True)
    schema = sc.iceberg_schema().as_arrow()

    t0 = time.time()
    rows = dropped = chunks = 0
    writer = pq.ParquetWriter(unsorted, schema, compression="snappy")
    try:
        for path in files:
            ft = time.time()
            n0 = rows
            for chunk in sc.iter_chunks_us(path, schema, args.batch_rows):
                chunk, d = sc.clip_to_day(chunk, args.day)
                dropped += d
                chunks += 1
                if chunk.num_rows:
                    writer.write_table(chunk)
                    rows += chunk.num_rows
                del chunk
            print(f"  wrote {os.path.basename(path)}: {rows - n0:,} rows "
                  f"in {time.time() - ft:.1f}s  total {rows:,}", flush=True)
    finally:
        writer.close()
    print(f"UNSORTED {rows:,} rows, {chunks} chunks, dropped {dropped}, "
          f"{os.path.getsize(unsorted) / 1e6:.1f} MB "
          f"in {time.time() - t0:.1f}s", flush=True)

    shutil.copy(unsorted, sorted_path)
    t1 = time.time()
    print(f"sorting at ROW_GROUP_ROWS={sc.ROW_GROUP_ROWS:,}...", flush=True)
    sc.sort_by_time(sorted_path)
    print(f"SORTED {os.path.getsize(sorted_path) / 1e6:.1f} MB "
          f"in {time.time() - t1:.1f}s", flush=True)


if __name__ == "__main__":
    main()
