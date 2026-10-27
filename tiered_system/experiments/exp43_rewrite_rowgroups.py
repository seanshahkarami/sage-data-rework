#!/usr/bin/env python
"""
Experiment 43: rewrite a time-sorted day at larger row-group sizes.

Same DuckDB settings as seed_cold.sort_by_time (UTC, 4 GB, spill to a temp
dir, Snappy, ORDER BY time). A size that does not fit in 4 GB fails the way
16,777,216 did on 2025-03-01; that failure is the result, not something to
retry with a higher cap.

Usage:
  python -I exp43_rewrite_rowgroups.py <sorted.parquet> <outdir>
  python -I exp43_rewrite_rowgroups.py sorted.parquet out/ 4194304 8388608
"""
import argparse
import os
import shutil
import tempfile
import time

import duckdb

DEFAULT_SIZES = [4_194_304, 8_388_608, 16_777_216]


def rewrite(src, dest, row_group_rows):
    tmp = dest + ".tmp"
    scratch = tempfile.mkdtemp(prefix="exp43-")
    t0 = time.time()
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        con.execute("SET memory_limit='4GB'")
        con.execute(f"SET temp_directory='{scratch}'")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{src}') ORDER BY time) "
            f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION SNAPPY, "
            f"ROW_GROUP_SIZE {row_group_rows})"
        )
    finally:
        con.close()
        shutil.rmtree(scratch, ignore_errors=True)
    os.replace(tmp, dest)
    print(f"  {os.path.getsize(dest) / 1e6:.1f} MB in {time.time() - t0:.1f}s",
          flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("src")
    ap.add_argument("outdir")
    ap.add_argument("sizes", nargs="*", type=int, default=DEFAULT_SIZES)
    args = ap.parse_args()
    if not args.sizes:
        args.sizes = DEFAULT_SIZES
    os.makedirs(args.outdir, exist_ok=True)
    src = os.path.abspath(args.src)
    for n in args.sizes:
        dest = os.path.join(args.outdir, f"rg_{n}.parquet")
        print(f"writing {n:,} -> {dest}", flush=True)
        rewrite(src, dest, n)


if __name__ == "__main__":
    main()
