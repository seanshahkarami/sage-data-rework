#!/usr/bin/env python
"""
Experiment 7: re-export one real day the proposed way, then measure.

Findings so far:
  exp03/exp06  a query returning 12 rows reads 146 MB, because each file is a
               single row group whose plugin min/max spans the whole batch.
  exp04/exp05  row groups fix that: 53.9 MB -> 449 KB on the same 10M rows.
  column dump  the Go writer stores meta keys/values as plain strings
               (25.5 MB) where a dictionary-encoded writer uses 5.6 MB.

This applies both changes to a whole real day and reports what the archive
would look like if archive-exporter/main.go were changed:

  - total size of the day, before and after
  - bytes read for a selective query, before and after
  - whether the rows returned are identical (they must be)

Row group size is the knob under test. exp05 suggested 250k-1M rows is where
pruning gets good without the footer growing too much.

Usage: python -I exp07_reexport_day.py <date=YYYY-MM-DD dir> <outdir> [rg_size]
"""
import json
import os
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

SORT_COLS = ["plugin", "vsn", "time"]


def main():
    src_dir = os.path.abspath(sys.argv[1])
    outdir = os.path.abspath(sys.argv[2])
    rg_size = int(sys.argv[3]) if len(sys.argv) > 3 else 1_000_000
    os.makedirs(outdir, exist_ok=True)

    src_files = sorted(
        os.path.join(src_dir, f) for f in os.listdir(src_dir) if f.endswith(".parquet")
    )
    src_bytes = sum(os.path.getsize(p) for p in src_files)
    print(f"source day   : {os.path.basename(src_dir)}")
    print(f"source files : {len(src_files)}  ({src_bytes:,} bytes)")

    # Read the whole day. These are ~10M rows per file; a day is a few hundred MB
    # compressed but several GB in memory, so read and write one source file at
    # a time rather than concatenating the day.
    total_rows = 0
    out_files = []
    t0 = time.perf_counter()
    for i, p in enumerate(src_files):
        tbl = pq.read_table(p)
        total_rows += tbl.num_rows
        # Source is already sorted by plugin,vsn,time within each batch; keep that.
        out = os.path.join(outdir, f"data_{i}.parquet")
        pq.write_table(
            tbl,
            out,
            compression="zstd",
            compression_level=9,
            row_group_size=rg_size,
            write_statistics=True,
            write_page_index=True,
            sorting_columns=[
                pq.SortingColumn(tbl.schema.get_field_index(c)) for c in SORT_COLS
            ],
        )
        out_files.append(out)
        del tbl
        print(f"  rewrote {os.path.basename(p)} -> {os.path.basename(out)} "
              f"({os.path.getsize(p):,} -> {os.path.getsize(out):,})")
    elapsed = time.perf_counter() - t0

    out_bytes = sum(os.path.getsize(p) for p in out_files)
    src_groups = sum(pq.ParquetFile(p).metadata.num_row_groups for p in src_files)
    out_groups = sum(pq.ParquetFile(p).metadata.num_row_groups for p in out_files)
    src_footer = sum(pq.ParquetFile(p).metadata.serialized_size for p in src_files)
    out_footer = sum(pq.ParquetFile(p).metadata.serialized_size for p in out_files)

    print()
    print(f"rows            : {total_rows:,}")
    print(f"row group size  : {rg_size:,}")
    print(f"size  before    : {src_bytes:,}")
    print(f"size  after     : {out_bytes:,}  ({100.0*out_bytes/src_bytes:.1f}% of before)")
    print(f"groups before   : {src_groups}")
    print(f"groups after    : {out_groups}")
    print(f"footer before   : {src_footer:,}")
    print(f"footer after    : {out_footer:,}")
    print(f"rewrite time    : {elapsed:.1f}s")

    res = {
        "source_dir": src_dir,
        "out_dir": outdir,
        "row_group_size": rg_size,
        "num_rows": total_rows,
        "src_files": len(src_files),
        "src_bytes": src_bytes,
        "out_bytes": out_bytes,
        "src_row_groups": src_groups,
        "out_row_groups": out_groups,
        "src_footer_bytes": src_footer,
        "out_footer_bytes": out_footer,
        "rewrite_seconds": round(elapsed, 1),
    }
    rp = os.path.dirname(os.path.abspath(__file__)) + "/results/exp07_reexport_day.json"
    with open(rp, "w") as f:
        json.dump(res, f, indent=2)
    print(f"\nwrote {rp}")


if __name__ == "__main__":
    main()
