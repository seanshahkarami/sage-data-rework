#!/usr/bin/env python
"""
Experiment 4: does splitting a file into multiple row groups unlock the
pruning that plans/1_draft.md assumes?

exp03 found that over HTTP every filter shape reads the same 53.9 MB from a
file that returns 3 rows. The files have page indexes, but the reader still
pulls whole column chunks. The suspected cause is in archive-exporter/main.go:

    SortingWriterConfig is set. MaxRowsPerRowGroup is not.

So each ~100 MB file is ONE row group, and that row group's min/max for
`plugin` spans every plugin in the batch. Row-group statistics can therefore
never reject it, and the reader falls back to scanning the chunk.

This rewrites the same 10M rows, same sort order, same ZSTD, at several row
group sizes, then replays the exp03 HTTP measurement against each. What we
want to know:

  1. Do bytes-read for a selective filter actually drop?
  2. What does the split cost in file size and footer size?
  3. Which size is the right default for the exporter?

Usage: python -I exp04_rowgroup_sizing.py <source.parquet> <outdir>
"""
import json
import os
import shutil
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP_SIZES = [10_000_000, 2_000_000, 1_000_000, 250_000, 100_000]


def rewrite(src_table, outdir, rg_size):
    """Write the table with a given row group size, preserving sort order."""
    path = os.path.join(outdir, f"rg_{rg_size}.parquet")
    t0 = time.perf_counter()
    pq.write_table(
        src_table,
        path,
        compression="zstd",
        compression_level=9,
        row_group_size=rg_size,
        # match the exporter's intent: let readers prune with stats + page index
        write_statistics=True,
        write_page_index=True,
        store_schema=True,
        sorting_columns=[
            pq.SortingColumn(src_table.schema.get_field_index("plugin")),
            pq.SortingColumn(src_table.schema.get_field_index("vsn")),
            pq.SortingColumn(src_table.schema.get_field_index("time")),
        ],
    )
    dt = time.perf_counter() - t0
    md = pq.ParquetFile(path).metadata
    return {
        "row_group_size": rg_size,
        "path": path,
        "file_size": os.path.getsize(path),
        "num_row_groups": md.num_row_groups,
        "footer_size": md.serialized_size,
        "write_seconds": round(dt, 2),
    }


def main():
    src = os.path.abspath(sys.argv[1])
    outdir = os.path.abspath(sys.argv[2])
    os.makedirs(outdir, exist_ok=True)

    print(f"reading {src} ...")
    t0 = time.perf_counter()
    tbl = pq.read_table(src)
    print(f"  {tbl.num_rows:,} rows in {time.perf_counter()-t0:.1f}s")

    # The source is already sorted by plugin, vsn, time. Confirm rather than assume.
    plugins = tbl["plugin"].to_pandas()
    is_sorted = (plugins.fillna("").values[:-1] <= plugins.fillna("").values[1:]).all()
    print(f"  source sorted by plugin: {is_sorted}")

    src_meta = pq.ParquetFile(src).metadata
    print(f"  source: {os.path.getsize(src):,} bytes, {src_meta.num_row_groups} row group(s), "
          f"footer {src_meta.serialized_size:,} bytes\n")

    variants = []
    for rg in ROW_GROUP_SIZES:
        v = rewrite(tbl, outdir, rg)
        variants.append(v)
        print(
            f"rg={rg:>10,}  file={v['file_size']:>13,}  groups={v['num_row_groups']:>4}  "
            f"footer={v['footer_size']:>10,}  write={v['write_seconds']:>6.1f}s"
        )

    out = {
        "source": src,
        "source_size": os.path.getsize(src),
        "source_row_groups": src_meta.num_row_groups,
        "source_footer": src_meta.serialized_size,
        "source_sorted_by_plugin": bool(is_sorted),
        "num_rows": tbl.num_rows,
        "variants": variants,
    }
    with open("results/exp04_rowgroup_sizing.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nwrote {len(variants)} variants to {outdir}")


if __name__ == "__main__":
    main()
