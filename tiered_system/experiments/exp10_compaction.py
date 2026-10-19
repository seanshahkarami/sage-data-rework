#!/usr/bin/env python
"""
Experiment 10: the "proposed first milestone" from plans/1_draft.md.

  "Demonstrate that Beehive can query a month of archived observations before
   and after that month's daily files are consolidated -- and return identical
   results while the superseded files are safely pruned."

This does that on real days, at a scale that fits the time available: take N
days of the archive, run a set of Beehive queries, compact the days into
larger files with the row-group fix from exp07, re-run the same queries, and
diff the records.

The test is not "did compaction run" but "are the answers identical". A
compaction that silently drops or reorders records would still produce files.

Also measures what compaction buys: file count, total size, and bytes read
for a selective query, before and after.

Usage: python -I exp10_compaction.py <archive-root> <workdir> [--days N]
"""
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cold_backend as cb

ROW_GROUP = 1_000_000
TARGET_FILE_ROWS = 20_000_000  # compaction target


def fingerprint(records):
    """Order-independent digest of a record set, so we compare content not order."""
    h = hashlib.sha256()
    for line in sorted(
        json.dumps(r, sort_keys=True, default=str) for r in records
    ):
        h.update(line.encode())
    return h.hexdigest()


def describe(root, days):
    files = [
        os.path.join(root, d, f)
        for d in days
        for f in sorted(os.listdir(os.path.join(root, d)))
        if f.endswith(".parquet")
    ]
    return {
        "files": len(files),
        "bytes": sum(os.path.getsize(p) for p in files),
        "rows": sum(pq.ParquetFile(p).metadata.num_rows for p in files),
        "row_groups": sum(pq.ParquetFile(p).metadata.num_row_groups for p in files),
    }


def compact(src_root, days, dst_root):
    """Rewrite several day directories into fewer, larger, row-grouped files.

    Keeps the date= layout (a day is still one directory) but merges that day's
    data_N files and gives them real row groups. This is the daily -> monthly
    lifecycle step in miniature.
    """
    os.makedirs(dst_root, exist_ok=True)
    t0 = time.perf_counter()
    out_files = []
    for d in days:
        sdir = os.path.join(src_root, d)
        ddir = os.path.join(dst_root, d)
        os.makedirs(ddir, exist_ok=True)
        srcs = [os.path.join(sdir, f) for f in sorted(os.listdir(sdir)) if f.endswith(".parquet")]
        # Merge in order, flushing when the target row count is reached, so
        # memory stays bounded instead of holding a whole day.
        writer = None
        out_idx = 0
        rows_in_file = 0
        for p in srcs:
            pf = pq.ParquetFile(p)
            for batch in pf.iter_batches(batch_size=500_000):
                tbl = pa.Table.from_batches([batch])
                if writer is None:
                    out = os.path.join(ddir, f"data_{out_idx}.parquet")
                    writer = pq.ParquetWriter(
                        out, tbl.schema, compression="zstd", compression_level=9,
                        write_statistics=True, write_page_index=True,
                    )
                    out_files.append(out)
                writer.write_table(tbl, row_group_size=ROW_GROUP)
                rows_in_file += tbl.num_rows
                if rows_in_file >= TARGET_FILE_ROWS:
                    writer.close()
                    writer = None
                    out_idx += 1
                    rows_in_file = 0
        if writer is not None:
            writer.close()
    return out_files, time.perf_counter() - t0


def main():
    src_root = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 3
    days = sorted(d for d in os.listdir(src_root) if d.startswith("date="))[:ndays]
    dst_root = os.path.join(work, "compacted")
    if os.path.exists(dst_root):
        shutil.rmtree(dst_root)

    before = describe(src_root, days)
    print(f"days        : {len(days)} ({days[0]} .. {days[-1]})")
    print(f"before      : {before['files']} files, {before['bytes']:,} bytes, "
          f"{before['rows']:,} rows, {before['row_groups']} row groups\n")

    # ---- queries to compare ----
    date = days[0].replace("date=", "")
    last = days[-1].replace("date=", "")
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    files0 = [os.path.join(src_root, days[0], f)
              for f in sorted(os.listdir(os.path.join(src_root, days[0]))) if f.endswith(".parquet")]
    fl = "[" + ",".join("'" + f + "'" for f in files0) + "]"
    top = con.execute(
        f"select name, count(*) c from read_parquet({fl}) group by name order by c desc limit 1"
    ).fetchone()[0]
    rare = con.execute(
        f"select plugin, count(*) c from read_parquet({fl}) group by plugin order by c asc limit 1"
    ).fetchone()[0]

    queries = {
        "one_hour_measurement": {
            "start": f"{date}T00:00:00Z", "end": f"{date}T01:00:00Z",
            "filter": {"name": top},
        },
        "rare_plugin_whole_range": {
            "start": f"{date}T00:00:00Z", "end": f"{last}T23:59:59Z",
            "filter": {"plugin": rare},
        },
        "tail_per_series": {
            "start": f"{date}T00:00:00Z", "end": f"{date}T06:00:00Z",
            "filter": {"name": top}, "tail": 2,
        },
        "regex_measurement": {
            "start": f"{date}T00:00:00Z", "end": f"{date}T00:30:00Z",
            "filter": {"name": top.rsplit(".", 1)[0] + ".*"},
        },
    }

    print("running queries against the ORIGINAL archive ...")
    pre = {}
    for name, q in queries.items():
        t0 = time.perf_counter()
        recs, _, nf = cb.query_archive(con, src_root, q)
        pre[name] = {"n": len(recs), "fp": fingerprint(recs),
                     "seconds": round(time.perf_counter() - t0, 2), "files": nf}
        print(f"  {name:26s} {len(recs):>10,} records  {pre[name]['seconds']:>6.2f}s")

    print("\ncompacting ...")
    out_files, comp_s = compact(src_root, days, dst_root)
    after = describe(dst_root, days)
    print(f"  wrote {after['files']} files in {comp_s:.1f}s")
    print(f"after       : {after['files']} files, {after['bytes']:,} bytes, "
          f"{after['rows']:,} rows, {after['row_groups']} row groups")
    print(f"  size   {100.0*after['bytes']/before['bytes']:.1f}% of before")
    print(f"  files  {before['files']} -> {after['files']}")

    print("\nrunning the SAME queries against the COMPACTED archive ...")
    post = {}
    for name, q in queries.items():
        t0 = time.perf_counter()
        recs, _, nf = cb.query_archive(con, dst_root, q)
        post[name] = {"n": len(recs), "fp": fingerprint(recs),
                      "seconds": round(time.perf_counter() - t0, 2), "files": nf}
        print(f"  {name:26s} {len(recs):>10,} records  {post[name]['seconds']:>6.2f}s")

    print()
    print(f"{'query':26s} {'records':>12s} {'identical':>10s} {'before s':>9s} {'after s':>8s}")
    allsame = True
    for name in queries:
        same = pre[name]["fp"] == post[name]["fp"] and pre[name]["n"] == post[name]["n"]
        allsame &= same
        print(f"{name:26s} {pre[name]['n']:>12,} {('YES' if same else 'NO'):>10s} "
              f"{pre[name]['seconds']:>9.2f} {post[name]['seconds']:>8.2f}")

    print()
    print("MILESTONE:", "PASS - identical results before and after compaction"
          if allsame else "FAIL - compaction changed query results")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp10_compaction.json", "w") as f:
        json.dump({"days": days, "before": before, "after": after,
                   "compaction_seconds": round(comp_s, 1),
                   "pre": pre, "post": post, "identical": allsame}, f, indent=2)


if __name__ == "__main__":
    main()
