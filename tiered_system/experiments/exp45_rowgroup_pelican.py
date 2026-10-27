#!/usr/bin/env python
"""
Experiment 45: Pelican wall time for the exp44 queries.

Pass Pelican object paths (the part after /sage). Each query uses a fresh
DuckDB connection and a read token for that one path.

  python -I exp45_rowgroup_pelican.py /cold-storage-demo-rowgroupbench-1/a.parquet

--put uploads local files first, under a new /cold-storage-demo-rowgroupbench-
prefix, and refuses a prefix that already exists. /node-data is never used.

  python -I exp45_rowgroup_pelican.py --put rg_4194304.parquet rg_8388608.parquet
"""
import argparse
import datetime as dt
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402
import pelican_fileio as pf  # noqa: E402
import pelican_token as pt  # noqa: E402


def queries_for(day, name, vsn):
    d0 = dt.date.fromisoformat(day)
    start = dt.datetime(d0.year, d0.month, d0.day, tzinfo=dt.timezone.utc)
    windows = {
        "minute": start + dt.timedelta(minutes=1),
        "hour": start + dt.timedelta(hours=1),
        "day": start + dt.timedelta(days=1),
    }
    out = []
    for label, hi in windows.items():
        where = (f"time >= TIMESTAMPTZ '{start:%Y-%m-%d %H:%M:%S+00}' "
                 f"AND time < TIMESTAMPTZ '{hi:%Y-%m-%d %H:%M:%S+00}' "
                 f"AND name = '{name}'")
        out.append((f"{label}_temp",
                    "select count(*) from read_parquet('{url}') where " + where))
        out.append((f"{label}_vsn",
                    "select count(*) from read_parquet('{url}') where "
                    + where + f" AND vsn = '{vsn}'"))
    out.append(("day_vsn_all",
                "select count(*) from read_parquet('{url}') where "
                f"time >= TIMESTAMPTZ '{start:%Y-%m-%d %H:%M:%S+00}' "
                f"AND time < TIMESTAMPTZ '{windows['day']:%Y-%m-%d %H:%M:%S+00}' "
                f"AND vsn = '{vsn}'"))
    return out


def put_files(paths):
    prefix = f"/cold-storage-demo-rowgroupbench-{time.time_ns()}"
    status, kids = pf.propfind(prefix)
    if status != 404:
        raise SystemExit(f"refusing to write, PROPFIND {prefix} -> {status} "
                         f"({len(kids)} children)")
    uploaded = []
    for src in paths:
        name = os.path.basename(src)
        path = f"{prefix}/{name}"
        size = os.path.getsize(src)
        print(f"PUT {path} ({size / 1e6:.1f} MB)", flush=True)
        t0 = time.time()
        with open(src, "rb") as fh:
            data = fh.read()
        status, _, body = pf._request(path, "PUT", data=data, timeout=1800)
        elapsed = time.time() - t0
        print(f"  status {status} in {elapsed:.1f}s  "
              f"{size / 1e6 / max(elapsed, 1e-9):.1f} MB/s", flush=True)
        if status not in (200, 201, 204):
            raise SystemExit(f"PUT failed: {status} {body[:200]!r}")
        uploaded.append(path)
    return uploaded


def once(path, label, sql):
    token = pt.mint(f"read:{path}", lifetime=900)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET TimeZone='UTC'")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("SET prefetch_all_parquet_files=false")
    except Exception:
        pass
    con.execute("CREATE SECRET (TYPE http, EXTRA_HTTP_HEADERS MAP "
                "{'Authorization': 'Bearer " + token + "'})")
    t0 = time.perf_counter()
    n = con.execute(sql.format(url=pt.ROOT_URL + path)).fetchone()[0]
    elapsed = time.perf_counter() - t0
    con.close()
    print(f"{label:14} {elapsed:7.2f}s  rows={n}  {path}", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("paths", nargs="+",
                    help="Pelican paths, or local files with --put")
    ap.add_argument("--put", action="store_true")
    ap.add_argument("--day", default="2025-03-01")
    ap.add_argument("--name", default="wxt.env.temp")
    ap.add_argument("--vsn", default="W0A0")
    ap.add_argument("--runs", type=int, default=1)
    args = ap.parse_args()

    paths = put_files(args.paths) if args.put else args.paths
    for path in paths:
        if not path.startswith("/cold-storage-demo"):
            raise SystemExit(f"refusing path outside /cold-storage-demo: {path}")
    queries = queries_for(args.day, args.name, args.vsn)
    for label, sql in queries:
        for path in paths:
            for _ in range(args.runs):
                once(path, label, sql)


if __name__ == "__main__":
    main()
