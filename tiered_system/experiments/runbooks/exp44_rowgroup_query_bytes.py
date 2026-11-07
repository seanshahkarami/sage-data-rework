#!/usr/bin/env python
"""
Experiment 44: bytes and range requests for a minute, an hour, and a day.

Serves every parquet in a directory over local HTTP and counts the bytes
DuckDB actually reads. A HEAD of the whole file is not counted. This is the
measurement behind plans/7_filter_sort_recommendation.md.

Usage: python -I exp44_rowgroup_query_bytes.py <dir-of-parquets>
"""
import argparse
import datetime as dt
import os
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn

import pyarrow.parquet as pq

STATS = {"ranges": []}
LOCK = threading.Lock()


class TServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class RangeHandler(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _serve(self, body):
        path = self.translate_path(self.path.split("?", 1)[0])
        if not os.path.isfile(path):
            self.send_error(404)
            return
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end, partial = 0, size - 1, False
        if rng:
            spec = rng.split("=", 1)[1]
            left, right = spec.split("-", 1)
            if left == "":
                n = int(right)
                start, end = max(0, size - n), size - 1
            else:
                start = int(left)
                end = int(right) if right else size - 1
            partial = True
        end = min(end, size - 1)
        length = max(0, end - start + 1)
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        sent = 0
        if body and length:
            try:
                with open(path, "rb") as fh:
                    fh.seek(start)
                    remaining = length
                    while remaining:
                        chunk = fh.read(min(remaining, 1 << 20))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        sent += len(chunk)
                        remaining -= len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass
        with LOCK:
            if rng:
                STATS["ranges"].append(sent)

    def do_GET(self):
        self._serve(True)

    def do_HEAD(self):
        self._serve(False)


def group_overlap(path, day):
    """How many row groups overlap the first minute and the first hour."""
    d0 = dt.date.fromisoformat(day)
    start = dt.datetime(d0.year, d0.month, d0.day, tzinfo=dt.timezone.utc)
    minute_hi = start + dt.timedelta(minutes=1)
    hour_hi = start + dt.timedelta(hours=1)
    pf = pq.ParquetFile(path)
    md = pf.metadata
    spans = []
    for i in range(md.num_row_groups):
        stats = md.row_group(i).column(0).statistics
        spans.append((stats.min, stats.max, md.row_group(i).num_rows))

    def hits(lo, hi):
        return sum(1 for a, b, _ in spans if a < hi and b >= lo)

    return {
        "groups": md.num_row_groups,
        "rows": md.num_rows,
        "bytes": os.path.getsize(path),
        "minute_groups": hits(start, minute_hi),
        "hour_groups": hits(start, hour_hi),
        "first": (str(spans[0][0]), str(spans[0][1]), spans[0][2]),
        "last": (str(spans[-1][0]), str(spans[-1][1]), spans[-1][2]),
    }


def cases_for(day, name, vsn):
    d0 = dt.date.fromisoformat(day)
    start = dt.datetime(d0.year, d0.month, d0.day, tzinfo=dt.timezone.utc)
    windows = {
        "minute": (start, start + dt.timedelta(minutes=1)),
        "hour": (start, start + dt.timedelta(hours=1)),
        "day": (start, start + dt.timedelta(days=1)),
    }
    out = []
    for label, (lo, hi) in windows.items():
        where = (f"time >= TIMESTAMPTZ '{lo:%Y-%m-%d %H:%M:%S+00}' "
                 f"AND time < TIMESTAMPTZ '{hi:%Y-%m-%d %H:%M:%S+00}' "
                 f"AND name = '{name}'")
        out.append((f"{label}_temp",
                    "select count(*) from read_parquet('{url}') where " + where))
        out.append((f"{label}_vsn",
                    "select count(*) from read_parquet('{url}') where "
                    + where + f" AND vsn = '{vsn}'"))
    out.append(("day_vsn_all",
                "select count(*) from read_parquet('{url}') where "
                f"time >= TIMESTAMPTZ '{windows['day'][0]:%Y-%m-%d %H:%M:%S+00}' "
                f"AND time < TIMESTAMPTZ '{windows['day'][1]:%Y-%m-%d %H:%M:%S+00}' "
                f"AND vsn = '{vsn}'"))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("directory")
    ap.add_argument("--day", default="2025-03-01")
    ap.add_argument("--name", default="wxt.env.temp")
    ap.add_argument("--vsn", default="W0A0")
    args = ap.parse_args()

    root = os.path.abspath(args.directory)
    names = sorted(f for f in os.listdir(root) if f.endswith(".parquet"))
    if not names:
        raise SystemExit(f"no parquet files in {root}")
    for name in names:
        info = group_overlap(os.path.join(root, name), args.day)
        print(f"{name}: {info['bytes'] / 1e6:.0f} MB  groups={info['groups']}  "
              f"minute_overlap={info['minute_groups']}  "
              f"hour_overlap={info['hour_groups']}", flush=True)
        print(f"  first {info['first']}", flush=True)
        print(f"  last  {info['last']}", flush=True)

    os.chdir(root)
    httpd = TServer(("127.0.0.1", 0), RangeHandler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    print(f"serving {base}", flush=True)

    import duckdb
    queries = cases_for(args.day, args.name, args.vsn)
    for name in names:
        url = f"{base}/{name}"
        print(f"\n== {name} ==", flush=True)
        for label, template in queries:
            with LOCK:
                STATS["ranges"] = []
            con = duckdb.connect()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("SET TimeZone='UTC'")
            con.execute("PRAGMA threads=4")
            try:
                con.execute("SET prefetch_all_parquet_files=false")
            except Exception:
                pass
            t0 = time.perf_counter()
            n = con.execute(template.format(url=url)).fetchone()[0]
            elapsed = time.perf_counter() - t0
            with LOCK:
                ranges = list(STATS["ranges"])
            sent = sum(ranges)
            small = sum(1 for r in ranges if r < 1000)
            print(f"  {label}: rows={n}  {elapsed:.3f}s  "
                  f"bytes={sent / 1e6:.2f} MB  requests={len(ranges)}  "
                  f"subKB={small}", flush=True)
            con.close()


if __name__ == "__main__":
    main()
