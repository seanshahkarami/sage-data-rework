#!/usr/bin/env python
"""
Experiment 3: how many bytes, in how many range requests, does a query pull
when the Parquet lives behind HTTP instead of local disk?

This is the Pelican question from the ticket:

  "Range reads matter because a historical query should fetch the Parquet
   pages it needs."
  "Pelican GET, range GET, existence, DELETE, overwrite, and failed PUT —
   whether NRDStor can back selective Parquet reads"

macOS gives no per-process disk counters, so exp02 could only report wall
time. Serving the same files over a local HTTP server solves both problems at
once: we count exactly the bytes and requests a reader asks for, and that
count is the thing that would become Pelican traffic.

The server logs every Range request. DuckDB reads through httpfs. What we
learn per query shape:
  - bytes transferred   -> Pelican egress, and whether page pruning is real
  - number of requests  -> round-trip latency against a remote object store
  - footer overhead     -> the argument for a manifest, measured

Usage: python -I exp03_range_reads.py <file-or-dir> [more files...]
"""
import json
import os
import re
import sys
import threading
import time
from http.server import SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from http.server import HTTPServer


class ThreadingHTTPServerQuiet(ThreadingMixIn, HTTPServer):
    """DuckDB issues concurrent range requests; a single-threaded server deadlocks."""

    daemon_threads = True
    allow_reuse_address = True

STATS = {"requests": 0, "bytes": 0, "ranges": []}
STATS_LOCK = threading.Lock()


class RangeHandler(SimpleHTTPRequestHandler):
    """Static file server that supports Range and records what it serves."""

    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # silence
        pass

    def _serve(self, body_wanted):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404)
            return
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        partial = False
        if rng:
            m = re.match(r"bytes=(\d*)-(\d*)", rng.strip())
            if m:
                g1, g2 = m.group(1), m.group(2)
                if g1 == "":  # suffix range: last N bytes (Parquet footer)
                    n = int(g2)
                    start, end = max(0, size - n), size - 1
                else:
                    start = int(g1)
                    end = int(g2) if g2 else size - 1
                partial = True
        end = min(end, size - 1)
        length = max(0, end - start + 1)

        with STATS_LOCK:
            STATS["requests"] += 1
            if body_wanted:
                STATS["bytes"] += length
            STATS["ranges"].append(
                {"path": os.path.basename(path), "start": start, "len": length, "range_hdr": rng}
            )

        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if body_wanted and length:
            with open(path, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(1 << 20, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)

    def do_GET(self):
        self._serve(True)

    def do_HEAD(self):
        self._serve(False)


def reset():
    with STATS_LOCK:
        STATS["requests"] = 0
        STATS["bytes"] = 0
        STATS["ranges"] = []


def snapshot():
    with STATS_LOCK:
        return {
            "requests": STATS["requests"],
            "bytes": STATS["bytes"],
            "ranges": list(STATS["ranges"]),
        }


def main():
    target = os.path.abspath(sys.argv[1])
    serve_root = target if os.path.isdir(target) else os.path.dirname(target)

    os.chdir(serve_root)
    httpd = ThreadingHTTPServerQuiet(("127.0.0.1", 0), RangeHandler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    print(f"serving {serve_root} on {base}")

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("PRAGMA threads=4")
    # DuckDB caches remote file metadata; disabling keeps runs comparable.
    try:
        con.execute("SET enable_http_metadata_cache=false")
    except Exception:
        pass

    if os.path.isdir(target):
        # Not used in the single-file pass; kept so the script can serve a tree.
        names = sorted(
            f for f in os.listdir(target) if f.endswith(".parquet")
        )
        url = f"{base}/{names[0]}"
        local = os.path.join(target, names[0])
    else:
        url = f"{base}/{os.path.basename(target)}"
        local = target

    size = os.path.getsize(local)
    print(f"file: {os.path.basename(local)}  size: {size:,} bytes\n")

    # Identify plugins locally (does not count toward HTTP stats).
    counts = con.execute(
        f"select plugin, count(*) c from read_parquet('{local}') group by plugin order by c desc"
    ).fetchall()
    big_plugin, big_n = counts[0]
    rare_plugin, rare_n = counts[-1]
    print(f"big : {big_plugin} ({big_n:,})")
    print(f"rare: {rare_plugin} ({rare_n:,})\n")

    cols = "time, name, plugin, vsn, value_float"
    cases = [
        ("metadata_only", f"select count(*) from read_parquet('{url}')"),
        ("full_scan", f"select {cols} from read_parquet('{url}')"),
        ("big_plugin", f"select {cols} from read_parquet('{url}') where plugin = '{big_plugin}'"),
        ("rare_plugin", f"select {cols} from read_parquet('{url}') where plugin = '{rare_plugin}'"),
        ("name_filter", f"select {cols} from read_parquet('{url}') where name = 'env.temperature'"),
        (
            "one_col_agg",
            f"select avg(value_float) from read_parquet('{url}') where plugin = '{big_plugin}'",
        ),
    ]

    results = []
    for label, sql in cases:
        reset()
        t0 = time.perf_counter()
        try:
            rows = con.execute(sql).fetchall()
            n = rows[0][0] if label in ("metadata_only", "one_col_agg") else len(rows)
            err = None
        except Exception as e:  # noqa: BLE001
            n, err = None, str(e)[:200]
        dt = time.perf_counter() - t0
        s = snapshot()
        r = {
            "case": label,
            "rows": n,
            "error": err,
            "seconds": round(dt, 3),
            "http_requests": s["requests"],
            "http_bytes": s["bytes"],
            "pct_of_file": round(100.0 * s["bytes"] / size, 2),
        }
        results.append(r)
        print(json.dumps(r))

    print()
    print(f"{'case':16s} {'rows':>12s} {'reqs':>6s} {'bytes':>14s} {'% file':>8s} {'sec':>7s}")
    for r in results:
        rows = f"{r['rows']:,}" if r["rows"] is not None else "ERR"
        print(
            f"{r['case']:16s} {rows:>12s} {r['http_requests']:>6d} {r['http_bytes']:>14,} "
            f"{r['pct_of_file']:>7.2f}% {r['seconds']:>7.2f}"
        )

    # Show the request pattern for the most selective case: this is what a
    # Pelican-backed read would look like on the wire.
    reset()
    con.execute(
        f"select {cols} from read_parquet('{url}') where plugin = '{rare_plugin}'"
    ).fetchall()
    s = snapshot()
    print(f"\nrare_plugin request pattern ({s['requests']} requests):")
    for rg in s["ranges"][:25]:
        print(f"  {rg['range_hdr'] or 'full':>28s}  -> {rg['len']:>12,} bytes")

    with open("results/exp03_range_reads.json", "w") as f:
        json.dump(
            {
                "file": local,
                "file_size": size,
                "big_plugin": big_plugin,
                "rare_plugin": rare_plugin,
                "results": results,
                "rare_plugin_ranges": s["ranges"][:200],
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
