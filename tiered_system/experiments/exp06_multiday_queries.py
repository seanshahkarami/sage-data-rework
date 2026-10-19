#!/usr/bin/env python
"""
Experiment 6: the Phase 2 numbers plans/1_draft.md asks for, on real days.

  "Phase 2 numbers are not measured yet: bytes read and files opened for a
   short query and a year-long query, with and without a plugin filter;
   footer-open time versus one manifest."

exp05 proved row groups unlock pruning inside one file. This runs the same
question across a real multi-day archive slice, where two more costs appear
that a single file cannot show:

  - files opened: a 3-day query opens every data_N.parquet in 3 date= dirs
  - footer reads: each opened file costs a footer read before any data

Both are what the manifest idea in section 7 is meant to remove, so both need
a number before anyone builds a manifest.

Usage: python -I exp06_multiday_queries.py <archive-dir> [--days N]
"""
import json
import os
import re
import sys
import threading
import time
from collections import defaultdict
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn

STATS = {"requests": 0, "bytes": 0, "per_file": defaultdict(lambda: {"reqs": 0, "bytes": 0})}
LOCK = threading.Lock()


class TServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class H(SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _serve(self, want_body):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404)
            return
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end, partial = 0, size - 1, False
        if rng:
            m = re.match(r"bytes=(\d*)-(\d*)", rng.strip())
            if m:
                g1, g2 = m.group(1), m.group(2)
                if g1 == "":
                    start, end = max(0, size - int(g2)), size - 1
                else:
                    start = int(g1)
                    end = int(g2) if g2 else size - 1
                partial = True
        end = min(end, size - 1)
        length = max(0, end - start + 1)
        key = self.path
        with LOCK:
            STATS["requests"] += 1
            if want_body:
                STATS["bytes"] += length
                STATS["per_file"][key]["reqs"] += 1
                STATS["per_file"][key]["bytes"] += length
        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.end_headers()
        if want_body and length:
            with open(path, "rb") as f:
                f.seek(start)
                rem = length
                while rem > 0:
                    c = f.read(min(1 << 20, rem))
                    if not c:
                        break
                    self.wfile.write(c)
                    rem -= len(c)

    def do_GET(self):
        self._serve(True)

    def do_HEAD(self):
        self._serve(False)


def reset():
    with LOCK:
        STATS["requests"] = 0
        STATS["bytes"] = 0
        STATS["per_file"] = defaultdict(lambda: {"reqs": 0, "bytes": 0})


def snap():
    with LOCK:
        return STATS["requests"], STATS["bytes"], dict(STATS["per_file"])


def main():
    root = os.path.abspath(sys.argv[1])
    ndays = 3
    if "--days" in sys.argv:
        ndays = int(sys.argv[sys.argv.index("--days") + 1])

    days = sorted(d for d in os.listdir(root) if d.startswith("date="))
    # Only use days that are fully present locally.
    days = days[:ndays]
    glob_days = "{" + ",".join(days) + "}" if len(days) > 1 else days[0]

    nfiles = sum(len([f for f in os.listdir(os.path.join(root, d)) if f.endswith(".parquet")]) for d in days)
    total_bytes = sum(
        os.path.getsize(os.path.join(root, d, f))
        for d in days
        for f in os.listdir(os.path.join(root, d))
        if f.endswith(".parquet")
    )

    print(f"archive root : {root}")
    print(f"days queried : {len(days)}  ({days[0]} .. {days[-1]})")
    print(f"files        : {nfiles}")
    print(f"bytes on disk: {total_bytes:,}\n")

    os.chdir(root)
    httpd = TServer(("127.0.0.1", 0), H)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("PRAGMA threads=4")
    try:
        con.execute("SET enable_http_metadata_cache=false")
    except Exception:
        pass

    # Local file list for picking plugins (not measured).
    local_files = [
        os.path.join(root, d, f)
        for d in days
        for f in sorted(os.listdir(os.path.join(root, d)))
        if f.endswith(".parquet")
    ]
    local_list = "[" + ",".join(f"'{p}'" for p in local_files) + "]"
    counts = con.execute(
        f"select plugin, count(*) c from read_parquet({local_list}) group by plugin order by c desc"
    ).fetchall()
    big = counts[0][0]
    rare = counts[-1][0]
    print(f"big : {big} ({counts[0][1]:,})")
    print(f"rare: {rare} ({counts[-1][1]:,})\n")

    # HTTP needs an explicit file list; DuckDB cannot glob over http.
    urls = []
    for d in days:
        for f in sorted(os.listdir(os.path.join(root, d))):
            if f.endswith(".parquet"):
                urls.append(f"{base}/{d}/{f}")
    url_list = "[" + ",".join(f"'{u}'" for u in urls) + "]"

    cols = "time, name, plugin, vsn, value_float"
    t = f"read_parquet({url_list})"
    cases = [
        ("count_star", f"select count(*) from {t}"),
        # full_scan over the Python HTTP server is bandwidth-bound on the
        # server, not the reader, so it measures this script instead of the
        # layout. Selective cases are the ones that answer the question.
        ("big_plugin_cnt", f"select count(*) from {t} where plugin = '{big}'"),
        ("big_plugin", f"select {cols} from {t} where plugin = '{big}'"),
        ("rare_plugin", f"select {cols} from {t} where plugin = '{rare}'"),
        ("name_filter", f"select {cols} from {t} where name = 'env.temperature'"),
        (
            "one_hour",
            f"select {cols} from {t} where time >= TIMESTAMPTZ '{days[0][5:]} 12:00:00+00' "
            f"and time < TIMESTAMPTZ '{days[0][5:]} 13:00:00+00'",
        ),
    ]

    results = []
    for label, sql in cases:
        reset()
        t0 = time.perf_counter()
        try:
            rows = con.execute(sql).fetchall()
            n = rows[0][0] if label == "count_star" else len(rows)
            err = None
        except Exception as e:
            n, err = None, str(e)[:160]
        dt = time.perf_counter() - t0
        reqs, byts, per_file = snap()
        touched = len([k for k, v in per_file.items() if v["bytes"] > 0])
        r = {
            "case": label,
            "rows": n,
            "error": err,
            "seconds": round(dt, 2),
            "http_requests": reqs,
            "http_bytes": byts,
            "files_touched": touched,
            "pct_of_archive_slice": round(100.0 * byts / total_bytes, 2),
        }
        results.append(r)
        print(json.dumps(r))

    print()
    print(f"{'case':14s} {'rows':>13s} {'reqs':>6s} {'files':>6s} {'bytes':>15s} {'%slice':>8s} {'sec':>7s}")
    for r in results:
        rows = f"{r['rows']:,}" if r["rows"] is not None else "ERR"
        print(
            f"{r['case']:14s} {rows:>13s} {r['http_requests']:>6d} {r['files_touched']:>6d} "
            f"{r['http_bytes']:>15,} {r['pct_of_archive_slice']:>7.2f}% {r['seconds']:>7.2f}"
        )

    # Footer cost: how much is spent just opening files before any data moves.
    reset()
    con.execute(f"select count(*) from {t}").fetchall()
    reqs, byts, _ = snap()
    print(f"\nmetadata-only (count_star): {reqs} requests, {byts:,} bytes "
          f"over {nfiles} files  -> {byts/max(nfiles,1):,.0f} bytes/file of footer+index")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp06_multiday.json", "w") as f:
        json.dump(
            {
                "root": root,
                "days": days,
                "num_files": nfiles,
                "slice_bytes": total_bytes,
                "big_plugin": big,
                "rare_plugin": rare,
                "results": results,
                "footer_requests": reqs,
                "footer_bytes": byts,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
