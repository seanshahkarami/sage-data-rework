#!/usr/bin/env python
"""
Experiment 47: the same Arrow-versus-newline-JSON queries as exp46, without
sage-data-client.

curl performs the POST. DuckDB reads the body: read_arrow for the IPC stream
(community arrow extension) and read_ndjson_auto for newline JSON. The DuckDB
CLI is not installed in this environment; this is the DuckDB 1.5.6 SQL engine
via its Python package, which is the same reader.

Needs demo-influxdb and demo-data-api up, and the API image built with
-tags=duckdb_arrow. Server time is read from `docker logs demo-data-api`.

  python -I exp47_arrow_vs_ndjson_curl.py

The first run may need network once, to install the DuckDB arrow extension:

  INSTALL arrow FROM community; LOAD arrow;
"""
import argparse
import gzip
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError

import duckdb

REPS = 3
TIMEOUT = "240"

CASES = [
    ("hot 1min temp+vsn", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", {"name": "wxt.env.temp", "vsn": "W0A0"}),
    ("hot 1hour temp+vsn", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", {"name": "wxt.env.temp", "vsn": "W0A0"}),
    ("hot 6hour temp+vsn", "2025-05-01T00:00:00Z", "2025-05-01T06:00:00Z", {"name": "wxt.env.temp", "vsn": "W0A0"}),
    ("cold 1min temp+vsn", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", {"name": "wxt.env.temp", "vsn": "W0A0"}),
    ("cold 1hour temp+vsn", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", {"name": "wxt.env.temp", "vsn": "W0A0"}),
    ("hot 1min all names", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", {"vsn": "W0A0"}),
    ("cold 1min all names", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", {"vsn": "W0A0"}),
    ("hot 1hour all names", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", {"vsn": "W0A0"}),
    ("cold 1hour all names", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", {"vsn": "W0A0"}),
]

DUR = re.compile(r"([0-9.]+)(ms|µs|us|ns|m|s|h)")


def parse_go_duration(text):
    total = 0.0
    for num, unit in DUR.findall(text):
        scale = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}[unit]
        total += float(num) * scale
    return total


def last_served():
    out = subprocess.check_output(
        ["docker", "logs", "demo-data-api", "--tail", "30"],
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines = [ln for ln in out.splitlines() if " served " in ln]
    return lines[-1] if lines else ""


def server_time(previous):
    for _ in range(50):
        line = last_served()
        if line and line != previous:
            match = re.search(r"served (\d+) records in (\S+)", line)
            if match:
                return int(match.group(1)), parse_go_duration(match.group(2)), line
        time.sleep(0.05)
    return None, None, last_served()


def curl_post(endpoint, accept, body, dest):
    previous = last_served()
    proc = subprocess.run(
        [
            "curl", "-sS", "--max-time", TIMEOUT,
            "-o", dest,
            "-w", "%{time_starttransfer} %{time_total} %{size_download} %{http_code}",
            "-H", f"Accept: {accept}",
            "-H", "Content-Type: application/json",
            "--data-binary", body,
            endpoint,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise URLError(proc.stderr.strip() or f"curl exit {proc.returncode}")
    ttfb, total, nbytes, code = proc.stdout.split()
    if code != "200":
        with open(dest, "rb") as fh:
            detail = fh.read(400).decode(errors="replace")
        raise RuntimeError(f"HTTP {code}: {detail}")
    served_rows, served_s, served_line = server_time(previous)
    return {
        "ttfb": float(ttfb),
        "download": float(total),
        "bytes": int(float(nbytes)),
        "server": served_s,
        "served_rows": served_rows,
        "log": served_line,
    }


def duckdb_read(con, fmt, path):
    # count(value) and count(meta) force DuckDB to read those columns.
    if fmt == "arrow":
        sql = f"SELECT count(*), count(value), count(meta) FROM read_arrow('{path}')"
    else:
        sql = f"SELECT count(*), count(value), count(meta) FROM read_ndjson_auto('{path}')"
    t0 = time.perf_counter()
    rows, values, metas = con.sql(sql).fetchone()
    return time.perf_counter() - t0, int(rows), int(values), int(metas)


def once(con, endpoint, fmt, start, end, filt, dest):
    accept = (
        "application/vnd.apache.arrow.stream"
        if fmt == "arrow"
        else "application/x-ndjson"
    )
    body = json.dumps({"start": start, "end": end, "filter": filt})
    fetched = curl_post(endpoint, accept, body, dest)
    decode, rows, values, metas = duckdb_read(con, fmt, dest)
    with open(dest, "rb") as fh:
        gz = len(gzip.compress(fh.read(), compresslevel=6))
    return {
        "rows": rows,
        "values": values,
        "metas": metas,
        "bytes": fetched["bytes"],
        "gzip": gz,
        "ttfb": fetched["ttfb"],
        "download": fetched["download"],
        "decode": decode,
        "total": fetched["download"] + decode,
        "server": fetched["server"],
        "served_rows": fetched["served_rows"],
        "log": fetched["log"],
    }


def median(rows, key):
    vals = [r[key] for r in rows if r[key] is not None]
    if not vals:
        return None
    return statistics.median(vals)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    ap.add_argument(
        "--json-out",
        default=os.path.join(os.path.dirname(__file__), "results", "exp47_arrow_vs_ndjson_curl.json"),
    )
    args = ap.parse_args()

    con = duckdb.connect()
    con.sql("INSTALL arrow FROM community")
    con.sql("LOAD arrow")
    print(f"duckdb {duckdb.__version__} arrow {con.sql('SELECT nanoarrow_version()').fetchone()[0]}", flush=True)

    summaries = []
    with tempfile.TemporaryDirectory(prefix="exp47-") as tmp:
        for label, start, end, filt in CASES:
            print(f"\n== {label}", flush=True)
            by_fmt = {}
            try:
                for fmt in ("ndjson", "arrow"):
                    dest = os.path.join(tmp, f"body.{fmt}")
                    warm = once(con, args.endpoint, fmt, start, end, filt, dest)
                    print(
                        f"  warmup {fmt:6} rows={warm['rows']} bytes={warm['bytes']} total={warm['total']:.3f}s server={warm['server']}",
                        flush=True,
                    )
                    runs = [once(con, args.endpoint, fmt, start, end, filt, dest) for _ in range(REPS)]
                    for i, run in enumerate(runs, 1):
                        srv = f"{run['server']:.3f}s" if run["server"] is not None else "?"
                        print(
                            f"  {fmt:6} #{i} rows={run['rows']:7d} raw={run['bytes']/1e6:7.2f}MB "
                            f"gzip={run['gzip']/1e6:6.2f}MB ttfb={run['ttfb']:.3f}s "
                            f"download={run['download']:.3f}s decode={run['decode']:.3f}s "
                            f"total={run['total']:.3f}s server={srv}",
                            flush=True,
                        )
                    by_fmt[fmt] = runs
            except Exception as exc:
                print(f"  FAILED {type(exc).__name__}: {exc}", flush=True)
                subprocess.run(["docker", "ps", "--filter", "name=demo-data-api", "--format", "{{.Status}}"])
                summaries.append((label, None))
                continue
            nd, ar = by_fmt["ndjson"], by_fmt["arrow"]
            if nd[0]["rows"] != ar[0]["rows"]:
                print(f"  ROW MISMATCH ndjson={nd[0]['rows']} arrow={ar[0]['rows']}", flush=True)
            summaries.append((label, by_fmt))

    print("\n== median of measured runs (warmup excluded)", flush=True)
    header = f"{'query':22} {'fmt':6} {'rows':>8} {'raw MB':>8} {'gzip MB':>8} {'ttfb':>7} {'download':>8} {'decode':>7} {'total':>7} {'server':>7}"
    print(header, flush=True)
    medians = []
    for label, by_fmt in summaries:
        if not by_fmt:
            print(f"{label:22} FAILED", flush=True)
            continue
        for fmt in ("ndjson", "arrow"):
            runs = by_fmt[fmt]
            row = {
                "query": label,
                "fmt": fmt,
                "rows": runs[0]["rows"],
                "raw_mb": round(median(runs, "bytes") / 1e6, 2),
                "gzip_mb": round(median(runs, "gzip") / 1e6, 2),
                "ttfb_s": round(median(runs, "ttfb"), 3),
                "download_s": round(median(runs, "download"), 3),
                "decode_s": round(median(runs, "decode"), 3),
                "total_s": round(median(runs, "total"), 3),
                "server_s": None if median(runs, "server") is None else round(median(runs, "server"), 3),
            }
            medians.append(row)
            print(
                f"{label:22} {fmt:6} {row['rows']:8d} {row['raw_mb']:8.2f} {row['gzip_mb']:8.2f} "
                f"{row['ttfb_s']:7.3f} {row['download_s']:8.3f} {row['decode_s']:7.3f} "
                f"{row['total_s']:7.3f} {row['server_s']:7.3f}",
                flush=True,
            )

    payload = {
        "when": "2026-10-20",
        "endpoint": args.endpoint,
        "reader": "curl + duckdb read_arrow / read_ndjson_auto",
        "duckdb": duckdb.__version__,
        "reps": REPS,
        "warmup_excluded_from_median": True,
        "gzip": "client-side gzip level 6 of the raw body; the server did not compress",
        "medians": medians,
    }
    os.makedirs(os.path.dirname(args.json_out), exist_ok=True)
    with open(args.json_out, "w") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    print(f"wrote {args.json_out}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
