#!/usr/bin/env python
"""
Experiment 49: Arrow versus newline JSON on one cold month.

The month is March 2025 in the live Iceberg table, one series: wxt.env.temp
on vsn W0A0, from 2025-03-01T00:00:00Z to 2025-04-01T00:00:00Z. A direct count
of that series was 25,940,207 rows and 0.41 GB of row groups (plans/7). The
watermark is 2025-05-01, so this whole window is cold. A month of every name
is not this query: a full cold day of one node has already OOM-killed the API.

Each query mints a 20 minute Pelican token. curl gives up at 18 minutes so a
slow scan fails before that token expires mid-body.

curl downloads. DuckDB 1.5.6 reads the body (read_arrow / read_ndjson_auto).
Gzip size is computed once per format, streaming, and is not part of the timed
total.

  python -I exp49_cold_month_arrow_vs_ndjson.py
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import tempfile
import time
import zlib
from urllib.error import URLError

import duckdb

REPS = 2
TIMEOUT = "1080"
START = "2025-03-01T00:00:00Z"
END = "2025-04-01T00:00:00Z"
FILT = {"name": "wxt.env.temp", "vsn": "W0A0"}
LABEL = "cold March temp+vsn"

DUR = re.compile(r"([0-9.]+)(ms|µs|us|ns|m|s|h)")


def parse_go_duration(text):
    total = 0.0
    for num, unit in DUR.findall(text):
        scale = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}[unit]
        total += float(num) * scale
    return total


def docker_text(*args):
    try:
        return subprocess.check_output(["docker", *args], stderr=subprocess.STDOUT, text=True)
    except (OSError, subprocess.CalledProcessError):
        return subprocess.check_output(["rdctl", "shell", "--", "docker", *args], stderr=subprocess.STDOUT, text=True)


def last_served():
    out = docker_text("logs", "demo-data-api", "--tail", "40")
    lines = [ln for ln in out.splitlines() if " served " in ln]
    return lines[-1] if lines else ""


def server_time(previous):
    for _ in range(80):
        line = last_served()
        if line and line != previous:
            match = re.search(r"served (\d+) records in (\S+)", line)
            if match:
                return int(match.group(1)), parse_go_duration(match.group(2)), line
        time.sleep(0.1)
    return None, None, last_served()


def container_mem():
    try:
        out = docker_text("stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}", "demo-data-api", "demo-influxdb")
    except (OSError, subprocess.CalledProcessError):
        return ""
    return " ".join(out.split())


def gzip_size(path):
    comp = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    n = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            n += len(comp.compress(chunk))
    n += len(comp.flush())
    return n


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
    parts = proc.stdout.split()
    if len(parts) != 4:
        raise RuntimeError(f"unexpected curl status line: {proc.stdout!r} err={proc.stderr!r}")
    ttfb, total, nbytes, code = parts
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
        "mem": container_mem(),
    }


def duckdb_read(con, fmt, path):
    if fmt == "arrow":
        sql = f"SELECT count(*), count(value), count(meta) FROM read_arrow('{path}')"
    else:
        sql = f"SELECT count(*), count(value), count(meta) FROM read_ndjson_auto('{path}')"
    t0 = time.perf_counter()
    rows, values, metas = con.sql(sql).fetchone()
    return time.perf_counter() - t0, int(rows), int(values), int(metas)


def once(con, endpoint, fmt, dest):
    accept = (
        "application/vnd.apache.arrow.stream"
        if fmt == "arrow"
        else "application/x-ndjson"
    )
    body = json.dumps({"start": START, "end": END, "filter": FILT})
    fetched = curl_post(endpoint, accept, body, dest)
    decode, rows, values, metas = duckdb_read(con, fmt, dest)
    return {
        "rows": rows,
        "values": values,
        "metas": metas,
        "bytes": fetched["bytes"],
        "ttfb": fetched["ttfb"],
        "download": fetched["download"],
        "decode": decode,
        "total": fetched["download"] + decode,
        "server": fetched["server"],
        "served_rows": fetched["served_rows"],
        "log": fetched["log"],
        "mem": fetched["mem"],
    }


def median(rows, key):
    vals = [r[key] for r in rows if r[key] is not None]
    if not vals:
        return None
    return statistics.median(vals)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    ap.add_argument("--reps", type=int, default=REPS)
    ap.add_argument("--only", choices=("ndjson", "arrow"))
    ap.add_argument(
        "--json-out",
        default=os.path.join(os.path.dirname(__file__), "results", "exp49_cold_month_arrow_vs_ndjson.json"),
    )
    args = ap.parse_args()

    con = duckdb.connect()
    con.sql("INSTALL arrow FROM community")
    con.sql("LOAD arrow")
    print(f"duckdb {duckdb.__version__} {LABEL} {START} .. {END} {FILT}", flush=True)

    by_fmt = {}
    gzip_mb = {}
    with tempfile.TemporaryDirectory(prefix="exp49-") as tmp:
        formats = (args.only,) if args.only else ("ndjson", "arrow")
        for fmt in formats:
            dest = os.path.join(tmp, f"body.{fmt}")
            print(f"\n== {fmt}", flush=True)
            warm = once(con, args.endpoint, fmt, dest)
            print(
                f"  warmup rows={warm['rows']} bytes={warm['bytes']} download={warm['download']:.3f}s "
                f"decode={warm['decode']:.3f}s total={warm['total']:.3f}s server={warm['server']} mem={warm['mem']}",
                flush=True,
            )
            runs = []
            if args.reps == 0:
                runs = [warm]
            for i in range(1, args.reps + 1):
                run = once(con, args.endpoint, fmt, dest)
                runs.append(run)
                srv = f"{run['server']:.3f}s" if run["server"] is not None else "?"
                print(
                    f"  #{i} rows={run['rows']:9d} raw={run['bytes']/1e9:6.2f}GB "
                    f"ttfb={run['ttfb']:.3f}s download={run['download']:.3f}s "
                    f"decode={run['decode']:.3f}s total={run['total']:.3f}s server={srv} mem={run['mem']}",
                    flush=True,
                )
            print("  gzipping...", flush=True)
            gz = gzip_size(dest)
            gzip_mb[fmt] = round(gz / 1e6, 2)
            print(f"  gzip {gzip_mb[fmt]:.2f} MB", flush=True)
            by_fmt[fmt] = runs
        if set(by_fmt) == {"ndjson", "arrow"} and by_fmt["ndjson"][0]["rows"] != by_fmt["arrow"][0]["rows"]:
            print(
                f"ROW MISMATCH ndjson={by_fmt['ndjson'][0]['rows']} arrow={by_fmt['arrow'][0]['rows']}",
                flush=True,
            )

    print("\n== median of measured runs (warmup excluded)", flush=True)
    medians = []
    for fmt in ("ndjson", "arrow"):
        if fmt not in by_fmt:
            continue
        runs = by_fmt[fmt]
        row = {
            "query": LABEL,
            "start": START,
            "end": END,
            "filter": FILT,
            "fmt": fmt,
            "rows": runs[0]["rows"],
            "raw_gb": round(median(runs, "bytes") / 1e9, 3),
            "gzip_mb": gzip_mb[fmt],
            "ttfb_s": round(median(runs, "ttfb"), 3),
            "download_s": round(median(runs, "download"), 3),
            "decode_s": round(median(runs, "decode"), 3),
            "total_s": round(median(runs, "total"), 3),
            "server_s": None if median(runs, "server") is None else round(median(runs, "server"), 3),
        }
        medians.append(row)
        print(
            f"{fmt:6} rows={row['rows']} raw={row['raw_gb']}GB gzip={row['gzip_mb']}MB "
            f"ttfb={row['ttfb_s']} download={row['download_s']} decode={row['decode_s']} "
            f"total={row['total_s']} server={row['server_s']}",
            flush=True,
        )

    payload = {
        "when": "2026-10-20",
        "endpoint": args.endpoint,
        "reader": "curl + duckdb read_arrow / read_ndjson_auto",
        "duckdb": duckdb.__version__,
        "reps": args.reps,
        "warmup_excluded_from_median": args.reps > 0,
        "gzip": "one streaming gzip level 6 of the last body of each format; not included in total_s; the server did not compress",
        "scope": "cold March 2025, wxt.env.temp on vsn W0A0, live Iceberg table. Not the whole fleet.",
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
