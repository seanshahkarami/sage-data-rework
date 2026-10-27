#!/usr/bin/env python
"""
Experiment 52: Arrow versus newline JSON on the streaming cold reader.

Same windows as exp47, plus the May hot month and the March cold month.
curl downloads the body. DuckDB 1.5.6 counts the rows. Latency is download
plus that count. Peak memory is the API container during the download.
One warmup plus two measured runs for the short queries. One measured run
for each month. The server does not gzip.

  python -I exp52_arrow_vs_ndjson_rerun.py
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import tempfile
import threading
import time
import zlib
from urllib.error import URLError

import duckdb

SHORT_REPS = 2
DUR = re.compile(r"([0-9.]+)(ms|µs|us|ns|m|s|h)")
MEM = re.compile(r"([0-9.]+)([KMGT]iB|B)")
ONE = {"name": "wxt.env.temp", "vsn": "W0A0"}
ALL = {"vsn": "W0A0"}
CASES = [
    ("hot 1 min, one name", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", ONE, False, "180"),
    ("hot 1 hour, one name", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", ONE, False, "180"),
    ("hot 6 hours, one name", "2025-05-01T00:00:00Z", "2025-05-01T06:00:00Z", ONE, False, "300"),
    ("hot 1 min, all names", "2025-05-01T00:00:00Z", "2025-05-01T00:01:00Z", ALL, False, "180"),
    ("hot 1 hour, all names", "2025-05-01T00:00:00Z", "2025-05-01T01:00:00Z", ALL, False, "300"),
    ("cold 1 min, one name", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", ONE, False, "180"),
    ("cold 1 hour, one name", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", ONE, False, "180"),
    ("cold 1 min, all names", "2025-03-01T00:00:00Z", "2025-03-01T00:01:00Z", ALL, False, "180"),
    ("cold 1 hour, all names", "2025-03-01T00:00:00Z", "2025-03-01T01:00:00Z", ALL, False, "300"),
    ("hot May, one name", "2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z", ONE, True, "1200"),
    ("cold March, one name", "2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z", ONE, True, "1200"),
]


def parse_go_duration(text):
    total = 0.0
    for num, unit in DUR.findall(text):
        scale = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "us": 1e-6, "µs": 1e-6, "ns": 1e-9}[unit]
        total += float(num) * scale
    return total


def mem_bytes(text):
    match = MEM.search(text or "")
    if not match:
        return 0
    scale = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}
    return float(match.group(1)) * scale[match.group(2)]


def docker_text(*args):
    try:
        return subprocess.check_output(["docker", *args], stderr=subprocess.STDOUT, text=True)
    except (OSError, subprocess.CalledProcessError):
        return subprocess.check_output(["rdctl", "shell", "--", "docker", *args], stderr=subprocess.STDOUT, text=True)


def last_served():
    try:
        out = docker_text("logs", "demo-data-api", "--tail", "40")
    except (OSError, subprocess.CalledProcessError):
        return ""
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


def api_mem_bytes():
    try:
        out = docker_text("stats", "--no-stream", "--format", "{{.MemUsage}}", "demo-data-api")
    except (OSError, subprocess.CalledProcessError):
        return 0
    return mem_bytes(out.split("/")[0])


class Peak:
    def __init__(self):
        self.stop = threading.Event()
        self.peak = 0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop.is_set():
            self.peak = max(self.peak, api_mem_bytes())
            self.stop.wait(1.0)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.peak = max(self.peak, api_mem_bytes())
        self.stop.set()
        self.thread.join(timeout=5)


def gzip_size(path):
    comp = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    n = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            n += len(comp.compress(chunk))
    return n + len(comp.flush())


def curl_post(endpoint, accept, body, dest, timeout):
    previous = last_served()
    with Peak() as peak:
        proc = subprocess.run(
            [
                "curl", "-sS", "--max-time", timeout,
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
        "peak": peak.peak,
    }


def duckdb_read(con, fmt, path):
    if fmt == "arrow":
        sql = f"SELECT count(*), count(value), count(meta) FROM read_arrow('{path}')"
    else:
        sql = f"SELECT count(*), count(value), count(meta) FROM read_ndjson_auto('{path}')"
    t0 = time.perf_counter()
    rows, values, metas = con.sql(sql).fetchone()
    return time.perf_counter() - t0, int(rows), int(values), int(metas)


def once(con, endpoint, fmt, start, end, filt, dest, timeout):
    accept = "application/vnd.apache.arrow.stream" if fmt == "arrow" else "application/x-ndjson"
    body = json.dumps({"start": start, "end": end, "filter": filt})
    fetched = curl_post(endpoint, accept, body, dest, timeout)
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
        "peak": fetched["peak"],
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
        default=os.path.join(os.path.dirname(__file__), "results", "exp52_arrow_vs_ndjson_rerun.json"),
    )
    args = ap.parse_args()
    con = duckdb.connect()
    con.sql("INSTALL arrow FROM community")
    con.sql("LOAD arrow")
    print("duckdb", duckdb.__version__, flush=True)
    rows_out = []
    with tempfile.TemporaryDirectory(prefix="exp52-") as tmp:
        for label, start, end, filt, month, timeout in CASES:
            print(f"\n== {label}", flush=True)
            by_fmt = {}
            for fmt in ("ndjson", "arrow"):
                dest = os.path.join(tmp, f"{label.replace(' ', '_')}.{fmt}")
                try:
                    if month:
                        run = once(con, args.endpoint, fmt, start, end, filt, dest, timeout)
                        runs = [run]
                        print(
                            f"  {fmt} rows={run['rows']} bytes={run['bytes']} ttfb={run['ttfb']:.3f}s "
                            f"download={run['download']:.3f}s decode={run['decode']:.3f}s "
                            f"total={run['total']:.3f}s server={run['server']} peak={run['peak']}",
                            flush=True,
                        )
                    else:
                        warm = once(con, args.endpoint, fmt, start, end, filt, dest, timeout)
                        print(f"  warmup {fmt} rows={warm['rows']} total={warm['total']:.3f}s", flush=True)
                        runs = []
                        for i in range(1, SHORT_REPS + 1):
                            run = once(con, args.endpoint, fmt, start, end, filt, dest, timeout)
                            runs.append(run)
                            print(
                                f"  {fmt} #{i} rows={run['rows']} total={run['total']:.3f}s "
                                f"server={run['server']} peak={run['peak']}",
                                flush=True,
                            )
                    print("  gzipping...", flush=True)
                    gz = gzip_size(dest)
                except (URLError, OSError, RuntimeError, duckdb.Error) as exc:
                    print(f"  {fmt} FAILED {type(exc).__name__}: {exc}", flush=True)
                    by_fmt[fmt] = {"error": str(exc)}
                    continue
                by_fmt[fmt] = {
                    "rows": runs[0]["rows"],
                    "bytes": median(runs, "bytes"),
                    "gzip": gz,
                    "ttfb": median(runs, "ttfb"),
                    "download": median(runs, "download"),
                    "decode": median(runs, "decode"),
                    "total": median(runs, "total"),
                    "server": median(runs, "server"),
                    "peak": max(r["peak"] for r in runs),
                    "reps": len(runs),
                }
            if "error" in by_fmt.get("ndjson", {}) or "error" in by_fmt.get("arrow", {}):
                rows_out.append({"query": label, "start": start, "end": end, "filter": filt, "error": by_fmt})
            else:
                nd, ar = by_fmt["ndjson"], by_fmt["arrow"]
                speedup = None if not ar["total"] else nd["total"] / ar["total"]
                rows_out.append({
                    "query": label,
                    "start": start,
                    "end": end,
                    "filter": filt,
                    "rows": nd["rows"],
                    "arrow_rows": ar["rows"],
                    "ndjson": nd,
                    "arrow": ar,
                    "speedup": speedup,
                })
                print(
                    f"  speedup {speedup:.2f}x rows ndjson={nd['rows']} arrow={ar['rows']}",
                    flush=True,
                )
            os.makedirs(os.path.dirname(args.json_out), exist_ok=True)
            with open(args.json_out, "w") as fh:
                json.dump(
                    {
                        "when": "2026-10-21",
                        "endpoint": args.endpoint,
                        "reader": "curl + duckdb read_arrow / read_ndjson_auto",
                        "duckdb": duckdb.__version__,
                        "short_reps": SHORT_REPS,
                        "month_reps": 1,
                        "warmup_excluded_from_short_median": True,
                        "peak_memory": "max sampled API container usage during the download",
                        "gzip": "level 6 of the last body, not sent by the server, not included in latency",
                        "results": rows_out,
                    },
                    fh,
                    indent=2,
                )
                fh.write("\n")
    print(f"wrote {args.json_out}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
