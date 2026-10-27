#!/usr/bin/env python
"""
Experiment 50: one month through sage-data-client, Arrow versus newline JSON.

Same windows as exp48 and exp49. Hot is May 2025 in Influx. Cold is March
2025 in Iceberg. One series: wxt.env.temp on vsn W0A0. Time is from the
request until the client's dataframe exists. Arrow uses _load_arrow, the
path query() takes when the response is an Arrow stream. Newline JSON uses
load. One pass each. The server does not gzip.

  python -I exp50_month_sage_data_client.py
  python -I exp50_month_sage_data_client.py --only hot --only-fmt ndjson
"""
import argparse
import gc
import importlib
import json
import os
import re
import resource
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "sage-data-client", "src")
)
sys.path.insert(0, _CLIENT_SRC)
query_mod = importlib.import_module("sage_data_client.query")

TIMEOUT = 720
FILT = {"name": "wxt.env.temp", "vsn": "W0A0"}
CASES = [
    ("hot May temp+vsn", "2025-05-01T00:00:00Z", "2025-06-01T00:00:00Z"),
    ("cold March temp+vsn", "2025-03-01T00:00:00Z", "2025-04-01T00:00:00Z"),
]
DUR = re.compile(r"([0-9.]+)(ms|µs|us|ns|m|s|h)")


class CountingReader:
    """File-like wrapper that counts bytes and marks the first one."""

    def __init__(self, raw):
        self.raw = raw
        self.n = 0
        self.t_first = None

    def _mark(self, n):
        if n and self.t_first is None:
            self.t_first = time.perf_counter()
        self.n += n

    def read(self, size=-1):
        data = self.raw.read(size)
        self._mark(len(data))
        return data

    def readinto(self, buf):
        data = self.raw.read(len(buf))
        n = len(data)
        buf[:n] = data
        self._mark(n)
        return n

    def readline(self, size=-1):
        data = self.raw.readline() if size is None or size < 0 else self.raw.readline(size)
        self._mark(len(data))
        return data

    def readable(self):
        return True

    @property
    def closed(self):
        return getattr(self.raw, "closed", False)

    def __iter__(self):
        return self

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def close(self):
        return self.raw.close()


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


def rss_gb():
    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return raw / 1e9
    return raw / 1e6


def once(endpoint, fmt, start, end):
    accept = (
        "application/vnd.apache.arrow.stream"
        if fmt == "arrow"
        else "application/x-ndjson"
    )
    q = {
        "start": query_mod.timestr(query_mod.resolve_time(start)),
        "end": query_mod.timestr(query_mod.resolve_time(end)),
        "filter": FILT,
    }
    req = Request(
        endpoint,
        json.dumps(q).encode(),
        {"Accept": accept, "Accept-Encoding": "gzip"},
    )
    previous = last_served()
    socket.setdefaulttimeout(TIMEOUT)
    t0 = time.perf_counter()
    with urlopen(req, timeout=TIMEOUT) as resp:
        reader = CountingReader(resp)
        body = reader
        encoding = resp.headers.get("Content-Encoding", "")
        if "gzip" in encoding:
            from gzip import GzipFile

            body = GzipFile(fileobj=reader, mode="rb")
        if fmt == "arrow":
            frame = query_mod._load_arrow(body)
        else:
            frame = query_mod.load(body)
        downloaded = reader.t_first - t0 if reader.t_first else None
    total = time.perf_counter() - t0
    rows = len(frame)
    sample = None
    if rows:
        sample = {
            "value": frame.iloc[0].value,
            "value_type": type(frame.iloc[0].value).__name__,
            "meta.vsn": frame.iloc[0]["meta.vsn"] if "meta.vsn" in frame.columns else None,
        }
    del frame
    gc.collect()
    served_rows, served_s, served_line = server_time(previous)
    return {
        "rows": rows,
        "bytes": reader.n,
        "ttfb": downloaded,
        "decode_and_rest": None if downloaded is None else total - downloaded,
        "total": total,
        "server": served_s,
        "served_rows": served_rows,
        "log": served_line,
        "sample": sample,
        "rss_gb": round(rss_gb(), 2),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    ap.add_argument("--only", choices=("hot", "cold"))
    ap.add_argument("--only-fmt", choices=("ndjson", "arrow"))
    ap.add_argument(
        "--json-out",
        default=os.path.join(os.path.dirname(__file__), "results", "exp50_month_sage_data_client.json"),
    )
    args = ap.parse_args()
    print(f"client {query_mod.__file__}", flush=True)
    results = []
    cases = CASES
    if args.only == "hot":
        cases = CASES[:1]
    elif args.only == "cold":
        cases = CASES[1:]
    formats = ("ndjson", "arrow") if args.only_fmt is None else (args.only_fmt,)
    for label, start, end in cases:
        print(f"\n== {label} {start} .. {end}", flush=True)
        for fmt in formats:
            print(f"  {fmt} ...", flush=True)
            try:
                run = once(args.endpoint, fmt, start, end)
            except (HTTPError, URLError, OSError, MemoryError, TypeError, ValueError) as exc:
                detail = exc
                if isinstance(exc, HTTPError):
                    detail = f"HTTP {exc.code}: {exc.read(400).decode(errors='replace')}"
                print(f"  {fmt} FAILED {type(exc).__name__}: {detail}", flush=True)
                results.append({"query": label, "start": start, "end": end, "fmt": fmt, "error": str(detail)})
                continue
            srv = f"{run['server']:.3f}s" if run["server"] is not None else "?"
            ttfb = f"{run['ttfb']:.3f}s" if run["ttfb"] is not None else "?"
            print(
                f"  {fmt} rows={run['rows']} bytes={run['bytes']} ttfb={ttfb} "
                f"total={run['total']:.3f}s server={srv} rss={run['rss_gb']}GB sample={run['sample']}",
                flush=True,
            )
            results.append(
                {
                    "query": label,
                    "start": start,
                    "end": end,
                    "filter": FILT,
                    "fmt": fmt,
                    "rows": run["rows"],
                    "bytes": run["bytes"],
                    "ttfb_s": None if run["ttfb"] is None else round(run["ttfb"], 3),
                    "total_s": round(run["total"], 3),
                    "server_s": None if run["server"] is None else round(run["server"], 3),
                    "rss_gb": run["rss_gb"],
                    "sample": run["sample"],
                }
            )
            os.makedirs(os.path.dirname(args.json_out), exist_ok=True)
            with open(args.json_out, "w") as fh:
                json.dump(
                    {
                        "when": "2026-10-20",
                        "endpoint": args.endpoint,
                        "reader": "sage-data-client load / _load_arrow",
                        "client": query_mod.__file__,
                        "reps": 1,
                        "results": results,
                    },
                    fh,
                    indent=2,
                )
                fh.write("\n")
    print(f"wrote {args.json_out}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
