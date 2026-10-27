#!/usr/bin/env python
"""
Experiment 46: Arrow IPC stream versus newline JSON on the local demo API.

Times the same POST /query both ways, from the request until a pandas
dataframe exists. Arrow uses sage-data-client's _load_arrow. Newline JSON
uses load. The server does not gzip; gzip MB is the raw body compressed
afterward at level 6.

Needs demo-influxdb and demo-data-api up (docker compose --profile cold),
and the API image built with -tags=duckdb_arrow. Docker socket access is
required because server time is read from `docker logs demo-data-api`.

  python -I exp46_arrow_vs_ndjson.py
  python -I exp46_arrow_vs_ndjson.py --endpoint http://127.0.0.1:10000/api/v1/query

One name is wxt.env.temp on vsn W0A0. All names is every measurement on
W0A0. Hot is Influx on 2025-05-01. Cold is Iceberg on 2025-03-01. The
watermark in the demo that produced results/exp46_arrow_vs_ndjson.json was
2025-05-01T00:00:00Z.
"""
import argparse
import gzip
import io
import json
import os
import re
import statistics
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

# The local client, not whatever sage_data_client is installed in the env.
# __init__ re-exports query() and shadows the submodule, so import the module
# by name after the source tree is first on the path.
_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "sage-data-client", "src")
)
sys.path.insert(0, _CLIENT_SRC)
import importlib  # noqa: E402

query_mod = importlib.import_module("sage_data_client.query")
_load_arrow = query_mod._load_arrow
load = query_mod.load

REPS = 3
TIMEOUT = 240

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


def once(endpoint, fmt, start, end, filt):
    accept = (
        "application/vnd.apache.arrow.stream"
        if fmt == "arrow"
        else "application/x-ndjson"
    )
    body_json = json.dumps({"start": start, "end": end, "filter": filt}).encode()
    req = Request(endpoint, body_json, {"Accept": accept, "Content-Type": "application/json"})
    previous = last_served()
    t0 = time.perf_counter()
    with urlopen(req, timeout=TIMEOUT) as resp:
        ttfb = time.perf_counter() - t0
        raw = resp.read()
        downloaded = time.perf_counter() - t0
        ctype = resp.headers.get("Content-Type", "")
    if fmt == "arrow":
        frame = _load_arrow(io.BytesIO(raw))
    else:
        frame = load(io.BytesIO(raw))
    total = time.perf_counter() - t0
    served_rows, served_s, served_line = server_time(previous)
    gz = len(gzip.compress(raw, compresslevel=6))
    return {
        "rows": len(frame),
        "bytes": len(raw),
        "gzip": gz,
        "ttfb": ttfb,
        "download": downloaded,
        "decode": total - downloaded,
        "total": total,
        "server": served_s,
        "served_rows": served_rows,
        "ctype": ctype.split(";")[0],
        "log": served_line,
    }


def median(rows, key):
    vals = [r[key] for r in rows if r[key] is not None]
    if not vals:
        return None
    return statistics.median(vals)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="http://127.0.0.1:10000/api/v1/query")
    args = ap.parse_args()

    print(f"client {query_mod.__file__}", flush=True)
    summaries = []
    for label, start, end, filt in CASES:
        print(f"\n== {label}", flush=True)
        by_fmt = {}
        try:
            for fmt in ("ndjson", "arrow"):
                warm = once(args.endpoint, fmt, start, end, filt)
                print(
                    f"  warmup {fmt:6} rows={warm['rows']} bytes={warm['bytes']} total={warm['total']:.3f}s server={warm['server']}",
                    flush=True,
                )
                runs = [once(args.endpoint, fmt, start, end, filt) for _ in range(REPS)]
                for i, run in enumerate(runs, 1):
                    srv = f"{run['server']:.3f}s" if run["server"] is not None else "?"
                    print(
                        f"  {fmt:6} #{i} rows={run['rows']:7d} raw={run['bytes']/1e6:7.2f}MB "
                        f"gzip={run['gzip']/1e6:6.2f}MB ttfb={run['ttfb']:.3f}s "
                        f"download={run['download']:.3f}s decode={run['decode']:.3f}s "
                        f"total={run['total']:.3f}s server={srv} type={run['ctype']}",
                        flush=True,
                    )
                by_fmt[fmt] = runs
        except HTTPError as exc:
            detail = exc.read(400).decode(errors="replace")
            print(f"  FAILED HTTP {exc.code}: {detail}", flush=True)
            subprocess.run(["docker", "ps", "--filter", "name=demo-data-api", "--format", "{{.Status}}"])
            summaries.append((label, None))
            continue
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
    for label, by_fmt in summaries:
        if not by_fmt:
            print(f"{label:22} FAILED", flush=True)
            continue
        for fmt in ("ndjson", "arrow"):
            runs = by_fmt[fmt]
            print(
                f"{label:22} {fmt:6} {runs[0]['rows']:8d} {median(runs, 'bytes')/1e6:8.2f} {median(runs, 'gzip')/1e6:8.2f} "
                f"{median(runs, 'ttfb'):7.3f} {median(runs, 'download'):8.3f} {median(runs, 'decode'):7.3f} "
                f"{median(runs, 'total'):7.3f} {median(runs, 'server'):7.3f}",
                flush=True,
            )
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
