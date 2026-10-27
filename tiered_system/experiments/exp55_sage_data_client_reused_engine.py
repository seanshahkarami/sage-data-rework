#!/usr/bin/env python
"""
Experiment 55: sage-data-client on the reused Arrow database.

Same windows as exp52. Latency is the time until the pandas frame exists.
Arrow uses _load_arrow. Newline JSON uses load. Short queries are one
warmup plus two measured runs. Each month is one run. query() itself
asks for Arrow first; this script requests one format at a time so the
two paths can be compared.

  python -I exp55_sage_data_client_reused_engine.py
"""
import gc
import importlib
import json
import os
import socket
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp52_arrow_vs_ndjson_rerun as exp52

_CLIENT_SRC = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "sage-data-client", "src")
)
sys.path.insert(0, _CLIENT_SRC)
query_mod = importlib.import_module("sage_data_client.query")


class CountingReader:
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


def once(endpoint, fmt, start, end, filt, timeout):
    accept = (
        "application/vnd.apache.arrow.stream"
        if fmt == "arrow"
        else "application/x-ndjson"
    )
    q = {
        "start": query_mod.timestr(query_mod.resolve_time(start)),
        "end": query_mod.timestr(query_mod.resolve_time(end)),
        "filter": filt,
    }
    req = Request(
        endpoint,
        json.dumps(q).encode(),
        {"Accept": accept, "Accept-Encoding": "gzip"},
    )
    previous = exp52.last_served()
    socket.setdefaulttimeout(timeout)
    # Peak samples docker stats on the side. Its shutdown calls docker stats
    # once more, and that call is not part of the query.
    t0 = time.perf_counter()
    peak = exp52.Peak()
    peak.__enter__()
    try:
        with urlopen(req, timeout=timeout) as resp:
            reader = CountingReader(resp)
            if fmt == "arrow":
                frame = query_mod._load_arrow(reader)
            else:
                frame = query_mod.load(reader)
            ttfb = reader.t_first - t0 if reader.t_first else None
            nbytes = reader.n
        total = time.perf_counter() - t0
    finally:
        peak.__exit__(None, None, None)
    rows = len(frame)
    del frame
    gc.collect()
    served_rows, served_s, served_line = exp52.server_time(previous)
    return {
        "rows": rows,
        "bytes": nbytes,
        "ttfb": ttfb,
        "total": total,
        "server": served_s,
        "served_rows": served_rows,
        "peak": peak.peak,
        "log": served_line,
    }


def pack(runs, warm):
    return {
        "rows": runs[0]["rows"],
        "bytes": exp52.median(runs, "bytes"),
        "ttfb": exp52.median(runs, "ttfb"),
        "total": exp52.median(runs, "total"),
        "server": exp52.median(runs, "server"),
        "peak": max(r["peak"] for r in runs),
        "warmup_total": None if warm is None else warm["total"],
        "reps": len(runs),
    }


def main():
    endpoint = "http://127.0.0.1:10000/api/v1/query"
    out = os.path.join(os.path.dirname(__file__), "results", "exp55_sage_data_client_reused_engine.json")
    print(f"client {query_mod.__file__}", flush=True)
    rows_out = []
    for label, start, end, filt, month, timeout in exp52.CASES:
        # A cold month through per-row JSON took about 68 minutes.
        limit = 10800 if month else int(timeout)
        print(f"\n== {label}", flush=True)
        by_fmt = {}
        failed = False
        for fmt in ("ndjson", "arrow"):
            try:
                warm = None
                if month:
                    run = once(endpoint, fmt, start, end, filt, limit)
                    runs = [run]
                    print(
                        f"  {fmt} rows={run['rows']} bytes={run['bytes']} ttfb={run['ttfb']:.3f}s "
                        f"total={run['total']:.3f}s server={run['server']} peak={run['peak']}",
                        flush=True,
                    )
                else:
                    warm = once(endpoint, fmt, start, end, filt, limit)
                    print(f"  warmup {fmt} rows={warm['rows']} total={warm['total']:.3f}s", flush=True)
                    runs = []
                    for i in range(1, exp52.SHORT_REPS + 1):
                        run = once(endpoint, fmt, start, end, filt, limit)
                        runs.append(run)
                        print(
                            f"  {fmt} #{i} rows={run['rows']} total={run['total']:.3f}s "
                            f"server={run['server']} peak={run['peak']}",
                            flush=True,
                        )
            except (HTTPError, URLError, OSError, MemoryError, TypeError, ValueError) as exc:
                detail = exc
                if isinstance(exc, HTTPError):
                    detail = f"HTTP {exc.code}: {exc.read(400).decode(errors='replace')}"
                print(f"  {fmt} FAILED {type(exc).__name__}: {detail}", flush=True)
                by_fmt[fmt] = {"error": str(detail)}
                failed = True
                continue
            by_fmt[fmt] = pack(runs, warm)
        if failed or by_fmt["ndjson"]["rows"] != by_fmt["arrow"]["rows"]:
            rows_out.append({"query": label, "start": start, "end": end, "filter": filt, "error": by_fmt})
            if not failed:
                print(
                    f"  ROW MISMATCH ndjson={by_fmt['ndjson']['rows']} arrow={by_fmt['arrow']['rows']}",
                    flush=True,
                )
        else:
            nd, ar = by_fmt["ndjson"], by_fmt["arrow"]
            speedup = nd["total"] / ar["total"] if ar["total"] else None
            rows_out.append({
                "query": label,
                "start": start,
                "end": end,
                "filter": filt,
                "rows": nd["rows"],
                "ndjson": nd,
                "arrow": ar,
                "speedup": speedup,
            })
            print(f"  speedup {speedup:.2f}x", flush=True)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as fh:
            json.dump(
                {
                    "when": "2026-10-21",
                    "endpoint": endpoint,
                    "reader": "sage-data-client load / _load_arrow",
                    "client": query_mod.__file__,
                    "image": "demo-data-api with one native Arrow DuckDB per cold backend",
                    "short_reps": exp52.SHORT_REPS,
                    "month_reps": 1,
                    "warmup_excluded_from_short_median": True,
                    "latency": "request start until the pandas dataframe exists",
                    "peak_memory": "max sampled API container usage during the call",
                    "results": rows_out,
                },
                fh,
                indent=2,
            )
            fh.write("\n")
    print("wrote", out, flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
