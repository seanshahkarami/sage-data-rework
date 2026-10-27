#!/usr/bin/env python
"""
Experiment 54: cold short queries after Arrow reuses one DuckDB.

Same reader as exp52. Only the four cold windows that paid for a new
database and extension load on every Arrow request. One warmup plus two
measured runs. The first Arrow request of the process still installs
httpfs and iceberg. Later requests use the shared database.

  python -I exp54_arrow_engine_reuse_e2e.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp52_arrow_vs_ndjson_rerun as exp


def main():
    endpoint = "http://127.0.0.1:10000/api/v1/query"
    out = os.path.join(os.path.dirname(__file__), "results", "exp54_arrow_engine_reuse_e2e.json")
    cases = [c for c in exp.CASES if c[0].startswith("cold") and not c[4]]
    con = exp.duckdb.connect()
    con.sql("INSTALL arrow FROM community")
    con.sql("LOAD arrow")
    print("duckdb", exp.duckdb.__version__, "cases", [c[0] for c in cases], flush=True)
    rows_out = []
    with tempfile.TemporaryDirectory(prefix="exp54-") as tmp:
        for label, start, end, filt, _month, timeout in cases:
            print(f"\n== {label}", flush=True)
            by_fmt = {}
            for fmt in ("ndjson", "arrow"):
                dest = os.path.join(tmp, f"{label.replace(' ', '_')}.{fmt}")
                warm = exp.once(con, endpoint, fmt, start, end, filt, dest, timeout)
                print(
                    f"  warmup {fmt} rows={warm['rows']} total={warm['total']:.3f}s server={warm['server']}",
                    flush=True,
                )
                runs = []
                for i in range(1, exp.SHORT_REPS + 1):
                    run = exp.once(con, endpoint, fmt, start, end, filt, dest, timeout)
                    runs.append(run)
                    print(
                        f"  {fmt} #{i} rows={run['rows']} total={run['total']:.3f}s "
                        f"server={run['server']} peak={run['peak']}",
                        flush=True,
                    )
                by_fmt[fmt] = {
                    "rows": runs[0]["rows"],
                    "values": runs[0]["values"],
                    "metas": runs[0]["metas"],
                    "bytes": exp.median(runs, "bytes"),
                    "ttfb": exp.median(runs, "ttfb"),
                    "download": exp.median(runs, "download"),
                    "decode": exp.median(runs, "decode"),
                    "total": exp.median(runs, "total"),
                    "server": exp.median(runs, "server"),
                    "peak": max(r["peak"] for r in runs),
                    "warmup_total": warm["total"],
                    "warmup_server": warm["server"],
                    "reps": len(runs),
                }
            nd, ar = by_fmt["ndjson"], by_fmt["arrow"]
            if nd["rows"] != ar["rows"]:
                raise SystemExit(f"row mismatch {label}: {nd['rows']} vs {ar['rows']}")
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
                        "image": "demo-data-api rebuilt with one native Arrow DuckDB per cold backend",
                        "endpoint": endpoint,
                        "reader": "exp52 curl + duckdb read_arrow / read_ndjson_auto",
                        "duckdb": exp.duckdb.__version__,
                        "short_reps": exp.SHORT_REPS,
                        "warmup_excluded_from_median": True,
                        "note": "The first Arrow request of the process installs httpfs and iceberg. Medians exclude that warmup.",
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
