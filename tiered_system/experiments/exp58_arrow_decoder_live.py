#!/usr/bin/env python
"""
Experiment 58: sage-data-client Arrow after the batched decoder.

Same windows as exp55. Newline JSON is unchanged, so this run requests
only the Arrow stream and loads it with the workspace _load_arrow.
Short queries are one warmup plus two measured runs. Each month is one
run. Client RSS is sampled with ps during the call. Major page faults
are the increase in ru_majflt across that call.

  python -I exp58_arrow_decoder_live.py
"""
import json
import os
import resource
import subprocess
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp52_arrow_vs_ndjson_rerun as exp52
import exp55_sage_data_client_reused_engine as exp55


class ClientRss:
    def __init__(self):
        self.peak = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        pid = str(os.getpid())
        while not self.stop.is_set():
            try:
                out = subprocess.check_output(["ps", "-o", "rss=", "-p", pid], text=True)
                self.peak = max(self.peak, int(out.split()[0]))
            except (OSError, subprocess.CalledProcessError, ValueError, IndexError):
                pass
            self.stop.wait(0.25)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=2)


def main():
    endpoint = "http://127.0.0.1:10000/api/v1/query"
    out = os.path.join(os.path.dirname(__file__), "results", "exp58_arrow_decoder_live.json")
    print(f"client {exp55.query_mod.__file__}", flush=True)
    rows_out = []
    for label, start, end, filt, month, timeout in exp52.CASES:
        limit = 10800 if month else int(timeout)
        print(f"\n== {label}", flush=True)
        try:
            warm = None
            if month:
                before = resource.getrusage(resource.RUSAGE_SELF).ru_majflt
                with ClientRss() as rss:
                    run = exp55.once(endpoint, "arrow", start, end, filt, limit)
                run["client_rss"] = rss.peak * 1024
                run["majflt"] = resource.getrusage(resource.RUSAGE_SELF).ru_majflt - before
                runs = [run]
                print(
                    f"  arrow rows={run['rows']} bytes={run['bytes']} ttfb={run['ttfb']:.3f}s "
                    f"total={run['total']:.3f}s server={run['server']} api_peak={run['peak']} "
                    f"client_rss={run['client_rss']} majflt={run['majflt']}",
                    flush=True,
                )
            else:
                warm = exp55.once(endpoint, "arrow", start, end, filt, limit)
                print(f"  warmup arrow rows={warm['rows']} total={warm['total']:.3f}s", flush=True)
                runs = []
                for i in range(1, exp52.SHORT_REPS + 1):
                    before = resource.getrusage(resource.RUSAGE_SELF).ru_majflt
                    with ClientRss() as rss:
                        run = exp55.once(endpoint, "arrow", start, end, filt, limit)
                    run["client_rss"] = rss.peak * 1024
                    run["majflt"] = resource.getrusage(resource.RUSAGE_SELF).ru_majflt - before
                    runs.append(run)
                    print(
                        f"  arrow #{i} rows={run['rows']} total={run['total']:.3f}s "
                        f"server={run['server']} api_peak={run['peak']} "
                        f"client_rss={run['client_rss']} majflt={run['majflt']}",
                        flush=True,
                    )
        except Exception as exc:
            print(f"  arrow FAILED {type(exc).__name__}: {exc}", flush=True)
            rows_out.append({"query": label, "error": f"{type(exc).__name__}: {exc}"})
            continue
        rows_out.append({
            "query": label,
            "start": start,
            "end": end,
            "filter": filt,
            "rows": runs[0]["rows"],
            "bytes": exp52.median(runs, "bytes"),
            "ttfb": exp52.median(runs, "ttfb"),
            "total": exp52.median(runs, "total"),
            "server": exp52.median(runs, "server"),
            "api_peak": max(r["peak"] for r in runs),
            "client_rss": max(r["client_rss"] for r in runs),
            "majflt": max(r["majflt"] for r in runs),
            "warmup_total": None if warm is None else warm["total"],
            "reps": len(runs),
        })
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as fh:
            json.dump(
                {
                    "when": "2026-10-21",
                    "endpoint": endpoint,
                    "reader": "sage-data-client _load_arrow after the batched decoder",
                    "client": exp55.query_mod.__file__,
                    "short_reps": exp52.SHORT_REPS,
                    "month_reps": 1,
                    "warmup_excluded_from_short_median": True,
                    "latency": "request start until the pandas dataframe exists",
                    "api_peak": "max sampled API container usage during the call",
                    "client_rss": "max ps RSS of this process during the measured call",
                    "majflt": "increase in ru_majflt across the measured call",
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
