"""Compare pinned DuckDB Go Arrow execution with native streaming, locally.

Build beehive-data-api's tests with `go test -c -o /tmp/arrow.test ./internal/backend/cold`.
Then: python -I exp51_cold_arrow_materialization.py /tmp/arrow.test

Each case runs in a fresh process. No Pelican, HTTP, containers, or credentials.
The query generates unique wide tags, sorts timestamps, encodes the same Arrow
IPC schema, and discards the body. Native RSS is sampled, not just Go's heap.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

import psutil


def run(binary, mode, rows, memory, source):
    env = dict(os.environ, BEEHIVE_ARROW_PROBE=mode, BEEHIVE_ARROW_ROWS=str(rows), BEEHIVE_ARROW_MEMORY=memory)
    if source:
        env["BEEHIVE_ARROW_SOURCE"] = source
    with tempfile.TemporaryFile(mode="w+") as log:
        proc = subprocess.Popen([binary, "-test.run=^TestColdArrowMemoryProbe$", "-test.v"],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
        monitor = psutil.Process(proc.pid)
        peak = 0
        start = time.monotonic()
        while proc.poll() is None:
            try:
                peak = max(peak, monitor.memory_info().rss)
            except psutil.NoSuchProcess:
                break
            if time.monotonic() - start > 600:
                proc.kill()
                break
            time.sleep(0.02)
        code = proc.wait()
        log.seek(0)
        output = log.read()
    result = dict(mode=mode, rows=rows, peak_rss_mb=round(peak / 1e6, 1),
                  exit_code=code, output=output)
    for key, value in re.findall(r"(ready_seconds|total_seconds)=([0-9.]+)", output):
        result[key] = float(value)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary")
    parser.add_argument("--rows", type=int, nargs="+", default=[1000000, 4000000])
    parser.add_argument("--modes", choices=["driver", "stream"], nargs="+", default=["driver", "stream"])
    parser.add_argument("--memory", default="256MB")
    parser.add_argument("--source", help="Optional local Parquet glob: run the March temp/W0A0 query on real archive data instead of generated rows")
    parser.add_argument("--output", type=Path, default=Path(__file__).parent / "results/exp51_cold_arrow_materialization.json")
    args = parser.parse_args()
    results = []
    for rows in args.rows:
        for mode in args.modes:
            result = run(str(Path(args.binary).resolve()), mode, rows, args.memory, args.source)
            results.append(result)
            print(json.dumps(result), flush=True)
            args.output.write_text(json.dumps(dict(memory_limit=args.memory, threads=2, source=args.source, results=results), indent=2) + "\n")
    if any(r["exit_code"] for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
