#!/usr/bin/env python
"""
Experiment 2: does DuckDB use Parquet page indexes to skip a plugin slice?

exp01 showed pyarrow reads the entire column chunk no matter the filter. That
might be a pyarrow limitation, not a fact about the files. DuckDB is the engine
the team actually uses (archive-exporter/show-archive-stats is a DuckDB
script), and it has its own Parquet reader, so it is the one that decides
whether the plans/1_draft.md section 5 claim holds:

  "page statistics can skip the other plugins' pages ... because plugin is the
   leading sort key"

Bytes read is the number that matters, not wall time: once these files sit
behind Pelican, every byte is an HTTP range request. We measure it from the
OS by running each query in a fresh subprocess and reading that process's
real disk-read counter (psutil io_counters), with the page cache dropped
between runs where possible.

Usage:
  python -I exp02_duckdb_pruning.py <file.parquet>            # run all cases
  python -I exp02_duckdb_pruning.py --child <file> <case> <plugin>  # internal
"""
import json
import os
import subprocess
import sys
import time

QUERY_COLS = "time, name, plugin, vsn, value_float"


def build_sql(path, case, big_plugin, rare_plugin):
    t = f"read_parquet('{path}')"
    return {
        "full_scan": f"select {QUERY_COLS} from {t}",
        "big_plugin": f"select {QUERY_COLS} from {t} where plugin = '{big_plugin}'",
        "rare_plugin": f"select {QUERY_COLS} from {t} where plugin = '{rare_plugin}'",
        "name_filter": f"select {QUERY_COLS} from {t} where name = 'env.temperature'",
        "time_slice": (
            f"select {QUERY_COLS} from {t} where time >= TIMESTAMPTZ '2025-07-21 12:00:00+00' "
            f"and time < TIMESTAMPTZ '2025-07-21 12:05:00+00'"
        ),
        "count_star": f"select count(*) from {t}",
        # time+plugin together: the shape a real Beehive query has
        "plugin_and_time": (
            f"select {QUERY_COLS} from {t} where plugin = '{rare_plugin}' "
            f"and time >= TIMESTAMPTZ '2025-07-21 12:00:00+00' "
            f"and time < TIMESTAMPTZ '2025-07-21 18:00:00+00'"
        ),
    }[case]


def child(path, case, big_plugin, rare_plugin):
    """Run one query in this process and report rows + bytes read from disk."""
    import duckdb
    import psutil

    proc = psutil.Process()

    def disk_read():
        try:
            return proc.io_counters().read_bytes
        except Exception:
            return None

    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    # Warm the connection/library without touching the data file.
    con.execute("select 1").fetchall()

    before = disk_read()
    t0 = time.perf_counter()
    rows = con.execute(build_sql(path, case, big_plugin, rare_plugin)).fetchall()
    elapsed = time.perf_counter() - t0
    after = disk_read()

    n = rows[0][0] if case == "count_star" else len(rows)
    print(
        json.dumps(
            {
                "case": case,
                "rows": n,
                "seconds": round(elapsed, 4),
                "bytes_read": (after - before) if (before is not None and after is not None) else None,
            }
        )
    )


def purge_cache():
    """Best-effort page-cache drop so bytes_read reflects real IO."""
    subprocess.run(["sync"], check=False)
    # `purge` on macOS needs root; ignore failure and note it in the report.
    return subprocess.run(["purge"], capture_output=True).returncode == 0


def main():
    if sys.argv[1] == "--child":
        child(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        return

    path = os.path.abspath(sys.argv[1])
    import duckdb

    con = duckdb.connect()
    counts = con.execute(
        f"select plugin, count(*) c from read_parquet('{path}') group by plugin order by c desc"
    ).fetchall()
    big_plugin, big_n = counts[0]
    rare_plugin, rare_n = counts[-1]
    total = sum(c for _, c in counts)
    con.close()

    size = os.path.getsize(path)
    print(f"file: {path}")
    print(f"size: {size:,} bytes   rows: {total:,}   distinct plugins: {len(counts)}")
    print(f"big : {big_plugin} ({big_n:,} rows, {100*big_n/total:.2f}%)")
    print(f"rare: {rare_plugin} ({rare_n:,} rows, {100*rare_n/total:.5f}%)")
    purged = purge_cache()
    print(f"page cache purge available: {purged}")
    print()

    cases = [
        "count_star",
        "full_scan",
        "big_plugin",
        "rare_plugin",
        "name_filter",
        "time_slice",
        "plugin_and_time",
    ]
    results = []
    for case in cases:
        purge_cache()
        out = subprocess.run(
            [sys.executable, "-I", __file__, "--child", path, case, big_plugin, rare_plugin],
            capture_output=True,
            text=True,
        )
        line = out.stdout.strip().splitlines()[-1] if out.stdout.strip() else None
        if line is None:
            print(f"{case}: FAILED {out.stderr[-300:]}")
            continue
        r = json.loads(line)
        results.append(r)
        print(json.dumps(r))

    print()
    full = next((r for r in results if r["case"] == "full_scan"), None)
    if full:
        print(f"{'case':16s} {'rows':>12s} {'bytes_read':>14s} {'vs full':>9s} {'sec':>8s}")
        for r in results:
            b = r["bytes_read"]
            pct = f"{100*b/full['bytes_read']:>8.1f}%" if b and full["bytes_read"] else "     n/a"
            bs = f"{b:>14,}" if b is not None else f"{'n/a':>14s}"
            print(f"{r['case']:16s} {r['rows']:>12,} {bs} {pct} {r['seconds']:>8.4f}")

    with open("results/exp02_duckdb_pruning.json", "w") as f:
        json.dump(
            {
                "file": path,
                "file_size": size,
                "total_rows": total,
                "distinct_plugins": len(counts),
                "big_plugin": big_plugin,
                "rare_plugin": rare_plugin,
                "cache_purge_available": purged,
                "results": results,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
