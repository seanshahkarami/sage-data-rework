#!/usr/bin/env python
"""
Experiment 14: at what size does a flat snapshot stop being good enough?

exp13 showed a 211-line catalog gives the required safety on 12 files. The
Iceberg argument is partly about scale: Iceberg splits metadata into manifest
files precisely so planning does not read one giant blob. If a flat snapshot
is fine at SAGE's real scale, that argument does not apply here yet.

The real archive is 4,564 files today, growing ~8/day. This projects the
snapshot out to 10 years and measures, at each size:

  - snapshot bytes (what every commit rewrites and every cold reader fetches)
  - time to parse it
  - time to plan a query from it

The number that actually matters operationally is the *commit* cost, because
a flat snapshot rewrites the whole file on every commit. Iceberg avoids that
by reusing unchanged manifests. So we measure rewrite cost too.

Usage: python exp14_catalog_scale.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

SCALES = [
    (4_564, "today"),
    (7_500, "~1 year of daily exports"),
    (15_000, "~4 years"),
    (30_000, "~10 years"),
    (100_000, "10 years, no compaction, 8x more files/day"),
]


def synth(n, groups_per_file=40):
    """A snapshot entry shaped like the real ones from exp13."""
    out = []
    for i in range(n):
        d = f"2025-{1 + (i // 300) % 12:02d}-{1 + i % 28:02d}"
        out.append({
            "path": f"date={d}/data_{i%18}.parquet",
            "date": d,
            "rows": 10_000_000,
            "size": 100_000_000,
            "generation": 0,
            "groups": [
                {"rows": 1_000_000,
                 "stats": {
                     "time": {"min": f"{d} 00:00:00", "max": f"{d} 23:59:59"},
                     "plugin": {"min": "registry.sagecontinuum.org/a", "max": "waggle/z"},
                     "name": {"min": "env.a", "max": "wxt.z"},
                 }}
                for _ in range(groups_per_file)
            ],
        })
    return out


def main():
    print(f"{'files':>8s} {'snapshot':>13s} {'gz':>11s} {'parse':>9s} {'plan':>9s} {'commit':>9s}  note")
    rows = []
    import gzip
    for n, note in SCALES:
        files = synth(n)
        blob = json.dumps({"snapshot_id": "x", "files": files})
        size = len(blob.encode())
        gz = len(gzip.compress(blob.encode(), 6))

        t0 = time.perf_counter()
        parsed = json.loads(blob)
        parse_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        target = "2025-06-15"
        hit = [e for e in parsed["files"] if e["date"] == target]
        plan_s = time.perf_counter() - t0

        # commit cost: a flat snapshot rewrites everything on every append
        t0 = time.perf_counter()
        json.dumps(parsed)
        commit_s = time.perf_counter() - t0

        print(f"{n:>8,} {size:>12,}B {gz:>10,}B {parse_s*1000:>8.0f}ms "
              f"{plan_s*1000:>8.1f}ms {commit_s*1000:>8.0f}ms  {note}")
        rows.append({"files": n, "bytes": size, "gz_bytes": gz,
                     "parse_ms": round(parse_s*1000, 1), "plan_ms": round(plan_s*1000, 2),
                     "commit_ms": round(commit_s*1000, 1), "note": note})

    print()
    print("A flat snapshot rewrites the whole file on every commit. At one")
    print("commit per day that is fine; at one per minute it is not. The")
    print("archive commits once per exported day.")
    print()
    print("Trimming the per-group stats to only what prunes (time + plugin)")
    print("and dropping per-group detail for single-plugin files would cut")
    print("these sizes several-fold; exp09 measured 80 KB for 242 real files")
    print("with 1 row group each.")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp14_catalog_scale.json", "w") as f:
        json.dump(rows, f, indent=2)


if __name__ == "__main__":
    main()
