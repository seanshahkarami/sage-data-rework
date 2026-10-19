#!/usr/bin/env python
"""
Experiment 8: run real Beehive query objects against the Parquet archive.

This is the plans/1_draft.md Phase 3 question ("bounded raw queries against
catalog-selected Parquet files") and the open point the handoff lists as
"head / tail per series on Parquet, including series identity when some tags
live in a map".

We cannot diff against live InfluxDB from here, so this checks the properties
that must hold regardless of backend:

  - half-open [start, end): a record at exactly `end` must NOT appear, and the
    same record must appear when it is the `start` of the adjoining query
  - splitting a range in two yields exactly the union, no gap, no duplicate
    (the tier-boundary property section 8 requires)
  - tail=N returns N per series, not N overall
  - the exact-vs-regex dot asymmetry behaves as influx_backend.go does
  - meta carries the promoted tags back, as the API returns them

Usage: python -I exp08_query_api.py <archive-root>
"""
import json
import os
import sys
from datetime import timezone

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cold_backend as cb

PASS, FAIL = "PASS", "FAIL"
results = []


def check(name, cond, detail=""):
    results.append({"check": name, "status": PASS if cond else FAIL, "detail": detail})
    print(f"[{PASS if cond else FAIL}] {name}" + (f"  {detail}" if detail else ""))
    return cond


def main():
    root = os.path.abspath(sys.argv[1])
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    day = sorted(d for d in os.listdir(root) if d.startswith("date="))[0]
    date = day.replace("date=", "")
    print(f"archive: {root}\nday    : {date}\n")

    # Pick a measurement that exists on this day.
    files = [os.path.join(root, day, f) for f in sorted(os.listdir(os.path.join(root, day)))
             if f.endswith(".parquet")]
    flist = "[" + ",".join("'" + f + "'" for f in files) + "]"
    top = con.execute(
        f"select name, count(*) c from read_parquet({flist}) group by name order by c desc limit 5"
    ).fetchall()
    print("top measurements:", [(n, f"{c:,}") for n, c in top], "\n")
    measurement = top[0][0]

    # ---- 1. half-open interval ----
    q_all = {"start": f"{date}T00:00:00Z", "end": f"{date}T01:00:00Z",
             "filter": {"name": measurement}}
    recs_all, _, nfiles = cb.query_archive(con, root, q_all)

    q_a = {"start": f"{date}T00:00:00Z", "end": f"{date}T00:30:00Z",
           "filter": {"name": measurement}}
    q_b = {"start": f"{date}T00:30:00Z", "end": f"{date}T01:00:00Z",
           "filter": {"name": measurement}}
    recs_a, _, _ = cb.query_archive(con, root, q_a)
    recs_b, _, _ = cb.query_archive(con, root, q_b)

    print(f"whole hour: {len(recs_all):,} records over {nfiles} files")
    print(f"first half: {len(recs_a):,}   second half: {len(recs_b):,}   "
          f"sum: {len(recs_a)+len(recs_b):,}\n")

    check("split range has no gap and no duplicate",
          len(recs_a) + len(recs_b) == len(recs_all),
          f"{len(recs_a)}+{len(recs_b)} vs {len(recs_all)}")

    key = lambda r: (r["timestamp"], r["name"], str(r["value"]), json.dumps(r["meta"], sort_keys=True))
    check("split range is exactly the union",
          sorted(map(key, recs_a + recs_b)) == sorted(map(key, recs_all)))

    boundary = f"{date}T00:30:00Z"
    in_a = [r for r in recs_a if r["timestamp"].startswith(boundary[:-1])]
    in_b = [r for r in recs_b if r["timestamp"].startswith(boundary[:-1])]
    check("boundary instant belongs to the later half only",
          len(in_a) == 0,
          f"at {boundary}: {len(in_a)} in first half, {len(in_b)} in second")

    # ---- 2. tail is per series ----
    q_tail = {"start": f"{date}T00:00:00Z", "end": f"{date}T01:00:00Z",
              "filter": {"name": measurement}, "tail": 1}
    recs_tail, _, _ = cb.query_archive(con, root, q_tail)
    series = {json.dumps(r["meta"], sort_keys=True) for r in recs_all}
    check("tail=1 returns one record per series, not one overall",
          len(recs_tail) == len(series),
          f"{len(recs_tail)} records for {len(series)} distinct series")

    # ---- 3. filter semantics ----
    q_exact = {"start": f"{date}T00:00:00Z", "end": f"{date}T00:05:00Z",
               "filter": {"name": measurement}}
    n_exact = len(cb.query_archive(con, root, q_exact)[0])
    dotted = measurement.replace(".", "x", 1)
    q_wrong = {"start": f"{date}T00:00:00Z", "end": f"{date}T00:05:00Z",
               "filter": {"name": dotted}}
    n_wrong = len(cb.query_archive(con, root, q_wrong)[0])
    check("exact match treats '.' literally",
          n_exact > 0 and n_wrong == 0,
          f"{measurement}={n_exact}, {dotted}={n_wrong}")

    if "." in measurement:
        prefix = measurement.rsplit(".", 1)[0]
        q_re = {"start": f"{date}T00:00:00Z", "end": f"{date}T00:05:00Z",
                "filter": {"name": prefix + ".*"}}
        n_re = len(cb.query_archive(con, root, q_re)[0])
        check("regex branch matches a prefix wildcard",
              n_re >= n_exact, f"{prefix}.* = {n_re} >= {n_exact}")

    # ---- 4. meta carries promoted tags ----
    if recs_all:
        sample = recs_all[0]
        check("meta includes promoted tags (plugin/vsn)",
              "plugin" in sample["meta"] or "vsn" in sample["meta"],
              f"keys: {sorted(sample['meta'])[:8]}")
        check("record has exactly the API's four fields",
              set(sample) == {"timestamp", "name", "value", "meta"},
              str(sorted(sample)))
        print("\nsample record:")
        print(json.dumps(sample, indent=2)[:600])

    # ---- 5. validation ----
    for bad, why in [
        ({"end": "x", "filter": {}}, "missing start field"),
        ({"start": "x", "head": 1, "tail": 1, "filter": {}}, "head and tail"),
        ({"start": "x", "experimental_window": "1h", "filter": {}}, "window without func"),
        ({"start": "x", "filter": {"bad-key": "v"}}, "invalid filter key"),
    ]:
        try:
            cb.validate(bad)
            ok = False
        except cb.QueryError:
            ok = True
        check(f"rejects: {why}", ok)

    npass = sum(1 for r in results if r["status"] == PASS)
    print(f"\n{npass}/{len(results)} checks passed")
    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp08_query_api.json", "w") as f:
        json.dump({"archive": root, "day": date, "measurement": measurement,
                   "checks": results}, f, indent=2)


if __name__ == "__main__":
    main()
