#!/usr/bin/env python
"""exp25: what does truncating ns -> us actually cost, beyond precision?

The decision on the table is to store microseconds so DuckDB can read the
table (SUMMARY.md §23). §13 already measured that 97.60% of rows carry a
non-zero nanosecond remainder, but that number answers the wrong question
for this decision. Losing sub-microsecond precision on a sensor reading is
almost certainly fine. What would NOT be fine is two DISTINCT readings
becoming the same (series, timestamp) pair, because that is no longer a
precision loss -- it is an identity collision, and it breaks:

  - de-duplication, if the pipeline ever dedupes on (series, time)
  - "last value wins" correction semantics, which need a unique key
  - any downstream join or upsert keyed on the timestamp

So this measures collisions, not remainders:

  1. How many rows share a (name, meta, time_us) key after truncation but
     had distinct time_ns before? Those are the rows that become
     indistinguishable.
  2. Of those, how many disagree on `value`? A collision between two
     identical readings is harmless; a collision between two DIFFERENT
     readings is a real loss of information.

Measured on the real archive, per day, across the whole span.

NOTE ON METHOD: the remainder must be read straight out of the Parquet
column as int64. DuckDB's TIMESTAMP is microsecond precision, so reading
through it downcasts on the way in and every remainder reads as 0 -- the
trap that produced a confident wrong answer in §13. pyarrow is used here and
the ns unit is asserted per file.

Usage: python -I probe_us_collision.py <archive-root> [--n N]
"""
import json
import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# The series identity in this archive: the measurement name plus the flat
# label columns. `meta` is a map and holds further labels, but a map cannot
# be grouped without stringifying 59M rows -- and leaving it out is the SAFE
# direction, because fewer key columns means MORE rows share a key. So every
# collision count below is an upper bound. If the upper bound is ~0, the
# exact figure cannot be worse.
KEY = ["name", "plugin", "vsn", "node", "host", "task", "job", "zone"]

# `value` is split across three typed columns, exactly one of which is
# populated per row. A collision matters only if the values disagree, so all
# three are checked.
VALS = ["value_float", "value_int", "value_str"]


def day_stats(files):
    """Collision counts for one day.

    Returns (rows, distinct_ns_keys, distinct_us_keys, lost_keys,
             conflicting_keys).

    `lost_keys` is how many readings cease to be uniquely addressable:
    distinct (series, time) keys before truncation minus after.
    `conflicting_keys` counts collided groups whose value is not constant --
    the subset where real information disagrees rather than a duplicate
    being folded onto itself.
    """
    tabs = []
    for p in files:
        pf = pq.ParquetFile(p)
        assert pf.schema_arrow.field("time").type.unit == "ns", \
            f"{p}: time is not timestamp[ns]"
        tabs.append(pq.read_table(p, columns=["time"] + KEY + VALS))
    t = pa.concat_tables(tabs)
    rows = t.num_rows

    ns = pc.cast(t.column("time"), pa.int64())
    us = pc.multiply(pc.divide(ns, pa.scalar(1000, pa.int64())),
                     pa.scalar(1000, pa.int64()))

    cols = {k: t.column(k) for k in KEY}
    cols.update({v: t.column(v) for v in VALS})
    cols["ns"] = ns
    cols["us"] = us
    base = pa.table(cols)

    before = base.group_by(KEY + ["ns"]).aggregate([]).num_rows
    after = base.group_by(KEY + ["us"]).aggregate([]).num_rows
    lost = before - after

    # Of the keys that collided, how many hold more than one distinct value?
    g = base.group_by(KEY + ["us"]).aggregate(
        [("ns", "count_distinct")] + [(v, "count_distinct") for v in VALS])
    differing = pc.greater(g.column("ns_count_distinct"), 1)
    any_val = None
    for v in VALS:
        c = pc.greater(g.column(f"{v}_count_distinct"), 1)
        any_val = c if any_val is None else pc.or_(any_val, c)
    conflicting = pc.sum(
        pc.cast(pc.and_(differing, any_val), pa.int64())).as_py() or 0

    return rows, before, after, lost, conflicting


def main():
    root = os.path.abspath(sys.argv[1])
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 8

    days = sorted(d for d in os.listdir(root) if d.startswith("date="))
    days = [d for d in days
            if any(f.endswith(".parquet") for f in os.listdir(os.path.join(root, d)))]
    sample = days[:: max(1, len(days) // n)][:n]

    print(f"local archive: {len(days)} days, {days[0]} .. {days[-1]}")
    print("question: does ns -> us truncation make distinct readings collide?\n")
    print(f"{'day':16s} {'rows':>13s} {'keys before':>13s} {'keys after':>13s} "
          f"{'lost':>10s} {'pct':>7s} {'conflicting':>12s}")

    out = []
    tot_rows = tot_before = tot_after = tot_conf = 0
    for d in sample:
        files = [os.path.join(root, d, f)
                 for f in sorted(os.listdir(os.path.join(root, d)))
                 if f.endswith(".parquet")]
        rows, before, after, lost, conf = day_stats(files)
        pct = 100.0 * lost / before if before else 0.0
        print(f"{d:16s} {rows:13,d} {before:13,d} {after:13,d} "
              f"{lost:10,d} {pct:6.3f}% {conf:12,d}")
        out.append({"day": d, "rows": rows, "keys_before": before,
                    "keys_after": after, "lost": lost, "pct": round(pct, 4),
                    "conflicting_keys": conf})
        tot_rows += rows
        tot_before += before
        tot_after += after
        tot_conf += conf

    tot_lost = tot_before - tot_after
    agg = 100.0 * tot_lost / tot_before if tot_before else 0.0
    print(f"\n{'TOTAL':16s} {tot_rows:13,d} {tot_before:13,d} {tot_after:13,d} "
          f"{tot_lost:10,d} {agg:6.3f}% {tot_conf:12,d}")

    print()
    if tot_lost == 0:
        print("No collisions: truncation loses precision but never identity.")
    elif tot_conf == 0:
        print(f"{tot_lost:,} rows collide, but every collision holds a single "
              f"value -- duplicates, not conflicts.")
    else:
        print(f"WARNING: {tot_conf:,} collided keys hold DIFFERING values. "
              f"Truncation would merge distinct readings.")

    res = {"days": out, "total_rows": tot_rows, "keys_before": tot_before,
           "keys_after": tot_after, "lost": tot_lost,
           "aggregate_pct": round(agg, 4), "conflicting_keys": tot_conf}
    dest = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "results", "us_collision.json")
    with open(dest, "w") as fh:
        json.dump(res, fh, indent=1)
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
