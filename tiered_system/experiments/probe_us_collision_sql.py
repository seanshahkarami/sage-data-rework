#!/usr/bin/env python
"""exp25b: the same collision question, computed in DuckDB.

Two reasons this exists alongside probe_us_collision.py rather than
replacing it:

  1. SPEED. Grouping 59M rows by nine string columns in pyarrow materialises
     the whole day in memory. DuckDB streams it and spills, so the whole
     608-day span is reachable instead of a handful of days.

  2. INDEPENDENCE. It is a different engine and a different implementation
     of the same question. If the two disagree, one of them is wrong, and
     that is worth knowing before a format decision rests on the answer.

THE TRAP THIS MUST AVOID: DuckDB's TIMESTAMP is microsecond precision, so
reading `time` as a timestamp silently downcasts it and every remainder
reads as zero -- the exact mistake that produced a confident wrong answer in
§13. Measured on DuckDB 1.5.6, the truncation happens at READ time and
cannot be undone in SQL:

    epoch_ns(time)                -> 1735689606915629000, 0 non-zero % 1000
    CAST(time AS BIGINT)          -> ConversionException
    epoch_ns(time::TIMESTAMP_NS)  -> 0 non-zero % 1000

`epoch_ns` looks like it preserves nanoseconds -- it returns a ns-scaled
number -- but the low three digits are already gone. So DuckDB cannot be
pointed at the Parquet directly for this question at all.

The fix: pyarrow reads `time` as int64 (where the nanoseconds are intact)
and the resulting Arrow table is handed to DuckDB, which does the grouping.
pyarrow for precision, DuckDB for speed. The guard below asserts non-zero
remainders are actually visible before any counting, so if this ever
regresses it aborts instead of reporting a clean zero.

Usage: python -I probe_us_collision_sql.py <archive-root> [--n N]
       python -I probe_us_collision_sql.py <archive-root> --all
"""
import json
import os
import sys

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

KEY = ["name", "plugin", "vsn", "node", "host", "task", "job", "zone"]
VALS = ["value_float", "value_int", "value_str"]


def read_day(files):
    """Read one day as Arrow with `time` as int64 nanoseconds.

    The cast to int64 happens in pyarrow, before DuckDB sees the column, so
    the nanosecond digits are still there when the grouping runs.
    """
    tabs = []
    for p in files:
        pf = pq.ParquetFile(p)
        assert pf.schema_arrow.field("time").type.unit == "ns", \
            f"{p}: time is not timestamp[ns]"
        t = pq.read_table(p, columns=["time"] + KEY + VALS)
        cols = {k: t.column(k) for k in KEY}
        cols.update({v: t.column(v) for v in VALS})
        ns = pc.cast(t.column("time"), pa.int64())
        cols["ns"] = ns
        cols["us"] = pc.multiply(pc.divide(ns, pa.scalar(1000, pa.int64())),
                                 pa.scalar(1000, pa.int64()))
        tabs.append(pa.table(cols))

    return pa.concat_tables(tabs)


def guard_precision(arrow_tbl):
    """Refuse to run if nanoseconds are not actually present.

    Without this the probe would report a clean zero for the wrong reason,
    which is exactly how the original nanosecond finding went wrong -- and
    how the first version of THIS script went wrong, reading through DuckDB.
    """
    rem = pc.sum(pc.cast(pc.not_equal(
        pc.subtract(arrow_tbl.column("ns"), arrow_tbl.column("us")),
        pa.scalar(0, pa.int64())), pa.int64())).as_py() or 0
    if rem == 0:
        raise SystemExit(
            "ABORT: no non-zero nanosecond remainders in the first day.\n"
            "Either it is genuinely us-aligned or precision was lost on "
            "read. Do not trust any collision number from this run.")

    return rem


# Counting is split in two deliberately. The cheap query answers the whole
# question whenever nothing collides -- if the number of distinct
# (series, ns) keys equals the number of distinct (series, us) keys, then no
# two readings merged, and there is nothing whose values could conflict. The
# expensive per-group count(DISTINCT ...) only runs when that is not the
# case. On this archive the cheap path is ~4s for 10M rows; the full
# aggregate did not finish a single day in ten minutes.
COUNT_SQL = """
SELECT (SELECT count(*) FROM src)                                   AS rows,
       (SELECT count(*) FROM (SELECT DISTINCT {keys}, ns FROM src)) AS keys_before,
       (SELECT count(*) FROM (SELECT DISTINCT {keys}, us FROM src)) AS keys_after
"""

# Only reached when keys_before != keys_after.
CONFLICT_SQL = """
WITH grp AS (
    SELECT {keys}, us,
           count(DISTINCT ns) AS n_ns,
           ({val_distinct})   AS vals_differ
    FROM src GROUP BY {keys}, us
)
SELECT (SELECT count(*) FROM grp WHERE n_ns > 1)                 AS collided,
       (SELECT count(*) FROM grp WHERE n_ns > 1 AND vals_differ) AS conflicting
"""


def count_sql():
    return COUNT_SQL.format(keys=", ".join(KEY))


def conflict_sql():
    return CONFLICT_SQL.format(
        keys=", ".join(KEY),
        val_distinct=" OR ".join(f"count(DISTINCT {v}) > 1" for v in VALS))


def main():
    root = os.path.abspath(sys.argv[1])
    days = sorted(d for d in os.listdir(root) if d.startswith("date="))
    days = [d for d in days
            if any(f.endswith(".parquet") for f in os.listdir(os.path.join(root, d)))]

    if "--all" in sys.argv:
        sample = days
    else:
        n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 8
        sample = days[:: max(1, len(days) // n)][:n]

    con = duckdb.connect()
    con.execute("PRAGMA threads=8")

    print(f"local archive: {len(days)} days, {days[0]} .. {days[-1]}")
    print(f"checking {len(sample)} day(s): does ns -> us truncation merge "
          f"distinct readings?\n")
    print(f"{'day':16s} {'rows':>13s} {'keys before':>13s} {'keys after':>13s} "
          f"{'lost':>9s} {'pct':>8s} {'conflicting':>12s}")

    out = []
    tr = tb = ta = tc = 0
    csql, xsql = count_sql(), conflict_sql()
    for i, d in enumerate(sample):
        files = [os.path.join(root, d, f)
                 for f in sorted(os.listdir(os.path.join(root, d)))
                 if f.endswith(".parquet")]
        src = read_day(files)  # noqa: F841 -- DuckDB resolves `src` by name
        if i == 0:
            nz = guard_precision(src)
            print(f"  (precision guard: {nz:,} non-zero ns remainders "
                  f"visible in {d})")
        rows, before, after = con.execute(csql).fetchone()
        lost = before - after
        # Nothing merged means nothing can conflict; skip the costly pass.
        collided, conflicting = (0, 0) if lost == 0 \
            else con.execute(xsql).fetchone()
        del src
        pct = 100.0 * lost / before if before else 0.0
        print(f"{d:16s} {rows:13,d} {before:13,d} {after:13,d} "
              f"{lost:9,d} {pct:7.4f}% {conflicting:12,d}")
        out.append({"day": d, "rows": rows, "keys_before": before,
                    "keys_after": after, "lost": lost, "pct": round(pct, 5),
                    "collided_keys": collided, "conflicting_keys": conflicting})
        tr += rows
        tb += before
        ta += after
        tc += conflicting

    tl = tb - ta
    agg = 100.0 * tl / tb if tb else 0.0
    print(f"\n{'TOTAL':16s} {tr:13,d} {tb:13,d} {ta:13,d} "
          f"{tl:9,d} {agg:7.4f}% {tc:12,d}")

    print()
    if tl == 0:
        print("No collisions: truncation loses precision but never identity.")
    elif tc == 0:
        print(f"{tl:,} readings collide, but every collided key holds a single "
              f"value -- duplicates folded together, not conflicting data.")
    else:
        print(f"WARNING: {tc:,} collided keys hold DIFFERING values. "
              f"Truncation merges distinct readings.")

    res = {"engine": "duckdb", "days": out, "total_rows": tr,
           "keys_before": tb, "keys_after": ta, "lost": tl,
           "aggregate_pct": round(agg, 5), "conflicting_keys": tc}
    dest = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "results", "us_collision_sql.json")
    with open(dest, "w") as fh:
        json.dump(res, fh, indent=1)
    print(f"\nwrote {dest}")


if __name__ == "__main__":
    main()
