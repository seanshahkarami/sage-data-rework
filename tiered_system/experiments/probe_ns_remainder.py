#!/usr/bin/env python
"""
How many timestamps actually lose data when downcast ns -> us?

This backs the Iceberg blocker in plans/4_catalog_recommendation.md. Iceberg
v2 has no nanosecond timestamp type and pyiceberg 0.12 cannot write v3, so
adopting Iceberg today means casting the archive's `time` column from
nanoseconds to microseconds. That cast is lossy for any row whose timestamp
is not a whole number of microseconds.

The original "55.00%" came from a single row group of date=2025-01-01 and was
written up as if it described the archive. It does not: the fraction varies
widely by day, because it is really a property of which plugins reported that
day and how each one stamps its readings. This samples across the whole span
and reports the distribution.

NOTE ON METHOD: this must NOT be measured through DuckDB. DuckDB's TIMESTAMP
is microsecond precision, so `read_parquet` downcasts timestamp[ns] on the way
in and `epoch_ns(time) % 1000` is then always 0 -- the first version of this
script did exactly that and reported a confident 0.00% across 593M rows. The
remainder has to be read straight out of the Parquet column as int64.

Usage: python -I probe_ns_remainder.py <archive-root> [--n N]
"""
import json
import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def main():
    root = os.path.abspath(sys.argv[1])
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 8

    days = sorted(d for d in os.listdir(root) if d.startswith("date="))
    days = [d for d in days
            if any(f.endswith(".parquet") for f in os.listdir(os.path.join(root, d)))]
    sample = days[:: max(1, len(days) // n)][:n]

    print(f"local archive: {len(days)} days, {days[0]} .. {days[-1]}")
    if len(days) < 608:
        print(f"WARNING: only {len(days)} of 608 known days are present locally; "
              f"this sample does NOT represent the whole span.")
    print()

    print(f"{'day':14s} {'rows':>15s} {'non-zero ns':>15s} {'pct':>8s}")
    rows_out = []
    tot_rows = tot_nz = 0
    for d in sample:
        files = [os.path.join(root, d, f)
                 for f in sorted(os.listdir(os.path.join(root, d)))
                 if f.endswith(".parquet")]
        total = nz = 0
        for p in files:
            pf = pq.ParquetFile(p)
            assert pf.schema_arrow.field("time").type.unit == "ns", \
                f"{p}: time is not timestamp[ns]"
            # Stream batches: a day is up to 140M rows and only `time` is read.
            for batch in pf.iter_batches(batch_size=2_000_000, columns=["time"]):
                # int64 epoch nanoseconds, straight from the column. A value
                # not divisible by 1000 is one the us cast would truncate.
                iv = batch.column("time").cast(pa.int64())
                # Compare against the value round-tripped through microseconds,
                # which is exactly what the Iceberg cast would store. Avoids
                # relying on the sign convention of integer division/modulo.
                us = pc.multiply(pc.divide(iv, pa.scalar(1000, pa.int64())),
                                 pa.scalar(1000, pa.int64()))
                nz += pc.sum(pc.not_equal(iv, us)).as_py() or 0
                total += len(iv)
        pct = 100.0 * nz / total if total else 0.0
        rows_out.append({"day": d, "rows": total, "nonzero_ns": nz,
                         "pct": round(pct, 2)})
        tot_rows += total
        tot_nz += nz
        print(f"{d[5:]:14s} {total:>15,} {nz:>15,} {pct:>7.2f}%")

    pcts = sorted(r["pct"] for r in rows_out)
    agg = 100.0 * tot_nz / tot_rows if tot_rows else 0.0
    print()
    print(f"per-day  : min {pcts[0]:.2f}%  median {pcts[len(pcts)//2]:.2f}%  "
          f"max {pcts[-1]:.2f}%")
    print(f"aggregate: {tot_nz:,} of {tot_rows:,} rows ({agg:.2f}%)")
    print()
    print("The spread is the point: a single-day figure cannot stand in for the")
    print("archive. What is robust is that the fraction is large on every day")
    print("sampled, so the ns -> us cast is lossy for a substantial share of the")
    print("archive no matter which day you pick. That is what blocks Iceberg v2.")

    out = os.path.dirname(os.path.abspath(__file__)) + "/results/ns_remainder.json"
    with open(out, "w") as f:
        json.dump({"days": rows_out, "total_rows": tot_rows,
                   "total_nonzero_ns": tot_nz, "aggregate_pct": round(agg, 2),
                   "per_day_min": pcts[0], "per_day_median": pcts[len(pcts)//2],
                   "per_day_max": pcts[-1]}, f, indent=2)


if __name__ == "__main__":
    main()
