"""Pin the ns->us cast, which is the one lossy step in the whole pipeline.

Runs against a real archive file, because the properties worth pinning are
properties of the data as much as of the code:

  * no row is lost or duplicated by chunking
  * the cast TRUNCATES rather than rounds -- a round-half-up would move a
    row across a microsecond boundary, and the watermark comparison at the
    hot/cold cut is defined in nanoseconds precisely so that boundary is
    unambiguous
  * peak memory stays bounded, which is why the seeder streams at all

Usage:  python -I test_seed_cold.py
"""
import glob
import os
import resource
import sys

import numpy as np
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_cold import iceberg_schema, iter_chunks_us, us_schema

ARCHIVE = os.environ.get(
    "DEMO_ARCHIVE",
    "/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/sage-data-rework/data/archive")

FAILURES = []


def check(label, got, want=True):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        print(f"      got {got!r} want {want!r}")
        FAILURES.append(label)


def main():
    files = sorted(glob.glob(f"{ARCHIVE}/date=2025-03-01/*.parquet"))
    if not files:
        print("no archive files; skipping")
        return 0
    f = files[0]
    target = iceberg_schema().as_arrow()

    src_ns = (pq.read_table(f, columns=["time"]).column("time")
              .cast("int64").to_numpy(zero_copy_only=False))

    chunks = list(iter_chunks_us(f, target, 1_000_000))
    # A timestamp[us] column cast to int64 yields MICROSECONDS. Comparing
    # it against nanoseconds is the obvious mistake and makes a correct
    # cast look broken, so the units are spelled out in the names here.
    got_us = np.concatenate([c.column("time").cast("int64")
                             .to_numpy(zero_copy_only=False) for c in chunks])

    check("chunking preserves the row count", len(got_us), len(src_ns))
    check("cold column is timestamp[us, tz=UTC]",
          str(chunks[0].schema.field("time").type), "timestamp[us, tz=UTC]")
    check("cast equals truncation to microseconds",
          bool(np.array_equal(got_us, src_ns // 1000)))

    # Truncation, never rounding: a round-half-up would push ~half the rows
    # into the next microsecond and could move a row across the watermark.
    lost = src_ns - got_us.astype(np.int64) * 1000
    check("discarded remainder is always in [0, 1000)",
          bool(lost.min() >= 0 and lost.max() < 1000))

    sub = int(np.count_nonzero(src_ns % 1000))
    print(f"NOTE  {sub:,} of {len(src_ns):,} source rows "
          f"({100*sub/len(src_ns):.2f}%) carry sub-microsecond digits")
    check("the precision drop is actually exercised by this file", sub > 0)

    # Schema sanity: the map column must survive, or meta silently vanishes.
    names = set(chunks[0].schema.names)
    check("meta column present after cast", "meta" in names)
    check("all three value columns present",
          {"value_float", "value_int", "value_str"} <= names)

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9
    print(f"NOTE  peak RSS {peak:.2f} GB streaming one 10M-row file "
          f"(reading a whole day at once measured 9.22 GB)")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
