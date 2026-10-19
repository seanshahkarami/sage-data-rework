#!/usr/bin/env python
"""
Experiment 12: how much does the writer fix save across the whole archive?

exp07 rewrote one day and got 74.2% of the original size. One day is thin
evidence for a 194 GiB claim, and the archive is not uniform: 2025 days are
~300 MB and 2026 days are ~20 MB, and the plugin mix changes over time.

This samples days spread across the full span, rewrites each with the two
fixes (row groups + dictionary-encoded meta via pyarrow), and reports the
distribution of the saving rather than a single number.

WARNING: the sample is `days[::len(days)//n]` over the days present *locally*,
so running this against a partial archive silently samples only the prefix
that has been synced. The first run did exactly that -- 240 of 608 days were
present, every sampled day landed in 2025-01..2025-07, and the result was
written up as "sampled over the whole span". Check the printed day list spans
the real range before quoting the output. See results/SUMMARY.md section 4.

Usage: python -I exp12_rewrite_sample.py <archive-root> <workdir> [--n N]
"""
import json
import os
import shutil
import statistics
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

ROW_GROUP = 1_000_000
SORT_COLS = ["plugin", "vsn", "time"]
DICT_COLS = ["name", "plugin", "vsn", "node", "host", "task", "job", "zone",
             "meta.key_value.key", "meta.key_value.value", "value_str"]


def rewrite_day(src_dir, dst_dir):
    os.makedirs(dst_dir, exist_ok=True)
    srcs = [os.path.join(src_dir, f) for f in sorted(os.listdir(src_dir)) if f.endswith(".parquet")]
    src_bytes = sum(os.path.getsize(p) for p in srcs)
    out_bytes = 0
    rows = 0
    t0 = time.perf_counter()
    for i, p in enumerate(srcs):
        pf = pq.ParquetFile(p)
        out = os.path.join(dst_dir, f"data_{i}.parquet")
        w = None
        for batch in pf.iter_batches(batch_size=500_000):
            tbl = pa.Table.from_batches([batch])
            if w is None:
                # Each writer wins on different columns: the Go exporter's
                # DELTA_BINARY_PACKED is far better for `time` (a sorted
                # nanosecond series), while dictionary encoding is far better
                # for the `meta` map, which Go stores as plain strings.
                # Combining both beats either writer alone.
                w = pq.ParquetWriter(out, tbl.schema, compression="zstd",
                                     compression_level=9, write_statistics=True,
                                     write_page_index=True,
                                     column_encoding={"time": "DELTA_BINARY_PACKED"},
                                     use_dictionary=DICT_COLS)
            w.write_table(tbl, row_group_size=ROW_GROUP)
            rows += tbl.num_rows
        if w:
            w.close()
            out_bytes += os.path.getsize(out)
    return src_bytes, out_bytes, rows, time.perf_counter() - t0


def main():
    root = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    n = int(sys.argv[sys.argv.index("--n") + 1]) if "--n" in sys.argv else 8

    days = sorted(d for d in os.listdir(root) if d.startswith("date="))
    # Only days that look complete (have at least one file).
    days = [d for d in days if any(f.endswith(".parquet") for f in os.listdir(os.path.join(root, d)))]
    step = max(1, len(days) // n)
    sample = days[::step][:n]

    # The sample is only meaningful if the local copy spans the real archive.
    # A partial rsync silently yields a prefix-only sample (see docstring).
    span = f"{days[0]} .. {days[-1]}"
    print(f"local archive: {len(days)} days, {span}")
    print(f"sample       : {', '.join(d[5:] for d in sample)}")
    if len(days) < 608:
        print(f"WARNING: only {len(days)} of 608 known days are present locally. "
              f"This sample covers {span} and does NOT represent the whole span.")
    print()

    sandbox = os.path.join(work, "rewrite_sample")
    if os.path.exists(sandbox):
        shutil.rmtree(sandbox)

    print(f"{'day':16s} {'before':>14s} {'after':>14s} {'ratio':>7s} {'rows':>14s} {'sec':>7s}")
    rows_out = []
    tot_b = tot_a = 0
    for d in sample:
        sb, ob, rw, el = rewrite_day(os.path.join(root, d), os.path.join(sandbox, d))
        tot_b += sb
        tot_a += ob
        ratio = 100.0 * ob / sb if sb else 0
        rows_out.append({"day": d, "before": sb, "after": ob, "ratio": round(ratio, 1),
                         "rows": rw, "seconds": round(el, 1)})
        print(f"{d:16s} {sb:>14,} {ob:>14,} {ratio:>6.1f}% {rw:>14,} {el:>7.1f}")
        # keep disk bounded
        shutil.rmtree(os.path.join(sandbox, d))

    ratios = [r["ratio"] for r in rows_out]
    print()
    print(f"days sampled : {len(rows_out)} of {len(days)} present locally")
    print(f"ratio        : min {min(ratios):.1f}%  median {statistics.median(ratios):.1f}%  max {max(ratios):.1f}%")
    print(f"aggregate    : {tot_b:,} -> {tot_a:,}  ({100.0*tot_a/tot_b:.1f}%)")
    saved_frac = 1 - tot_a / tot_b
    print()
    print(f"full archive is 208,584,864,752 bytes (194 GiB)")
    print(f"projected after rewrite: {208584864752*tot_a/tot_b/2**30:,.0f} GiB "
          f"(saves ~{208584864752*saved_frac/2**30:,.0f} GiB)")
    secs = sum(r["seconds"] for r in rows_out) / max(len(rows_out), 1)
    print(f"rewrite cost : ~{secs:.0f}s per sampled day, ~{secs*608/3600:.1f}h for 608 days single-threaded")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp12_rewrite_sample.json", "w") as f:
        json.dump({"days": rows_out, "total_before": tot_b, "total_after": tot_a,
                   "ratio_pct": round(100.0*tot_a/tot_b, 1)}, f, indent=2)


if __name__ == "__main__":
    main()
