#!/usr/bin/env python
"""
Experiment 11: how much late data is there, and does a correction file work?

Two open points in plans/1_draft.md:

  "How long do late SAGE observations actually take to arrive?"
  "Late data after a day directory is published."

archive-exporter/main.go skips any date whose archive/date=... already exists:

    ok, err := dirExists(fmt.Sprintf("archive/date=%s", date))
    if ok { log.Printf("export for %s already exists. skipping!", date); return nil }

So once a day is published, an observation that arrives afterwards has nowhere
to go. Section 9 proposes "a small correction file or replacement partition"
but nothing measures whether that works with the reader.

Part A measures the evidence the archive itself carries. Each file's footer
records `exported_at`. Comparing that to the day the file covers gives the lag
between the observation window and the export, which bounds how long the
exporter waited for late data.

Part B tests the proposed fix: write a correction file into an already
published day directory, and check that a reader picks it up without the day
being rewritten, and without duplicating anything.

WARNING: Part A's sample is `days[::len(days)//n]` over the days present
*locally*, so running against a partial archive silently samples only the
synced prefix. The first run did exactly that -- ~210 of 608 days were present,
every sampled day landed in 2025-01..2025-07, and the result was written up as
"40 days spread across the archive". The guard below prints the real span;
check it before quoting the output. Same trap as exp12, see results/SUMMARY.md.

Usage: python -I exp11_late_data.py <archive-root> <workdir> [--days N]
"""
import json
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cold_backend as cb


def part_a(root, days):
    """Export lag per day, from the exported_at key in each footer."""
    rows = []
    for d in days:
        ddir = os.path.join(root, d)
        covered = datetime.fromisoformat(d.replace("date=", "")).replace(tzinfo=timezone.utc)
        for fn in sorted(os.listdir(ddir)):
            if not fn.endswith(".parquet"):
                continue
            md = pq.ParquetFile(os.path.join(ddir, fn)).metadata
            kv = md.metadata or {}
            raw = kv.get(b"exported_at")
            if not raw:
                continue
            exported = datetime.fromisoformat(raw.decode().replace("Z", "+00:00"))
            rows.append({
                "date": d, "file": fn,
                "exported_at": exported.isoformat(),
                "lag_days": round((exported - covered).total_seconds() / 86400, 2),
            })
    return rows


def part_b(root, work, day):
    """Append a correction file to a published day and verify the reader."""
    sandbox = os.path.join(work, "late")
    if os.path.exists(sandbox):
        shutil.rmtree(sandbox)
    os.makedirs(sandbox)
    sday = os.path.join(sandbox, day)
    shutil.copytree(os.path.join(root, day), sday)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    date = day.replace("date=", "")

    files = [os.path.join(sday, f) for f in sorted(os.listdir(sday)) if f.endswith(".parquet")]
    fl = "[" + ",".join("'" + f + "'" for f in files) + "]"
    name = con.execute(
        f"select name, count(*) c from read_parquet({fl}) group by name order by c desc limit 1"
    ).fetchone()[0]

    q = {"start": f"{date}T00:00:00Z", "end": f"{date}T00:10:00Z", "filter": {"name": name}}
    before, _, _ = cb.query_archive(con, sandbox, q)

    # Build one late observation inside that window, copying the schema exactly.
    src = pq.ParquetFile(files[0])
    schema = src.schema_arrow
    # Take a real row of this measurement from anywhere in the day: the
    # first file may not contain it, since files are sorted by plugin.
    sample = con.execute(
        f"select * from read_parquet({fl}) where name = '{name}' limit 1"
    ).arrow().read_all()
    assert sample.num_rows == 1, f"no sample row for {name}"
    # Shift it to a timestamp inside the queried window but unlikely to collide.
    t = pa.array(
        [datetime.fromisoformat(f"{date}T00:05:00.123456+00:00")], type=schema.field("time").type
    )
    cols = []
    for f in schema:
        cols.append(t if f.name == "time" else sample.column(f.name))
    late = pa.Table.from_arrays(cols, schema=schema)

    corr = os.path.join(sday, "correction_0.parquet")
    pq.write_table(late, corr, compression="zstd", row_group_size=1000,
                   write_statistics=True, write_page_index=True)

    after, _, nfiles = cb.query_archive(con, sandbox, q)

    # A second reader run must not double-count: idempotence of the read.
    again, _, _ = cb.query_archive(con, sandbox, q)

    return {
        "day": day,
        "measurement": name,
        "records_before": len(before),
        "records_after": len(after),
        "delta": len(after) - len(before),
        "stable_on_reread": len(after) == len(again),
        "files_read_after": nfiles,
        "correction_bytes": os.path.getsize(corr),
        "correction_naming_works": os.path.basename(corr) not in (
            f for f in os.listdir(sday) if f.startswith("data_")
        ),
    }


def main():
    root = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 60
    days = sorted(d for d in os.listdir(root) if d.startswith("date="))

    print("=== Part A: export lag from footer exported_at ===")
    sample_days = days[:: max(1, len(days) // ndays)][:ndays]

    # The sample only represents the archive if the local copy spans it.
    # A partial rsync silently yields a prefix-only sample (see docstring).
    print(f"local archive : {len(days)} days, {days[0]} .. {days[-1]}")
    print(f"sample        : {len(sample_days)} days, "
          f"{sample_days[0][5:]} .. {sample_days[-1][5:]}")
    if len(days) < 608:
        print(f"WARNING: only {len(days)} of 608 known days are present locally. "
              f"This sample covers {sample_days[0][5:]} .. {sample_days[-1][5:]} "
              f"and does NOT represent the whole span.")

    rows = part_a(root, sample_days)
    if rows:
        lags = sorted(r["lag_days"] for r in rows)
        print(f"files sampled : {len(rows)} across {len(sample_days)} days")
        print(f"lag (days) min={lags[0]:.2f}  p50={lags[len(lags)//2]:.2f}  max={lags[-1]:.2f}")
        print("\nThis is export lag, not data lateness: it says how long after a")
        print("day the exporter ran, which upper-bounds the settling window that")
        print("was in effect. It cannot show observations that arrived later still.")
        for r in rows[:3]:
            print(f"  {r['date']} {r['file']:18s} exported {r['exported_at']} (+{r['lag_days']:.0f}d)")
    else:
        print("no exported_at metadata found")

    print("\n=== Part B: correction file in a published day ===")
    b = part_b(root, work, days[0])
    print(json.dumps(b, indent=2))
    print()
    ok = b["delta"] == 1 and b["stable_on_reread"]
    print("RESULT:", "PASS - a correction file is picked up, once, without rewriting the day"
          if ok else "FAIL")
    print()
    print("Caveat: this works because the reader globs the directory. Nothing")
    print("distinguishes a correction from an original, so there is no way to")
    print("tell a late arrival from a duplicate, and no way to supersede a bad")
    print("record. That is the job of the catalog in section 7.")

    # lag_rows is truncated to keep the file small, so the summary stats must
    # be computed over *all* rows and stored alongside -- otherwise anyone
    # recomputing min/median/max from the JSON gets a different answer than
    # the one printed above, which is exactly what happened the first time.
    lags = sorted(r["lag_days"] for r in rows)
    out = {
        "local_days_present": len(days),
        "local_span": [days[0], days[-1]],
        "sampled_days": sample_days,
        "files_sampled": len(rows),
        "lag_days": {
            "min": lags[0],
            "median": lags[len(lags) // 2],
            "max": lags[-1],
        } if lags else None,
        "lag_rows_truncated_to": 200,
        "lag_rows": rows[:200],
        "correction": b,
    }
    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp11_late_data.json", "w") as f:
        json.dump(out, f, indent=2)


if __name__ == "__main__":
    main()
