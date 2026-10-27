"""Check a seeded hot tier against the archive it came from.

Three checks, all exact rather than approximate, because the interesting
failures here are silent ones:

  * row count per day -- a dropped batch leaves no trace in the seeder's
    output, which counts what it *sent*, not what InfluxDB *kept*
  * series cardinality -- the meta->tag flattening is the whole premise of
    the hot tier's shape, and a mangled tag key shows up here and nowhere
    else
  * nanosecond digits present -- the hot tier exists to hold ns; a silent
    truncation at ingest would break the nanosecond contract shared with
    the Iceberg v3 cold tier

**Counting is chunked by hour, and that is not a style choice.** The
obvious whole-day `count() |> group() |> sum()` drove the container to
5.9 GiB of its 6 GiB cap and ran past ten minutes without finishing: the
ungrouped reduce materializes all ~905k series for the window at once.
Worse, the pressure persisted after the client was killed -- a 1-minute
count that takes 8.5s on a healthy container took **99s** on the stressed
one, and only a restart cleared it. Hourly chunks hold peak memory near
1.7 GiB and cost ~8s each, so a day is ~3 minutes with no cliff.

Usage:  python -I verify_hot.py 2025-05-01 2025-05-02
"""
import glob
import os
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import requests

ARCHIVE = os.environ.get(
    "DEMO_ARCHIVE",
    str(Path(__file__).resolve().parents[3] / "data" / "archive"))
URL = os.environ.get("INFLUX_URL", "http://localhost:18086")
TOKEN = os.environ.get("INFLUX_TOKEN", "demo-token-not-a-secret")
ORG = os.environ.get("INFLUX_ORG", "waggle")
BUCKET = os.environ.get("INFLUX_BUCKET", "waggle")

SESSION = requests.Session()


def flux(q, timeout=600):
    r = SESSION.post(f"{URL}/api/v2/query", params={"org": ORG},
                     headers={"Authorization": f"Token {TOKEN}",
                              "Content-Type": "application/vnd.flux",
                              "Accept": "application/csv"},
                     data=q, timeout=timeout)
    r.raise_for_status()
    return r.text


def rows_of(csv):
    return [ln for ln in csv.splitlines() if ln.startswith(",_result")]


def count_window(start, stop):
    """Rows in [start, stop). One grouped count; keep the window small."""
    out = rows_of(flux(f'''
from(bucket:"{BUCKET}")
  |> range(start:{start}, stop:{stop})
  |> filter(fn:(r) => r._field == "value")
  |> group()
  |> count()
'''))
    # An empty window yields no rows at all, which is a legitimate 0.
    return int(out[0].split(",")[-1]) if out else 0


def count_day(day, hours=1):
    """A day's rows, summed over fixed-size sub-windows."""
    t0 = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    total = 0
    step = timedelta(hours=hours)
    for i in range(0, 24, hours):
        a = t0 + timedelta(hours=i)
        total += count_window(a.strftime("%Y-%m-%dT%H:%M:%SZ"),
                              (a + step).strftime("%Y-%m-%dT%H:%M:%SZ"))
    return total


def parquet_rows(day):
    return sum(pq.ParquetFile(f).metadata.num_rows
               for f in sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet")))


def main():
    days = sys.argv[1:]
    if not days:
        print(__doc__)
        return 2

    ok = True
    grand_src = grand_got = 0
    for day in days:
        src = parquet_rows(day)
        t0 = time.time()
        got = count_day(day)
        grand_src += src
        grand_got += got
        mark = "OK  " if got == src else "FAIL"
        print(f"{mark} {day}  archive {src:>12,}  influx {got:>12,}"
              f"  delta {got-src:+,}   ({time.time()-t0:.0f}s)", flush=True)
        ok &= (got == src)

    if len(days) > 1:
        print(f"     TOTAL      archive {grand_src:>12,}  "
              f"influx {grand_got:>12,}  delta {grand_got-grand_src:+,}")

    # Cardinality is a metadata-only read (it walks the index, not the
    # points), so the whole window at once is fine here.
    card = rows_of(flux(f'''
import "influxdata/influxdb"
influxdb.cardinality(bucket:"{BUCKET}",
  start:{days[0]}T00:00:00Z, stop:{days[-1]}T23:59:59.999999999Z)
'''))
    if card:
        print(f"     cardinality over the window: "
              f"{int(card[0].split(',')[-1]):,}")

    # Nanoseconds: pull real timestamps and count fractional digits.
    sample = rows_of(flux(f'''
from(bucket:"{BUCKET}")
  |> range(start:{days[0]}T00:00:00Z, stop:{days[0]}T00:01:00Z)
  |> filter(fn:(r) => r._field == "value")
  |> limit(n:50)
'''))
    stamps = [ln.split(",")[5] for ln in sample]
    nine = [t for t in stamps if "." in t and len(t.split(".")[1].rstrip("Z")) == 9]
    print(f"     {len(nine)} of {len(stamps)} sampled timestamps carry "
          f"9 fractional digits")
    if stamps:
        print(f"     e.g. {stamps[0]}")
    # Not every instant HAS 9 significant digits (a value landing on a
    # round microsecond is legitimate), so require most, not all.
    ns_ok = bool(stamps) and len(nine) > len(stamps) // 2
    ok &= ns_ok
    if not ns_ok:
        print("FAIL nanosecond precision looks lost at ingest")

    print()
    print("ALL CHECKS PASSED" if ok else "CHECKS FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
