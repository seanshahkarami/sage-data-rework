"""Seed the cold tier: archive Parquet -> Iceberg on Pelican.

Mar + Apr 2025 go straight to cold, bypassing InfluxDB; May seeds hot and
crosses the boundary later via migrate.py. This is the "already archived"
half of the demo, so it is a plain load, not a migration.

Two things make it more than a copy:

  * Timestamps are cast ns -> us. pyiceberg 0.12.0 cannot store nanoseconds:
    a v2 table rejects timestamp[ns] outright and v3, which has the type,
    raises NotImplementedError on write (apache/iceberg-python#1551). The
    cast rewrites the data, so `add_files()` on the archive in place is NOT
    available -- this is a full read+write of ~36 GB across the two months.
    Measured safe: truncating a full 146M-row day to us collided 0 times on
    the full series key, so no observation is merged or lost.

  * One commit per MONTH, day-partitioned. Production evicts from InfluxDB
    monthly, so the seeder publishes on the same rhythm. Partitioning is a
    separate axis and stays daily: exp36 measured that a one-week filter
    then plans 7 files instead of 608.

    These two are independent because `add_files` derives each file's
    partition from its Parquet footer statistics, not from the commit
    boundary (pyiceberg io/pyarrow.py:2905 -> io/fileformat.py:86).

Getting to one snapshot per month needs the two-stage shape below, because
a transaction does NOT coalesce appends -- measured, 10 `tx.append()` calls
produce 10 snapshots, and the first real day produced 141. Each day is
staged to Parquet and PUT, then the whole month is registered with a single
`add_files()`. Concatenating a month in memory is the other way to get one
snapshot and is not available: one day alone is 49 GB in Arrow.

Writes only under a fresh /cold-storage-demo-<nanos> prefix, and refuses to
start if the prefix is not empty. /node-data is live at the Pelican root and
is never touched.

Usage:
  python -I seed_cold.py --start 2025-03-01 --end 2025-04-30
  python -I seed_cold.py --start 2025-03-01 --end 2025-03-02 --prefix /cold-storage-demo-123
"""
import argparse
import glob
import json
import os
import sys
import tempfile
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# `python -I` (how these scripts are run) leaves the script directory off
# sys.path. The Pelican modules live in demo/reconciler_service/.
_HERE = os.path.dirname(os.path.abspath(__file__))
_DEMO = os.path.dirname(_HERE)
_PELICAN = os.path.join(_DEMO, "reconciler_service")
if _PELICAN not in sys.path:
    sys.path.insert(0, _PELICAN)

import pelican_fileio
import pelican_token as pt
from pelican_fileio import PelicanFileIO

ARCHIVE = os.environ.get(
    "DEMO_ARCHIVE",
    "/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/sage-data-rework/data/archive")

# `catalog_name` is part of the iceberg_tables primary key, so a different
# name here yields a private catalog that nothing else can resolve.
CATALOG_NAME = "sage"
NAMESPACE = "sage"
TABLE = "obs"
FILEIO = "pelican_fileio.PelicanFileIO"


def us_schema(arrow_schema):
    """Every timestamp column as microseconds; everything else unchanged."""
    return pa.schema([
        pa.field(f.name,
                 pa.timestamp("us", tz="UTC") if pa.types.is_timestamp(f.type)
                 else f.type,
                 nullable=f.nullable)
        for f in arrow_schema
    ])


def iceberg_schema():
    """The Iceberg schema, mirroring the archive's columns."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (DoubleType, LongType, MapType, NestedField,
                                 StringType, TimestamptzType)
    f = []
    f.append(NestedField(1, "time", TimestamptzType(), required=True))
    f.append(NestedField(2, "name", StringType(), required=False))
    fid = 3
    for tag in ("plugin", "vsn", "node", "host", "task", "job", "zone"):
        f.append(NestedField(fid, tag, StringType(), required=False))
        fid += 1
    f.append(NestedField(fid, "meta",
                         MapType(fid + 100, StringType(), fid + 101,
                                 StringType(), value_required=False),
                         required=False))
    fid += 1
    f.append(NestedField(fid, "value_float", DoubleType(), required=False))
    f.append(NestedField(fid + 1, "value_int", LongType(), required=False))
    f.append(NestedField(fid + 2, "value_str", StringType(), required=False))
    return Schema(*f)


def day_partition_spec():
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import DayTransform
    return PartitionSpec(
        PartitionField(source_id=1, field_id=1000,
                       transform=DayTransform(), name="time_day"))


def days_between(start, end):
    d, out = start, []
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def months_of(days):
    """Group day strings into {'YYYY-MM': [day, ...]}, order preserved.

    The commit unit. Days stay the partition unit.
    """
    out = OrderedDict()
    for day in days:
        out.setdefault(day[:7], []).append(day)
    return out


def day_bounds(day):
    """[D, D+1) as timestamps, for the clip below."""
    d0 = date.fromisoformat(day)
    lo = datetime(d0.year, d0.month, d0.day, tzinfo=timezone.utc)
    return lo, lo + timedelta(days=1)


def clip_to_day(table, day):
    """Keep only [D, D+1). Returns (table, n_dropped).

    The archive's date=D is inclusive at BOTH ends: date=2025-03-31 carries
    rows stamped exactly 2025-04-01T00:00:00, and date=2025-04-01 carries
    that same set again -- verified identical, EXCEPT ALL both ways returns
    0, so this drops duplicates rather than data.

    It is also required, not merely tidy. add_files infers a file's
    partition from its footer min/max and RAISES when they fall in
    different partitions ("Cannot infer partition value ... more than one
    partition values", pyiceberg io/fileformat.py:78-82). An unclipped day
    spans midnight, so the monthly add_files would abort on it.
    """
    lo, hi = day_bounds(day)
    t = table.column("time")
    keep = pc.and_(pc.greater_equal(t, pa.scalar(lo, t.type)),
                   pc.less(t, pa.scalar(hi, t.type)))
    out = table.filter(keep)
    return out, table.num_rows - out.num_rows


def stage_day(day, files, target, batch_rows, prefix, verbose=True):
    """Write one day as a single microsecond Parquet file on Pelican.

    Streams: a ParquetWriter takes chunk after chunk, so peak memory stays
    at one chunk rather than the 49 GB a whole day occupies in Arrow.
    Measured on 2025-03-01: 9.00 GB RSS at 10M rows, 4.64 GB at 2M, and the
    2M batch was also faster (53s vs 74s). The local temp file is deleted
    once PUT.

    Returns (url, rows, dropped).
    """
    fd, tmp = tempfile.mkstemp(prefix=f"seedcold-{day}-", suffix=".parquet")
    os.close(fd)
    rows = dropped = 0
    t0 = time.time()
    try:
        w = pq.ParquetWriter(tmp, target)
        try:
            for f in files:
                for chunk in iter_chunks_us(f, target, batch_rows):
                    chunk, d = clip_to_day(chunk, day)
                    dropped += d
                    if chunk.num_rows:
                        w.write_table(chunk)
                        rows += chunk.num_rows
                    del chunk
        finally:
            w.close()

        size = os.path.getsize(tmp)
        path = f"{prefix}/staged/{day}.parquet"
        with open(tmp, "rb") as fh:
            st, _, body = pelican_fileio._request(path, "PUT", data=fh.read())
        if st not in (200, 201, 204):
            raise IOError(f"PUT {path} -> {st} {body[:200]!r}")
    finally:
        os.unlink(tmp)

    if verbose:
        el = time.time() - t0
        print(f"    {day}: {rows:>12,} rows  {size/1e6:7.1f} MB  {el:6.1f}s  "
              f"{size/1e6/max(el, 1e-9):5.1f} MB/s  dropped {dropped}",
              flush=True)
    return f"{pt.ROOT_URL}{path}", rows, dropped


def iter_chunks_us(path, target_schema, batch_rows):
    """Yield microsecond-cast Arrow tables from one archive file.

    Streaming, because materializing is not affordable: measured on
    2025-03-01, reading the day's 15 files and concatenating peaked at
    **9.22 GB RSS for a 49.28 GB Arrow table** -- one day. Holding a day
    whole would not survive the 8 GB VM, let alone 61 of them.

    The archive's 0.6 GB/day of Parquet expands ~80x in Arrow because the
    `meta` map and the string columns are dictionary-encoded on disk and
    materialized per row in memory.
    """
    pf = pq.ParquetFile(path)
    for rb in pf.iter_batches(batch_size=batch_rows):
        t = pa.Table.from_batches([rb])
        # safe=False is required twice over: the ns->us cast is lossy by
        # construction (that is the point -- cold is microsecond) and a
        # checked cast refuses it.
        t = t.cast(us_schema(t.schema), safe=False)
        yield t.cast(target_schema, safe=False)


def load_progress(path):
    """Staged-but-uncommitted work from an earlier run.

    A month is one commit now, so a crash mid-month leaves staged files
    that nothing references. Without this, a resume re-uploads every one of
    them -- hours of PUTs for data already on Pelican. Shape:

        {"staged": {"2025-03": {"2025-03-01": [url, rows, dropped], ...}},
         "committed": ["2025-03"]}
    """
    if not os.path.exists(path):
        return {"staged": {}, "committed": []}
    with open(path) as fh:
        p = json.load(fh)
    p.setdefault("staged", {})
    p.setdefault("committed", [])
    return p


def save_progress(path, prog):
    """Write via a temp file in the same directory, then rename.

    The point of the progress file is to survive a crash, so it must never
    be observed half-written. rename(2) is atomic within a filesystem.
    """
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        json.dump(prog, fh, indent=1, sort_keys=True)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--prefix",
                    help="Pelican prefix; default /cold-storage-demo-<nanos>")
    ap.add_argument("--db", default=os.path.join(_DEMO, "cold_catalog.db"),
                    help="local sqlite catalog file")
    ap.add_argument("--batch-rows", type=int, default=2_000_000,
                    help="rows per Arrow chunk; bounds peak memory. A whole "
                         "day is ~49 GB in Arrow, so this must stay modest. "
                         "2M peaks at 4.64 GB RSS on 2025-03-01; 10M peaks "
                         "at 9.00 GB and is slower.")
    ap.add_argument("--progress", default=os.path.join(_HERE, "logs", "seed_cold.progress"),
                    help="resume file; records staged URLs and committed "
                         "months so a crash mid-month does not re-upload")
    ap.add_argument("--dry-run", action="store_true",
                    help="read and cast, but do not write to Pelican")
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    days = days_between(start, end)
    months = months_of(days)

    prefix = args.prefix or f"/cold-storage-demo-{time.time_ns()}"
    if not prefix.startswith("/cold-storage-demo"):
        ap.error("prefix must start with /cold-storage-demo "
                 "(the archive root holds live /node-data)")
    warehouse = f"{pt.ROOT_URL}{prefix}/warehouse"

    print(f"days      : {days[0]} .. {days[-1]} ({len(days)})")
    print(f"months    : {', '.join(months)}  ({len(months)} commits)")
    print(f"prefix    : {prefix}")
    print(f"warehouse : {warehouse}")

    if args.dry_run:
        import resource
        schema = iceberg_schema().as_arrow()
        tot = 0
        for day in days:
            files = sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet"))
            if not files:
                print(f"  {day}: no files")
                continue
            t0, rows, chunks = time.time(), 0, 0
            for f in files:
                for chunk in iter_chunks_us(f, schema, args.batch_rows):
                    rows += chunk.num_rows
                    chunks += 1
                    del chunk
            tot += rows
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9
            print(f"  {day}: {rows:>12,} rows in {chunks} chunks, "
                  f"{time.time()-t0:5.1f}s  peak RSS {peak:4.2f} GB")
        print(f"dry run ok, {tot:,} rows")
        return 0

    prog_path = os.path.abspath(args.progress)
    prog = load_progress(prog_path)
    resuming = prog.get("prefix") == prefix and (prog["staged"] or
                                                 prog["committed"])

    # Module-level function, NOT a method on PelicanFileIO -- calling it as
    # one raised AttributeError, which meant this guard had never actually
    # run. It is the check that stops a seed landing on top of existing
    # data, so it is the last thing that should fail silently.
    st, kids = pelican_fileio.propfind(prefix)
    if st != 404 and not resuming:
        # Refuse to write into anything that already exists. The one thing
        # this script must never do is land on top of live data.
        print(f"ABORT: prefix not empty (PROPFIND -> {st}, {len(kids)} children)")
        print(f"       (no resumable progress for this prefix in {prog_path})")
        return 2
    if resuming:
        # A resume is the one legitimate reason to meet a non-empty prefix:
        # the progress file names this exact prefix, so the occupant is our
        # own staged output. Anything else still aborts above.
        print(f"resuming into {prefix} "
              f"({len(prog['committed'])} months committed, "
              f"{sum(len(v) for v in prog['staged'].values())} days staged)")
    else:
        print("fresh prefix: PROPFIND -> 404")
        prog["prefix"] = prefix
        save_progress(prog_path, prog)

    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog(CATALOG_NAME, **{
        "uri": f"sqlite:///{os.path.abspath(args.db)}",
        "warehouse": warehouse,
        "py-io-impl": FILEIO,
    })
    try:
        cat.create_namespace(NAMESPACE)
    except Exception:
        pass  # already there on a resumed run

    ident = f"{NAMESPACE}.{TABLE}"
    try:
        tbl = cat.load_table(ident)
        print(f"loaded existing table {ident}")
    except Exception:
        tbl = cat.create_table(ident, schema=iceberg_schema(),
                               partition_spec=day_partition_spec())
        print(f"created table {ident} (day-partitioned)")

    target = tbl.schema().as_arrow()
    grand, dropped_all, t_all = 0, 0, time.time()

    for month, mdays in months.items():
        if month in prog["committed"]:
            print(f"{month}: already committed, skipping", flush=True)
            continue
        t_m = time.time()
        staged = prog["staged"].setdefault(month, {})

        # Stage 1: one microsecond Parquet file per day, PUT to Pelican.
        # Nothing is registered yet, so a crash here costs only the
        # unstaged remainder -- the progress file keeps the rest.
        print(f"{month}: staging {len(mdays)} days", flush=True)
        for day in mdays:
            if day in staged:
                print(f"    {day}: already staged, skipping", flush=True)
                continue
            files = sorted(glob.glob(f"{ARCHIVE}/date={day}/*.parquet"))
            if not files:
                print(f"    {day}: no archive files, skipping", flush=True)
                continue
            url, rows, dropped = stage_day(day, files, target,
                                           args.batch_rows, prefix)
            staged[day] = [url, rows, dropped]
            save_progress(prog_path, prog)

        if not staged:
            print(f"{month}: nothing staged, no commit", flush=True)
            continue

        # Stage 2: ONE add_files for the whole month -> ONE snapshot.
        #
        # This is the cadence the design calls for: production evicts from
        # InfluxDB monthly, so cold publishes monthly. It cannot be done
        # with appends -- a transaction does not coalesce them, so a month
        # of appends is ~465 snapshots, each ~0.7 KB in the metadata.json
        # that every consumer fetches on every hint resolution.
        #
        # Day-partitioning survives because add_files reads each file's
        # footer statistics to place it (io/pyarrow.py:2905), so the
        # partition comes from the data, not from the commit boundary.
        urls = [staged[d][0] for d in sorted(staged)]
        m_rows = sum(staged[d][1] for d in staged)
        m_drop = sum(staged[d][2] for d in staged)
        t0 = time.time()
        tbl.add_files(urls)
        tbl = cat.load_table(ident)   # refresh to the new head

        prog["committed"].append(month)
        save_progress(prog_path, prog)
        grand += m_rows
        dropped_all += m_drop
        print(f"{month}: committed {len(urls)} files, {m_rows:,} rows in "
              f"{time.time()-t0:.1f}s  (stage+commit {(time.time()-t_m)/60:.1f} min)"
              f"  snapshots now {len(tbl.metadata.snapshots)}", flush=True)

    el = time.time() - t_all
    print(f"\nloaded {grand:,} rows in {el/60:.1f} min")
    print(f"snapshots : {len(tbl.metadata.snapshots)} "
          f"(expected {len(prog['committed'])}, one per month)")
    print(f"dropped   : {dropped_all} duplicate rows at day boundaries "
          f"(archive date=D is inclusive at both ends)")
    print(f"catalog db: {os.path.abspath(args.db)}")
    print(f"hint path : {prefix}/warehouse/{NAMESPACE}/{TABLE}"
          f"/metadata/version-hint.text")
    print("run reconciler.py next to publish the hint")
    return 0


if __name__ == "__main__":
    sys.exit(main())
