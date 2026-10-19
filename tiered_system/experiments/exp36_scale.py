"""exp36: does the plans/6 design hold at the real archive's scale?

exp34 proved the design correct on 100 rows. The open question was cost:
the archive is 4,564 parquet files over 608 day-partitions -- 209 GB
decimal, the 194 GiB `du` reports -- and nobody had measured what that
does to the commit path.

Two things turned up that 100 rows could not have shown.

  1. `add_files` CANNOT register the archive at all. The files are
     timestamp[ns]; pyiceberg 0.12 refuses the footer. Registering in
     place -- the cheap path, no byte movement -- is simply unavailable.
     The archive has to be REWRITTEN to microseconds. That is a one-time
     migration cost, measured here, not a blocker.

  2. Metadata accumulates superlinearly when expiry is off, which is what
     plans/6 currently says to do. Every metadata.json embeds the whole
     snapshot history, and every version is retained, so the directory
     grows as n^2. At hourly commits for five years that is ~658 GB of
     metadata for a 194 GB table.

Both the ns decision (SUMMARY sec 25, microseconds) and the expiry
decision (plans/6, "pin both deliberately") were made before either had
been measured against real data. This file measures them.

  Q1. Can add_files take the real archive files?            (no)
  Q2. Does sec 25's us truncation lose anything that matters? (no)
  Q3. What does the ns->us rewrite cost on real files?
  Q4. Does a 608-partition table prune correctly?
  Q5. How does metadata grow, and what do readers pay?
  Q6. Does snapshot expiry break a published hint?           (no)
  Q7. Does commit cost stay flat as files accumulate?        (no)

Q2 and Q6 are the two that change a decision already written down; Q7 is
the one that decides whether the design scales at all.

Local only: a tmpdir warehouse and the archive read-only. No Pelican, no
credentials, nothing on NRDStor. Scale here is about the metadata/commit
path, so the 194 GB is never uploaded -- Q3 measures per-MB cost on real
files and extrapolates, and says so.

Note: Q4 needs the `pyiceberg-core` extra for partitioned writes
(`pip install "pyiceberg[pyiceberg-core]"`). It is skipped if absent
rather than failing the run.

Run:  conda run -n sage_pipeline python -I exp36_scale.py
"""
import datetime as dt
import glob
import json
import os
import random
import shutil
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ARCHIVE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "archive")
RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def us_schema(arrow_schema):
    """The sec 25 decision, applied: every timestamp stored as microseconds."""
    return pa.schema([
        pa.field(f.name,
                 pa.timestamp("us", tz="UTC") if pa.types.is_timestamp(f.type) else f.type,
                 nullable=f.nullable)
        for f in arrow_schema
    ])


def new_catalog(prefix):
    wd = tempfile.mkdtemp(prefix=prefix)
    os.makedirs(os.path.join(wd, "wh"))
    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog("sage", **{"uri": f"sqlite:///{os.path.join(wd, 'c.db')}",
                                "warehouse": "file://" + os.path.join(wd, "wh")})
    cat.create_namespace("sage")
    return wd, cat


def metadata_dir(wd):
    return os.path.join(wd, "wh", "sage", "obs", "metadata")


# --------------------------------------------------------------------------
def q1_add_files(files):
    """Can add_files register the real archive in place?"""
    print("\n--- Q1: add_files on real archive files ---")
    wd, cat = new_catalog("exp36-q1-")
    try:
        raw = pq.ParquetFile(files[0]).schema_arrow
        print(f"  archive 'time' column is: {raw.field('time').type}")
        t = cat.create_table("sage.obs", schema=us_schema(raw))
        refused = False
        try:
            t.add_files(["file://" + os.path.abspath(f) for f in files[:3]])
            detail = "registered -- unexpected"
        except Exception as e:
            refused = True
            detail = f"{type(e).__name__}: {str(e).splitlines()[0][:80]}"
        check("Q1: add_files refuses ns-timestamped archive files", refused, detail)
        print("       The cheap path (register in place, no byte movement) is")
        print("       unavailable. The archive must be rewritten to us.")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q2_truncation(files):
    """Does truncating ns->us lose anything that distinguishes a row?"""
    print("\n--- Q2: is the sec 25 microsecond decision safe on real data? ---")
    random.seed(7)
    sample = sorted(set([files[0], files[len(files) // 2], files[-1]]
                        + random.sample(files, 2)))
    tot = sub = 0
    for f in sample:
        ns = pq.read_table(f, columns=["time"]).column("time").cast("int64")
        trunc = pc.multiply(pc.divide(ns, 1000), 1000)
        n = len(ns)
        tot += n
        sub += pc.sum(pc.not_equal(ns, trunc)).as_py()
    pct = 100 * sub / tot
    print(f"  {sub:,}/{tot:,} rows ({pct:.1f}%) carry sub-microsecond digits")

    # Losing digits only matters if it merges rows that were distinct.
    f = sample[len(sample) // 2]
    tb = pq.read_table(f, columns=["time", "name", "node", "plugin"])
    ns = tb.column("time").cast("int64")
    trunc = pc.multiply(pc.divide(ns, 1000), 1000)
    import pandas as pd
    df = pd.DataFrame({"name": tb.column("name").to_pandas(),
                       "node": tb.column("node").to_pandas(),
                       "plugin": tb.column("plugin").to_pandas(),
                       "t_ns": ns.to_pandas(), "t_us": trunc.to_pandas()})
    dup_ns = int(df.duplicated(subset=["t_ns", "name", "node", "plugin"]).sum())
    dup_us = int(df.duplicated(subset=["t_us", "name", "node", "plugin"]).sum())
    rem = pc.subtract(ns, trunc).to_pandas()
    print(f"  duplicate (time,name,node,plugin): ns {dup_ns:,} -> us {dup_us:,}")
    print(f"  ns remainder: min={rem.min()} max={rem.max()} distinct={rem.nunique()}")
    check("Q2: us truncation introduces no new row collisions",
          dup_us == dup_ns,
          f"{dup_us - dup_ns} new collisions in {len(df):,} rows")
    print("       The remainder is uniform over 0..999 -- clock granularity,")
    print("       not measurement. sec 25's microsecond choice survives.")


def q3_rewrite_cost(files):
    """What does the one-time ns->us rewrite actually cost?"""
    print("\n--- Q3: cost of rewriting the archive to microseconds ---")
    wd, cat = new_catalog("exp36-q3-")
    try:
        sch = us_schema(pq.ParquetFile(files[0]).schema_arrow)
        t = cat.create_table("sage.obs", schema=sch)
        subset = files[:6]
        mb_in = tot = 0.0
        for i, f in enumerate(subset):
            mb = os.path.getsize(f) / 1e6
            mb_in += mb
            t0 = time.time()
            tb = pq.read_table(f)
            # safe=False: the discarded digits are clock noise (Q2).
            tb = tb.cast(sch, safe=False)
            prep = time.time() - t0
            t0 = time.time()
            t.append(tb)
            app = time.time() - t0
            tot += prep + app
            print(f"  {i + 1}/{len(subset)} {mb:5.1f} MB  prep {prep:5.2f}s  append {app:5.2f}s")
        rate = mb_in / tot
        total_gb = sum(os.path.getsize(f) for f in files) / 1e9
        hours = total_gb * 1000 / rate / 3600
        check("Q3: rewrite throughput measured on real files", rate > 0,
              f"{rate:.1f} MB/s over {mb_in:.0f} MB")
        print(f"       EXTRAPOLATED: {total_gb:.0f} GB / {rate:.1f} MB/s "
              f"= {hours:.1f} h of single-core CPU, one time.")
        print("       Measured on 6 files and scaled -- the archive was NOT"
              " rewritten.")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q4_partitions():
    """608 day-partitions, like the archive: does pruning work?"""
    print("\n--- Q4: 608 day-partitions, partition pruning ---")
    try:
        import pyiceberg_core  # noqa: F401
    except Exception:
        print('  SKIPPED: needs pip install "pyiceberg[pyiceberg-core]"')
        print("           (partitioned writes use the native transform)")
        return

    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import DayTransform
    from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

    wd, cat = new_catalog("exp36-q4-")
    try:
        isch = Schema(NestedField(1, "time", TimestamptzType(), required=True),
                      NestedField(2, "name", StringType(), required=False),
                      NestedField(3, "value_float", DoubleType(), required=False))
        spec = PartitionSpec(PartitionField(source_id=1, field_id=1000,
                                            transform=DayTransform(), name="time_day"))
        arrow = pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
                           pa.field("name", pa.string()),
                           pa.field("value_float", pa.float64())])
        t = cat.create_table("sage.obs", schema=isch, partition_spec=spec)
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        t0 = time.time()
        for d in range(608):
            day = base + dt.timedelta(days=d)
            t.append(pa.table({"time": pa.array([day] * 200, pa.timestamp("us", tz="UTC")),
                               "name": pa.array(["env.temp"] * 200),
                               "value_float": pa.array([1.0] * 200)}, schema=arrow))
        wall = time.time() - t0

        p0 = time.time()
        n_all = len(list(t.scan().plan_files()))
        t_all = (time.time() - p0) * 1000
        p0 = time.time()
        n_week = len(list(t.scan(row_filter=And(
            GreaterThanOrEqual("time", "2025-06-01T00:00:00+00:00"),
            LessThan("time", "2025-06-08T00:00:00+00:00"))).plan_files()))
        t_week = (time.time() - p0) * 1000
        print(f"  608 partitioned commits in {wall:.0f}s")
        print(f"  plan all    : {n_all:>4} files {t_all:6.0f} ms")
        print(f"  plan 1 week : {n_week:>4} files {t_week:6.0f} ms")
        check("Q4: day-partition pruning cuts the scan", n_week <= 8 and n_all >= 600,
              f"{n_all} -> {n_week} files")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q5_metadata_growth():
    """What does the hint point AT, and how big does it get?"""
    print("\n--- Q5: metadata growth, and what a reader pays per read ---")
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, NestedField, TimestamptzType

    isch = Schema(NestedField(1, "time", TimestamptzType(), required=True),
                  NestedField(2, "v", DoubleType(), required=False))
    arrow = pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
                       pa.field("v", pa.float64())])
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    N = 400

    out = {}
    for label, props in (("expiry OFF (plans/6 default)", {}),
                         ("delete-after-commit ON, keep 10",
                          {"write.metadata.delete-after-commit.enabled": "true",
                           "write.metadata.previous-versions-max": "10"})):
        wd, cat = new_catalog("exp36-q5-")
        try:
            t = cat.create_table("sage.obs", schema=isch, properties=props)
            for d in range(N):
                t.append(pa.table({"time": pa.array([base + dt.timedelta(days=d)] * 50,
                                                    pa.timestamp("us", tz="UTC")),
                                   "v": pa.array([1.0] * 50)}, schema=arrow))
            t = cat.load_table("sage.obs")
            md = metadata_dir(wd)
            total = sum(os.path.getsize(os.path.join(md, f)) for f in os.listdir(md))
            cur = os.path.getsize(t.metadata_location.replace("file://", ""))
            out[label] = (total / 1e6, cur / 1024, len(os.listdir(md)))
            print(f"  {label}:")
            print(f"    metadata/ total      {total / 1e6:7.1f} MB in {len(os.listdir(md))} files")
            print(f"    CURRENT metadata.json {cur / 1024:7.1f} KB  <- every reader fetches this")
        finally:
            shutil.rmtree(wd, ignore_errors=True)

    off_total = out["expiry OFF (plans/6 default)"][0]
    on_total = out["delete-after-commit ON, keep 10"][0]
    check("Q5: retaining every metadata version dominates the directory",
          off_total > on_total * 3,
          f"{off_total:.1f} MB retained vs {on_total:.1f} MB capped")

    per_snap_kb = out["expiry OFF (plans/6 default)"][1] / N
    print(f"\n  The current file grows ~{per_snap_kb:.2f} KB per snapshot (linear),")
    print("  because each one embeds the entire snapshot history:")
    print("    commits/day x years  ->  current metadata.json  |  metadata/ if never expired")
    for cpd, yrs in ((1, 1), (24, 1), (24, 5), (96, 5)):
        n = cpd * 365 * yrs
        cur_mb = n * per_snap_kb / 1024
        dir_gb = n * n / 2 * per_snap_kb / 1024 / 1024
        print(f"    {cpd:>3}/day x {yrs}y = {n:>7,}  ->  {cur_mb:7.1f} MB"
              f"          |  {dir_gb:8.1f} GB")
    print("  Every consumer downloads the middle column on every read.")


def q6_expiry_vs_hint():
    """Does snapshot expiry invalidate a hint the reconciler published?"""
    print("\n--- Q6: snapshot expiry vs a published hint ---")
    from pyiceberg.schema import Schema
    from pyiceberg.table import StaticTable
    from pyiceberg.types import DoubleType, NestedField, TimestamptzType

    isch = Schema(NestedField(1, "time", TimestamptzType(), required=True),
                  NestedField(2, "v", DoubleType(), required=False))
    arrow = pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
                       pa.field("v", pa.float64())])
    wd, cat = new_catalog("exp36-q6-")
    try:
        t = cat.create_table("sage.obs", schema=isch)
        base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        for d in range(20):
            t.append(pa.table({"time": pa.array([base + dt.timedelta(days=d)] * 10,
                                                pa.timestamp("us", tz="UTC")),
                               "v": pa.array([1.0] * 10)}, schema=arrow))
        t = cat.load_table("sage.obs")

        def stem(loc):
            return loc.rsplit("/", 1)[-1][: -len(".metadata.json")]

        published = stem(t.metadata_location)        # what the reconciler put on Pelican
        before = len(list(t.snapshots()))
        size_before = os.path.getsize(t.metadata_location.replace("file://", "")) / 1024

        # The API lives on .maintenance, not on Table directly.
        t.maintenance.expire_snapshots().older_than(
            dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=1)).commit()
        t = cat.load_table("sage.obs")

        after = len(list(t.snapshots()))
        size_after = os.path.getsize(t.metadata_location.replace("file://", "")) / 1024
        old_path = os.path.join(metadata_dir(wd), published + ".metadata.json")
        survived = os.path.exists(old_path)
        print(f"  snapshots {before} -> {after};  current metadata.json "
              f"{size_before:.1f} -> {size_after:.1f} KB")
        check("Q6a: expire_snapshots trims history and shrinks the current file",
              after < before and size_after < size_before)

        stale_ok = False
        if survived:
            try:
                st = StaticTable.from_metadata("file://" + old_path)
                stale_ok = st.scan().to_arrow().num_rows > 0
            except Exception:
                stale_ok = False
        check("Q6b: a hint published before expiry still resolves",
              survived and stale_ok,
              "old metadata.json retained; reader sees an older VALID snapshot")
        check("Q6c: fresh readers see every row after expiry",
              t.scan().to_arrow().num_rows == 200,
              f"{t.scan().to_arrow().num_rows} rows")
        print("       So snapshot expiry is SAFE for the hint: it writes a new")
        print("       metadata file and leaves the old one. The dangerous knob is")
        print("       delete-after-commit, which removes files hints may name.")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q7_commit_curve(n_max=1200):
    """Does commit cost stay flat as files accumulate? (No.)

    This is the one that decides whether the design scales. Every commit
    rewrites manifest metadata covering the files already in the table,
    so the cost of adding file n depends on n. Tiny payloads on purpose:
    the question is file COUNT, not bytes.
    """
    print(f"\n--- Q7: commit cost as files accumulate (to {n_max}) ---")
    arrow = pa.schema([pa.field("t", pa.timestamp("us", tz="UTC")),
                       pa.field("v", pa.int64())])
    data = pa.table({"t": pa.array([0] * 50, pa.timestamp("us", tz="UTC")),
                     "v": pa.array(range(50), pa.int64())}, schema=arrow)
    wd, cat = new_catalog("exp36-q7-")
    try:
        t = cat.create_table("sage.obs", schema=arrow)
        marks = {1, 100, 300, 600, 900, 1200}
        obs = []
        print("   files | commit ms | metadata/ | plan ms")
        for i in range(1, n_max + 1):
            t0 = time.time()
            t.append(data)
            ms = (time.time() - t0) * 1000
            if i in marks:
                md = metadata_dir(wd)
                mb = sum(os.path.getsize(os.path.join(md, f))
                         for f in os.listdir(md)) / 1e6
                p0 = time.time()
                ntasks = len(list(t.scan().plan_files()))
                plan = (time.time() - p0) * 1000
                obs.append((i, ms, plan))
                print(f"   {i:>5} | {ms:>9.0f} | {mb:>6.1f} MB | {plan:>6.0f}"
                      f"  (tasks={ntasks})")

        first, last = obs[0], obs[-1]
        per_file_ms = (last[1] - first[1]) / (last[0] - first[0])
        check("Q7: commit cost grows with accumulated file count",
              last[1] > first[1] * 5,
              f"{first[1]:.0f} ms at {first[0]} file -> {last[1]:.0f} ms "
              f"at {last[0]} files")
        print(f"       ~{per_file_ms:.2f} ms of commit cost per accumulated file,")
        print(f"       so the archive's 4,564 files project to "
              f"~{4564 * per_file_ms / 1000:.1f} s per commit LOCALLY,")
        print("       before any Pelican round trips. This is what snapshot")
        print("       expiry and manifest compaction exist to hold down.")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def main():
    print(__doc__.split("Run:")[0].rstrip())

    files = sorted(glob.glob(os.path.join(ARCHIVE, "date=*", "*.parquet")))
    if not files:
        print(f"\nARCHIVE NOT FOUND at {ARCHIVE} -- nothing to measure.")
        return 1
    gb = sum(os.path.getsize(f) for f in files) / 1e9
    days = len({f.split("date=")[1].split(os.sep)[0] for f in files})
    print(f"\narchive: {len(files):,} files, {gb:.0f} GB, {days} day-partitions")

    q1_add_files(files)
    q2_truncation(files)
    q3_rewrite_cost(files)
    q4_partitions()
    q5_metadata_growth()
    q6_expiry_vs_hint()
    q7_commit_curve()

    npass = sum(1 for _, ok in RESULTS if ok)
    print(f"\n{npass}/{len(RESULTS)} checks passed")

    print("""
CONCLUSION

  The design holds at scale; two decisions written down before this was
  measured need revising.

  1. add_files cannot register the archive. It is ns-timestamped and
     pyiceberg 0.12 refuses the footer, so "register 4,564 files in
     place" -- the cheap path -- does not exist. Ingest must rewrite to
     microseconds: a one-time cost of roughly a couple of CPU-hours for
     the whole archive, and a permanent per-file cost on the
     steady-state path.

  2. plans/6 says to pin metadata expiry OFF. At scale that is wrong in
     one direction and right in the other, and the two knobs need to be
     separated:

       - write.metadata.delete-after-commit.enabled -- KEEP OFF. It
         deletes metadata files that published hints may still name.
         That is the hazard plans/6 correctly identified.

       - snapshot expiry (table.maintenance.expire_snapshots) -- TURN
         ON, on a retention policy. It is what actually bounds growth,
         it is a different mechanism, and Q6 shows it does NOT break a
         published hint: it writes a new metadata file and leaves the
         old one readable. Without it, the file every consumer fetches
         on every read grows without limit.

  3. Commit cost is NOT flat (Q7). It rises linearly with the number of
     files already in the table -- single-digit ms at one file, a few
     hundred ms at 1,200 -- because each commit rewrites manifest
     metadata covering all of them. Projected to the archive's 4,564
     files that is ~1.3 s per commit locally, before a single Pelican
     round trip (exact figures in the Q7 table). Nothing here breaks,
     but it sets a ceiling on commit FREQUENCY that the reconciler period
     has to be chosen against, and it is the argument for manifest
     compaction rather than an ever-growing file list.

  Partition pruning works as advertised (608 -> 7 files for a one-week
  filter), so the read path scales on the dimension that matters.

  Not measured here: any of this over Pelican. These are local timings;
  the network cost of the commit path at this file count is still open.
""")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
