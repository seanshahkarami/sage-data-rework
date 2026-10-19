"""exp40: how much storage does Iceberg add on top of the parquet files?

Every experiment so far measured Iceberg's metadata in REQUESTS (sec 37:
4 requests, O(1)) or in the growth of one file (sec 36 Q5: ~0.7 KB per
snapshot inside metadata.json). Nobody measured the total BYTES the
metadata layer occupies next to a 209 GB archive, which is the question
somebody sizing storage actually asks.

The pieces Iceberg writes that plain parquet does not:

  metadata/vNNNNN-*.metadata.json  table schema + the whole snapshot
                                   history. One NEW FILE per commit, and
                                   plans/6 says keep the old ones (a
                                   published hint may still name one).
  metadata/snap-*.avro             manifest list: one row per manifest.
  metadata/*-m0.avro               manifest: one row per data file, with
                                   per-column lower/upper bounds. This is
                                   the part that scales with file count.

So the overhead has two shapes and they must not be conflated:

  per data file   -- manifest entries. Grows with the table.
  per commit      -- a new metadata.json embedding ALL history. Grows
                     with how often we write, and quadratically in total
                     bytes when old versions are retained.

A number like "Iceberg costs X% extra" is meaningless without saying
which of those is being counted and at what commit rate, so this
measures them separately and only then combines them.

  Q1. Metadata bytes per data file, and the data:metadata ratio.
  Q2. Where do those bytes go -- manifest, manifest list, or json?
  Q3. Does the per-file cost stay flat as the table grows?
  Q4. What does commit granularity cost (per-file vs per-day)?
  Q5. Does retaining every metadata.json dominate everything else?
  Q6. Does the custom catalog's snapshot JSON cost more or less?
  Q7. Does expire_snapshots() actually reclaim storage?        (NO)
  Q8. Does expiry reclaim DATA files after a delete?           (NO)

Q7 exists because my first write-up of this experiment got it wrong. I
carried "snapshot expiry bounds metadata growth" forward from sec 36 Q5,
which had measured expiry against the *current* metadata.json -- the
per-READ cost. Applied to stored bytes the claim is false: expiry writes a
NEW metadata file and leaves every old one in place. Two different costs,
one conclusion reused across them.

Q1 uses files holding REAL archive rows, because manifest entries store
per-column lower/upper bounds and the archive's string columns (name,
plugin, host, task, ...) are what set their width. Synthetic data would
understate this.

Local only: a tmpdir warehouse, the archive opened READ-ONLY, nothing
uploaded, no credentials, no Pelican. Metadata bytes are identical
wherever the table lives, so the network adds nothing to this question.

Run:  conda run -n sage_pipeline python -I exp40_metadata_storage.py
"""
import glob
import json
import os
import shutil
import sys
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ARCHIVE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "archive")
ARCHIVE_FILES = 4564          # measured, exp36
ARCHIVE_BYTES = 209_000_000_000   # 209 GB decimal, the 194 GiB du reports
PRUNE_COL = "name"                # a non-null archive column, for Q8's delete
RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def us_schema(arrow_schema):
    """The sec 25 decision: every timestamp stored as microseconds."""
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


def meta_dir(wd):
    return os.path.join(wd, "wh", "sage", "obs", "metadata")


def dir_bytes(d, pattern="*"):
    return sum(os.path.getsize(p) for p in glob.glob(os.path.join(d, pattern))
               if os.path.isfile(p))


def breakdown(wd):
    """Metadata bytes split by the three kinds of file Iceberg writes."""
    d = meta_dir(wd)
    json_b = dir_bytes(d, "*.metadata.json")
    snap_b = dir_bytes(d, "snap-*.avro")
    # manifests are the *.avro that are not manifest lists
    man_b = sum(os.path.getsize(p) for p in glob.glob(os.path.join(d, "*.avro"))
                if not os.path.basename(p).startswith("snap-"))
    return {"json": json_b, "snaplist": snap_b, "manifest": man_b,
            "total": json_b + snap_b + man_b}


def sample_rows(n_rows):
    """Real archive rows, so string lower/upper bounds are realistic."""
    src = sorted(glob.glob(os.path.join(ARCHIVE, "date=*", "*.parquet")))[0]
    pf = pq.ParquetFile(src)
    tb = next(pf.iter_batches(batch_size=n_rows))
    tb = pa.Table.from_batches([tb])
    # safe=False is the sec 25 decision applied: 94% of archive rows carry
    # sub-microsecond digits, and exp36 Q2 showed truncating them merges no
    # rows that were distinct. A safe cast refuses outright.
    return tb.cast(us_schema(tb.schema), safe=False)


def write_parts(tbl, data, n_files, rows_each, tmp):
    """Append n_files separate data files, one commit each."""
    paths = []
    for i in range(n_files):
        chunk = data.slice((i * rows_each) % max(1, len(data) - rows_each), rows_each)
        p = os.path.join(tmp, f"part_{i:05d}.parquet")
        pq.write_table(chunk, p)
        paths.append("file://" + p)
    return paths


# --------------------------------------------------------------------------
def q1_q2_q3_scaling():
    """Per-file metadata cost, its breakdown, and whether it stays flat."""
    print("\n--- Q1/Q2/Q3: metadata bytes per data file ---")
    data = sample_rows(2000)
    wd, cat = new_catalog("exp40-q1-")
    tmp = tempfile.mkdtemp(prefix="exp40-parts-")
    rows = []
    try:
        t = cat.create_table("sage.obs", schema=data.schema)
        paths = write_parts(t, data, 64, 100, tmp)

        # Register in batches, sampling the breakdown as the table grows.
        done = 0
        for target in (1, 2, 4, 8, 16, 32, 64):
            t.add_files(paths[done:target])
            done = target
            b = breakdown(wd)
            data_b = sum(os.path.getsize(p[7:]) for p in paths[:target])
            rows.append((target, b, data_b))

        print(f"\n  {'files':>6} {'manifest':>10} {'snaplist':>9} {'json':>10} "
              f"{'total':>10} {'B/file':>8}")
        for n, b, _ in rows:
            print(f"  {n:>6} {b['manifest']:>10,} {b['snaplist']:>9,} "
                  f"{b['json']:>10,} {b['total']:>10,} {b['total']//n:>8,}")

        last_n, last_b, last_data = rows[-1]

        # Q2: which component dominates at this file count?
        biggest = max(("manifest", last_b["manifest"]),
                      ("snaplist", last_b["snaplist"]),
                      ("json", last_b["json"]), key=lambda kv: kv[1])
        print(f"\n  at {last_n} files the largest component is {biggest[0]} "
              f"({100*biggest[1]/last_b['total']:.0f}% of metadata)")

        # Q3: the MARGINAL manifest cost per file -- the part that scales
        # with the table rather than with commit count.
        n0, b0, _ = rows[-2]
        marg = (last_b["manifest"] - b0["manifest"]) / (last_n - n0)
        print(f"  marginal manifest cost: {marg:,.0f} B per added data file")

        check("Q1: metadata is measurable and non-zero",
              last_b["total"] > 0,
              f"{last_b['total']:,} B of metadata for {last_n} data files")

        # Per-file manifest cost should not blow up as the table grows;
        # a few hundred bytes per entry is the expected shape.
        check("Q3: marginal manifest cost per file is bounded (< 4 KB)",
              0 < marg < 4096, f"{marg:,.0f} B/file")

        return marg, rows, last_data
    finally:
        shutil.rmtree(wd, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)


def q4_commit_granularity():
    """Same files, same data: 1 commit per file vs one commit for all."""
    print("\n--- Q4: what does commit granularity cost in metadata bytes? ---")
    data = sample_rows(2000)
    out = {}
    for label, batch in (("per-file (32 commits)", 1), ("batched (1 commit)", 32)):
        wd, cat = new_catalog("exp40-q4-")
        tmp = tempfile.mkdtemp(prefix="exp40-q4p-")
        try:
            t = cat.create_table("sage.obs", schema=data.schema)
            paths = write_parts(t, data, 32, 100, tmp)
            for i in range(0, 32, batch):
                t.add_files(paths[i:i + batch])
            b = breakdown(wd)
            n_json = len(glob.glob(os.path.join(meta_dir(wd), "*.metadata.json")))
            out[label] = (b, n_json)
            print(f"  {label:<24} total {b['total']:>9,} B  "
                  f"(json {b['json']:>8,} in {n_json:>2} files, "
                  f"manifest {b['manifest']:>7,})")
        finally:
            shutil.rmtree(wd, ignore_errors=True)
            shutil.rmtree(tmp, ignore_errors=True)

    per = out["per-file (32 commits)"][0]["total"]
    bat = out["batched (1 commit)"][0]["total"]
    print(f"\n  batching the same 32 files cuts metadata {per/bat:.1f}x "
          f"({per:,} -> {bat:,} B)")
    check("Q4: per-file commits cost strictly more metadata than batching",
          per > bat, f"{per:,} vs {bat:,} B for identical data")
    return per, bat


def q5_retention():
    """Does keeping every metadata.json dominate the total?"""
    print("\n--- Q5: retained metadata.json versions vs everything else ---")
    data = sample_rows(2000)
    wd, cat = new_catalog("exp40-q5-")
    tmp = tempfile.mkdtemp(prefix="exp40-q5p-")
    try:
        t = cat.create_table("sage.obs", schema=data.schema)
        paths = write_parts(t, data, 40, 100, tmp)
        for p in paths:
            t.add_files([p])
        b = breakdown(wd)
        files = sorted(glob.glob(os.path.join(meta_dir(wd), "*.metadata.json")),
                       key=os.path.getsize)
        newest = os.path.getsize(files[-1])
        print(f"  {len(files)} retained metadata.json totalling {b['json']:,} B")
        print(f"  the CURRENT one alone is {newest:,} B "
              f"({100*newest/b['json']:.0f}% of that)")
        print(f"  a reader downloads only the current one; storage holds all "
              f"{len(files)}")

        # The quadratic shape: each new version embeds all prior snapshots,
        # so retained total grows ~n^2/2 while the current file grows ~n.
        share = b["json"] / b["total"]
        check("Q5: retained metadata.json dominates total metadata",
              share > 0.5, f"{100*share:.0f}% of all metadata bytes")
        return b, newest, len(files)
    finally:
        shutil.rmtree(wd, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)


def q6_custom_catalog():
    """The custom catalog's snapshot JSON, same 40 files, for comparison."""
    print("\n--- Q6: custom catalog metadata for the same file count ---")
    import catalog as cc
    wd = tempfile.mkdtemp(prefix="exp40-q6-")
    try:
        cat = cc.Catalog(os.path.join(wd, "cat"))
        entries = [{"path": f"date=2025-01-{1 + i % 28:02d}/data_{i}.parquet",
                    "rows": 100, "bytes": 50_000,
                    "min_time": "2025-01-01T00:00:00Z",
                    "max_time": "2025-01-01T23:59:59Z"} for i in range(40)]
        for e in entries:
            cat.add_files([e])
        total = sum(os.path.getsize(os.path.join(dp, f))
                    for dp, _, fs in os.walk(os.path.join(wd, "cat")) for f in fs)
        snaps = glob.glob(os.path.join(wd, "cat", "_catalog", "snapshots", "*.json"))
        newest = max((os.path.getsize(p) for p in snaps), default=0)
        print(f"  {len(snaps)} retained snapshots totalling {total:,} B")
        print(f"  the CURRENT snapshot alone is {newest:,} B "
              f"({newest/40:,.0f} B per live file)")
        check("Q6: custom catalog also retains every snapshot",
              len(snaps) == 40, f"{len(snaps)} snapshot files for 40 commits")
        return total, newest
    finally:
        shutil.rmtree(wd, ignore_errors=True)


def q7_expiry_vs_storage():
    """Does expire_snapshots() reclaim stored bytes, or only read bytes?"""
    print("\n--- Q7: does snapshot expiry reclaim STORAGE? ---")
    data = sample_rows(200)
    wd, cat = new_catalog("exp40-q7-")
    try:
        t = cat.create_table("sage.obs", schema=data.schema)
        for i in range(40):
            t.append(data.slice(i * 5, 5))
        t = cat.load_table("sage.obs")
        d = meta_dir(wd)
        cur = lambda: max(glob.glob(os.path.join(d, "*.metadata.json")),
                          key=os.path.getmtime)
        b_cur, b_all = os.path.getsize(cur()), dir_bytes(d, "*.metadata.json")

        ids = [s.snapshot_id for s in t.metadata.snapshots][:-1]
        t.maintenance.expire_snapshots().by_ids(ids).commit()
        t = cat.load_table("sage.obs")
        a_cur, a_all = os.path.getsize(cur()), dir_bytes(d, "*.metadata.json")

        print(f"  current metadata.json (per READ): {b_cur:,} -> {a_cur:,} B "
              f"({100*a_cur/b_cur:.0f}%)")
        print(f"  all retained json     (STORAGE):  {b_all:,} -> {a_all:,} B "
              f"({100*a_all/b_all:.0f}%)")
        print(f"  rows still readable: {len(t.scan().to_arrow()):,}")

        check("Q7a: expiry shrinks the per-READ metadata file", a_cur < b_cur * 0.6,
              f"{100*a_cur/b_cur:.0f}% of original -- this is what sec 36 Q5 measured")
        check("Q7b: expiry does NOT reclaim stored bytes", a_all >= b_all,
              f"{b_all:,} -> {a_all:,} B; it writes a NEW file and keeps the old ones")
    finally:
        shutil.rmtree(wd, ignore_errors=True)

    # The knob that does reclaim, and the one plans/6 keeps off on purpose.
    print("\n  what `delete-after-commit` would do instead:")
    for label, props in (("off (plans/6 today)", {}),
                         ("on, previous-versions-max=5",
                          {"write.metadata.delete-after-commit.enabled": "true",
                           "write.metadata.previous-versions-max": "5"})):
        wd, cat = new_catalog("exp40-q7b-")
        try:
            t = cat.create_table("sage.obs", schema=data.schema, properties=props)
            for i in range(40):
                t.append(data.slice(i * 5, 5))
            d = meta_dir(wd)
            n = len(glob.glob(os.path.join(d, "*.metadata.json")))
            print(f"    {label:<30} {n:>3} json files, "
                  f"{dir_bytes(d, '*.metadata.json'):>8,} B")
        finally:
            shutil.rmtree(wd, ignore_errors=True)
    print("    (off is deliberate: a published hint must never name a deleted")
    print("     file. previous-versions-max is a SAFETY bound on the reconciler")
    print("     period, not just a size knob.)")


def q8_orphans():
    """After a delete, does expiry clean up superseded DATA files?"""
    print("\n--- Q8: does expiry reclaim DATA files? ---")
    data = sample_rows(200)
    wd, cat = new_catalog("exp40-q8-")
    try:
        t = cat.create_table("sage.obs", schema=data.schema)
        for i in range(8):
            t.append(data.slice(i * 5, 5))
        t = cat.load_table("sage.obs")
        ddir = os.path.join(wd, "wh", "sage", "obs", "data")
        n_before = len(glob.glob(os.path.join(ddir, "*.parquet")))

        ids = [s.snapshot_id for s in t.metadata.snapshots][:-1]
        t.maintenance.expire_snapshots().by_ids(ids).commit()
        t = cat.load_table("sage.obs")
        n_after = len(glob.glob(os.path.join(ddir, "*.parquet")))
        check("Q8a: append-only -- expiry strands nothing", n_before == n_after,
              f"{n_before} data files before and after; all rows still readable")

        # Now supersede some files, which is the shape compaction has.
        # Any predicate that drops rows forces a rewrite; the point is only
        # that the ORIGINAL files stay on disk afterwards.
        t.delete(f"{PRUNE_COL} is not null")
        t = cat.load_table("sage.obs")
        live = {task.file.file_path for task in t.scan().plan_files()}
        ondisk = {"file://" + p for p in glob.glob(os.path.join(ddir, "*.parquet"))}
        orphan = len(ondisk) - len(live)
        print(f"  after a delete: live={len(live)}, on disk={len(ondisk)}, "
              f"orphaned={orphan}")
        check("Q8b: superseded data files are NOT reclaimed", orphan > 0,
              f"{orphan} orphaned files; pyiceberg 0.12 has no orphan cleanup")
    finally:
        shutil.rmtree(wd, ignore_errors=True)


# --------------------------------------------------------------------------
def project(marg, cur_json_per_snap, custom_per_file):
    """Extrapolate to the real archive. Every number here is EXTRAPOLATED."""
    print("\n--- Projection to the real archive (EXTRAPOLATED) ---")
    print(f"  4,564 files, 209 GB, {marg:,.0f} B of manifest per file\n")

    man = ARCHIVE_FILES * marg
    print(f"  manifests for 4,564 files:        {man/1e6:>8.1f} MB  "
          f"({100*man/ARCHIVE_BYTES:.4f}% of data)")

    print("\n  steady state, by commit cadence and retention policy:")
    print(f"  {'cadence':<18} {'commits/yr':>11} {'current json':>13} "
          f"{'retained json':>14}")
    for label, per_day in (("1/day", 1), ("24/day", 24), ("96/day", 96)):
        n = per_day * 365
        cur = n * cur_json_per_snap
        # Every version embeds all prior history, so retaining them all
        # sums to ~n^2/2 * per-snapshot.
        ret = cur_json_per_snap * n * n / 2
        print(f"  {label:<18} {n:>11,} {cur/1e6:>12.1f} MB "
              f"{ret/1e9:>13.1f} GB")

    print(f"\n  custom catalog, 4,564 files: "
          f"{ARCHIVE_FILES*custom_per_file/1e6:.1f} MB per snapshot, "
          f"every reader downloads all of it")


def main():
    if not os.path.isdir(ARCHIVE):
        print(f"archive not found at {ARCHIVE}")
        return 1
    print(f"exp40: Iceberg's storage overhead over plain parquet")
    print(f"archive (read-only): {ARCHIVE}")

    marg, rows, _ = q1_q2_q3_scaling()
    q4_commit_granularity()
    b5, newest, n_json = q5_retention()
    _, custom_newest = q6_custom_catalog()
    q7_expiry_vs_storage()
    q8_orphans()

    # per-snapshot growth of the current metadata.json, and per-file cost
    # of the custom catalog's snapshot, both measured just above.
    per_snap = newest / n_json
    project(marg, per_snap, custom_newest / 40)

    print("\n" + "=" * 60)
    ok = sum(1 for _, r in RESULTS if r)
    print(f"{ok}/{len(RESULTS)} checks passed")
    for name, r in RESULTS:
        if not r:
            print(f"  FAILED: {name}")
    return 0 if ok == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
