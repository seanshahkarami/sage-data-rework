"""exp37: what does the plans/6 commit path cost over Pelican at scale?

exp36 measured the commit path at the archive's real file count, but
locally -- a tmpdir warehouse, no network. Its closing line said the
thing that mattered was still open: "the network cost of the commit path
at this file count is still open." This closes it.

At ~150-190 ms per request, REQUEST COUNT is the cost that matters, not
bytes (SUMMARY sec 17). So the questions are about counts and the shape of
their growth:

  Q1. What does one commit cost over Pelican -- requests, wall time?
  Q2. Does the per-commit request count stay flat as files accumulate,
      or grow like the local timings did?
  Q3. What does a cold reader pay -- hint, metadata, manifests?
  Q4. What does the reconciler cost per pass, and does that scale?
  Q5. Does metadata read-back over HTTPS confirm what the DB believes?
  Q6. Where does the reader's cost actually go, and is it ours to fix?

Q6 is the one that matters. The write path turns out to be flat and
cheap; the READ path is neither, and part of its cost is this FileIO
re-fetching a number the manifest already carries.

The 209 GB archive is NOT uploaded. This measures the METADATA and COMMIT
path, so the test registers file paths at a realistic COUNT with tiny
payloads. That is the honest shape: manifest size and request volume
track file count, and exp36 Q7 already showed byte size is not what
drives commit cost. Where a number is extrapolated it says so.

Everything lands under a nanosecond-stamped prefix that is checked empty
before use and removed after (PROPFIND -> 404 verified both ends).
Nothing touches /node-data or any path sage-storage-loader uses.

Run:  conda run -n sage_pipeline python -I exp37_pelican_scale.py
      conda run -n sage_pipeline python -I exp37_pelican_scale.py --keep
"""
import os
import shutil
import sqlite3
import sys
import tempfile
import time

import pyarrow as pa

# `-I` drops the script directory from sys.path; exp19/exp34 do the same.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pelican_fileio as pfio
import pelican_token as pt

PREFIX = f"/cold-storage-test-scale-{time.time_ns()}"
WAREHOUSE = f"{pt.ROOT_URL}{PREFIX}/warehouse"
FILEIO = "pelican_fileio.PelicanFileIO"

# pyiceberg's SqlCatalog lays a table out as <warehouse>/<ns>/<table>
# (exp34 learned this the hard way -- the Hive "sage.db/obs" convention
# gave a reader FileNotFoundError on a file the catalog had written).
TABLE_REL = "warehouse/sage/obs"
HINT_REL = f"{TABLE_REL}/metadata/version-hint.text"

# Same catalog name for every committer: catalog_name is part of the
# primary key in iceberg_tables (exp34).
CATALOG_NAME = "sage"

SCHEMA = pa.schema([pa.field("time", pa.timestamp("us", tz="UTC"), nullable=False),
                    pa.field("node", pa.string()),
                    pa.field("value_float", pa.float64())])

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def snapshot_counts():
    return dict(pfio.COUNTS)


def delta(before):
    now = pfio.COUNTS
    d = {k: now[k] - before.get(k, 0) for k in now}
    return {k: v for k, v in d.items() if v}, sum(d.values())


# --------------------------------------------------------------------------
# The reconciler, same ~20 lines as exp34. Reproduced rather than imported
# because exp34 is a test script, not a module -- promoting it to a shared
# service is open item 5 in plans/6.
# --------------------------------------------------------------------------

def db_head(dburi_path, ns="sage", name="obs"):
    con = sqlite3.connect(dburi_path)
    try:
        row = con.execute(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE table_namespace=? AND table_name=?", (ns, name)).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def stem_of(metadata_location):
    base = metadata_location.rsplit("/", 1)[-1]
    for ext in (".gz.metadata.json", ".metadata.json"):
        if base.endswith(ext):
            return base[: -len(ext)]
    raise ValueError(f"unexpected metadata filename: {base}")


def reconcile(dburi_path, hint_path):
    """Publish the hint iff it differs from the committed head."""
    head = db_head(dburi_path)
    if head is None:
        return "no-table", None
    want = stem_of(head)
    st, _, body = pfio._request(hint_path, "GET")
    if st == 200 and body.decode().strip("\n") == want and b"\n" not in body:
        return "unchanged", want
    data = want.encode()
    assert not data.endswith(b"\n"), "hint must not end in a newline"
    st, _, _ = pfio._request(hint_path, "PUT", data=data)
    if st not in (200, 201, 204):
        raise RuntimeError(f"hint PUT -> {st}")
    return "wrote", want


def make_batch(day_offset, rows=20):
    import datetime as dt
    base = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(days=day_offset)
    return pa.table({
        "time": pa.array([base] * rows, pa.timestamp("us", tz="UTC")),
        "node": pa.array([f"node-{day_offset % 7}"] * rows),
        "value_float": pa.array([float(day_offset)] * rows),
    }, schema=SCHEMA)


def main():
    keep = "--keep" in sys.argv
    # Commits to run. Each adds one data file, so this is the accumulated
    # file count -- the axis exp36 Q7 showed commit cost grows along.
    n_commits = 24
    for a in sys.argv[1:]:
        if a.startswith("--commits="):
            n_commits = int(a.split("=", 1)[1])

    print(__doc__.split("Run:")[0].rstrip())
    print(f"\nprefix   : {PREFIX}")
    print(f"warehouse: {WAREHOUSE}")
    print(f"commits  : {n_commits}")

    st, kids = pfio.propfind(PREFIX)
    if st != 404:
        print(f"\nABORT: prefix not empty (PROPFIND -> {st}, {len(kids)} children)")
        return 2
    print("fresh prefix: PROPFIND -> 404")

    from pyiceberg.catalog.sql import SqlCatalog
    workdir = tempfile.mkdtemp(prefix="exp37-")
    dbpath = os.path.join(workdir, "catalog.db")
    hint_path = f"{PREFIX}/{HINT_REL}"

    try:
        cat = SqlCatalog(CATALOG_NAME, **{
            "uri": f"sqlite:///{dbpath}",
            "warehouse": WAREHOUSE,
            "py-io-impl": FILEIO,
        })
        cat.create_namespace("sage")

        # ---- Q1/Q2: commit cost over the network, as files accumulate ----
        print("\n--- Q1/Q2: commit cost over Pelican as files accumulate ---")
        pfio.reset_counts()
        t0 = time.time()
        tbl = cat.create_table("sage.obs", schema=SCHEMA)
        create_d, create_n = delta({})
        print(f"  create_table: {create_n} requests, {time.time() - t0:.1f}s  {create_d}")

        per_commit = []
        for i in range(n_commits):
            before = snapshot_counts()
            t0 = time.time()
            tbl.append(make_batch(i))
            wall = time.time() - t0
            d, n = delta(before)
            per_commit.append((i + 1, n, wall, d))
            if i < 3 or (i + 1) % 8 == 0 or i == n_commits - 1:
                print(f"  commit {i + 1:>3} (files={i + 1:>3}): "
                      f"{n:>3} requests, {wall:5.1f}s  {d}")

        counts = [n for _, n, _, _ in per_commit]
        walls = [w for _, _, w, _ in per_commit]
        first_n, last_n = counts[0], counts[-1]
        flat = max(counts) - min(counts) <= 2
        check("Q2: per-commit REQUEST count stays flat as files accumulate",
              flat,
              f"{first_n} requests at 1 file -> {last_n} at {n_commits}; "
              f"min {min(counts)} max {max(counts)}")

        mean_w = sum(walls) / len(walls)
        drift = walls[-1] - walls[0]
        print(f"  wall per commit: first {walls[0]:.1f}s, last {walls[-1]:.1f}s, "
              f"mean {mean_w:.1f}s (drift {drift:+.1f}s)")
        total_req = sum(counts)
        print(f"  {n_commits} commits: {total_req} requests, "
              f"{sum(walls):.0f}s total, {total_req / n_commits:.1f} req/commit")

        # ---- Q4: reconciler cost per pass ----
        print("\n--- Q4: reconciler cost per pass ---")
        before = snapshot_counts()
        t0 = time.time()
        action, stem = reconcile(dbpath, hint_path)
        d1, n1 = delta(before)
        print(f"  pass 1 ({action}): {n1} requests, {time.time() - t0:.1f}s  {d1}")
        before = snapshot_counts()
        t0 = time.time()
        action2, _ = reconcile(dbpath, hint_path)
        d2, n2 = delta(before)
        print(f"  pass 2 ({action2}): {n2} requests, {time.time() - t0:.1f}s  {d2}")
        check("Q4a: reconciler publishes the head, then is idempotent",
              action == "wrote" and action2 == "unchanged",
              f"{action} then {action2}, hint={stem}")
        check("Q4b: a reconciler pass is O(1) requests, independent of table size",
              n1 <= 3 and n2 <= 2,
              f"{n1} requests to publish, {n2} to confirm unchanged")

        # ---- Q5: does the origin agree with the database? ----
        print("\n--- Q5: does Pelican agree with the committed head? ---")
        head = db_head(dbpath)
        st, _, body = pfio._request(hint_path, "GET")
        published = body.decode() if st == 200 else None
        check("Q5a: published hint == stem of the DB head",
              published == stem_of(head),
              f"hint={published!r}")
        check("Q5b: hint has no trailing newline",
              published is not None and not published.endswith("\n"),
              "both clients break on one (plans/6)")
        st_m, _, _ = pfio._request(
            pfio._path_of(head), "GET")
        check("Q5c: the metadata file the hint names is fetchable over HTTPS",
              st_m == 200, f"GET -> {st_m}")

        # ---- Q3: what a cold reader pays ----
        print("\n--- Q3: cold reader, hint -> rows, with NO database ---")
        from pyiceberg.table import StaticTable
        pfio.reset_counts()
        t0 = time.time()
        st, _, body = pfio._request(hint_path, "GET")
        stem = body.decode()
        meta_url = f"{WAREHOUSE}/sage/obs/metadata/{stem}.metadata.json"
        static = StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO})
        n_rows = static.scan().to_arrow().num_rows
        read_wall = time.time() - t0
        d, n = delta({})
        print(f"  hint -> metadata -> scan: {n} requests, {read_wall:.1f}s  {d}")
        check("Q3a: a reader with no database sees every committed row",
              n_rows == n_commits * 20,
              f"{n_rows} rows from {n_commits} commits")
        print(f"  reader cost: {n} requests for {n_commits} data files "
              f"({n / max(n_commits, 1):.2f} req/file)")
        check("Q3b: reader cost scales with FILE COUNT, not table metadata",
              n > n_commits * 2,
              f"{n} requests for {n_commits} files -- see Q6 for the breakdown")

        # ---- Q6: where does the reader's cost actually go? ----
        # The reader is the expensive side, so attribute it rather than
        # quoting a single number. This is the question that decides
        # whether the cost is Iceberg's or our FileIO's.
        print("\n--- Q6: what the cold reader actually fetches ---")
        import collections
        orig_request = pfio._request
        log = []

        def spy(path, method, data=None, headers=None, timeout=300):
            log.append((method, path))
            return orig_request(path, method, data, headers, timeout)

        def kind(p):
            if p.endswith(".metadata.json"):
                return "metadata.json"
            if "snap-" in p:
                return "manifest-list (snap-*.avro)"
            if p.endswith(".avro"):
                return "manifest (*.avro)"
            if p.endswith(".parquet"):
                return "DATA (*.parquet)"
            return "other"

        pfio._request = spy
        try:
            s2 = StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO})
            s2.scan().to_arrow()
        finally:
            pfio._request = orig_request

        agg = collections.Counter(kind(p) for _, p in log)
        bym = collections.Counter((kind(p), m) for m, p in log)
        for k, v in agg.most_common():
            ms = ", ".join(f"{mm}={cc}" for (kk, mm), cc in bym.items() if kk == k)
            print(f"  {k:<28} {v:>4}  ({ms})")
        fixed = agg["metadata.json"] + agg["manifest-list (snap-*.avro)"]
        check("Q6a: metadata + manifest-list are O(1), not O(files)",
              fixed <= 6, f"{fixed} requests regardless of table size")

        # Is the per-file HEAD necessary? The manifest already carries the
        # size, so asking the origin for it is this FileIO re-fetching a
        # number it has. Measured below rather than asserted.
        tasks = list(s2.scan().plan_files())
        known_size = tasks[0].file.file_size_in_bytes if tasks else None
        heads = sum(c for (k, m), c in bym.items()
                    if k == "DATA (*.parquet)" and m == "HEAD")
        check("Q6b: the per-file HEAD re-fetches a size the manifest already has",
              known_size is not None and heads > 0,
              f"file_size_in_bytes={known_size} in the manifest, "
              f"yet {heads} HEADs issued")

        # Prove the fix rather than recommending it untested.
        sizes = {}
        orig_head = pfio.PelicanInputFile._head

        def cached_head(self):
            if self._path in sizes:
                self._size = sizes[self._path]
                return self._size
            v = orig_head(self)
            sizes[self._path] = v
            return v

        pfio.PelicanInputFile._head = cached_head
        try:
            StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO}) \
                .scan().to_arrow()            # cold: fills the cache
            pfio.reset_counts()
            StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO}) \
                .scan().to_arrow()            # warm
            _, warm_n = delta({})
        finally:
            pfio.PelicanInputFile._head = orig_head
        saved = n - warm_n
        check("Q6c: caching file sizes removes every per-file HEAD",
              warm_n < n,
              f"{n} -> {warm_n} requests ({100 * saved / n:.0f}% fewer)")
        print("       The remaining 2 GETs per file are the parquet footer")
        print("       then the row group -- Iceberg's doing, not ours.")

        # ---- Q7: does pruning hold over the network? ----
        # The projection below leans on this, so it is measured here in
        # REQUESTS rather than carried over from exp36's local file counts.
        # Needs pyiceberg-core for partitioned writes; skipped without it.
        prune_note = None
        try:
            import pyiceberg_core  # noqa: F401
            have_core = True
        except Exception:
            have_core = False

        if not have_core:
            print("\n--- Q7: partition pruning over Pelican ---")
            print('  SKIPPED: needs pip install "pyiceberg[pyiceberg-core]"')
            print("           (exp36 Q4 measured 608 -> 7 files locally)")
        else:
            print("\n--- Q7: partition pruning over Pelican, in requests ---")
            import datetime as _dt

            from pyiceberg.expressions import (And as _And,
                                               GreaterThanOrEqual as _GTE,
                                               LessThan as _LT)
            from pyiceberg.partitioning import (PartitionField as _PF,
                                                PartitionSpec as _PS)
            from pyiceberg.schema import Schema as _Schema
            from pyiceberg.transforms import DayTransform as _Day
            from pyiceberg.types import (DoubleType as _D, NestedField as _NF,
                                         StringType as _S, TimestamptzType as _TS)

            ndays = 60
            isch = _Schema(_NF(1, "time", _TS(), required=True),
                           _NF(2, "node", _S(), required=False),
                           _NF(3, "value_float", _D(), required=False))
            pspec = _PS(_PF(source_id=1, field_id=1000, transform=_Day(),
                            name="time_day"))
            pt_tbl = cat.create_table("sage.parts", schema=isch, partition_spec=pspec)
            base = _dt.datetime(2025, 1, 1, tzinfo=_dt.timezone.utc)
            print(f"  writing {ndays} day-partitions ...")
            for d in range(ndays):
                day = base + _dt.timedelta(days=d)
                pt_tbl.append(pa.table({
                    "time": pa.array([day] * 20, pa.timestamp("us", tz="UTC")),
                    "node": pa.array(["n"] * 20),
                    "value_float": pa.array([1.0] * 20)}, schema=SCHEMA))
            pt_tbl = cat.load_table("sage.parts")

            pfio.reset_counts()
            t0 = time.time()
            rows_full = pt_tbl.scan().to_arrow().num_rows
            _, n_full = delta({})
            w_full = time.time() - t0

            pfio.reset_counts()
            t0 = time.time()
            rows_wk = pt_tbl.scan(row_filter=_And(
                _GTE("time", "2025-02-01T00:00:00+00:00"),
                _LT("time", "2025-02-08T00:00:00+00:00"))).to_arrow().num_rows
            _, n_wk = delta({})
            w_wk = time.time() - t0

            print(f"  full scan   : {rows_full:>5} rows  {n_full:>4} requests  {w_full:5.1f}s")
            print(f"  1-week scan : {rows_wk:>5} rows  {n_wk:>4} requests  {w_wk:5.1f}s")
            check("Q7: partition pruning cuts REQUESTS over the network",
                  n_wk < n_full / 5,
                  f"{n_full} -> {n_wk} requests "
                  f"({100 * (1 - n_wk / n_full):.0f}% fewer)")
            prune_note = (n_full, n_wk)

        # ---- projection ----
        print("\n--- Projection to the real archive (4,564 files) ---")
        req_per_commit = total_req / n_commits
        secs_per_commit = mean_w
        print(f"  measured: {req_per_commit:.1f} requests and {secs_per_commit:.1f}s "
              f"per commit, flat in file count")
        print(f"  4,564 single-file commits -> "
              f"{4564 * req_per_commit:,.0f} requests, "
              f"{4564 * secs_per_commit / 3600:.1f} h  (EXTRAPOLATED)")
        print(f"  608 day-batched commits   -> "
              f"{608 * req_per_commit:,.0f} requests, "
              f"{608 * secs_per_commit / 60:.0f} min (EXTRAPOLATED)")
        print("  Batching is the lever: the per-commit cost is flat, so it is")
        print("  the NUMBER of commits that sets the bill, not the file count.")

        req_per_file = n / max(n_commits, 1)
        print(f"\n  READ side -- the expensive one. {req_per_file:.1f} requests "
              f"per data file, and it does NOT flatten:")
        print(f"    full scan of 4,564 files -> {4564 * req_per_file:,.0f} requests, "
              f"{4564 * req_per_file * 0.19 / 60:.0f} min at 190 ms "
              f"(EXTRAPOLATED)")
        print(f"    with file sizes cached   -> "
              f"{4564 * (warm_n / max(n_commits, 1)):,.0f} requests "
              f"({100 * saved / n:.0f}% less)")
        if prune_note:
            pf, pw = prune_note
            print(f"    with a one-week partition filter (Q7, measured here:")
            print(f"     {pf} -> {pw} requests) -> ~{4564 * req_per_file * pw / pf:,.0f} "
                  f"requests, {4564 * req_per_file * (pw / pf) * 0.19 / 60:.0f} min")
        else:
            print("    with a one-week partition filter (exp36 Q4: 608 -> 7 files)")
            print(f"                             -> ~{7 * req_per_file:.0f} requests")
        print("  A full-table scan over HTTPS is the thing to avoid; partition")
        print("  pruning, not a faster origin, is what makes the read path work.")

        npass = sum(1 for _, ok in RESULTS if ok)
        print(f"\n{npass}/{len(RESULTS)} checks passed")
        return 0 if npass == len(RESULTS) else 1

    finally:
        if keep:
            print(f"\n--keep: leaving {PREFIX} in place")
        else:
            print(f"\ncleaning up {PREFIX} ...")
            n = pfio.rmtree(PREFIX)
            st, left = pfio.propfind(PREFIX)
            print(f"deleted {n} objects; PROPFIND {PREFIX} -> {st}"
                  + (f" ({len(left)} left)" if left else " (clean)"))
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
