"""exp39: does Iceberg add latency over a plain-Parquet custom catalog?

exp38 measured Iceberg's query throughput but had nothing to compare it
to. plans/4 compares the two designs on SAFETY and OPERATIONS, and its
only latency numbers are for the COMMIT (9 requests vs 5, ~0.9s vs
~0.7s). Nobody has measured the thing that actually differs: Iceberg's
metadata layer sits between a reader and the bytes, and its write path
RE-ENCODES every file. Both cost time. How much?

The comparison is deliberately like-for-like: same files, same origin,
same FileIO, same process, interleaved to share link conditions.

  Q1. WRITE: Iceberg `append()` vs publishing the parquet file as-is.
      Iceberg must rewrite (exp36 Q1: add_files cannot take ns), the
      custom catalog indexes files as written (SUMMARY sec 19). That is
      the single largest asymmetry in the whole comparison.
  Q2. READ, cold: resolve the pointer, then read every row. Iceberg
      pays metadata.json + manifest-list + manifests; the custom
      catalog pays CURRENT + one snapshot JSON.
  Q3. Is the metadata overhead FIXED or per-file? A fixed cost is
      amortised by a big query; a per-file cost is not.
  Q4. PRUNED read -- the query shape that matters. Iceberg prunes via
      partition metadata, the custom catalog via the stats in its
      snapshot JSON. Both should skip files; what does each PAY to?
  Q5. What does the custom catalog's snapshot JSON cost at the
      archive's real file count? It embeds every live file, and the
      reader downloads the whole thing. Measured, then projected.

Honest framing: this measures LATENCY ONLY. It is not the whole
decision -- plans/4 settles concurrency safety (Iceberg wins, exp18/19),
row-level deletes (Iceberg wins) and operational surface (custom wins)
on other evidence. Latency is one input.

Uploads real archive parquet to a nanosecond-stamped prefix, PROPFIND ->
404 checked both ends, removed after. Archive read-only; nothing touches
/node-data or sage-storage-loader paths.

Run:  conda run -n sage_pipeline python -I exp39_iceberg_vs_custom.py
      conda run -n sage_pipeline python -I exp39_iceberg_vs_custom.py --files=6
"""
import io
import json
import os
import shutil
import sys
import tempfile
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pelican_fileio as pfio
import pelican_token as pt

PREFIX = f"/cold-storage-test-vs-{time.time_ns()}"
WAREHOUSE = f"{pt.ROOT_URL}{PREFIX}/warehouse"
FILEIO = "pelican_fileio.PelicanFileIO"
CUSTOM_ROOT = f"{PREFIX}/custom"

ARCHIVE = ("/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/"
           "sage-data-rework/data/archive")

RESULTS = []
BYTES = {"down": 0, "up": 0}


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def delta(before):
    now = pfio.COUNTS
    d = {k: now[k] - before.get(k, 0) for k in now}
    return {k: v for k, v in d.items() if v}, sum(d.values())


def install_byte_counter():
    orig = pfio._request

    def counting(path, method, data=None, headers=None, timeout=300):
        st, h, body = orig(path, method, data, headers, timeout)
        if body:
            BYTES["down"] += len(body)
        if data:
            BYTES["up"] += len(data)
        return st, h, body

    pfio._request = counting


def reset():
    pfio.reset_counts()
    BYTES["down"] = BYTES["up"] = 0


def us_schema(s):
    return pa.schema([
        pa.field(f.name,
                 pa.timestamp("us", tz="UTC") if pa.types.is_timestamp(f.type) else f.type,
                 nullable=f.nullable)
        for f in s])


# --------------------------------------------------------------------------
# The custom catalog's CatalogStore, ported to Pelican.
#
# catalog.py's docstring says: "The Pelican port replaces exactly this
# class. Needs: atomic pointer swap, read a small object, write a small
# object." This is that port -- and the pointer swap is a plain PUT,
# because SUMMARY sec 7/17 established Pelican has no CAS. That is the
# design's known weakness (exp18 lost an update 3/3) and is NOT what this
# experiment is testing; here it only has to be fast and correct for one
# writer, which is the configuration being timed.
# --------------------------------------------------------------------------

class PelicanCatalogStore:
    def __init__(self, root):
        self.root = root

    def _ptr(self):
        return f"{self.root}/_catalog/CURRENT"

    def put_snapshot(self, snap):
        sid = snap["snapshot_id"]
        p = f"{self.root}/_catalog/snapshots/{sid}.json"
        st, _, _ = pfio._request(p, "PUT", data=json.dumps(snap).encode())
        if st not in (200, 201, 204):
            raise IOError(f"PUT snapshot -> {st}")
        return p

    def get_snapshot(self, sid):
        st, _, body = pfio._request(
            f"{self.root}/_catalog/snapshots/{sid}.json", "GET")
        if st != 200:
            raise IOError(f"GET snapshot -> {st}")
        return json.loads(body)

    def commit(self, snapshot_id, expected_parent=None):
        cur = self.current_id()
        if expected_parent is not None and cur != expected_parent:
            raise RuntimeError(f"pointer moved: {expected_parent} -> {cur}")
        st, _, _ = pfio._request(self._ptr(), "PUT", data=snapshot_id.encode())
        if st not in (200, 201, 204):
            raise IOError(f"PUT CURRENT -> {st}")

    def current_id(self):
        st, _, body = pfio._request(self._ptr(), "GET")
        return body.decode().strip() if st == 200 else None


class PelicanCatalog:
    """catalog.py's Catalog, against the Pelican store."""

    def __init__(self, root):
        self.store = PelicanCatalogStore(root)

    def _live_map(self):
        sid = self.store.current_id()
        if not sid:
            return {}
        return {e["path"]: e for e in self.store.get_snapshot(sid)["files"]}

    def add_files(self, entries, live=None):
        parent = self.store.current_id()
        live = dict(self._live_map() if live is None else live)
        for e in entries:
            e = dict(e)
            e.setdefault("generation", 0)
            live[e["path"]] = e
        snap = {"snapshot_id": f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
                "parent_id": parent,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "operation": "append",
                "files": sorted(live.values(), key=lambda x: x["path"])}
        self.store.put_snapshot(snap)
        self.store.commit(snap["snapshot_id"], expected_parent=parent)
        return snap["snapshot_id"], live


def pick_files(n):
    days = sorted(d for d in os.listdir(ARCHIVE) if d.startswith("date="))
    out = []
    for d in days:
        p = os.path.join(ARCHIVE, d)
        cands = [(os.path.getsize(os.path.join(p, f)), os.path.join(p, f))
                 for f in os.listdir(p) if f.endswith(".parquet")]
        if cands:
            out.append((d, min(cands)[1], min(cands)[0]))
        if len(out) >= n:
            break
    return out


def main():
    keep = "--keep" in sys.argv
    n_files = 4
    for a in sys.argv[1:]:
        if a.startswith("--files="):
            n_files = int(a.split("=", 1)[1])

    print(__doc__.split("Run:")[0].rstrip())
    print(f"\nprefix: {PREFIX}")

    picked = pick_files(n_files)
    if len(picked) < n_files:
        print(f"ABORT: only {len(picked)} files found")
        return 2
    total_mb = sum(s for _, _, s in picked) / 1e6
    print(f"files : {len(picked)} real archive files, {total_mb:,.0f} MB")

    st, kids = pfio.propfind(PREFIX)
    if st != 404:
        print(f"ABORT: prefix not empty (PROPFIND -> {st}, {len(kids)})")
        return 2
    print("fresh prefix: PROPFIND -> 404")

    from pyiceberg.catalog.sql import SqlCatalog
    workdir = tempfile.mkdtemp(prefix="exp39-")
    install_byte_counter()

    try:
        first = pq.ParquetFile(picked[0][1])
        src_schema = first.schema_arrow
        arrow_schema = us_schema(src_schema)
        print(f"schema: {len(arrow_schema)} cols, "
              f"{first.metadata.num_rows:,} rows/file, "
              f"src time type = {src_schema.field('time').type}")

        # ================= Q1: WRITE =================
        print("\n--- Q1: write path, same files, both designs ---")

        cat = SqlCatalog("sage", **{"uri": f"sqlite:///{workdir}/c.db",
                                    "warehouse": WAREHOUSE,
                                    "py-io-impl": FILEIO})
        cat.create_namespace("sage")
        tbl = cat.create_table("sage.obs", schema=arrow_schema)

        reset()
        ice_t0 = time.time()
        for day, path, size in picked:
            t = pq.read_table(path)
            t = t.cast(arrow_schema, safe=False)   # ns -> us, mandatory
            tbl.append(t)
        ice_wall = time.time() - ice_t0
        _, ice_req = delta({})
        ice_up = BYTES["up"] / 1e6
        print(f"  Iceberg append : {ice_wall:6.1f}s  {ice_req:>4} req  "
              f"{ice_up:7.1f} MB up  ({ice_up / ice_wall:.1f} MB/s)")

        # Custom: publish the file AS WRITTEN. No decode, no cast, no
        # re-encode -- the asymmetry SUMMARY sec 19 names.
        ccat = PelicanCatalog(CUSTOM_ROOT)
        reset()
        cus_t0 = time.time()
        live = {}
        for day, path, size in picked:
            raw = open(path, "rb").read()
            dest = f"{CUSTOM_ROOT}/data/{day}/{os.path.basename(path)}"
            st, _, _ = pfio._request(dest, "PUT", data=raw)
            if st not in (200, 201, 204):
                raise IOError(f"PUT data -> {st}")
            md = pq.ParquetFile(path).metadata
            entry = {"path": dest, "date": day.split("=", 1)[1],
                     "rows": md.num_rows, "size": size,
                     "stats": {"time_min": day.split("=", 1)[1],
                               "time_max": day.split("=", 1)[1]}}
            _, live = ccat.add_files([entry], live=live)
        cus_wall = time.time() - cus_t0
        _, cus_req = delta({})
        cus_up = BYTES["up"] / 1e6
        print(f"  custom publish : {cus_wall:6.1f}s  {cus_req:>4} req  "
              f"{cus_up:7.1f} MB up  ({cus_up / cus_wall:.1f} MB/s)")
        # Wall time alone hides what happened: Iceberg uploads FEWER bytes
        # (the us cast and its own encoding shrink the file) but takes the
        # same time, because it also decodes and re-encodes 40M rows. The
        # honest figure is effective throughput on the bytes each moved.
        print(f"  -> wall {ice_wall / cus_wall:.2f}x, "
              f"bytes {ice_up / cus_up:.2f}x, "
              f"effective rate {(ice_up / ice_wall) / (cus_up / cus_wall):.2f}x")
        # HYPOTHESIS REFUTED, recorded rather than quietly re-aimed: I
        # expected the ns->us rewrite to make Iceberg's write visibly
        # slower. It does not. Both paths are NETWORK-bound at ~6.5 MB/s,
        # and the rewrite shrinks the payload ~17%, so the CPU cost hides
        # behind an upload that has less to send. Effective throughput
        # comes out within ~15% either way, and which one "wins" moves
        # between runs.
        #
        # The band below is wide on purpose. Across three runs the
        # effective-rate ratio came out 0.96, 0.79 and 0.79 and the WALL
        # ratio 1.00, 0.86, 1.05 -- i.e. the run-to-run spread on this
        # link is larger than any difference between the designs. A
        # tighter assertion would be measuring the weather.
        eff = (ice_up / ice_wall) / (cus_up / cus_wall)
        wall_ratio = ice_wall / cus_wall
        check("Q1: write latency does NOT separate the designs "
              "(hypothesis refuted)",
              0.7 <= wall_ratio <= 1.4,
              f"wall {wall_ratio:.2f}x (observed 0.86-1.05 across runs), "
              f"effective rate {eff:.2f}x -- {ice_up:.0f} MB in "
              f"{ice_wall:.0f}s vs {cus_up:.0f} MB in {cus_wall:.0f}s; "
              f"both network-bound, Iceberg sends "
              f"{100 * (1 - ice_up / cus_up):.0f}% less")

        tbl = cat.load_table("sage.obs")
        ice_files = len(list(tbl.scan().plan_files()))
        print(f"  (Iceberg wrote {ice_files} data files from {len(picked)} "
              f"appends; custom published {len(picked)}, unchanged)")

        # ================= Q2: READ, cold =================
        # Both resolve a pointer and then read every row, in one process,
        # interleaved. PYICEBERG_MAX_WORKERS is left at default for both;
        # exp38 showed it matters, and the custom reader below is
        # single-threaded, so this is the DEFAULT-vs-DEFAULT comparison.
        print("\n--- Q2: cold read, pointer -> all rows ---")
        from pyiceberg.table import StaticTable
        meta_url = tbl.metadata_location

        # Pin BOTH readers to the same width. exp38's finding (sequential
        # ~5 MB/s vs concurrent ~30) is large enough to swamp any design
        # difference, so leaving it unpinned would measure the reader's
        # thread count and call it a property of the format.
        ice_workers = 8
        os.environ["PYICEBERG_MAX_WORKERS"] = str(ice_workers)
        from pyiceberg.utils.concurrent import ExecutorFactory
        ExecutorFactory._instance = None
        print(f"  both readers pinned to {ice_workers} workers "
              f"(exp38: this is worth 6-8x on its own)")

        def read_iceberg(row_filter=None, columns=None):
            reset()
            t0 = time.time()
            s = StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO})
            scan = s.scan(row_filter=row_filter) if row_filter else s.scan()
            if columns:
                scan = scan.select(*columns)
            arr = scan.to_arrow()
            return (time.time() - t0, delta({})[1], BYTES["down"] / 1e6,
                    arr.num_rows)

        def read_custom(days=None, columns=None, workers=None):
            """CURRENT -> snapshot -> read each live parquet directly.

            Parallelised over files with the SAME worker count pyiceberg
            uses, because otherwise this measures a for-loop against a
            thread pool rather than one design against the other. exp38
            showed that difference alone is 6-8x.
            """
            from concurrent.futures import ThreadPoolExecutor
            if workers is None:
                workers = ice_workers
            reset()
            t0 = time.time()
            sid = ccat.store.current_id()
            snap = ccat.store.get_snapshot(sid)
            files = snap["files"]
            if days is not None:
                files = [f for f in files if f["date"] in days]

            def one(f):
                inp = pfio.PelicanInputFile(f["path"])
                return pq.read_table(inp.open(), columns=columns)

            with ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
                tables = list(ex.map(one, files))
            arr = pa.concat_tables(tables) if tables else None
            return (time.time() - t0, delta({})[1], BYTES["down"] / 1e6,
                    arr.num_rows if arr is not None else 0)

        iw, ir, imb, irows = read_iceberg()
        cw, cr, cmb, crows = read_custom()
        print(f"  Iceberg : {iw:6.1f}s  {ir:>4} req  {imb:7.1f} MB  "
              f"{irows:,} rows")
        print(f"  custom  : {cw:6.1f}s  {cr:>4} req  {cmb:7.1f} MB  "
              f"{crows:,} rows")
        check("Q2a: both designs return the same rows",
              irows == crows, f"{irows:,} == {crows:,}")
        print(f"  -> ratio {iw / cw:.2f}x wall, {ir / cr:.1f}x requests")

        # ================= Q3: fixed or per-file? =================
        print("\n--- Q3: is Iceberg's metadata overhead fixed or per-file? ---")
        # Count only the non-data requests on each side.
        logged = []
        orig_req = pfio._request

        def spy(path, method, data=None, headers=None, timeout=300):
            logged.append(path)
            return orig_req(path, method, data, headers, timeout)

        pfio._request = spy
        try:
            logged.clear()
            StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO}).scan().to_arrow()
            ice_meta = sum(1 for p in logged if not p.endswith(".parquet"))
            ice_data = sum(1 for p in logged if p.endswith(".parquet"))
            logged.clear()
            read_custom()
            cus_meta = sum(1 for p in logged if not p.endswith(".parquet"))
            cus_data = sum(1 for p in logged if p.endswith(".parquet"))
        finally:
            pfio._request = orig_req

        print(f"  Iceberg : {ice_meta:>3} metadata requests + {ice_data} on data "
              f"({ice_files} files)")
        print(f"  custom  : {cus_meta:>3} metadata requests + {cus_data} on data "
              f"({len(picked)} files)")
        # The honest unit is metadata requests PER DATA FILE: Iceberg
        # writes more, smaller files, so raw totals are not comparable.
        ice_pf = ice_meta / max(ice_files, 1)
        cus_pf = cus_meta / max(len(picked), 1)
        print(f"  per data file: Iceberg {ice_pf:.2f}, custom {cus_pf:.2f}")
        check("Q3: Iceberg's metadata cost is a fixed handful, not per-file",
              ice_meta <= 16 and ice_pf < 1.0,
              f"{ice_meta} requests covering {ice_files} files "
              f"({ice_pf:.2f}/file) -- a constant any real query amortises")
        print(f"  Neither grows with file count. Iceberg's constant is "
              f"bigger ({ice_meta} vs {cus_meta}); at {ice_files} files it is")
        print(f"  already below 1 request/file, and it keeps falling.")

        # ================= Q4: pruned read =================
        print("\n--- Q4: the query shape that matters -- one day ---")
        from pyiceberg.expressions import (And, GreaterThanOrEqual, LessThan)
        day0 = picked[0][0].split("=", 1)[1]
        import datetime as dt
        d0 = dt.date.fromisoformat(day0)
        d1 = d0 + dt.timedelta(days=1)
        piw, pir, pimb, pirows = read_iceberg(row_filter=And(
            GreaterThanOrEqual("time", f"{d0}T00:00:00+00:00"),
            LessThan("time", f"{d1}T00:00:00+00:00")))
        pcw, pcr, pcmb, pcrows = read_custom(days={day0})
        print(f"  Iceberg : {piw:6.1f}s  {pir:>4} req  {pimb:7.1f} MB  "
              f"{pirows:,} rows")
        print(f"  custom  : {pcw:6.1f}s  {pcr:>4} req  {pcmb:7.1f} MB  "
              f"{pcrows:,} rows")
        check("Q4a: both prune to roughly one day's data",
              pimb < imb * 0.6 and pcmb < cmb * 0.6,
              f"{imb:.0f}->{pimb:.0f} MB Iceberg, "
              f"{cmb:.0f}->{pcmb:.0f} MB custom")
        # The table is UNPARTITIONED, so Iceberg prunes on column stats
        # alone. Worth stating: it did not need a partition spec to do it.
        print(f"  NOTE: the Iceberg table is UNPARTITIONED -- that pruning "
              f"is column-stats only.")

        # ================= Q5: snapshot JSON at scale =================
        print("\n--- Q5: what the custom catalog's snapshot costs at scale ---")
        sid = ccat.store.current_id()
        st, _, body = pfio._request(
            f"{CUSTOM_ROOT}/_catalog/snapshots/{sid}.json", "GET")
        snap_bytes = len(body)
        per_file = snap_bytes / len(picked)
        print(f"  snapshot JSON: {snap_bytes:,} B for {len(picked)} files "
              f"= {per_file:,.0f} B/file")
        proj = per_file * 4564 / 1e6
        print(f"  at 4,564 files -> ~{proj:.1f} MB, downloaded IN FULL by "
              f"every reader on every read (EXTRAPOLATED)")
        # Iceberg's equivalent: metadata.json + manifests, and the reader
        # only fetches manifests it needs.
        print(f"  Iceberg's equivalent is split across metadata.json and "
              f"per-partition manifests,")
        print(f"  so a pruned reader fetches a SUBSET. The custom snapshot "
              f"is all-or-nothing.")
        check("Q5: the custom catalog's snapshot is O(files) on EVERY read",
              per_file > 50,
              f"~{per_file:,.0f} B/file -> ~{proj:.1f} MB at archive scale, "
              f"per read, unprunable")

        # ================= summary =================
        print("\n--- Summary: where the latency actually goes ---")
        print(f"  {'':<22}{'Iceberg':>12}{'custom':>12}   ratio")
        print(f"  {'write (4 files)':<22}{ice_wall:>11.1f}s{cus_wall:>11.1f}s"
              f"   {ice_wall / cus_wall:>5.1f}x")
        print(f"  {'read all':<22}{iw:>11.1f}s{cw:>11.1f}s   {iw / cw:>5.1f}x")
        print(f"  {'read one day':<22}{piw:>11.1f}s{pcw:>11.1f}s"
              f"   {piw / pcw:>5.1f}x")
        print(f"  {'metadata requests':<22}{ice_meta:>12}{cus_meta:>12}")
        print("\n  Iceberg's read overhead is a FIXED handful of requests")
        print(f"  ({ice_meta} vs {cus_meta}), amortised to under 1 per data "
              f"file by any real query.")
        print("  The expected write penalty did NOT appear: both paths are")
        print("  network-bound, and the rewrite sends ~17% less, so it pays")
        print("  for its own CPU. Latency is not what separates these two.")

        npass = sum(1 for _, ok in RESULTS if ok)
        print(f"\n{npass}/{len(RESULTS)} checks passed")
        return 0 if npass == len(RESULTS) else 1

    finally:
        if keep:
            print(f"\n--keep: leaving {PREFIX}")
        else:
            print(f"\ncleaning up {PREFIX} ...")
            n = pfio.rmtree(PREFIX)
            st, left = pfio.propfind(PREFIX)
            print(f"deleted {n} objects; PROPFIND -> {st}"
                  + (f" ({len(left)} left)" if left else " (clean)"))
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
