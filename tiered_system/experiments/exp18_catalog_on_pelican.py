#!/usr/bin/env python
"""
Experiment 18: re-run exp13's catalog safety checks with the pointer living
on real Pelican instead of a POSIX filesystem.

exp13 proved the design on `os.replace`. Its own docstring flagged the gap:
"What it costs on Pelican is exactly the open question." exp17 then showed
the primitive exists (MOVE onto an existing path, atomic under concurrent
readers). This closes the loop: the same checks, same assertions, with
`CatalogStore` swapped for `PelicanCatalogStore`.

Scope, stated honestly: the CATALOG lives on Pelican -- snapshots and the
CURRENT pointer are real objects on NRDStor, every commit is a real MOVE.
The DATA files stay local, because exp17 already measured data reads over
Pelican and uploading 300 MB per check would measure upload bandwidth
rather than catalog safety. The catalog indexes paths; it never opens a
data file. That is exactly the separation `catalog.py` was built around.

Two checks exist here that exp13 could not have:

  7. the commit race window. On POSIX, check-then-swap is microseconds.
     On Pelican it is a network round trip, and there is no compare-and-swap
     to fall back on. This measures how wide the window actually is and
     whether two real writers lose it.
  8. a reader pinned to a snapshot keeps working after the pointer moves --
     over the network, where the snapshot object must still be fetchable.

Everything is written under /sage/<prefix>/ (default cold-storage-test/
exp18) and deleted at the end. /node-data is never touched.

Usage: python exp18_catalog_on_pelican.py <archive-root> <workdir> [--days N]
"""
import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from catalog import Catalog, ConcurrentUpdate  # noqa: E402
from pelican_store import PelicanCatalogStore, PelicanIO  # noqa: E402

PREFIX = "/cold-storage-test/exp18"
results = []


def check(name, cond, detail=""):
    results.append({"check": name, "status": "PASS" if cond else "FAIL",
                    "detail": detail})
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return cond


class PelicanCatalog(Catalog):
    """catalog.Catalog with its store swapped. Nothing else changes."""

    def __init__(self, data_root, prefix):
        self.root = data_root          # data files: local
        self.store = PelicanCatalogStore(prefix)   # metadata: Pelican


def entry_for(root, relpath):
    """Identical to exp13's: footer facts only, no data read."""
    p = os.path.join(root, relpath)
    md = pq.ParquetFile(p).metadata
    groups = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        st = {}
        for c in range(rg.num_columns):
            col = rg.column(c)
            if col.path_in_schema in ("time", "plugin", "name") and col.is_stats_set:
                s = col.statistics
                if s.has_min_max:
                    st[col.path_in_schema] = {"min": str(s.min), "max": str(s.max)}
        groups.append({"rows": rg.num_rows, "stats": st})
    return {"path": relpath, "date": relpath.split("/")[0].replace("date=", ""),
            "rows": md.num_rows, "size": os.path.getsize(p), "groups": groups}


def cleanup(prefix):
    """Delete every object this experiment created."""
    import re
    import urllib.error
    import urllib.request
    io = PelicanIO(prefix)
    removed = 0
    for d in (f"{prefix}/_catalog/snapshots/", f"{prefix}/_catalog/", f"{prefix}/"):
        try:
            req = urllib.request.Request(
                io._url(d), method="PROPFIND")
            req.add_header("Depth", "1")
            import ssl
            with urllib.request.urlopen(req, timeout=60,
                                        context=ssl.create_default_context()) as r:
                body = r.read().decode()
        except urllib.error.HTTPError:
            continue
        for href in re.findall(r"<D:href>([^<]*)</D:href>", body):
            p = href[len("/sage"):] if href.startswith("/sage") else href
            if p.rstrip("/") == d.rstrip("/"):
                continue
            if io.delete(p) in (200, 204):
                removed += 1
    for d in (f"{prefix}/_catalog/snapshots", f"{prefix}/_catalog", prefix):
        io.delete(d)
    return removed


def main():
    src = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 2

    sandbox = os.path.join(work, "catalog_pelican")
    if os.path.exists(sandbox):
        shutil.rmtree(sandbox)
    os.makedirs(sandbox)
    days = sorted(d for d in os.listdir(src) if d.startswith("date="))[:ndays]
    for d in days:
        shutil.copytree(os.path.join(src, d), os.path.join(sandbox, d))

    # Start from a clean prefix so a previous run cannot mask a failure.
    cleanup(PREFIX)

    cat = PelicanCatalog(sandbox, PREFIX)
    rels = [f"{d}/{f}" for d in days
            for f in sorted(os.listdir(os.path.join(sandbox, d)))
            if f.endswith(".parquet")]

    entries = [entry_for(sandbox, r) for r in rels]
    t0 = time.perf_counter()
    sid0 = cat.add_files(entries, op="initial")
    publish_s = time.perf_counter() - t0
    snap_bytes = len(json.dumps(cat.store.get_snapshot(sid0)).encode())
    print(f"published {len(entries)} files as snapshot {sid0}")
    print(f"  over Pelican: {publish_s:.2f}s, snapshot {snap_bytes:,} bytes")
    print(f"  verbs so far: {cat.store.io.counts}\n")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    def run(paths, plugin):
        if not paths:
            return 0
        fl = "[" + ",".join("'" + p + "'" for p in paths) + "]"
        return con.execute(
            f"select count(*) from read_parquet({fl}) where plugin = '{plugin}'"
        ).fetchone()[0]

    allp = "[" + ",".join("'" + os.path.join(sandbox, r) + "'" for r in rels) + "]"
    rare = con.execute(
        f"select plugin, count(*) c from read_parquet({allp}) "
        "group by plugin order by c asc limit 1").fetchone()[0]

    # ---- 1. planning from statistics (now a network fetch of the snapshot) ----
    t0 = time.perf_counter()
    snap = cat.pin()
    pin_s = time.perf_counter() - t0
    t0 = time.perf_counter()
    planned = snap.plan(filters={"plugin": rare})
    plan_s = time.perf_counter() - t0
    check("planning rejects files from statistics alone",
          len(planned) < len(rels),
          f"{len(planned)}/{len(rels)} files survive; pin (network) "
          f"{pin_s*1000:.0f} ms, plan (local) {plan_s*1000:.2f} ms")

    truth = run([os.path.join(sandbox, r) for r in rels], rare)
    check("planning does not lose rows", truth == run(planned, rare),
          f"{truth} rows")

    # ---- 2 & 3. pinned reader across a compaction ----
    reader = cat.pin()
    before = run(reader.plan(filters={"plugin": rare}), rare)

    day = days[0]
    srcs = [os.path.join(sandbox, r) for r in rels if r.startswith(day)]
    merged = os.path.join(sandbox, day, "compacted_0.parquet")
    w = None
    for p in srcs:
        for b in pq.ParquetFile(p).iter_batches(batch_size=500_000):
            tb = pa.Table.from_batches([b])
            if w is None:
                w = pq.ParquetWriter(merged, tb.schema, compression="zstd",
                                     write_statistics=True, write_page_index=True)
            w.write_table(tb, row_group_size=1_000_000)
    w.close()

    superseded = [r for r in rels if r.startswith(day)]
    t0 = time.perf_counter()
    cat.replace(superseded, [entry_for(sandbox, f"{day}/compacted_0.parquet")])
    replace_s = time.perf_counter() - t0

    check("pinned reader is unaffected by a compaction published mid-query",
          before == run(reader.plan(filters={"plugin": rare}), rare),
          f"{before} rows, reader still on {reader.sid}")

    fresh = cat.pin()
    check("a new reader sees identical results after compaction",
          before == run(fresh.plan(filters={"plugin": rare}), rare),
          f"{before} rows, now on {fresh.sid}")

    live = {e["path"] for e in fresh.files}
    check("compaction swapped the files atomically",
          not (live & set(superseded)) and f"{day}/compacted_0.parquet" in live,
          f"{len(superseded)} superseded, {len(live)} live, "
          f"commit took {replace_s:.2f}s over Pelican")

    # ---- 4. crash between snapshot write and pointer move ----
    before_ptr = cat.store.current_id()
    orphan = cat._new_snapshot([], before_ptr, "append")
    cat.store.put_snapshot(orphan)      # written to Pelican...
    # ...process dies here. The pointer object is untouched.
    recovered = cat.pin()
    check("a snapshot written but never committed is invisible",
          cat.store.current_id() == before_ptr and recovered.sid == before_ptr,
          f"pointer still {before_ptr}")
    check("archive frontier unchanged after the simulated crash",
          run(recovered.plan(filters={"plugin": rare}), rare) == before)

    # ---- 5. superseded files retained ----
    # garbage() scans the local data root; the live set comes from Pelican.
    garbage = cat.garbage()
    check("superseded files are identifiable for later deletion",
          set(garbage) == set(superseded), f"{len(garbage)} files")
    check("superseded files still exist (readers may hold them)",
          all(os.path.exists(os.path.join(sandbox, g)) for g in garbage))

    # ---- 6. concurrent commit detected (sequential, as exp13 tested it) ----
    stale = cat.store.current_id()
    cat.add_files([entry_for(sandbox, f"{day}/compacted_0.parquet")], op="append")
    try:
        s = cat._new_snapshot([], stale, "append")
        cat.store.put_snapshot(s)
        cat.store.commit(s["snapshot_id"], expected_parent=stale)
        detected = False
    except ConcurrentUpdate:
        detected = True
    check("a commit against a stale parent is rejected (sequential)", detected)

    # ---- 7. NEW: the race window exp13 could not see ----
    # Two writers commit at the same instant, each having read the same
    # parent. On POSIX the check-to-swap gap is microseconds; here it is a
    # network round trip and nothing at the storage layer arbitrates.
    print("\n--- the commit race (no compare-and-swap on this origin) ---")
    base = cat.store.current_id()
    outcomes = {"committed": 0, "rejected": 0}

    def racer(n):
        s = cat._new_snapshot([], base, f"racer{n}")
        cat.store.put_snapshot(s)
        try:
            cat.store.commit(s["snapshot_id"], expected_parent=base)
            return ("committed", s["snapshot_id"])
        except ConcurrentUpdate:
            return ("rejected", s["snapshot_id"])

    # Repeat it: one trial cannot distinguish a real race from bad luck.
    trials, lost_updates = 3, 0
    for t in range(trials):
        base = cat.store.current_id()
        with ThreadPoolExecutor(max_workers=2) as ex:
            got = list(ex.map(racer, [1, 2]))
        n_ok = sum(1 for k, _ in got if k == "committed")
        outcomes["committed"] += n_ok
        outcomes["rejected"] += 2 - n_ok
        if n_ok == 2:
            lost_updates += 1
        print(f"  trial {t+1}: {n_ok}/2 committed"
              + ("  <-- lost update" if n_ok == 2 else ""))
    print(f"  pointer now {cat.store.current_id()}")
    check("CONCURRENT writers: at most one commit succeeds",
          lost_updates == 0,
          f"{lost_updates}/{trials} trials lost an update "
          f"(both writers committed; the later silently clobbered the earlier)")

    # Measure the window itself, regardless of who won.
    t0 = time.perf_counter()
    cat.store.current_id()
    read_ms = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    cat.store.commit(cat.store.current_id())
    swap_ms = (time.perf_counter() - t0) * 1000
    print(f"  check costs {read_ms:.0f} ms, swap costs {swap_ms:.0f} ms "
          f"-> race window ~{swap_ms:.0f} ms wide")

    # ---- 8. NEW: a pinned snapshot survives pointer movement, over the network ----
    # The race above can leave an EMPTY snapshot current, which would make
    # this check pass vacuously (0 == 0). Republish real content first and
    # assert the pinned view is non-empty, so the check can actually fail.
    cat.add_files(entries, op="restore")
    held = cat.pin()
    held_rows = run(held.plan(filters={"plugin": rare}), rare)
    for _ in range(3):
        s = cat._new_snapshot([], cat.store.current_id(), "churn")
        cat.store.put_snapshot(s)
        cat.store.commit(s["snapshot_id"])
    still = run(held.plan(filters={"plugin": rare}), rare)
    check("a pinned snapshot is still readable after the pointer moved 3x",
          held_rows > 0 and still == held_rows,
          f"{held_rows} rows (non-vacuous), still on {held.sid}; "
          f"CURRENT is now an empty snapshot")

    print(f"\nverbs total: {cat.store.io.counts}")
    npass = sum(1 for r in results if r["status"] == "PASS")
    print(f"{npass}/{len(results)} checks passed")

    out = {"checks": results, "snapshot_bytes": snap_bytes,
           "race_trials": trials, "race_lost_updates": lost_updates,
           "files": len(rels), "publish_seconds": round(publish_s, 2),
           "replace_seconds": round(replace_s, 2),
           "pin_ms": round(pin_s * 1000),
           "race": {"outcomes": outcomes, "check_ms": round(read_ms),
                    "swap_ms": round(swap_ms)},
           "verbs": cat.store.io.counts}

    print("\ncleaning up Pelican objects...")
    print(f"  deleted {cleanup(PREFIX)} objects under {PREFIX}")

    p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     "results", "exp18_catalog_on_pelican.json")
    with open(p, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
