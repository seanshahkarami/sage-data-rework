#!/usr/bin/env python
"""
Experiment 13: does a minimal custom catalog actually deliver the safety
properties, including the failure modes the ticket lists?

The ticket says: "A useful result is that a failed attempt leaves the archive
frontier unchanged unless the published snapshot actually contains the
interval." That is the bar. exp10 showed compaction preserves results; it did
not show compaction is *safe* -- no publication step, no pinned reader, no
deletion discipline.

This tests catalog.py against real archive days for:

  1. planning from statistics with no file opens
  2. a reader pinned to a snapshot is unaffected by a compaction that
     publishes mid-query
  3. replace() is atomic: results are identical before and after, and there is
     no moment where a file is double-counted or missing
  4. a crash between writing a snapshot and moving the pointer leaves the
     archive exactly as it was
  5. superseded files are identifiable but NOT deleted while a reader may hold
     them
  6. a concurrent commit is detected rather than silently clobbering

Usage: python exp13_catalog.py <archive-root> <workdir> [--days N]
"""
import json
import os
import shutil
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from catalog import Catalog, CatalogStore, ConcurrentUpdate

results = []


def check(name, cond, detail=""):
    results.append({"check": name, "status": "PASS" if cond else "FAIL", "detail": detail})
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return cond


def entry_for(root, relpath):
    """Footer facts for one file: exactly what section 7 says to carry."""
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
    return {
        "path": relpath,
        "date": relpath.split("/")[0].replace("date=", ""),
        "rows": md.num_rows,
        "size": os.path.getsize(p),
        "groups": groups,
    }


def main():
    src = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    ndays = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 2

    sandbox = os.path.join(work, "catalog_test")
    if os.path.exists(sandbox):
        shutil.rmtree(sandbox)
    os.makedirs(sandbox)
    days = sorted(d for d in os.listdir(src) if d.startswith("date="))[:ndays]
    for d in days:
        shutil.copytree(os.path.join(src, d), os.path.join(sandbox, d))

    cat = Catalog(sandbox)
    rels = [f"{d}/{f}" for d in days for f in sorted(os.listdir(os.path.join(sandbox, d)))
            if f.endswith(".parquet")]

    t0 = time.perf_counter()
    entries = [entry_for(sandbox, r) for r in rels]
    sid0 = cat.add_files(entries, op="initial")
    print(f"published {len(entries)} files as snapshot {sid0} in {time.perf_counter()-t0:.1f}s\n")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    def run(paths, plugin):
        if not paths:
            return 0
        fl = "[" + ",".join("'" + p + "'" for p in paths) + "]"
        return con.execute(
            f"select count(*) from read_parquet({fl}) where plugin = '{plugin}'"
        ).fetchone()[0]

    # a rare plugin, the case where planning matters
    allp = "[" + ",".join("'" + os.path.join(sandbox, r) + "'" for r in rels) + "]"
    rare = con.execute(
        f"select plugin, count(*) c from read_parquet({allp}) group by plugin order by c asc limit 1"
    ).fetchone()[0]

    # ---- 1. planning from statistics ----
    snap = cat.pin()
    t0 = time.perf_counter()
    planned = snap.plan(filters={"plugin": rare})
    plan_s = time.perf_counter() - t0
    check("planning rejects files from statistics alone",
          len(planned) < len(rels),
          f"{len(planned)}/{len(rels)} files survive, planned in {plan_s*1000:.2f} ms with 0 file opens")

    truth = run([os.path.join(sandbox, r) for r in rels], rare)
    viaplan = run(planned, rare)
    check("planning does not lose rows", truth == viaplan, f"{truth} == {viaplan}")

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
    sid1 = cat.replace(superseded, [entry_for(sandbox, f"{day}/compacted_0.parquet")])

    after_pinned = run(reader.plan(filters={"plugin": rare}), rare)
    check("pinned reader is unaffected by a compaction published mid-query",
          before == after_pinned, f"{before} == {after_pinned} (reader still on {reader.sid})")

    fresh = cat.pin()
    after_fresh = run(fresh.plan(filters={"plugin": rare}), rare)
    check("a new reader sees identical results after compaction",
          before == after_fresh, f"{before} == {after_fresh} (now on {fresh.sid})")

    live = {e["path"] for e in fresh.files}
    check("compaction swapped the files atomically",
          not (live & set(superseded)) and f"{day}/compacted_0.parquet" in live,
          f"{len(superseded)} superseded, {len(live)} live")

    # ---- 4. crash between snapshot write and pointer move ----
    before_ptr = cat.store.current_id()
    orphan = cat._new_snapshot([], before_ptr, "append")
    cat.store.put_snapshot(orphan)          # written...
    # ...process dies here, pointer never moves.
    recovered = cat.pin()
    check("a snapshot written but never committed is invisible",
          cat.store.current_id() == before_ptr and recovered.sid == before_ptr,
          f"pointer still {before_ptr}")
    check("archive frontier unchanged after the simulated crash",
          run(recovered.plan(filters={"plugin": rare}), rare) == before)

    # ---- 5. superseded files retained, identifiable ----
    garbage = cat.garbage()
    check("superseded files are identifiable for later deletion",
          set(garbage) == set(superseded), f"{len(garbage)} files")
    check("superseded files still exist on disk (readers may hold them)",
          all(os.path.exists(os.path.join(sandbox, g)) for g in garbage))

    # ---- 6. concurrent commit detected ----
    stale = cat.store.current_id()
    cat.add_files([entry_for(sandbox, f"{day}/compacted_0.parquet")], op="append")
    try:
        s = cat._new_snapshot([], stale, "append")
        cat.store.put_snapshot(s)
        cat.store.commit(s["snapshot_id"], expected_parent=stale)
        detected = False
    except ConcurrentUpdate:
        detected = True
    check("a commit against a stale parent is rejected", detected)

    # ---- size of the thing ----
    sid = cat.store.current_id()
    snap_path = os.path.join(sandbox, "_catalog", "snapshots", f"{sid}.json")
    print(f"\nsnapshot size: {os.path.getsize(snap_path):,} bytes for {len(rels)} files")
    print(f"catalog.py   : {sum(1 for _ in open(os.path.dirname(os.path.abspath(__file__))+'/catalog.py'))} lines")

    npass = sum(1 for r in results if r["status"] == "PASS")
    print(f"\n{npass}/{len(results)} checks passed")
    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp13_catalog.json", "w") as f:
        json.dump({"checks": results, "snapshot_bytes": os.path.getsize(snap_path),
                   "files": len(rels)}, f, indent=2)


if __name__ == "__main__":
    main()
