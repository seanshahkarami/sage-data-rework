#!/usr/bin/env python
"""
Experiment 16: build a real Iceberg table from archive data and test the two
claims the recommendation leans on.

The recommendation says: adopt a custom catalog now, but keep the option of
emitting Iceberg metadata later as an *export* format for external readers,
because DuckDB can read an Iceberg table path directly with no catalog. Both
halves of that are load-bearing and neither was tested.

  Claim A: real archive data round-trips into an Iceberg table unchanged.
  Claim B: a reader can open that table WITHOUT the catalog that wrote it,
           i.e. the table is a self-describing handoff artifact.

Claim B is the one that matters for `plans/Data Pipeline Considerations.md`
(pushing finalised data to a partner's AWS, search hosted at cached sites).
If the table is only readable through the service that created it, it is not
a handoff artifact and that argument collapses.

Usage: python exp16_iceberg_interop.py <archive-root> <workdir>
"""
import json
import os
import shutil
import sys
import time

import duckdb
import pyarrow.parquet as pq

results = []


def check(name, cond, detail=""):
    results.append({"check": name, "status": "PASS" if cond else "FAIL", "detail": detail})
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return cond


def main():
    src = os.path.abspath(sys.argv[1])
    work = os.path.abspath(sys.argv[2])
    sandbox = os.path.join(work, "iceberg_test")
    if os.path.exists(sandbox):
        shutil.rmtree(sandbox)
    os.makedirs(sandbox)

    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = os.path.join(sandbox, "warehouse")
    os.makedirs(warehouse)
    cat = SqlCatalog("t", **{"uri": f"sqlite:///{sandbox}/cat.db",
                             "warehouse": f"file://{warehouse}"})

    day = sorted(d for d in os.listdir(src) if d.startswith("date="))[0]
    f0 = sorted(f for f in os.listdir(os.path.join(src, day)) if f.endswith(".parquet"))[0]
    srcfile = os.path.join(src, day, f0)

    # One row group's worth: enough to be real, small enough to be quick.
    tbl = pq.ParquetFile(srcfile).read_row_group(0) if pq.ParquetFile(srcfile).metadata.num_row_groups > 1 \
        else pq.read_table(srcfile).slice(0, 2_000_000)
    print(f"source: {day}/{f0}")
    print(f"rows  : {tbl.num_rows:,}\n")

    # Iceberg v2 has no nanosecond timestamp type, and pyiceberg 0.12 cannot
    # write v3 yet, so the archive's timestamp[ns] must be downcast to
    # microseconds to land in a table at all. 97.60% of rows across an 8-day
    # sample carry a non-zero nanosecond remainder, so this is lossy for
    # essentially the whole archive -- measured by probe_ns_remainder.py,
    # results/ns_remainder.json. (Do not re-measure this through DuckDB: it
    # downcasts ns to us on read and will tell you the answer is 0%.)
    import pyarrow as pa
    if pa.types.is_timestamp(tbl.schema.field("time").type) and \
            tbl.schema.field("time").type.unit == "ns":
        i = tbl.schema.get_field_index("time")
        tbl = tbl.set_column(i, pa.field("time", pa.timestamp("us", tz="UTC")),
                             tbl.column("time").cast(pa.timestamp("us", tz="UTC"),
                                                     safe=False))
        print("NOTE: downcast time ns -> us to satisfy Iceberg v2.")
        print("      pyarrow refuses this cast unless safe=False, because it")
        print("      loses data. That refusal is the finding.\n")

    cat.create_namespace("sage")
    t0 = time.perf_counter()
    ice = cat.create_table("sage.obs", schema=tbl.schema)
    ice.append(tbl)
    write_s = time.perf_counter() - t0
    print(f"wrote Iceberg table in {write_s:.1f}s\n")

    # ---- Claim A: data round-trips ----
    back = ice.scan().to_arrow()
    check("row count survives the round trip", back.num_rows == tbl.num_rows,
          f"{tbl.num_rows:,} -> {back.num_rows:,}")

    con = duckdb.connect()
    src_cnt = con.execute(
        "select count(*), count(distinct plugin), round(sum(value_float),2) "
        "from tbl where value_float is not null"
    ).fetchone() if False else None
    con.register("a", tbl)
    con.register("b", back)
    agg = "select count(*), count(distinct plugin), count(distinct name), " \
          "round(sum(value_float)::DOUBLE, 2)"
    ra = con.execute(f"{agg} from a").fetchone()
    rb = con.execute(f"{agg} from b").fetchone()
    check("content identical (rows, plugins, names, value sum)", ra == rb, f"{ra} == {rb}")

    meta_a = con.execute("select count(*) from a where meta is not null").fetchone()[0]
    meta_b = con.execute("select count(*) from b where meta is not null").fetchone()[0]
    check("the meta map survives", meta_a == meta_b, f"{meta_a:,} == {meta_b:,}")

    # ---- Claim B: readable WITHOUT the catalog ----
    tloc = ice.metadata_location
    print(f"\nmetadata location: {tloc.replace(sandbox, '<sandbox>')}")

    # Simulate the handoff: copy only the table directory, drop the catalog DB.
    handoff = os.path.join(sandbox, "handoff")
    tabledir = os.path.dirname(os.path.dirname(tloc.replace("file://", "")))
    shutil.copytree(tabledir, handoff)
    os.remove(os.path.join(sandbox, "cat.db"))
    check("catalog database deleted (simulating handoff to a third party)",
          not os.path.exists(os.path.join(sandbox, "cat.db")))

    moved_meta = os.path.join(handoff, "metadata", os.path.basename(tloc))

    con2 = duckdb.connect()
    try:
        con2.execute("INSTALL iceberg; LOAD iceberg;")
        ext = True
    except Exception as e:
        ext = False
        print(f"  (duckdb iceberg extension unavailable: {str(e)[:80]})")

    if ext:
        try:
            n = con2.execute(
                f"select count(*) from iceberg_scan('{moved_meta}')"
            ).fetchone()[0]
            check("DuckDB reads the table with NO catalog, after it moved",
                  n == tbl.num_rows, f"{n:,} rows from a bare metadata path")
        except Exception as e:
            check("DuckDB reads the table with NO catalog, after it moved", False,
                  str(e)[:160])

    # pyarrow can always read the data files themselves
    dfiles = []
    for root, _, fs in os.walk(os.path.join(handoff, "data")):
        dfiles += [os.path.join(root, f) for f in fs if f.endswith(".parquet")]
    if dfiles:
        n = sum(pq.ParquetFile(p).metadata.num_rows for p in dfiles)
        check("underlying Parquet files remain plain and readable",
              n == tbl.num_rows, f"{len(dfiles)} file(s), {n:,} rows")

    # ---- size of the metadata ----
    msize = 0
    nmeta = 0
    for root, _, fs in os.walk(os.path.join(handoff, "metadata")):
        for f in fs:
            msize += os.path.getsize(os.path.join(root, f))
            nmeta += 1
    print(f"\nIceberg metadata: {nmeta} files, {msize:,} bytes for 1 data file")
    print("(custom catalog snapshot, measured in exp15: ~318 bytes per data file)")

    npass = sum(1 for r in results if r["status"] == "PASS")
    print(f"\n{npass}/{len(results)} checks passed")
    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp16_iceberg_interop.json", "w") as f:
        json.dump({"checks": results, "metadata_files": nmeta,
                   "metadata_bytes": msize, "rows": tbl.num_rows}, f, indent=2)


if __name__ == "__main__":
    main()
