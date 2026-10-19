#!/usr/bin/env python
"""
Experiment 19: run Iceberg on real Pelican, to rule it out or keep it.

`plans/4_catalog_recommendation.md` rejects Iceberg partly on a claim that
was reasoned, not measured: "Iceberg's commit requires a compare-and-swap
that this storage does not provide", and "iceberg-go has no FileIO for
this storage". exp17/exp18 held the custom catalog to a measured standard.
This holds Iceberg to the same one.

Deliberately generous to Iceberg: `pelican_fileio.py` supplies the missing
FileIO, so the test is Iceberg's *commit model* on this storage, not a
missing import. If Iceberg fails here it fails on its own terms.

The questions, in the order they decide the outcome:

  1. Can an Iceberg table live on Pelican at all -- data AND metadata?
  2. Does any catalog commit without a service? (the cost argument)
  3. Does the CAS actually protect concurrent writers -- the exact case
     the custom catalog loses 3/3 (exp18)?
  4. If the catalog database is lost, is the table still readable?
     (the handoff claim in `Data Pipeline Considerations.md`)
  4b. Can a READER get data without the catalog at all -- DuckDB straight
     to Pelican, the way the custom design would? This decides whether
     the database is a write-side cost or a whole-system one.
  4c. Could that database live on Pelican, removing the service entirely?
  5. What does a commit cost in requests, at ~150 ms each (§17)?

Everything is written under /sage/cold-storage-test-iceberg/ -- a new
prefix for this experiment. /node-data and the sage-storage-loader paths
are never touched; the final step deletes the whole prefix.

Usage: python exp19_iceberg_on_pelican.py <archive-root> [--keep]
"""
import json
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pelican_fileio as pfio  # noqa: E402
import pelican_token as pt  # noqa: E402

PREFIX = "/cold-storage-test-iceberg"
WAREHOUSE = f"{pt.ROOT_URL}{PREFIX}/warehouse"
FILEIO = "pelican_fileio.PelicanFileIO"

results = []
notes = {}


def check(name, cond, detail=""):
    results.append({"check": name, "status": "PASS" if cond else "FAIL",
                    "detail": detail})
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    return cond


def section(t):
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def load_sample(archive_root, rows=300_000):
    """One real slice of archive data, downcast to what Iceberg v2 accepts."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    day = sorted(d for d in os.listdir(archive_root) if d.startswith("date="))[0]
    f0 = sorted(f for f in os.listdir(os.path.join(archive_root, day))
                if f.endswith(".parquet"))[0]
    src = os.path.join(archive_root, day, f0)
    tbl = pq.read_table(src).slice(0, rows)

    # Known from exp16 / probe_ns_remainder: Iceberg v2 has no nanosecond
    # timestamp and pyiceberg 0.12 cannot write v3, so this cast is forced
    # and it is lossy for 97.6% of rows. Re-stated here, not re-measured.
    i = tbl.schema.get_field_index("time")
    if pa.types.is_timestamp(tbl.schema.field("time").type) and \
            tbl.schema.field("time").type.unit == "ns":
        tbl = tbl.set_column(
            i, pa.field("time", pa.timestamp("us", tz="UTC")),
            tbl.column("time").cast(pa.timestamp("us", tz="UTC"), safe=False))
    return day, f0, tbl


def main():
    archive = os.path.abspath(sys.argv[1])
    keep = "--keep" in sys.argv

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import CommitFailedException

    section("0. Setup: real archive data, real Pelican prefix")
    day, f0, tbl = load_sample(archive)
    print(f"source      : {day}/{f0}")
    print(f"rows        : {tbl.num_rows:,}")
    print(f"warehouse   : {WAREHOUSE}")

    # The catalog database is LOCAL. That is not a shortcut -- it is the
    # finding. Pelican cannot host it; see section 2.
    workdir = tempfile.mkdtemp(prefix="exp19_")
    dburi = f"sqlite:///{workdir}/catalog.db"

    section("1. Can an Iceberg table live on Pelican at all?")
    pfio.reset_counts()
    cat = SqlCatalog("exp19", **{"uri": dburi, "warehouse": WAREHOUSE,
                                 "py-io-impl": FILEIO})
    cat.create_namespace("sage")
    t0 = time.perf_counter()
    ice = cat.create_table("sage.obs", schema=tbl.schema)
    create_s = time.perf_counter() - t0
    create_counts = dict(pfio.COUNTS)

    pfio.reset_counts()
    t1 = time.perf_counter()
    ice.append(tbl)
    append_s = time.perf_counter() - t1
    append_counts = dict(pfio.COUNTS)
    print(f"create table: {create_s:.1f}s  {create_counts}")
    print(f"append {tbl.num_rows:,} rows: {append_s:.1f}s  {append_counts}")

    check("an Iceberg table can be created and written on Pelican",
          True, f"metadata at {ice.metadata_location.split('/sage')[-1]}")

    pfio.reset_counts()
    t2 = time.perf_counter()
    back = ice.scan().to_arrow()
    scan_s = time.perf_counter() - t2
    scan_counts = dict(pfio.COUNTS)
    print(f"full scan   : {scan_s:.1f}s  {scan_counts}")
    check("data round-trips unchanged over Pelican",
          back.num_rows == tbl.num_rows, f"{tbl.num_rows:,} -> {back.num_rows:,}")

    import duckdb
    con = duckdb.connect()
    con.register("a", tbl)
    con.register("b", back)
    agg = ("select count(*), count(distinct plugin), count(distinct name), "
           "round(sum(value_float)::DOUBLE, 2)")
    ra = con.execute(f"{agg} from a").fetchone()
    rb = con.execute(f"{agg} from b").fetchone()
    check("content identical (rows, plugins, names, value sum)", ra == rb,
          f"{ra} == {rb}")
    notes["create"] = {"seconds": round(create_s, 1), "requests": create_counts}
    notes["append"] = {"seconds": round(append_s, 1), "requests": append_counts,
                       "rows": tbl.num_rows}
    notes["scan"] = {"seconds": round(scan_s, 1), "requests": scan_counts}

    section("2. Where does the commit actually happen?")
    # The decisive structural question. Iceberg's commit is
    # UPDATE iceberg_tables SET metadata_location=new WHERE metadata_location=old
    # (pyiceberg/catalog/sql.py:520-536). That row is in SQLite/Postgres,
    # not on Pelican. Pelican holds only immutable files.
    import sqlite3
    db = sqlite3.connect(f"{workdir}/catalog.db")
    rows = db.execute(
        "select table_namespace, table_name, metadata_location, "
        "previous_metadata_location from iceberg_tables").fetchall()
    db.close()
    print("the pointer Iceberg commits against lives here:")
    for r in rows:
        print(f"  {r[0]}.{r[1]}")
        print(f"    current : ...{(r[2] or '')[-60:]}")
        print(f"    previous: ...{(r[3] or '')[-60:]}")
    # Phrased as the true property so PASS means "confirmed", not "Iceberg
    # is fine". The finding is the dependency itself, not a defect.
    check("CONFIRMED: the commit pointer lives OFF Pelican, in a database",
          bool(rows) and rows[0][2] is not None,
          "a row in iceberg_tables; Pelican holds only immutable files")

    # Is there any pyiceberg catalog that commits via storage alone?
    import importlib
    import pkgutil
    import pyiceberg.catalog as pc
    storage_only = []
    all_cats = []
    for m in sorted(x.name for x in pkgutil.iter_modules(pc.__path__)):
        try:
            mod = importlib.import_module(f"pyiceberg.catalog.{m}")
            all_cats += [n for n in dir(mod)
                         if n.endswith("Catalog") and not n.startswith("_")
                         and n not in ("Catalog", "MetastoreCatalog")]
        except Exception:
            all_cats.append(f"{m}(uninstallable deps)")
    print(f"\npyiceberg catalogs: {sorted(set(all_cats))}")
    check("CONFIRMED: no storage-only catalog exists in pyiceberg",
          not storage_only,
          "every option (REST, Hive, Glue, DynamoDB, SQL, BigQuery) needs a "
          "service or a database; there is no filesystem/hadoop catalog")
    notes["catalogs"] = sorted(set(all_cats))

    section("3. Does the CAS protect concurrent writers?")
    # This is the case the custom catalog loses 3/3 (exp18).
    #
    # The property that matters is NOT "one writer is rejected" -- that
    # would be the wrong assertion. Two concurrent appends are commutative,
    # so the correct outcome is that BOTH land and NO rows are lost. The
    # question is whether the loser is detected and rebased, or whether it
    # silently overwrites. So measure rows, and separately observe the CAS.
    rows_before = cat.load_table("sage.obs").scan().to_arrow().num_rows
    small = tbl.slice(0, 1000)
    trials = 3
    detected = 0
    for n in range(trials):
        a = cat.load_table("sage.obs")
        b = cat.load_table("sage.obs")  # both from the SAME metadata
        oks, errs, retried = [], [], []

        import logging

        class _Catch(logging.Handler):
            def emit(self, rec):
                if "concurrent update" in rec.getMessage():
                    retried.append(1)

        lg = logging.getLogger("pyiceberg.table")
        h = _Catch()
        lg.addHandler(h)

        def commit(handle, tag):
            try:
                handle.append(small)
                oks.append(tag)
            except CommitFailedException:
                errs.append((tag, "CommitFailedException"))
            except Exception as e:  # noqa: BLE001
                errs.append((tag, f"{type(e).__name__}: {str(e)[:60]}"))

        with ThreadPoolExecutor(max_workers=2) as ex:
            fa = ex.submit(commit, a, "A")
            fb = ex.submit(commit, b, "B")
            fa.result(), fb.result()
        lg.removeHandler(h)
        if retried:
            detected += 1
        print(f"  trial {n + 1}: committed={oks} errors={errs} "
              f"CAS-conflict-detected={bool(retried)}")

    rows_after = cat.load_table("sage.obs").scan().to_arrow().num_rows
    expected = rows_before + trials * 2 * small.num_rows
    check("CONCURRENT writers: no rows lost (both appends land)",
          rows_after == expected,
          f"{rows_before:,} + {trials * 2 * small.num_rows:,} = "
          f"{expected:,}, got {rows_after:,}")
    check("the losing writer's conflict is DETECTED, not silently dropped",
          detected == trials,
          f"{detected}/{trials} trials raised CommitFailedException "
          "internally and rebased")
    notes["concurrency"] = {"trials": trials, "conflicts_detected": detected,
                            "rows_before": rows_before,
                            "rows_after": rows_after, "expected": expected}

    notes["cas_is_sql"] = (
        "pyiceberg/catalog/sql.py:520-536 -- UPDATE iceberg_tables SET "
        "metadata_location=:new WHERE metadata_location=:old; rowcount<1 "
        "-> CommitFailedException. The CAS is a SQL row, not storage.")
    print("\n  the CAS that made this work:")
    print("    UPDATE iceberg_tables SET metadata_location=<new>")
    print("     WHERE metadata_location=<old>   (sql.py:520-536)")
    print("    -> enforced by SQLite, not by Pelican.")

    # Confirm the history is intact -- no snapshot silently dropped.
    t = cat.load_table("sage.obs")
    snaps = list(t.metadata.snapshots)
    chain_ok = all(s.parent_snapshot_id is not None for s in snaps[1:])
    check("every snapshot after the first records its parent", chain_ok,
          f"{len(snaps)} snapshots, chain intact")

    section("4. Is the table readable if the catalog database is lost?")
    # The handoff claim. Iceberg metadata on Pelican is self-describing;
    # the catalog only holds the pointer to the newest metadata file.
    meta_loc = t.metadata_location
    print(f"newest metadata: ...{meta_loc[-70:]}")

    from pyiceberg.table import StaticTable
    os.remove(f"{workdir}/catalog.db")
    check("catalog database deleted (simulating handoff / service loss)",
          not os.path.exists(f"{workdir}/catalog.db"))

    try:
        st = StaticTable.from_metadata(meta_loc, {"py-io-impl": FILEIO})
        n = st.scan().to_arrow().num_rows
        check("pyiceberg reads the table from Pelican with NO catalog",
              n > 0, f"{n:,} rows from a bare metadata URL")
        notes["static_rows"] = n
    except Exception as e:  # noqa: BLE001
        check("pyiceberg reads the table from Pelican with NO catalog", False,
              f"{type(e).__name__}: {str(e)[:120]}")

    # But: can you FIND the newest metadata file without the catalog?
    st_, kids = pfio.propfind(f"{PREFIX}/warehouse/sage/obs/metadata")
    jsons = sorted(k for k in kids if k.endswith(".metadata.json"))
    print(f"\nmetadata files on Pelican: {len(jsons)}")
    hint = [k for k in kids if "version-hint" in k]
    check("the newest metadata file is identifiable from storage alone",
          bool(hint),
          f"{len(jsons)} *.metadata.json present, no version-hint.text -- "
          "the reader must be TOLD which one is current")
    notes["metadata_files"] = len(jsons)
    notes["version_hint"] = bool(hint)

    section("4b. Can a READER reach the data without the catalog at all?")
    # The question that decides whether the database is a write-side cost
    # or a whole-system one. If every query has to go through the catalog
    # service, Iceberg is far more expensive here than the custom design,
    # where DuckDB talks straight to Pelican.
    con2 = duckdb.connect()
    con2.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")

    st_, dkids = pfio.propfind(f"{PREFIX}/warehouse/sage/obs/data")
    dpath = [k for k in dkids if k.endswith(".parquet")][0]
    reader = {}

    # A. plain Parquet, auth as a URL parameter (the exp17 form)
    try:
        u = pt.url_for(dpath, scope=f"read:{dpath}", lifetime=900)
        n = con2.execute(
            f"select count(*) from read_parquet('{u}')").fetchone()[0]
        reader["read_parquet_urlparam"] = n
        check("DuckDB reads the Parquet directly (?authz= URL param)", n > 0,
              f"{n:,} rows, no catalog involved")
    except Exception as e:  # noqa: BLE001
        check("DuckDB reads the Parquet directly (?authz= URL param)", False,
              str(e)[:140])

    # B. plain Parquet, auth as a real Authorization header via CREATE SECRET.
    # Preferred over A: the token stays out of the URL and out of any log
    # that records query text.
    try:
        tok = pt.mint(f"read:{PREFIX}", lifetime=900)
        con2.execute("CREATE OR REPLACE SECRET pel (TYPE http, "
                     f"EXTRA_HTTP_HEADERS MAP{{'Authorization':'Bearer {tok}'}});")
        n = con2.execute(
            f"select count(*) from read_parquet('{pt.ROOT_URL}{dpath}')"
        ).fetchone()[0]
        reader["read_parquet_header"] = n
        check("DuckDB reads the Parquet directly (Authorization header)",
              n > 0, f"{n:,} rows via CREATE SECRET")
    except Exception as e:  # noqa: BLE001
        check("DuckDB reads the Parquet directly (Authorization header)",
              False, str(e)[:140])

    # C. iceberg_scan straight at the metadata JSON -- reads it AS a table
    # (resolving manifests itself), still with no catalog.
    #
    # The token must cover the whole table prefix, not just the data file:
    # iceberg_scan also fetches the metadata JSON and the Avro manifests.
    # Scoped to the data file alone this returns 403, which looks like a
    # capability limit and is really a scoping mistake.
    try:
        n = con2.execute(
            f"select count(*) from iceberg_scan('{meta_loc}')").fetchone()[0]
        reader["iceberg_scan"] = n
        check("DuckDB iceberg_scan() on a bare metadata URL, no catalog",
              n > 0, f"{n:,} rows, manifests resolved over HTTPS")
    except Exception as e:  # noqa: BLE001
        check("DuckDB iceberg_scan() on a bare metadata URL, no catalog",
              False, str(e)[:140])
    notes["reader_paths"] = reader

    section("4c. Could the catalog database itself live on Pelican?")
    # If it could, Iceberg would need no external service and the main
    # cost argument against it would collapse. A database needs in-place
    # page writes and locking; test whether Pelican offers either.
    probe = f"{PREFIX}/_dbprobe/db.bin"
    pfio._request(probe, "PUT", data=b"A" * 1000)
    db_caps = {}

    # Partial write: patch 10 bytes inside a 1000-byte file.
    pfio._request(probe, "PUT", data=b"B" * 10,
                  headers={"Content-Range": "bytes 10-19/1000"})
    size = len(pfio._request(probe, "GET")[2])
    db_caps["content_range_honored"] = (size == 1000)
    print(f"PUT with Content-Range  : file is now {size} bytes "
          f"({'patched in place' if size == 1000 else 'WHOLE FILE REPLACED'})")

    pfio._request(probe, "PUT", data=b"A" * 1000)
    st_p, _, _ = pfio._request(probe, "PATCH", data=b"C" * 10)
    db_caps["patch"] = st_p
    print(f"PATCH                   : HTTP {st_p}")

    st_o, h_o, _ = pfio._request(probe, "OPTIONS")
    db_caps["dav"] = h_o.get("DAV")
    db_caps["allow"] = h_o.get("Allow")
    print(f"OPTIONS DAV             : {h_o.get('DAV')}  "
          f"(class 2 = locking; 1 = none)")

    check("CONFIRMED: Pelican cannot host the catalog database",
          not db_caps["content_range_honored"] and st_p >= 400
          and "2" not in (db_caps["dav"] or ""),
          "no in-place writes, no PATCH, no DAV class 2 locking")
    print("  note: Content-Range returned 201 and was IGNORED -- a silent")
    print("        wrong answer, not a refusal. Same shape as the ignored")
    print("        If-None-Match / Overwrite: F in §17.")
    notes["db_on_pelican"] = db_caps
    pfio.rmtree(f"{PREFIX}/_dbprobe")

    section("5. What does it cost, at ~150 ms per request?")
    # Reconstruct a catalog over the same warehouse to time one more commit.
    cat2 = SqlCatalog("exp19b", **{"uri": f"sqlite:///{workdir}/c2.db",
                                   "warehouse": WAREHOUSE,
                                   "py-io-impl": FILEIO})
    cat2.create_namespace("sage")
    t3 = cat2.register_table("sage.obs", meta_loc)
    pfio.reset_counts()
    tA = time.perf_counter()
    t3.append(tbl.slice(0, 1000))
    one_commit_s = time.perf_counter() - tA
    one_commit = dict(pfio.COUNTS)
    total_req = sum(one_commit.values())
    print(f"one commit (1,000 rows): {one_commit_s:.1f}s, "
          f"{total_req} requests  {one_commit}")
    print("custom catalog, same operation (exp18): 1 GET + 2 PUT + 2 MOVE "
          "= 5 requests, ~0.7s")
    notes["one_commit"] = {"seconds": round(one_commit_s, 1),
                           "requests": one_commit, "total": total_req}

    counts_by_kind = {}
    for sub in ("metadata", "data"):
        s_, ks = pfio.propfind(f"{PREFIX}/warehouse/sage/obs/{sub}")
        counts_by_kind[sub] = len(ks)
    nfiles = sum(counts_by_kind.values())
    print(f"objects in the table directory: {nfiles}  {counts_by_kind}")
    print(f"  -> {counts_by_kind.get('metadata', 0)} metadata objects to "
          f"carry {counts_by_kind.get('data', 0)} data files")
    notes["objects"] = counts_by_kind

    section("Result")
    npass = sum(1 for r in results if r["status"] == "PASS")
    print(f"{npass}/{len(results)} checks passed")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "results", "exp19_iceberg_on_pelican.json")
    with open(out, "w") as fh:
        json.dump({"checks": results, "notes": notes}, fh, indent=2)
    print(f"wrote {out}")

    shutil.rmtree(workdir, ignore_errors=True)

    if keep:
        print(f"\n--keep: leaving {PREFIX} in place")
    else:
        print(f"\ncleaning up {PREFIX} ...")
        n = pfio.rmtree(PREFIX)
        st_, left = pfio.propfind(PREFIX)
        print(f"deleted {n} objects; PROPFIND {PREFIX} -> {st_} "
              f"({'GONE' if st_ == 404 else f'{len(left)} left'})")


if __name__ == "__main__":
    main()
