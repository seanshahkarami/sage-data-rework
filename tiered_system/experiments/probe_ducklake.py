"""Does DuckLake clear the constraints Iceberg was measured against?

Not a feature comparison. The three questions are the ones that already
decided things in SUMMARY.md, so DuckLake is judged on the same evidence:

  1. nanoseconds (SS 13/20/25) -- can it store the archive's timestamps?
  2. commit safety on storage with no CAS (SS 24) -- does it need a database?
  3. relocation (SS 27 Q2) -- can a written table be moved?

Q3 is the interesting one: DuckLake's catalog has a `path_is_relative`
column, which is exactly the property Iceberg lacks. The move test deletes
the source first, for the same reason exp27 does -- otherwise the reader
follows absolute paths home and the test passes vacuously.

Local disk only. Run:  python -I probe_ducklake.py
"""

import os
import shutil
import sys

import duckdb

SRC = "/tmp/dlprobe/src"
DST = "/tmp/dlprobe/dst"

ok = fail = 0


def ck(label, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"[PASS] {label}" + (f"  ({detail})" if detail else ""))
    else:
        fail += 1
        print(f"[FAIL] {label}" + (f"  ({detail})" if detail else ""))


def con():
    c = duckdb.connect()
    c.execute("INSTALL ducklake")
    c.execute("LOAD ducklake")
    return c


def main():
    shutil.rmtree("/tmp/dlprobe", ignore_errors=True)
    os.makedirs(SRC)

    print("== Q1: timestamp precision ==")
    c = con()
    for t in ("TIMESTAMP_NS", "TIMESTAMPTZ", "TIMESTAMPTZ_NS"):
        try:
            c.execute(f"CREATE TEMP TABLE probe_{t}(x {t})")
            exists = True
        except duckdb.Error:
            exists = False
        print(f"       DuckDB core has {t}: {exists}")

    c.execute(f"ATTACH 'ducklake:{SRC}/meta.db' AS dl (DATA_PATH '{SRC}/data/')")
    c.execute("CREATE TABLE dl.obs(t TIMESTAMP_NS, name VARCHAR, v DOUBLE)")
    # the exact value shape the archive has: a non-zero sub-microsecond
    # remainder, without which a round-trip check passes vacuously (SS 25).
    #
    # NOTE: build the value from an int64 epoch, NOT by adding an INTERVAL to a
    # TIMESTAMP_NS literal. An earlier version of this probe did the latter and
    # measured remainder 0 -- DuckDB's INTERVAL arithmetic goes through
    # microseconds, so the test destroyed the precision itself and would have
    # blamed DuckLake for it.
    c.execute(
        "INSERT INTO dl.obs SELECT "
        "make_timestamp_ns(1735689600123456789 + i * 1000000000), "
        "'n' || i, i * 1.0 FROM range(200) tbl(i)"
    )
    got = c.execute("SELECT epoch_ns(t) FROM dl.obs ORDER BY t LIMIT 1").fetchone()[0]
    ck(
        "TIMESTAMP_NS survives the round trip with its ns remainder",
        got % 1000 == 789,
        f"epoch_ns={got}, remainder={got % 1000}",
    )

    tztz = True
    try:
        c.execute("CREATE TABLE dl.obs_tz(t TIMESTAMPTZ_NS)")
    except duckdb.Error:
        tztz = False
    ck(
        "but there is NO tz-aware nanosecond type (TIMESTAMPTZ_NS)",
        not tztz,
        "spec has timestamp_ns; timestamptz is us-only -- so tz + ns is a choice of one",
    )

    nrows = c.execute("SELECT count(*) FROM dl.obs").fetchone()[0]
    ck("200 rows written", nrows == 200, f"{nrows} rows")

    print("\n== Q2: what the catalog is ==")
    files = sorted(os.listdir(SRC))
    ck(
        "a catalog database file exists on disk -- it is NOT optional",
        "meta.db" in files,
        f"{files}",
    )
    c.close()

    cat = duckdb.connect(f"{SRC}/meta.db")
    tabs = sorted(r[0] for r in cat.execute("SELECT table_name FROM duckdb_tables()").fetchall())
    ck(
        "the catalog is a full relational schema, not a pointer file",
        len(tabs) > 20,
        f"{len(tabs)} tables, e.g. {tabs[:4]}",
    )
    # SS 24 found Iceberg's per-table catalog state is ONE row. Here it is spread
    # across many tables, which matters for the SS 28 one-INSERT recovery.
    nfiles = cat.execute("SELECT count(*) FROM ducklake_data_file").fetchone()[0]
    ck(
        "per-file rows live in the catalog, not in a manifest on storage",
        nfiles >= 1,
        f"{nfiles} data_file rows -- storage alone does not describe the table",
    )

    print("\n== Q3: are paths relative? (the SS 27 Q2 failure point) ==")
    rows = cat.execute("SELECT path, path_is_relative FROM ducklake_data_file").fetchall()
    allrel = all(r[1] for r in rows)
    ck(
        "data-file paths are stored RELATIVE",
        allrel,
        f"e.g. {rows[0][0]!r} relative={rows[0][1]}",
    )
    dp = cat.execute(
        "SELECT value FROM ducklake_metadata WHERE key = 'data_path'"
    ).fetchall()
    print(f"       data_path in catalog: {dp}")
    cat.close()

    print("\n== Q3b: move the whole lake, with the source destroyed ==")
    shutil.copytree(SRC, DST)
    shutil.rmtree(SRC)
    if os.path.exists(SRC):
        print("source not actually destroyed; the rest would be vacuous")
        sys.exit(1)
    ck("source destroyed before any read", not os.path.exists(SRC))

    c = con()
    try:
        # The catalog records the ORIGINAL data_path and refuses a mismatched
        # one rather than silently guessing -- so the move needs an explicit
        # override. That refusal is itself the right behaviour: compare SS 27,
        # where Iceberg's moved table opened happily and failed only on read,
        # and SS 28, where a stale pointer served old data with no error.
        c.execute(
            f"ATTACH 'ducklake:{DST}/meta.db' AS mv "
            f"(DATA_PATH '{DST}/data/', OVERRIDE_DATA_PATH true)"
        )
        n = c.execute("SELECT count(*) FROM mv.obs").fetchone()[0]
        v = c.execute("SELECT epoch_ns(t) FROM mv.obs ORDER BY t LIMIT 1").fetchone()[0]
        ck(
            "the MOVED lake reads all its data with the source gone",
            n == 200,
            f"{n} rows, first epoch_ns={v}",
        )
        ck("and nanoseconds survived the move", v % 1000 == 789, f"remainder {v % 1000}")
        c.execute(
            "INSERT INTO mv.obs VALUES (TIMESTAMP_NS '2025-06-01 00:00:00.000000001', 'after', 1.0)"
        )
        n2 = c.execute("SELECT count(*) FROM mv.obs").fetchone()[0]
        ck("the moved lake accepts new writes, not read-only", n2 == 201, f"{n2} rows")
    except duckdb.Error as e:
        ck("the MOVED lake reads all its data with the source gone", False, str(e)[:160])
    c.close()

    print(f"\n{ok} passed, {fail} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
