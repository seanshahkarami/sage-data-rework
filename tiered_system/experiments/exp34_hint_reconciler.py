"""exp34: the plans/6 design, end to end on NRDStor.

Many committers, one reconciler, a reader with no database.

plans/6 proposes: writers commit through an Iceberg SQL catalog (the
database is the CAS, so two committers are safe), and a SINGLE reconciler
publishes `metadata/version-hint.text` to Pelican from the committed head,
so consumers can find the current metadata JSON without reaching a
Sage-hosted endpoint.

Every piece had been measured separately and the combination never had:

  - the SQL catalog commits on Pelican and leaves no hint         (exp19)
  - the filesystem catalog writes a hint, independent reader OK   (exp32)
  - DuckDB refuses a metadata directory with no hint       (ndp_interop)
  - stem resolves, `\\n` breaks it, stale hint is silent    (local fixture)

The last line was a LOCAL fixture -- a tmpdir, not Pelican. So the design
rests on a join that has never been run. This runs it.

What is being asked:

  Q1. Two concurrent committers against one table: does the database
      actually arbitrate, with no commit silently lost? This is the
      contrast with exp32, where 3 of 4 writers were told they won and
      2 commits vanished.
  Q2. Does the SQL catalog really leave no hint on Pelican? (exp19 says
      so; it is the reconciler's reason to exist, so re-check it here.)
  Q3. Does the reconciler publish the stem, with no trailing newline?
  Q4. Can a reader with NO database -- warehouse URL and hint only --
      load the table and see every committed row?
  Q5. Is a stale hint silent, and does re-running the reconciler repair
      it? This is the design's failure mode; it should be stale-but-valid
      and self-healing, not lossy.
  Q6. Is the reconciler idempotent -- does a second pass PUT nothing?
  Q7. Can an orphan metadata file from a failed commit fool it? It must
      take its answer from the database, never a directory listing.

Writes under /sage/cold-storage-test-hint-<ns>, its own prefix, checked
404 before use and removed afterwards. Never touches /sage/node-data or
any path sage-storage-loader uses.

Run:  conda run -n sage_pipeline python -I exp34_hint_reconciler.py
"""
import os
import re
import sys
import time
import shutil
import sqlite3
import tempfile
import threading
import traceback
import urllib.error
import urllib.request

import pyarrow as pa

# `-I` drops the script directory from sys.path; exp19 does the same.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pelican_token as pt
import pelican_fileio as pfio

PREFIX = f"/cold-storage-test-hint-{time.time_ns()}"
WAREHOUSE = f"{pt.ROOT_URL}{PREFIX}/warehouse"
FILEIO = "pelican_fileio.PelicanFileIO"

# pyiceberg's SqlCatalog lays a table out as <warehouse>/<ns>/<table>. An
# earlier draft wrote "sage.db/obs", the Hive convention, and the reader got
# FileNotFoundError on a metadata file the catalog had certainly written --
# so the layout is derived from metadata_location below rather than assumed,
# and this constant is only the table a reader is told to open.
TABLE_REL = "warehouse/sage/obs"
HINT_REL = f"{TABLE_REL}/metadata/version-hint.text"

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


# ---------------------------------------------------------------------------
# The reconciler. This is the whole proposal, and it is deliberately this
# small: read the committed head from the database, publish its stem.
#
# It is the only writer of the hint. That is what removes the race -- a
# single publisher cannot race itself -- and it is why correctness never
# depends on it: the database stays the head whether or not this ran.
# ---------------------------------------------------------------------------

def db_head(dburi_path, table_ns="sage", table_name="obs"):
    """The committed head, straight from the catalog table."""
    con = sqlite3.connect(dburi_path)
    try:
        row = con.execute(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE table_namespace=? AND table_name=?",
            (table_ns, table_name)).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def stem_of(metadata_location):
    """`.../00002-<uuid>.metadata.json` -> `00002-<uuid>`.

    Not a bare version number: a hint containing `2` sends DuckDB and
    pyiceberg looking for `v2.metadata.json`, which the SQL catalog does
    not create (measured, plans/6).
    """
    base = metadata_location.rsplit("/", 1)[-1]
    for ext in (".gz.metadata.json", ".metadata.json"):
        if base.endswith(ext):
            return base[: -len(ext)]
    raise ValueError(f"unexpected metadata filename: {base}")


def reconcile(dburi_path, hint_path):
    """Publish the hint iff it differs from the database head.

    Returns (action, stem): action is "wrote" or "unchanged".
    """
    head = db_head(dburi_path)
    if head is None:
        return "no-table", None
    want = stem_of(head)

    st, _, body = pfio._request(hint_path, "GET")
    if st == 200 and body.decode().strip("\n") == want and b"\n" not in body:
        return "unchanged", want

    # No trailing newline. Neither DuckDB nor pyiceberg trims, and the
    # resulting error names a path that looks correct.
    data = want.encode()
    assert not data.endswith(b"\n"), "hint must not end in a newline"
    st, _, body = pfio._request(hint_path, "PUT", data=data)
    if st not in (200, 201, 204):
        raise IOError(f"PUT hint -> {st} {body[:200]!r}")
    return "wrote", want


# ---------------------------------------------------------------------------
# A reader that has no database. Warehouse URL and hint only -- this is the
# NDP consumer the whole design is for.
# ---------------------------------------------------------------------------

def read_without_database(table_url):
    """Open a table knowing only its URL. No database, no catalog."""
    from pyiceberg.table import StaticTable

    md_dir = f"{table_url}/metadata"
    hint_url_path = pfio._path_of(f"{md_dir}/version-hint.text")
    st, _, body = pfio._request(hint_url_path, "GET")
    if st != 200:
        raise IOError(f"no hint: GET -> {st}")
    stem = body.decode()
    if stem != stem.strip():
        raise ValueError(f"hint has surrounding whitespace: {stem!r}")

    meta_loc = f"{md_dir}/{stem}.metadata.json"
    t = StaticTable.from_metadata(meta_loc, properties={"py-io-impl": FILEIO})
    return t, stem, meta_loc


def main():
    keep = "--keep" in sys.argv
    workdir = tempfile.mkdtemp(prefix="exp34-")
    dbpath = os.path.join(workdir, "catalog.db")
    dburi = f"sqlite:///{dbpath}"

    print(__doc__.split("Run:")[0].rstrip())
    table_url = f"{pt.ROOT_URL}{PREFIX}/{TABLE_REL}"
    globals()["TABLE_URL"] = table_url

    print(f"\nprefix      : {PREFIX}")
    print(f"warehouse   : {WAREHOUSE}")
    print(f"catalog db  : {dbpath}  (stands in for the Sage-hosted DB)")

    # Never run against a prefix that already has something in it.
    st, kids = pfio.propfind(PREFIX)
    if st != 404:
        print(f"\nABORT: prefix not empty (PROPFIND -> {st}, {len(kids)} children)")
        return 2
    print(f"fresh prefix: PROPFIND -> 404")

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import CommitFailedException

    # Every instance uses the SAME catalog name on purpose. `catalog_name` is
    # part of the primary key in `iceberg_tables`, so two services that pick
    # different names do not share a catalog at all -- they silently get
    # private ones over the same warehouse. An early draft of this test named
    # them writer0/writer1 and the second committer got
    # "NoSuchTableError: sage.obs", which is a confusing way for that to
    # present. Worth stating in the deployment config.
    CATALOG_NAME = "sage"

    def open_catalog(_role):
        """A separate instance == a separate committing service."""
        return SqlCatalog(CATALOG_NAME, **{"uri": dburi, "warehouse": WAREHOUSE,
                                           "py-io-impl": FILEIO})

    try:
        cat = open_catalog("writer0")
        cat.create_namespace("sage")

        schema = pa.schema([
            pa.field("committer", pa.int64(), nullable=False),
            pa.field("seq", pa.int64(), nullable=False),
        ])
        cat.create_table("sage.obs", schema=schema)

        # ---- Q1: two committers, concurrently, through the database -----
        print("\n--- Q1: two concurrent committers, arbitrated by the database ---")
        NCOMMIT, ROWS = 2, 50
        errors, landed = {}, {}
        barrier = threading.Barrier(NCOMMIT)

        def committer(cid):
            try:
                c = open_catalog(f"writer{cid}")
                data = pa.table({
                    "committer": pa.array([cid] * ROWS, pa.int64()),
                    "seq": pa.array(list(range(ROWS)), pa.int64()),
                }, schema=schema)
                barrier.wait()  # maximise overlap
                # The database CAS may reject one; that is the arbitration
                # working. Retry is the correct response, and it is what
                # exp32's storage could not offer at all.
                for attempt in range(8):
                    try:
                        t = c.load_table("sage.obs")
                        t.append(data)
                        landed[cid] = attempt + 1
                        return
                    except CommitFailedException:
                        time.sleep(0.4 * (attempt + 1))
                    except Exception as e:  # sqlite lock contention
                        if "locked" not in str(e).lower():
                            raise
                        time.sleep(0.4 * (attempt + 1))
                errors[cid] = "exhausted retries"
            except Exception as e:
                errors[cid] = f"{type(e).__name__}: {e}"

        threads = [threading.Thread(target=committer, args=(i,))
                   for i in range(NCOMMIT)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        dt = time.time() - t0

        check("Q1a: both committers completed without error", not errors,
              f"errors={errors}" if errors else f"attempts={landed}, {dt:.1f}s")

        verify = open_catalog("verify").load_table("sage.obs")
        tbl_now = verify.scan().to_arrow()
        per = {}
        for v in tbl_now.column("committer").to_pylist():
            per[v] = per.get(v, 0) + 1
        check("Q1b: no commit silently lost (contrast exp32: 2 of 4 lost)",
              len(tbl_now) == NCOMMIT * ROWS and len(per) == NCOMMIT,
              f"{len(tbl_now)} rows, per-committer={per}")

        # ---- Q2: the SQL catalog leaves no hint ------------------------
        print("\n--- Q2: does the SQL catalog publish a hint itself? ---")
        st, _, _ = pfio._request(f"{PREFIX}/{HINT_REL}", "GET")
        check("Q2: no version-hint.text after commits (reconciler needed)",
              st == 404, f"GET hint -> {st}")

        st, kids = pfio.propfind(f"{PREFIX}/{TABLE_REL}/metadata")
        jsons = [k for k in kids if k.endswith(".metadata.json")]
        print(f"       metadata dir holds {len(jsons)} *.metadata.json files")

        # ---- Q3: the reconciler publishes the stem ---------------------
        print("\n--- Q3: the reconciler publishes the committed head ---")
        head = db_head(dbpath)
        action, stem = reconcile(dbpath, f"{PREFIX}/{HINT_REL}")
        st, _, body = pfio._request(f"{PREFIX}/{HINT_REL}", "GET")
        check("Q3a: hint published, exactly the stem of the DB head",
              st == 200 and body.decode() == stem,
              f"action={action}, hint={body.decode()!r}")
        check("Q3b: no trailing newline (breaks both clients)",
              not body.endswith(b"\n") and body.decode() == body.decode().strip(),
              f"{body!r}")
        check("Q3c: stem is not a bare version number",
              not stem.isdigit(), f"stem={stem!r}")

        # ---- Q4: a reader with no database -----------------------------
        print("\n--- Q4: reader with NO database, warehouse + hint only ---")
        rt, rstem, rloc = read_without_database(TABLE_URL)
        rtbl = rt.scan().to_arrow()
        rper = {}
        for v in rtbl.column("committer").to_pylist():
            rper[v] = rper.get(v, 0) + 1
        check("Q4a: independent reader resolved the hint and opened the table",
              rstem == stem, f"followed hint -> {rstem}")
        check("Q4b: reader sees every committed row, no DB in the picture",
              len(rtbl) == NCOMMIT * ROWS and rper == per,
              f"{len(rtbl)} rows, per-committer={rper}")

        # ---- Q5: stale hint is silent, and repairs -----------------------
        print("\n--- Q5: the design's failure mode: stale, not lost ---")
        st, kids = pfio.propfind(f"{PREFIX}/{TABLE_REL}/metadata")
        stems = sorted(os.path.basename(k)[: -len(".metadata.json")]
                       for k in kids if k.endswith(".metadata.json"))
        older = stems[-2] if len(stems) >= 2 else None
        if older:
            pfio._request(f"{PREFIX}/{HINT_REL}", "PUT", data=older.encode())
            ot, ostem, _ = read_without_database(TABLE_URL)
            orows = len(ot.scan().to_arrow())
            check("Q5a: a stale hint yields an OLDER VALID snapshot, silently",
                  ostem == older and orows < NCOMMIT * ROWS,
                  f"hint={older[:13]}..., reader saw {orows} rows (no warning)")

            action2, stem2 = reconcile(dbpath, f"{PREFIX}/{HINT_REL}")
            ht, hstem, _ = read_without_database(TABLE_URL)
            hrows = len(ht.scan().to_arrow())
            check("Q5b: re-running the reconciler repairs it (self-healing)",
                  action2 == "wrote" and hstem == stem
                  and hrows == NCOMMIT * ROWS,
                  f"action={action2}, reader back to {hrows} rows")
        else:
            check("Q5: needs >= 2 metadata files", False, f"found {stems}")

        # ---- Q6: idempotent ---------------------------------------------
        print("\n--- Q6: is the reconciler idempotent? ---")
        before = dict(pfio.COUNTS)
        action3, _ = reconcile(dbpath, f"{PREFIX}/{HINT_REL}")
        puts = pfio.COUNTS["PUT"] - before["PUT"]
        check("Q6: a second pass writes nothing", action3 == "unchanged" and puts == 0,
              f"action={action3}, PUTs={puts}")

        # ---- Q7: an orphan must not fool it ------------------------------
        print("\n--- Q7: orphan metadata from a failed commit ---")
        # A higher-numbered metadata file that the database never accepted.
        # iceberg-go's fallback scan would prefer this (hadoop.go:102);
        # the reconciler must not, because it reads metadata_location.
        orphan_stem = "09999-deadbeef-0000-0000-0000-000000000000"
        orphan = (f"{PREFIX}/{TABLE_REL}/metadata/"
                  f"{orphan_stem}.metadata.json")
        pfio._request(orphan, "PUT", data=b'{"orphan":true}')
        action4, stem4 = reconcile(dbpath, f"{PREFIX}/{HINT_REL}")
        st, _, body = pfio._request(f"{PREFIX}/{HINT_REL}", "GET")
        check("Q7: reconciler follows the DB head, not the directory listing",
              body.decode() == stem and stem4 == stem,
              f"orphan 09999 present; hint still {body.decode()[:13]}...")
        pfio._request(orphan, "DELETE")

        # ---- summary -----------------------------------------------------
        print(f"\nrequests: {pfio.COUNTS}")
        npass = sum(1 for _, ok in RESULTS if ok)
        print(f"\n{npass}/{len(RESULTS)} checks passed")
        for name, ok in RESULTS:
            if not ok:
                print(f"  FAILED: {name}")
        return 0 if npass == len(RESULTS) else 1

    except Exception:
        traceback.print_exc()
        return 3
    finally:
        if keep:
            print(f"\n--keep: leaving {PREFIX} in place")
        else:
            print(f"\ncleaning up {PREFIX} ...")
            n = pfio.rmtree(PREFIX)
            st, left = pfio.propfind(PREFIX)
            print(f"deleted {n} objects; PROPFIND {PREFIX} -> {st}"
                  + (f" !! {len(left)} LEFT" if left else " (gone)"))
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
