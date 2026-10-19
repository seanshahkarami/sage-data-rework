"""Publish `version-hint.text` so database-less consumers can find the head.

Promoted out of experiments/exp34_hint_reconciler.py (open item 5 of
plans/6). Unchanged in substance -- the three behaviours below are load
bearing and each was established by measurement:

  1. The head is read straight from sqlite, NOT via SqlCatalog. The catalog
     would happily resolve a metadata file that the database no longer
     points at, so an orphan could masquerade as the head.
  2. The hint holds the STEM (`00002-<uuid>`), not a bare version number.
     A hint containing `2` sends DuckDB and pyiceberg looking for
     `v2.metadata.json`, which the SQL catalog never creates.
  3. The hint is written with NO trailing newline. Neither DuckDB nor
     pyiceberg trims it, and the resulting error names a path that looks
     correct, which makes it a genuinely expensive mistake to debug.

This is the single writer of the hint. One publisher cannot race itself,
which is what lets many committers coexist; correctness never depends on
the reconciler having run, because the database remains the real head.
"""
import argparse
import os
import sqlite3
import sys
import time

# `python -I` leaves the script directory off sys.path. The Pelican client
# lives next to this file.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


def db_head(dburi_path, table_ns="sage", table_name="obs"):
    """The committed head, straight from the catalog table.

    Opened read-only, for two reasons. The reconciler never writes the
    catalog -- committers do -- so read-only states that in the one place
    it could otherwise go wrong. And a plain `connect()` on a missing path
    CREATES an empty database, turning "the catalog is not where you think
    it is" into "no such table: iceberg_tables", which reads like a
    seeding bug. `mode=ro` raises instead, naming the path.
    """
    try:
        con = sqlite3.connect(f"file:{dburi_path}?mode=ro", uri=True)
    except sqlite3.OperationalError as e:
        # sqlite says only "unable to open database file". In a container
        # the catalog arrives by bind mount, so the path is exactly the
        # thing the operator got wrong and exactly what sqlite omits.
        raise sqlite3.OperationalError(f"{e}: {dburi_path}") from None
    try:
        row = con.execute(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE table_namespace=? AND table_name=?",
            (table_ns, table_name)).fetchone()
    finally:
        con.close()
    return row[0] if row else None


def stem_of(metadata_location):
    """`.../00002-<uuid>.metadata.json` -> `00002-<uuid>`."""
    base = metadata_location.rsplit("/", 1)[-1]
    for ext in (".gz.metadata.json", ".metadata.json"):
        if base.endswith(ext):
            return base[: -len(ext)]
    raise ValueError(f"unexpected metadata filename: {base}")


def reconcile(dburi_path, hint_path, table_ns="sage", table_name="obs"):
    """Publish the hint iff it differs from the database head.

    Returns (action, stem) where action is "wrote", "unchanged" or
    "no-table".

    `_request` is a function in pelican_fileio, not a method of
    PelicanFileIO. Calling it on an instance raises AttributeError and
    publishes nothing.
    """
    import pelican_fileio

    head = db_head(dburi_path, table_ns, table_name)
    if head is None:
        return "no-table", None
    want = stem_of(head)

    st, _, body = pelican_fileio._request(hint_path, "GET")
    if st == 200 and body.decode().strip("\n") == want and b"\n" not in body:
        return "unchanged", want

    data = want.encode()
    assert not data.endswith(b"\n"), "hint must not end in a newline"
    st, _, body = pelican_fileio._request(hint_path, "PUT", data=data)
    if st not in (200, 201, 204):
        raise IOError(f"PUT hint -> {st} {body[:200]!r}")
    return "wrote", want


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True, help="path to the catalog sqlite file")
    ap.add_argument("--hint", required=True, help="Pelican path of version-hint.text")
    ap.add_argument("--namespace", default="sage")
    ap.add_argument("--table", default="obs")
    ap.add_argument("--interval", type=float, default=0,
                    help="seconds between runs; 0 means run once and exit")
    args = ap.parse_args()

    # `required=True` does not catch `--hint ""`, and compose passes exactly
    # that when COLD_HINT_PATH is unset (profiles do not gate interpolation,
    # so a `:?` guard in the compose file is not available). An empty hint
    # would PUT the stem to the namespace root instead of the table's
    # metadata directory -- a write to the wrong place, which is the one
    # class of mistake this script must never make.
    if not args.hint.strip():
        ap.error("--hint is empty; set COLD_HINT_PATH (seed_cold.py prints it)")
    if not args.hint.startswith("/cold-storage-demo"):
        ap.error(f"--hint {args.hint!r} must be under /cold-storage-demo "
                 "(the Pelican root holds live /node-data)")

    while True:
        try:
            action, stem = reconcile(args.db, args.hint,
                                     args.namespace, args.table)
            print(f"{time.strftime('%H:%M:%S')} {action} {stem or ''}", flush=True)
        except Exception as e:
            # A failed publish is survivable: the database is still the
            # head and the previous hint still resolves. Log and retry.
            print(f"{time.strftime('%H:%M:%S')} error: {type(e).__name__}: {e}",
                  file=sys.stderr, flush=True)
            if not args.interval:
                return 1
        if not args.interval:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
