#!/usr/bin/env python3
"""Where does a DuckLake catalog serialize? (local disk, no credentials)

`probe_ducklake_pelican.py` Q7 could not answer the §18/§19 concurrency
question: both writers failed with "database is locked", raised while
ATTACHing rather than while committing, and the retry loop exhausted ~32s
of backoff. That is the signature of a lock held for the life of the
attachment, not of commit contention -- but "looks like" is not a
measurement, and the distinction decides whether the limitation is
DuckLake's commit protocol or the catalog backend's locking.

So this isolates the question. No Pelican: the data path is local, because
the contended resource under test is the catalog database. Pelican is
where the *data* lives and it never participates in a DuckLake commit --
including it would only add 150ms/request noise to a lock measurement.

Three backends, same test. DuckLake's own catalog documentation scopes each
one, and the measurement below reproduces that scoping rather than taking
it on trust:
  duckdb file -- docs: "limited to a single client"
  sqlite      -- docs: "multiple local clients"; what Q7 and §18/§19 used
  postgres    -- docs: the recommended backend for "a multi-user lakehouse
                 with potentially remote clients". Skipped unless
                 DUCKLAKE_PG is set, and that is the gap in this result.

The test itself distinguishes three outcomes that Q7 collapsed into one:
  A. second writer cannot ATTACH at all      -> backend serializes sessions
  B. both ATTACH, one commit fails loudly    -> correct conflict detection
  C. both ATTACH, both succeed, rows missing -> the §18 silent lost update
Only C is a correctness failure. A is a deployment constraint and B is
right behaviour, so reporting all three as "FAIL" would be wrong.
"""
import os
import shutil
import sys
import tempfile
import threading
import time

import duckdb
import fsspec
import fsspec.implementations.local

ok = 0
fail = 0


def ck(label, passed, detail=""):
    global ok, fail
    if passed:
        ok += 1
    else:
        fail += 1
    print(f"[{'PASS' if passed else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))


def note(label, detail=""):
    print(f"[ -- ] {label}" + (f"  ({detail})" if detail else ""))


class _SlowFS(fsspec.implementations.local.LocalFileSystem):
    """Local disk with Pelican's latency bolted on.

    §17 measured ~150 ms per request against NRDStor. The Pelican probe's Q7
    behaves differently from the local run here, and latency is the only
    variable that changed -- every data write in that probe is a network
    round trip, which widens the window in which the catalog lock is held.
    Injecting the delay on local disk tests that explanation without needing
    the origin, and without attributing to DuckLake something that is really
    a property of slow storage.
    """

    protocol = "slowfs"
    LATENCY = 0.15

    def _open(self, path, mode="rb", **kw):
        time.sleep(self.LATENCY)
        return super()._open(self._strip_protocol(path), mode, **kw)

    def info(self, path, **kw):
        time.sleep(self.LATENCY)
        return super().info(self._strip_protocol(path), **kw)


def run_backend(name, catalog, datadir, extra_load=None, slow=False):
    """Return (attached_count, acked, total_rows, errors)."""
    os.makedirs(datadir, exist_ok=True)

    def mk():
        c = duckdb.connect()
        c.execute("INSTALL ducklake")
        c.execute("LOAD ducklake")
        if extra_load:
            c.execute(f"INSTALL {extra_load}")
            c.execute(f"LOAD {extra_load}")
        if slow:
            c.register_filesystem(_SlowFS())
        return c

    base = mk()
    base.execute(f"ATTACH '{catalog}' AS cc (DATA_PATH '{datadir}/')")
    base.execute("CREATE TABLE cc.obs(w INTEGER, i INTEGER)")
    base.close()

    attached = []
    acked = {1: 0, 2: 0}
    errors = []
    barrier = threading.Barrier(2)

    def writer(wid):
        w = None
        try:
            w = mk()
            # Both threads reach ATTACH at the same moment on purpose: a
            # staggered start would let the first finish and hide exactly
            # the contention being measured.
            barrier.wait(timeout=30)
            w.execute(f"ATTACH '{catalog}' AS cc (DATA_PATH '{datadir}/')")
            attached.append(wid)
            for i in range(3):
                w.execute(f"INSERT INTO cc.obs VALUES ({wid}, {i})")
                acked[wid] += 1
                time.sleep(0.01)
        except Exception as e:  # noqa: BLE001
            errors.append(f"w{wid}: {type(e).__name__}: {str(e)[:150]}")
        finally:
            # In a finally because the losing writer leaves via except, and
            # a connection left open keeps the SQLite lock -- which would
            # then break the verify ATTACH below and look like a lost
            # update. The Pelican probe hit exactly that.
            if w is not None:
                w.close()

    ts = [threading.Thread(target=writer, args=(k,)) for k in (1, 2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=90)

    total = None
    try:
        chk = mk()
        chk.execute(f"ATTACH '{catalog}' AS cc (DATA_PATH '{datadir}/')")
        total = chk.execute("SELECT count(*) FROM cc.obs").fetchone()[0]
        chk.close()
    except Exception as e:  # noqa: BLE001
        errors.append(f"verify: {type(e).__name__}: {str(e)[:150]}")

    return len(attached), acked, total, errors


def report(name, attached, acked, total, errors):
    nacked = sum(acked.values())
    print(f"\n-- {name} --")
    note("writers that attached", f"{attached}/2")
    note("inserts acknowledged", f"{nacked} (w1={acked[1]} w2={acked[2]})")
    note("rows in table", str(total))
    for e in errors:
        note("error", e)

    if attached < 2:
        # Outcome A. Not a correctness failure: nothing was acknowledged and
        # lost. It is a deployment constraint, so it is recorded as such.
        ck(f"{name}: no acknowledged row was silently lost", nacked == total,
           "second writer could not attach -- backend serializes sessions, "
           "so this backend cannot host two concurrent writers at all")
    else:
        ck(f"{name}: both writers attached", True)
        ck(f"{name}: no acknowledged row was silently lost",
           total is not None and total == nacked,
           f"{nacked} acked vs {total} present"
           + (" -- SILENT LOST UPDATE (the §18 failure)"
              if total is not None and total < nacked else ""))


def main():
    root = tempfile.mkdtemp(prefix="dlconc_")
    try:
        print("== DuckDB-file catalog ==")
        a = run_backend("duckdb-file", f"ducklake:{root}/c1.ducklake",
                        f"{root}/d1")
        report("duckdb-file", *a)

        print("\n== SQLite catalog (what Q7 and §18/§19 used) ==")
        b = run_backend("sqlite", f"ducklake:sqlite:{root}/c2.sqlite",
                        f"{root}/d2", extra_load="sqlite")
        report("sqlite", *b)

        print("\n== SQLite catalog, data on ~150ms storage (the Pelican shape) ==")
        s = run_backend("sqlite-slow", f"ducklake:sqlite:{root}/c4.sqlite",
                        f"slowfs://{root}/d4", extra_load="sqlite", slow=True)
        report("sqlite-slow", *s)

        pg = os.environ.get("DUCKLAKE_PG")
        print("\n== Postgres catalog ==")
        if not pg:
            note("skipped", "set DUCKLAKE_PG to a libpq connection string. "
                            "This is the gap that matters: DuckLake's catalog "
                            "docs recommend Postgres for 'a multi-user "
                            "lakehouse with potentially remote clients' and "
                            "scope SQLite to 'multiple local clients', so the "
                            "SQLite result above does NOT settle the "
                            "production question")
        else:
            c = run_backend("postgres", f"ducklake:postgres:{pg}",
                            f"{root}/d3", extra_load="postgres")
            report("postgres", *c)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print(f"\n{ok} passed, {fail} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
