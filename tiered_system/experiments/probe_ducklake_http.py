"""Can a DuckLake be read over HTTP with no local catalog? (the SS 23 analogue)

SS 23 is the strongest Iceberg result here: an NDP reader needs only a URL --
no database, no credentials beyond read. DuckLake always needs a catalog, but
the FAQ describes a "frozen DuckLake" where the catalog is a static DuckDB
file served over HTTPS. If that works, DuckLake has an equivalent story and
Pelican can host it; if not, the publishing model is materially worse.

Serves a lake over localhost HTTP and reads it from a client that has no
local copy of either the catalog or the data. Also logs every request, the
same method the Parquet experiments use (see README "Method note"), because
on Pelican each one costs ~150 ms (SS 17).

Local only. Run:  python -I probe_ducklake_http.py
"""

import functools
import http.server
import os
import shutil
import socketserver
import sys
import threading

import duckdb

ROOT = "/tmp/dlhttp"
LAKE = f"{ROOT}/lake"
REQS = []

ok = fail = 0


def ck(label, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"[PASS] {label}" + (f"  ({detail})" if detail else ""))
    else:
        fail += 1
        print(f"[FAIL] {label}" + (f"  ({detail})" if detail else ""))


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        REQS.append(("GET", self.path, self.headers.get("Range")))
        super().do_GET()

    def do_HEAD(self):
        REQS.append(("HEAD", self.path, None))
        super().do_HEAD()


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(LAKE)

    w = duckdb.connect()
    w.execute("INSTALL ducklake")
    w.execute("LOAD ducklake")
    w.execute(f"ATTACH 'ducklake:{LAKE}/meta.db' AS dl (DATA_PATH '{LAKE}/data/')")
    w.execute("CREATE TABLE dl.obs(t TIMESTAMP_NS, name VARCHAR, v DOUBLE)")
    w.execute(
        "INSERT INTO dl.obs SELECT "
        "make_timestamp_ns(1735689600123456789 + i * 1000000000), "
        "'n' || i, i * 1.0 FROM range(200) tbl(i)"
    )
    w.execute("CHECKPOINT")
    w.close()

    handler = functools.partial(Handler, directory=LAKE)
    socketserver.TCPServer.allow_reuse_address = True
    srv = socketserver.TCPServer(("127.0.0.1", 0), handler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"
    print(f"serving {LAKE} at {base}\n")

    r = duckdb.connect()
    r.execute("INSTALL ducklake")
    r.execute("LOAD ducklake")
    r.execute("INSTALL httpfs")
    r.execute("LOAD httpfs")

    # The reader has no local copy of anything; both catalog and data are remote.
    attached = True
    err = ""
    try:
        r.execute(
            f"ATTACH '{base}/meta.db' AS rd "
            f"(TYPE ducklake, DATA_PATH '{base}/data/', OVERRIDE_DATA_PATH true)"
        )
    except duckdb.Error as e:
        attached = False
        err = str(e).replace("\n", " ")[:220]
    ck("a remote DuckLake catalog attaches over plain HTTP", attached, err)

    if attached:
        n = v = None
        try:
            n = r.execute("SELECT count(*) FROM rd.obs").fetchone()[0]
            v = r.execute("SELECT epoch_ns(t) FROM rd.obs ORDER BY t LIMIT 1").fetchone()[0]
        except duckdb.Error as e:
            err = str(e).replace("\n", " ")[:220]
        ck("all rows read with NO local catalog and NO local data", n == 200, err or f"{n} rows")
        ck("nanoseconds intact over the wire", v is not None and v % 1000 == 789, f"epoch_ns={v}")

        # Is it read-only? A published archive should not be writable by readers.
        ro = False
        try:
            r.execute("INSERT INTO rd.obs VALUES (make_timestamp_ns(1), 'x', 1.0)")
        except duckdb.Error:
            ro = True
        ck("the HTTP-served lake is read-only, as a published archive should be", ro)

    print(f"\n{len(REQS)} HTTP requests; first few:")
    for m, p, rng in REQS[:6]:
        print(f"       {m} {p}" + (f"  Range: {rng}" if rng else ""))
    ranged = sum(1 for m, p, rng in REQS if rng)
    print(f"       {ranged} of {len(REQS)} used Range (partial reads, not whole-file GETs)")

    srv.shutdown()
    r.close()
    print(f"\n{ok} passed, {fail} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
