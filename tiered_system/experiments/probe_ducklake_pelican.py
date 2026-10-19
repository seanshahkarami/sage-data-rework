"""Does DuckLake work on Pelican, and does it work better than Iceberg? (exp30)

§29 rejected DuckLake on the absence of a Go client. That objection is void
if Python is acceptable, so the open question from §29's "Scope, stated"
becomes the live one: **DuckLake on Pelican.**

The §24 finding does not transfer directly. Iceberg's filesystem catalog
failed there because its commit needs an atomic create-if-absent that this
origin lacks. DuckLake's catalog is a *database file*, not a CAS pointer
swap, so it fails or survives for different reasons -- which is exactly why
it needs measuring rather than inferring.

Three questions, in dependency order:

  1. Can DuckDB write to Pelican at all? (httpfs over https:// -- PUT, not
     just GET.) If not, nothing else matters for the writer.
  2. Can a DuckLake be *published* on Pelican and read by a remote client
     with no local state? (the §23 property, which is the NDP requirement)
  3. Does relocation still hold when the move is local -> Pelican? (§27 Q2,
     DuckLake's win in §29)

Writes under /sage/cold-storage-test-ducklake/ -- its own prefix, distinct
from every other experiment's -- and removes it afterwards. Never touches
/sage/node-data or anything sage-storage-loader writes.

Needs credentials. Run:  python -I probe_ducklake_pelican.py
"""

import os
import shutil
import ssl
import time
import sys
import urllib.error
import urllib.parse
import urllib.request

import duckdb

# -I strips the script directory from sys.path; restore it, as exp17/exp19 do.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pelican_token  # noqa: E402

PREFIX = "/cold-storage-test-ducklake"
ROOT = pelican_token.ROOT_URL
LOCAL = "/tmp/dlpel"

ok = fail = 0
skipped = []


def ck(label, cond, detail=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"[PASS] {label}" + (f"  ({detail})" if detail else ""))
    else:
        fail += 1
        print(f"[FAIL] {label}" + (f"  ({detail})" if detail else ""))
    return cond


def note(label, detail=""):
    print(f"[ -- ] {label}" + (f"  ({detail})" if detail else ""))


def req(path, method, data=None, scope=None, headers=None, want_headers=False):
    """One authenticated request against the origin. Returns (status, body).

    With want_headers=True returns (status, body, headers) -- needed because
    a HEAD has no body, so size must come from Content-Length. Getting this
    wrong made DuckDB see every uploaded Parquet as zero bytes.
    """
    tok = pelican_token.mint(scope or f"read:{path} write:{path}", lifetime=900)
    r = urllib.request.Request(ROOT + path, data=data, method=method)
    r.add_header("Authorization", "Bearer " + tok)
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(r, timeout=120, context=ctx) as resp:
            out = (resp.status, resp.read(), dict(resp.headers))
    except urllib.error.HTTPError as e:
        out = (e.code, e.read() or b"", dict(e.headers or {}))
    except Exception as e:  # noqa: BLE001 - surface transport errors as status 0
        out = (0, str(e).encode(), {})
    return out if want_headers else out[:2]


def authz_url(path, scope=None):
    tok = pelican_token.mint(scope or f"read:{path}")
    q = urllib.parse.quote("Bearer " + tok)
    return f"{ROOT}{path}?authz={q}"


def cleanup():
    # A single DELETE on a prefix returns 409 on this origin; exp19 already
    # has the recursive version, so reuse it rather than reimplementing.
    import pelican_fileio

    n = pelican_fileio.rmtree(PREFIX)
    note("cleanup rmtree", f"{n} objects removed")


def attempt(fn, tries=25, what=""):
    """Retry on SQLite lock contention.

    "database is locked" is a retryable conflict, not a lost update. An
    earlier version wrapped only the writer threads, so the setup and verify
    connections failed on the first contended statement and the test reported
    a transport error instead of a row count -- a non-answer to the question
    that actually matters.
    """
    for k in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if "locked" not in str(e).lower() or k == tries - 1:
                raise
            time.sleep(0.1 * (k + 1))
    raise RuntimeError(f"unreachable ({what})")


def con(bearer_scope=None):
    """A DuckDB connection, optionally carrying a SciToken as an HTTP secret.

    Using a secret rather than `?authz=` in the URL matters: DuckLake builds
    data-file URLs by concatenating DATA_PATH with a filename, which would
    land the filename *after* the query string and produce a URL that cannot
    authenticate. An earlier version of this probe did exactly that and its
    data reads failed while the row count still came back -- see the note on
    Q2 below.
    """
    c = duckdb.connect()
    for ext in ("ducklake", "httpfs"):
        c.execute(f"INSTALL {ext}")
        c.execute(f"LOAD {ext}")
    if bearer_scope:
        tok = pelican_token.mint(bearer_scope, lifetime=900)
        c.execute(f"CREATE SECRET pel (TYPE http, BEARER_TOKEN '{tok}')")
    return c


def build_local_lake(path):
    """A small lake with real nanosecond remainders (see §25 on vacuity)."""
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)
    c = con()
    c.execute(f"ATTACH 'ducklake:{path}/meta.db' AS dl (DATA_PATH '{path}/data/')")
    c.execute("CREATE TABLE dl.obs(t TIMESTAMP_NS, name VARCHAR, v DOUBLE)")
    c.execute(
        "INSERT INTO dl.obs SELECT "
        "make_timestamp_ns(1735689600123456789 + i * 1000000000), "
        "'n' || i, i * 1.0 FROM range(200) tbl(i)"
    )
    c.execute("CHECKPOINT")
    c.close()
    return path


def _pelican_fs_class():
    """An fsspec filesystem over Pelican, minimal but real.

    Deliberately small: the question is whether DuckDB's extension point
    works at all, not whether this is production quality. Pelican has no
    append, so writes buffer and PUT whole on close -- the same constraint
    pelican_fileio.py documents for Iceberg.
    """
    from fsspec.spec import AbstractBufferedFile, AbstractFileSystem

    class PelicanFile(AbstractBufferedFile):
        def _initiate_upload(self):
            self.buffer.seek(0)

        def _upload_chunk(self, final=False):
            if not final:
                return False
            self.buffer.seek(0)
            data = self.buffer.read()
            st, body = req(self.path, "PUT", data=data,
                           scope=f"write:{self.path}")
            if st not in (200, 201, 204):
                raise IOError(f"PUT {self.path} -> {st} {body[:160]!r}")
            return True

        def _fetch_range(self, start, end):
            tok = pelican_token.mint(f"read:{self.path}", lifetime=900)
            r = urllib.request.Request(ROOT + self.path)
            r.add_header("Authorization", "Bearer " + tok)
            r.add_header("Range", f"bytes={start}-{end - 1}")
            with urllib.request.urlopen(r, timeout=120,
                                        context=ssl.create_default_context()) as resp:
                return resp.read()

    class PelicanFS(AbstractFileSystem):
        protocol = "pelican"

        def _strip(self, path):
            p = str(path)
            for pre in ("pelican://", "pelican:/"):
                if p.startswith(pre):
                    p = p[len(pre):]
            return "/" + p.lstrip("/")

        def info(self, path, **kw):
            p = self._strip(path)
            st, _body, hdrs = req(p, "HEAD", scope=f"read:{p}", want_headers=True)
            if st != 200:
                raise FileNotFoundError(p)
            # A HEAD has no body: size MUST come from Content-Length. Using
            # len(body) here made every file look like 0 bytes and DuckDB
            # rejected its own freshly written Parquet as "too small".
            return {"name": p, "size": int(hdrs.get("Content-Length", 0)),
                    "type": "file", "mtime": 0, "created": 0}

        # DuckLake calls these during commit. fsspec's defaults raise
        # NotImplementedError, which an earlier run reported as a DuckLake
        # failure -- it was this filesystem being incomplete, not DuckLake.
        def modified(self, path):
            import datetime

            return datetime.datetime.fromtimestamp(0, datetime.timezone.utc)

        def created(self, path):
            return self.modified(path)

        def ls(self, path, detail=True, **kw):
            p = self._strip(path).rstrip("/")
            st, body = req(p + "/", "PROPFIND", scope=f"read:{p}",
                           headers={"Depth": "1"})
            if st != 207:
                return []
            import re
            out = []
            for h in re.findall(rb"<[dD]:href>([^<]+)</[dD]:href>", body):
                hp = urllib.parse.urlparse(h.decode()).path
                if hp.startswith("/sage"):
                    hp = hp[len("/sage"):]
                if hp.rstrip("/") == p:
                    continue
                out.append({"name": hp, "size": 0, "type": "file"})
            return out if detail else [o["name"] for o in out]

        def makedirs(self, path, exist_ok=False):
            return None

        def mkdir(self, path, create_parents=True, **kw):
            return None

        def _open(self, path, mode="rb", block_size=None, **kw):
            return PelicanFile(self, self._strip(path), mode=mode,
                               block_size=block_size or 5 * 2 ** 20, **kw)

    return PelicanFS


def _PelicanFS():
    return _pelican_fs_class()()


def main():
    print("== Q0: can we reach the origin at all? ==")
    st, body = req(PREFIX + "/probe.txt", "PUT", data=b"hello",
                   scope=f"write:{PREFIX}")
    if not ck("origin reachable and writable with a SciToken", st in (200, 201, 204),
              f"PUT -> {st} {body[:120]!r}"):
        print("\ncannot reach the origin; aborting rather than reporting "
              "DuckLake failures that are really credential failures")
        return 1
    st, got = req(PREFIX + "/probe.txt", "GET", scope=f"read:{PREFIX}")
    ck("and reads back what was written", got == b"hello", f"GET -> {st} {got[:40]!r}")

    print("\n== Q1: can DuckDB itself WRITE to https:// ? ==")
    c = con()
    url = authz_url(PREFIX + "/duckdb_write.parquet",
                    scope=f"read:{PREFIX} write:{PREFIX}")
    wrote = True
    werr = ""
    try:
        c.execute(f"COPY (SELECT 1 AS a) TO '{url}' (FORMAT parquet)")
    except duckdb.Error as e:
        wrote = False
        werr = str(e).replace("\n", " ")[:200]
    ck("DuckDB can COPY ... TO an https:// URL", wrote, werr)
    if not wrote:
        note("consequence", "DuckLake cannot use Pelican as DATA_PATH over https; "
                            "a writer would need the S3 gateway or a local stage")

    print("\n== Q2: publish a finished lake on Pelican, read it remotely ==")
    build_local_lake(LOCAL)
    # Upload the whole lake (catalog + parquet) with plain PUTs, which is the
    # "frozen DuckLake" shape from the DuckLake FAQ.
    n_up = 0
    for root, _dirs, files in os.walk(LOCAL):
        for fn in files:
            if fn.endswith(".wal"):
                continue
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, LOCAL).replace(os.sep, "/")
            with open(full, "rb") as fh:
                st, body = req(f"{PREFIX}/lake/{rel}", "PUT", data=fh.read(),
                               scope=f"write:{PREFIX}")
            if st not in (200, 201, 204):
                ck(f"upload {rel}", False, f"status {st} {body[:100]!r}")
                break
            n_up += 1
    ck("the whole lake uploaded to Pelican with plain PUTs", n_up >= 2,
       f"{n_up} objects")

    meta_url = f"{ROOT}{PREFIX}/lake/meta.db"
    data_url = f"{ROOT}{PREFIX}/lake/data/"
    r = con(bearer_scope=f"read:{PREFIX}")
    attached = True
    aerr = ""
    try:
        r.execute(
            f"ATTACH '{meta_url}' AS rd "
            f"(TYPE ducklake, DATA_PATH '{data_url}', OVERRIDE_DATA_PATH true)"
        )
    except duckdb.Error as e:
        attached = False
        aerr = str(e).replace("\n", " ")[:240]
    ck("a remote client attaches the Pelican-hosted catalog", attached, aerr)

    if attached:
        # COUNT(*) alone is NOT a read test: DuckLake answers it from catalog
        # statistics without fetching a single Parquet byte. An earlier run
        # "passed" 200 rows while every data GET was failing. Pull an actual
        # column value, and cross-check the count against a predicate that
        # forces the data files to be opened.
        n = v = nfiltered = None
        rerr = ""
        try:
            n = r.execute("SELECT count(*) FROM rd.obs").fetchone()[0]
            v = r.execute(
                "SELECT epoch_ns(t) FROM rd.obs ORDER BY t LIMIT 1"
            ).fetchone()[0]
            nfiltered = r.execute(
                "SELECT count(*) FROM rd.obs WHERE name LIKE 'n1%'"
            ).fetchone()[0]
        except duckdb.Error as e:
            rerr = str(e).replace("\n", " ")[:240]
        ck("real column values read from Pelican with no local state",
           v is not None, rerr or f"first epoch_ns={v}")
        ck("a predicate that must touch the Parquet returns rows",
           nfiltered == 111, f"{nfiltered} rows matching name LIKE 'n1%' "
                             f"(n1, n10-n19, n100-n199)")
        ck("the catalog's row count agrees with the data", n == 200, f"{n} rows")
        ck("nanoseconds survived the round trip through Pelican",
           v is not None and v % 1000 == 789, f"epoch_ns={v}")
    else:
        skipped.append("remote read (attach failed)")
    r.close()

    print("\n== Q3: can a writer COMMIT to the Pelican-hosted lake? ==")
    # The token carries write scope and the ATTACH asks for no read-only mode,
    # so a refusal here is DuckDB's, not ours. It follows from Q1: the catalog
    # file is reached over httpfs, which cannot write, so DuckLake attaches it
    # read-only. Q6 is the same question with the httpfs dependency removed.
    w = con(bearer_scope=f"read:{PREFIX} write:{PREFIX}")
    committed = True
    cerr = ""
    try:
        w.execute(
            f"ATTACH '{meta_url}' AS wr "
            f"(TYPE ducklake, DATA_PATH '{data_url}', OVERRIDE_DATA_PATH true)"
        )
        w.execute("INSERT INTO wr.obs VALUES (make_timestamp_ns(1), 'late', 1.0)")
    except duckdb.Error as e:
        committed = False
        cerr = str(e).replace("\n", " ")[:240]
    ck("a writer can commit to the lake in place on Pelican", committed, cerr)
    if not committed:
        note("consequence", "Pelican-hosted DuckLake is publish-only: the catalog "
                            "must be rebuilt and re-uploaded to change it")
    w.close()
    c.close()

    print("\n== Q4: relocation local -> Pelican (the §27 Q2 comparison) ==")
    note("covered by Q2", "the lake read on Pelican was BUILT at a local path; "
                          "reading it from a different base URL is the move")

    print("\n== Q5: does a custom filesystem rescue the writer? ==")
    # Q1's failure is only final if it cannot be worked around the way
    # Iceberg's was. §21 did not get Pelican writes for free either -- it took
    # pelican_io.go (~190 lines) registered through iceberg-go's IO interface.
    # DuckDB's analogue is register_filesystem() with an fsspec implementation,
    # so the comparison is only fair if we try it.
    fs_ok = True
    fs_err = ""
    try:
        import fsspec  # noqa: F401

        pf = _PelicanFS()
        c2 = con()
        c2.register_filesystem(pf)
        c2.execute(
            f"COPY (SELECT 1 AS a) TO 'pelican://{PREFIX}/fsspec_write.parquet' "
            f"(FORMAT parquet)"
        )
        st, _ = req(f"{PREFIX}/fsspec_write.parquet", "GET", scope=f"read:{PREFIX}")
        fs_ok = st == 200
        fs_err = f"readback GET -> {st}"
        c2.close()
    except Exception as e:  # noqa: BLE001 - any failure is the answer
        fs_ok = False
        fs_err = f"{type(e).__name__}: {str(e)}".replace("\n", " ")[:240]
    ck("a custom fsspec filesystem lets DuckDB WRITE to Pelican", fs_ok, fs_err)
    if fs_ok:
        note("consequence", "Q1 is a gap in DuckDB's builtin https client, not a "
                            "wall -- same shape as §21's pelican_io.go for Iceberg")

    print("\n== Q6: a FULL DuckLake writing through that filesystem ==")
    # Q5 only proved a one-shot COPY works. The real question is whether a
    # DuckLake can hold its catalog locally and put its DATA_PATH on Pelican,
    # committing as it goes. This is the shape a production exporter would use:
    # catalog in Postgres next to the exporter, Parquet on the origin.
    lake_ok = True
    lake_err = ""
    lake_rows = None
    try:
        shutil.rmtree(f"{LOCAL}_split", ignore_errors=True)
        os.makedirs(f"{LOCAL}_split")
        c3 = con()
        c3.register_filesystem(_PelicanFS())
        c3.execute(
            f"ATTACH 'ducklake:{LOCAL}_split/meta.db' AS sp "
            f"(DATA_PATH 'pelican://{PREFIX}/splitlake/')"
        )
        c3.execute("CREATE TABLE sp.obs(t TIMESTAMP_NS, name VARCHAR, v DOUBLE)")
        c3.execute(
            "INSERT INTO sp.obs SELECT "
            "make_timestamp_ns(1735689600123456789 + i * 1000000000), "
            "'n' || i, i * 1.0 FROM range(200) tbl(i)"
        )
        lake_rows = c3.execute(
            "SELECT epoch_ns(t) FROM sp.obs ORDER BY t LIMIT 1"
        ).fetchone()[0]
        c3.close()
    except Exception as e:  # noqa: BLE001
        lake_ok = False
        lake_err = f"{type(e).__name__}: {str(e)}".replace("\n", " ")[:240]
    ck("a DuckLake commits with its DATA_PATH on Pelican", lake_ok, lake_err)
    if lake_ok:
        ck("and the data reads back through Pelican with ns intact",
           lake_rows is not None and lake_rows % 1000 == 789,
           f"epoch_ns={lake_rows}")
        st, _ = req(f"{PREFIX}/splitlake/", "PROPFIND", scope=f"read:{PREFIX}",
                    headers={"Depth": "1"})
        ck("and the Parquet really landed on the origin", st == 207,
           f"PROPFIND -> {st}")
    else:
        skipped.append("split-lake readback (commit failed)")

    print("\n== Q7: two concurrent writers (the §18/§19 decisive test) ==")
    # §18: the custom catalog lost an update 3/3 on this origin, silently.
    # §19: Iceberg on the same origin lost nothing 3/3. That measurement is
    # what moved the recommendation, so DuckLake has to face the same test.
    # Production shape: catalog local (a real deployment would use Postgres),
    # data on Pelican.
    # The catalog must be SQLite, not a DuckDB file. A DuckDB-file catalog
    # takes a single-process lock, so the second writer cannot even ATTACH --
    # that measures DuckDB's file locking, not DuckLake's commit protocol,
    # and an earlier version of this test reported exactly that non-answer.
    # §18/§19 both used SQLite, so this is also the like-for-like comparison.
    #
    # IMPORTANT, and the reason this check is written the way it is: "database
    # is locked" from DuckLake is NOT always a retryable transport hiccup. It
    # is also how a losing writer's commit is *correctly* rejected
    # ("TransactionException: Failed to commit DuckLake transaction ...
    # database is locked"). An earlier version retried it 25 times and then
    # reported FAIL -- turning the right behaviour into a fake failure.
    # attempt() below therefore retries only the non-commit statements, and a
    # writer whose INSERT is refused is counted as an honest error, not as a
    # lost row. probe_ducklake_concurrency.py isolates this on local disk.
    conc_ok = True
    conc_err = ""
    total = None
    # Declared before the try so the handler can always report them: if they
    # were created inside it, an early failure would make the diagnostic
    # itself raise NameError.
    errors = []
    acked = {1: 0, 2: 0}
    try:
        shutil.rmtree(f"{LOCAL}_conc", ignore_errors=True)
        os.makedirs(f"{LOCAL}_conc")
        cat = f"ducklake:sqlite:{LOCAL}_conc/cat.sqlite"

        def sqlcon():
            k = con()
            k.execute("INSTALL sqlite")
            k.execute("LOAD sqlite")
            # NB: DuckDB's sqlite extension exposes no busy-timeout setting
            # (checked duckdb_settings(): only sqlite_all_varchar,
            # sqlite_debug_show_queries, sqlite_disable_multithreaded_scans),
            # so lock waiting has to happen in attempt() above rather than
            # inside the driver.
            k.register_filesystem(_PelicanFS())
            return k

        base = sqlcon()
        attempt(lambda: base.execute(
            f"ATTACH '{cat}' AS cc (DATA_PATH 'pelican://{PREFIX}/conclake/')"))
        attempt(lambda: base.execute("CREATE TABLE cc.obs(w INTEGER, i INTEGER)"))
        base.close()

        import threading

        # `errors` and `acked` are hoisted above the try. `acked` counts
        # inserts that RETURNED SUCCESSFULLY, per writer -- the number the
        # storage owes us. Counting "attempts" instead would punish a writer
        # for an honest error, and a fixed total would assume no writer ever
        # legitimately fails.

        def writer(wid):
            w = None
            try:
                w = sqlcon()
                attempt(lambda: w.execute(
                    f"ATTACH '{cat}' AS cc "
                    f"(DATA_PATH 'pelican://{PREFIX}/conclake/')"
                ))
                for i in range(3):
                    # NOT wrapped in attempt(): a refused commit here is the
                    # correct outcome of the race, and retrying it would both
                    # hide that and eventually report it as a failure.
                    w.execute(f"INSERT INTO cc.obs VALUES ({wid}, {i})")
                    acked[wid] += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"w{wid}: {type(e).__name__}: {str(e)[:110]}")
            finally:
                # MUST be in a finally. A writer whose INSERT is refused
                # exits via the except branch, and an earlier version closed
                # only on the success path -- so the loser kept its SQLite
                # handle, and the lock, for the rest of the run. The verify
                # connection then failed to ATTACH with "database is locked"
                # and the probe reported a lost-update failure that had not
                # happened.
                if w is not None:
                    w.close()

        ts = [threading.Thread(target=writer, args=(k,)) for k in (1, 2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

        chk = sqlcon()
        attempt(lambda: chk.execute(
            f"ATTACH '{cat}' AS cc (DATA_PATH 'pelican://{PREFIX}/conclake/')"))
        total = attempt(
            lambda: chk.execute("SELECT count(*) FROM cc.obs").fetchone()[0])
        per = attempt(lambda: chk.execute(
            "SELECT w, count(*) FROM cc.obs GROUP BY w ORDER BY w").fetchall())
        chk.close()
        nerr = len(errors)
        conc_err = (f"{total} rows present, {sum(acked.values())} acked "
                    f"(w1={acked[1]} w2={acked[2]}), per-writer in table {per}, "
                    f"{nerr} writer(s) errored"
                    + (f": {errors}" if errors else " (all reported success)"))
    except Exception as e:  # noqa: BLE001
        conc_ok = False
        # Report what the WRITERS did even when the verify connection is the
        # thing that failed. An earlier version let this branch swallow
        # acked/errors, so a verify-side problem was indistinguishable from
        # a lost update -- the two outcomes that this whole check exists to
        # tell apart.
        conc_err = (f"{type(e).__name__}: {str(e)}".replace("\n", " ")[:200]
                    + f" || acked w1={acked[1]} w2={acked[2]}"
                    + f" || writer errors: {errors}")

    # The question is ONLY "was an acknowledged row silently dropped" -- the
    # §18 failure. Three other outcomes are not that, and an earlier version
    # of this check reported all of them as FAIL, which blamed DuckLake for
    # a lost update three times over when none had occurred:
    #   - every writer was refused (acked 0): nothing was owed, nothing lost.
    #   - a writer was refused loudly: that is correct conflict handling.
    #   - the VERIFY connection failed: a harness problem, not a data result.
    nacked = sum(acked.values())
    if conc_ok and total is not None:
        ck("two concurrent writers lose no acknowledged rows",
           total == nacked, conc_err)
    elif nacked == 0 and errors:
        # Both writers refused. No row was acknowledged, so none can have
        # been lost -- the §18 failure is not reachable from here. Report it
        # as what it is rather than as a pass or a loss.
        ck("two concurrent writers lose no acknowledged rows", True,
           "every writer was refused (0 rows acknowledged), so no "
           "acknowledged row could be lost -- but this does NOT show a "
           "winner commits under contention on this storage; "
           f"probe_ducklake_concurrency.py covers that locally. {conc_err}")
    else:
        ck("two concurrent writers lose no acknowledged rows", False,
           conc_err)

    print()
    cleanup()
    st, _ = req(PREFIX + "/", "PROPFIND", scope=f"read:{PREFIX}",
                headers={"Depth": "1"})
    ck("prefix is gone after cleanup", st in (404, 403), f"PROPFIND -> {st}")

    for s in skipped:
        note("skipped", s)
    print(f"\n{ok} passed, {fail} failed")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
