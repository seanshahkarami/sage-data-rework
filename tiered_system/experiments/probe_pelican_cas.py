#!/usr/bin/env python3
"""Is there ANY compare-and-swap primitive on this origin? (§31)

§24 concluded "no database-free Iceberg commit can be made safe on this
storage" after testing exactly two primitives: MOVE with `Overwrite: F`
and PUT with `If-None-Match: *`. That conclusion is load-bearing -- it is
the single surviving objection to Iceberg in
plans/4_catalog_recommendation.md, and it is why SAGE must host a catalog
database at all. Two probes is thin evidence for a negative that large,
so this sweeps the rest of the conditional surface.

What a filesystem catalog actually needs is one operation with the shape
"publish X only if the current state is still Y". Anything that can
express that is enough; it does not have to be MOVE. So:

  A. PUT      If-None-Match: *          (create-only)       -- §24 retested
  B. MOVE     Overwrite: F              (no-clobber rename) -- §24 retested
  C. PUT      If-Match: <etag>          (replace-if-unchanged)
  D. PUT      If-Match: "bogus"         (must fail; proves C is honoured)
  E. COPY     Overwrite: F              (no-clobber copy)
  F. LOCK/UNLOCK                        (WebDAV mutual exclusion)
  G. MKCOL on an existing collection    (dir create as a mutex)

C is the interesting one and the reason this probe exists. ETag-based
conditional replace is how S3 and most HTTP origins express CAS today,
`version-hint.text` is a single small file, and nothing in §24 tested it.
D is its control: if a conditional PUT with a *wrong* ETag also succeeds,
then C succeeding proves nothing -- the header is being ignored rather
than honoured. A probe without D could report a working CAS that does not
exist, which is the §19 near-miss shape (a check that cannot fail is not
a check).

G is included because `MKCOL` on an existing collection is required by
RFC 4918 to return 405, and a reliable 405 is a mutex even if every other
primitive is ignored.

Writes under /sage/cold-storage-test-cas/ -- its own prefix, distinct from
every other experiment -- and removes it afterwards. Never touches
/sage/node-data or the sage-storage-loader paths.
"""
import os
import ssl
import sys
import urllib.error
import urllib.request

# -I strips the script directory from sys.path; restore it, as exp17 does.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pelican_token  # noqa: E402

PREFIX = "/cold-storage-test-cas"
ROOT = pelican_token.ROOT_URL

ok = 0
fail = 0
findings = []


def ck(label, passed, detail=""):
    global ok, fail
    if passed:
        ok += 1
    else:
        fail += 1
    print(f"[{'PASS' if passed else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""))


def note(label, detail=""):
    print(f"[ -- ] {label}" + (f"  ({detail})" if detail else ""))


def req(path, method, data=None, scope=None, headers=None):
    """Returns (status, body, headers)."""
    tok = pelican_token.mint(scope or f"read:{path} write:{path}", lifetime=900)
    r = urllib.request.Request(ROOT + path, data=data, method=method)
    r.add_header("Authorization", "Bearer " + tok)
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(r, timeout=120, context=ctx) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, (e.read() or b""), dict(e.headers or {})
    except Exception as e:  # noqa: BLE001
        return 0, str(e).encode(), {}


def put(path, body, scope_root=PREFIX):
    return req(path, "PUT", data=body,
               scope=f"read:{scope_root} write:{scope_root}")


def get(path):
    return req(path, "GET", scope=f"read:{PREFIX}")


def rmtree(prefix):
    """Recursive delete -- a single DELETE on a prefix 409s on this origin."""
    st, body, _ = req(prefix + "/", "PROPFIND", scope=f"read:{prefix}",
                      headers={"Depth": "1"})
    n = 0
    if st == 207:
        import re
        for href in re.findall(r"<[^>]*href>([^<]+)</", body.decode("utf-8", "replace")):
            name = urllib.parse.unquote(href).rstrip("/").split("/")[-1]
            if not name or name == prefix.strip("/").split("/")[-1]:
                continue
            s, _, _ = req(f"{prefix}/{name}", "DELETE",
                          scope=f"read:{prefix} write:{prefix}")
            if s in (200, 204, 404):
                n += 1
    req(prefix, "DELETE", scope=f"read:{prefix} write:{prefix}")
    return n


import urllib.parse  # noqa: E402  (used by rmtree)


def main():
    print("== setup ==")
    st, _, _ = put(f"{PREFIX}/probe.txt", b"hello")
    ck("origin reachable and writable", st in (200, 201, 204), f"PUT -> {st}")
    if st not in (200, 201, 204):
        return 1

    # ---------------------------------------------------------------- A
    print("\n== A: PUT If-None-Match: * (create-only) ==")
    tgt = f"{PREFIX}/a_target.txt"
    put(tgt, b"COMMITTED")
    st, _, _ = req(tgt, "PUT", data=b"OVERWRITTEN",
                   scope=f"read:{PREFIX} write:{PREFIX}",
                   headers={"If-None-Match": "*"})
    _, body, _ = get(tgt)
    honoured = st == 412 and body == b"COMMITTED"
    ck("PUT If-None-Match:* refuses to clobber", honoured,
       f"status {st}, file now {body!r}")
    if honoured:
        findings.append("PUT If-None-Match:*")

    # ---------------------------------------------------------------- B
    print("\n== B: MOVE Overwrite: F (no-clobber rename) ==")
    src, dst = f"{PREFIX}/b_src.txt", f"{PREFIX}/b_dst.txt"
    put(src, b"NEW")
    put(dst, b"COMMITTED")
    st, _, _ = req(src, "MOVE", scope=f"read:{PREFIX} write:{PREFIX}",
                   headers={"Destination": ROOT + dst, "Overwrite": "F"})
    _, body, _ = get(dst)
    honoured = st in (412, 405) and body == b"COMMITTED"
    ck("MOVE Overwrite:F refuses to clobber", honoured,
       f"status {st}, dst now {body!r}")
    if honoured:
        findings.append("MOVE Overwrite:F")

    # ---------------------------------------------------------------- C/D
    print("\n== C: PUT If-Match: <etag> (replace-if-unchanged) ==")
    # The primitive §24 never tried, and the one most origins actually
    # implement. Needs an ETag to exist at all first.
    etg = f"{PREFIX}/c_target.txt"
    put(etg, b"V1")
    _, _, hdrs = req(etg, "HEAD", scope=f"read:{PREFIX}")
    etag = hdrs.get("ETag") or hdrs.get("Etag")
    note("ETag advertised by the origin", repr(etag))

    if not etag:
        note("skipped", "no ETag header, so If-Match cannot be expressed "
                        "-- this is itself the finding")
        ck("PUT If-Match is usable as CAS", False, "origin advertises no ETag")
    else:
        # D FIRST, deliberately: the control runs before the real test.
        # If a WRONG etag is also accepted, then C passing means the header
        # is ignored, not honoured -- and reporting C as a working CAS
        # would be a false positive of exactly the kind §19 nearly made.
        st_bogus, _, _ = req(etg, "PUT", data=b"BOGUS",
                             scope=f"read:{PREFIX} write:{PREFIX}",
                             headers={"If-Match": '"definitely-not-the-etag"'})
        _, body_bogus, _ = get(etg)
        control = st_bogus == 412 and body_bogus == b"V1"
        ck("control: PUT If-Match with a WRONG etag is refused", control,
           f"status {st_bogus}, file now {body_bogus!r}"
           + ("" if control else "  <-- header ignored; C below proves nothing"))

        st_good, _, _ = req(etg, "PUT", data=b"V2",
                            scope=f"read:{PREFIX} write:{PREFIX}",
                            headers={"If-Match": etag})
        _, body_good, _ = get(etg)
        accepted = st_good in (200, 201, 204) and body_good == b"V2"
        ck("PUT If-Match with the CORRECT etag succeeds", accepted,
           f"status {st_good}, file now {body_good!r}")

        if control and accepted:
            findings.append("PUT If-Match:<etag>")

    # ---------------------------------------------------------------- E
    print("\n== E: COPY Overwrite: F ==")
    csrc, cdst = f"{PREFIX}/e_src.txt", f"{PREFIX}/e_dst.txt"
    put(csrc, b"NEW")
    put(cdst, b"COMMITTED")
    st, _, _ = req(csrc, "COPY", scope=f"read:{PREFIX} write:{PREFIX}",
                   headers={"Destination": ROOT + cdst, "Overwrite": "F"})
    _, body, _ = get(cdst)
    honoured = st in (412, 405) and body == b"COMMITTED"
    ck("COPY Overwrite:F refuses to clobber", honoured,
       f"status {st}, dst now {body!r}")
    if honoured:
        findings.append("COPY Overwrite:F")

    # ---------------------------------------------------------------- F
    print("\n== F: WebDAV LOCK ==")
    lk = f"{PREFIX}/f_target.txt"
    put(lk, b"LOCKME")
    lockbody = (b'<?xml version="1.0" encoding="utf-8"?>'
                b'<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/>'
                b'</D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>')
    st, body, hdrs = req(lk, "LOCK", data=lockbody,
                         scope=f"read:{PREFIX} write:{PREFIX}",
                         headers={"Content-Type": "application/xml",
                                  "Timeout": "Second-30"})
    got_lock = st == 200 and (hdrs.get("Lock-Token") or b"locktoken" in body.lower())
    ck("LOCK grants an exclusive write lock", got_lock,
       f"status {st}, Lock-Token={hdrs.get('Lock-Token')!r}")
    if got_lock:
        findings.append("WebDAV LOCK")

    # ---------------------------------------------------------------- G
    print("\n== G: MKCOL on an existing collection (405 as a mutex) ==")
    # RFC 4918: MKCOL on an existing resource MUST fail (405). A reliable
    # refusal is a usable mutex even when every other primitive is ignored.
    coll = f"{PREFIX}/g_coll"
    st1, _, _ = req(coll, "MKCOL", scope=f"read:{PREFIX} write:{PREFIX}")
    st2, _, _ = req(coll, "MKCOL", scope=f"read:{PREFIX} write:{PREFIX}")
    honoured = st1 in (200, 201) and st2 in (405, 409, 412)
    ck("MKCOL twice: second is refused", honoured,
       f"first -> {st1}, second -> {st2}")
    if honoured:
        findings.append("MKCOL (405 on existing)")

    # ---------------------------------------------------------------- verdict
    print("\n== verdict ==")
    if findings:
        note("USABLE CAS PRIMITIVES FOUND", ", ".join(findings))
        note("consequence", "§24's 'no database-free Iceberg commit can be "
                            "made safe on this storage' needs revisiting -- "
                            "a filesystem catalog could be built on this")
    else:
        note("no usable primitive", "§24 stands: every conditional operation "
                                    "this origin exposes is ignored, so a "
                                    "database-free commit cannot be made safe")

    print()
    n = rmtree(PREFIX)
    note("cleanup rmtree", f"{n} objects removed")
    st, _, _ = req(PREFIX + "/", "PROPFIND", scope=f"read:{PREFIX}",
                   headers={"Depth": "1"})
    ck("prefix is gone after cleanup", st in (404, 403), f"PROPFIND -> {st}")

    print(f"\n{ok} passed, {fail} failed")
    # NB: a "FAIL" here means the primitive is absent, which is information,
    # not a broken test. The verdict line above is what matters.
    return 0


if __name__ == "__main__":
    sys.exit(main())
