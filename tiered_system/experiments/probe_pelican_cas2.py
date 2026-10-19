#!/usr/bin/env python3
"""Round 2: are the round-1 negatives real refusals or just a 201-for-everything origin? (§31b)

Round 1 (probe_pelican_cas.py) found no usable CAS primitive, but two of
its results do not mean what a casual reading suggests:

  COPY Overwrite:F -> 201, yet the destination still held its OLD bytes
  LOCK             -> 200, yet no Lock-Token header came back

A 201 on a copy that did not copy is not "the conditional was ignored";
it is "this method is not implemented and the origin acked anyway". That
is a different claim, and §24's conclusion is load-bearing enough that
the difference belongs in the record. The failure mode to avoid is
reporting "the origin evaluated our conditional and overwrote anyway"
when the truth is "the origin never parsed the request at all".

So, controls:

  1. Does COPY work AT ALL (to a free destination)?
  2. Does a second exclusive LOCK on an already-locked resource also
     return 200? A lock that never refuses is not a lock.
  3. Does a nonsense method get 201 too? If so, 201 is the origin's
     default ack and carries no information whatsoever.
  4. Is there a Last-Modified to hang If-Unmodified-Since on, since
     there is no ETag for If-Match?
  5. The WebDAV `If:` header (RFC 4918 10.4) -- the one remaining
     conditional form, untried in round 1.

Same prefix discipline: /sage/cold-storage-test-cas2/, removed after.
"""
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pelican_token  # noqa: E402

PREFIX = "/cold-storage-test-cas2"
ROOT = pelican_token.ROOT_URL


def note(label, detail=""):
    print(f"[ -- ] {label}" + (f"  ({detail})" if detail else ""))


def req(path, method, data=None, scope=None, headers=None):
    tok = pelican_token.mint(scope or f"read:{PREFIX} write:{PREFIX}", lifetime=900)
    r = urllib.request.Request(ROOT + path, data=data, method=method)
    r.add_header("Authorization", "Bearer " + tok)
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    try:
        with urllib.request.urlopen(r, timeout=120,
                                    context=ssl.create_default_context()) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, (e.read() or b""), dict(e.headers or {})
    except Exception as e:  # noqa: BLE001
        return 0, str(e).encode(), {}


def put(path, body):
    return req(path, "PUT", data=body)


def get(path):
    return req(path, "GET")


def rmtree(prefix):
    st, body, _ = req(prefix + "/", "PROPFIND", headers={"Depth": "1"})
    n = 0
    if st == 207:
        for href in re.findall(r"<[^>]*href>([^<]+)</", body.decode("utf-8", "replace")):
            name = urllib.parse.unquote(href).rstrip("/").split("/")[-1]
            if not name or name == prefix.strip("/").split("/")[-1]:
                continue
            s, _, _ = req(f"{prefix}/{name}", "DELETE")
            if s in (200, 204, 404):
                n += 1
    req(prefix, "DELETE")
    return n


def main():
    put(f"{PREFIX}/probe.txt", b"hello")

    # -- 1. Does COPY do anything at all? --------------------------------
    print("== 1: COPY control (free destination) ==")
    src, free = f"{PREFIX}/c_src.txt", f"{PREFIX}/c_free.txt"
    put(src, b"PAYLOAD")
    st, _, _ = req(src, "COPY", headers={"Destination": ROOT + free})
    gst, body, _ = get(free)
    note("COPY to a FREE destination", f"status {st}, GET dst -> {gst} {body!r}")
    if body == b"PAYLOAD":
        note("=> COPY is implemented", "so round-1's Overwrite:F result was a "
                                       "real ignore of the conditional")
    else:
        note("=> COPY IS A NO-OP", "it returns 201 without copying. Round 1's "
                                   "'COPY Overwrite:F' line says nothing about "
                                   "conditionals -- the method does not work.")

    # -- 2. Does LOCK ever refuse? ---------------------------------------
    print("\n== 2: LOCK control (double lock) ==")
    lk = f"{PREFIX}/l_target.txt"
    put(lk, b"LOCKME")
    lockbody = (b'<?xml version="1.0" encoding="utf-8"?>'
                b'<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/>'
                b'</D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>')
    h = {"Content-Type": "application/xml", "Timeout": "Second-300"}
    st1, b1, hd1 = req(lk, "LOCK", data=lockbody, headers=h)
    st2, b2, hd2 = req(lk, "LOCK", data=lockbody, headers=h)
    note("first  LOCK", f"status {st1}, Lock-Token={hd1.get('Lock-Token')!r}, "
                        f"body[:120]={b1[:120]!r}")
    note("second LOCK", f"status {st2}, Lock-Token={hd2.get('Lock-Token')!r}")
    if st2 == 423:
        note("=> LOCK IS REAL", "423 Locked on the second -- this is a usable mutex!")
    else:
        note("=> LOCK IS FAKE", "the second exclusive lock was granted too, so "
                                "LOCK provides no mutual exclusion")

    # -- 3. Is 201 just the default ack? ---------------------------------
    print("\n== 3: does a NONSENSE method also get acked? ==")
    st, _, _ = req(f"{PREFIX}/probe.txt", "FLOOBLE")
    note("FLOOBLE /probe.txt", f"status {st}")
    if st in (200, 201, 204):
        note("=> 201 CARRIES NO INFORMATION", "the origin acks methods it does "
                                              "not implement, so a 201 on any "
                                              "conditional means 'unparsed', "
                                              "not 'evaluated and allowed'")
    else:
        note("=> unknown methods are rejected", f"{st} -- so a 201 elsewhere "
                                                "does mean the request was handled")

    # -- 4. Last-Modified, for If-Unmodified-Since -----------------------
    print("\n== 4: If-Unmodified-Since (no ETag exists, so this is the fallback) ==")
    us = f"{PREFIX}/u_target.txt"
    put(us, b"V1")
    _, _, hd = req(us, "HEAD")
    lm = hd.get("Last-Modified")
    note("Last-Modified advertised", repr(lm))
    if lm:
        # A date far in the PAST means "only if unmodified since then", which
        # must fail for a file modified now. If it succeeds, header ignored.
        st, _, _ = req(us, "PUT", data=b"V2",
                       headers={"If-Unmodified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"})
        _, body, _ = get(us)
        note("PUT If-Unmodified-Since: 1990 (must be refused)",
             f"status {st}, file now {body!r}")
        if st == 412 and body == b"V1":
            note("=> If-Unmodified-Since IS HONOURED", "usable as CAS")
        else:
            note("=> ignored", "the write landed despite an impossible precondition")
    else:
        note("=> no Last-Modified", "If-Unmodified-Since cannot be expressed either")

    # -- 5. WebDAV If: header --------------------------------------------
    print("\n== 5: WebDAV `If:` header (RFC 4918 10.4) ==")
    ifp = f"{PREFIX}/i_target.txt"
    put(ifp, b"V1")
    # An If: with an etag that cannot match. Must yield 412.
    st, _, _ = req(ifp, "PUT", data=b"V2",
                   headers={"If": '([\"definitely-not-the-etag\"])'})
    _, body, _ = get(ifp)
    note("PUT with unsatisfiable If: (must be refused)",
         f"status {st}, file now {body!r}")
    if st == 412 and body == b"V1":
        note("=> If: IS HONOURED", "usable as CAS")
    else:
        note("=> ignored", "the write landed despite an unsatisfiable condition")

    print()
    n = rmtree(PREFIX)
    note("cleanup rmtree", f"{n} objects removed")
    st, _, _ = req(PREFIX + "/", "PROPFIND", headers={"Depth": "1"})
    note("prefix gone", f"PROPFIND -> {st}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
