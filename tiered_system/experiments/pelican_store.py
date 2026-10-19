"""
A Pelican-backed CatalogStore: the port `catalog.py` was designed for.

`catalog.py:38-44` isolates every storage assumption in one class and says
"The Pelican port replaces exactly this class." This is that class, with an
identical interface, so `Catalog`, `Snapshot` and exp13 work unchanged.

The POSIX original needs three things. On NRDStor (measured in exp17):

  write a small object   -> PUT                      (201)
  read a small object    -> GET                      (200)
  atomic pointer swap    -> MOVE onto existing path  (201, atomic)

The one thing the origin does NOT provide is compare-and-swap:
`If-None-Match: *` and `MOVE` with `Overwrite: F` are both accepted and
ignored. So `commit(expected_parent=...)` cannot be enforced by the storage.
It is implemented here as read-then-compare-then-swap, which is honest but
NOT atomic -- there is a real window between the check and the swap. exp18
measures that window rather than hiding it.

Everything lives under a caller-supplied prefix. Nothing here touches
/node-data or the loader's paths.
"""
import json
import ssl
import urllib.error
import urllib.request
import uuid

import pelican_token as pt

_CTX = ssl.create_default_context()


class PelicanIO:
    """Raw verbs against one /sage prefix, with per-request scoped tokens."""

    def __init__(self, prefix, timeout=120):
        self.prefix = "/" + prefix.strip("/")
        self.timeout = timeout
        self.counts = {"GET": 0, "PUT": 0, "MOVE": 0, "DELETE": 0, "HEAD": 0}

    def _url(self, path):
        # Scope to the whole prefix: MOVE needs authority over both ends.
        return pt.url_for(path, scope=f"read:{self.prefix} write:{self.prefix}",
                          lifetime=900)

    def _do(self, path, method, data=None, headers=None):
        req = urllib.request.Request(self._url(path), method=method, data=data)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        self.counts[method] = self.counts.get(method, 0) + 1
        try:
            with urllib.request.urlopen(req, timeout=self.timeout,
                                        context=_CTX) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, (e.read() or b"")

    def put(self, path, data):
        st, _ = self._do(path, "PUT", data=data)
        if st not in (200, 201, 204):
            raise IOError(f"PUT {path} -> {st}")

    def get(self, path):
        st, body = self._do(path, "GET")
        if st == 404:
            raise FileNotFoundError(path)
        if st != 200:
            raise IOError(f"GET {path} -> {st}")
        return body

    def move(self, src, dst):
        st, _ = self._do(src, "MOVE",
                         headers={"Destination": f"{pt.ROOT_URL}{dst}"})
        if st not in (200, 201, 204):
            raise IOError(f"MOVE {src} -> {dst}: {st}")

    def delete(self, path):
        st, _ = self._do(path, "DELETE")
        return st


class PelicanCatalogStore:
    """Drop-in replacement for catalog.CatalogStore, backed by Pelican.

    Interface parity with the POSIX original: put_snapshot, get_snapshot,
    commit, current_id. `root` is a /sage-relative prefix, not a local path.
    """

    def __init__(self, root):
        self.root = "/" + root.strip("/")
        self.io = PelicanIO(self.root)
        # No mkdir: PUT creates parent directories implicitly (exp17).

    def _ptr(self):
        return f"{self.root}/_catalog/CURRENT"

    def _snap_path(self, sid):
        return f"{self.root}/_catalog/snapshots/{sid}.json"

    def put_snapshot(self, snap):
        """Write the snapshot so it is never visible half-written.

        Mirrors the POSIX write-temp-then-rename: a PUT that fails midway
        could otherwise leave a truncated object a reader might parse.
        """
        sid = snap["snapshot_id"]
        final = self._snap_path(sid)
        tmp = f"{final}.{uuid.uuid4().hex}.tmp"
        self.io.put(tmp, json.dumps(snap).encode())
        self.io.move(tmp, final)
        return final

    def get_snapshot(self, sid):
        return json.loads(self.io.get(self._snap_path(sid)))

    def commit(self, snapshot_id, expected_parent=None):
        """Advance the pointer.

        NOT atomic with respect to the check. On POSIX the same code is also
        check-then-act, but the window is microseconds; here it is a network
        round trip (~150 ms, exp17), so the race is far easier to lose.
        """
        cur = self.current_id()
        if expected_parent is not None and cur != expected_parent:
            raise ConcurrentUpdate(
                f"pointer moved: expected {expected_parent}, found {cur}")
        tmp = f"{self._ptr()}.{uuid.uuid4().hex}.tmp"
        self.io.put(tmp, snapshot_id.encode())
        self.io.move(tmp, self._ptr())

    def current_id(self):
        try:
            return self.io.get(self._ptr()).decode().strip()
        except FileNotFoundError:
            return None


# Re-exported so callers can catch one exception type regardless of backend.
from catalog import ConcurrentUpdate  # noqa: E402,F401
