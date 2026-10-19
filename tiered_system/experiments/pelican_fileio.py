"""
A pyiceberg FileIO backed by Pelican/NRDStor WebDAV.

`results/SUMMARY.md` §12 says Iceberg cannot address this storage because
"the FileSystem support table lists only S3, GCS, Azure Blob and local".
That was read off iceberg-go's docs. It is true of pyiceberg too -- its
scheme table (`pyiceberg/io/fsspec.py:321`, `io/__init__.py:319`) has no
http/https entry, so `https://...` raises
"No registered filesystem for scheme: https".

But pyiceberg accepts a custom FileIO via the `py-io-impl` property, and
the interface is only three methods. So "no FileIO exists" is a packaging
fact, not a capability limit, and the honest way to test Iceberg here is
to supply the missing FileIO and see what *then* breaks. That is this
file. It exists so exp19 tests Iceberg's commit model rather than testing
a missing import.

Auth note: exp17 used `?authz=<token>` because the production download
path needs a redirect to carry credentials. Probed here, NRDStor also
accepts a normal `Authorization: Bearer` header, which is what a library
client would send. This uses the header.

Writes are buffered and PUT whole on close. Pelican has no append and no
multipart; Iceberg metadata/manifests are small, and data files are
written once by the writer, so buffering is the correct shape rather than
a shortcut.
"""
import io as _io
import ssl
import urllib.error
import urllib.request
from urllib.parse import urlparse

from pyiceberg.io import FileIO, InputFile, OutputFile

import pelican_token as pt

_CTX = ssl.create_default_context()

# Every request is counted so exp19 can compare Iceberg's request volume
# against the custom catalog's. On Pelican each one costs ~150 ms (§17),
# so request count is the cost that matters, not bytes.
COUNTS = {"GET": 0, "PUT": 0, "HEAD": 0, "DELETE": 0, "MOVE": 0}


def reset_counts():
    for k in COUNTS:
        COUNTS[k] = 0


def _path_of(location):
    """Map an Iceberg location to a /sage-relative path.

    Accepts both the full https URL (what we store in table metadata) and
    a bare path, because pyiceberg echoes locations back in several forms.
    """
    if location.startswith("http"):
        p = urlparse(location).path
        # ROOT_URL already ends in /sage; strip it to get the namespace path.
        return p[len("/sage"):] if p.startswith("/sage") else p
    return location


def _request(path, method, data=None, headers=None, timeout=300):
    scope = f"read:{path} write:{path}"
    req = urllib.request.Request(f"{pt.ROOT_URL}{path}", method=method, data=data)
    req.add_header("Authorization", "Bearer " + pt.mint(scope, lifetime=900))
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    COUNTS[method] = COUNTS.get(method, 0) + 1
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), (e.read() or b"")


class _InputStream:
    """Seekable reader over HTTP ranges.

    `closed` is an attribute, not a method: pyarrow reads it directly and a
    bound method is always truthy -> "I/O operation on closed file" (the
    same trap hit in exp17).
    """

    closed = False

    def __init__(self, path, size):
        self.path, self.size, self._pos = path, size, 0

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self._pos
        if n <= 0 or self._pos >= self.size:
            return b""
        lo = self._pos
        hi = min(self._pos + n, self.size) - 1
        st, _, body = _request(self.path, "GET",
                               headers={"Range": f"bytes={lo}-{hi}"})
        if st not in (200, 206):
            raise IOError(f"GET {self.path} range {lo}-{hi} -> {st}")
        self._pos += len(body)
        return body

    def seek(self, offset, whence=0):
        self._pos = {0: offset, 1: self._pos + offset,
                     2: self.size + offset}[whence]
        return self._pos

    def tell(self):
        return self._pos

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def close(self):
        pass

    def flush(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class PelicanInputFile(InputFile):
    def __init__(self, location):
        super().__init__(location)
        self._path = _path_of(location)
        self._size = None

    def _head(self):
        # KNOWN COST (exp37 Q6): this cache is per *instance*, and pyiceberg
        # builds a fresh PelicanInputFile for every data file in every scan,
        # so it never survives -- we HEAD each file once per read. That is a
        # third of all reader requests (measured: 40 files -> 205 requests,
        # 40 of them these HEADs). The manifest already carries the answer in
        # `file_size_in_bytes`; a process-wide {path: size} cache fed from it
        # took the same read to 122 requests (-40%). See SUMMARY.md sec 37.
        if self._size is None:
            st, h, _ = _request(self._path, "HEAD")
            self._size = int(h.get("Content-Length", 0)) if st == 200 else -1
        return self._size

    def __len__(self):
        n = self._head()
        if n < 0:
            raise FileNotFoundError(self.location)
        return n

    def exists(self):
        return self._head() >= 0

    def open(self, seekable=True):
        return _InputStream(self._path, len(self))


class _OutputStream(_io.BytesIO):
    """Buffer locally, PUT once on close. Pelican has no append."""

    def __init__(self, path, on_close):
        super().__init__()
        self._path, self._on_close = path, on_close
        self._done = False

    def close(self):
        if not self._done:
            self._done = True
            data = self.getvalue()
            st, _, body = _request(self._path, "PUT", data=data)
            if st not in (200, 201, 204):
                raise IOError(f"PUT {self._path} -> {st} {body[:200]!r}")
            self._on_close(len(data))
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class PelicanOutputFile(OutputFile):
    def __init__(self, location):
        super().__init__(location)
        self._path = _path_of(location)
        self._written = None

    def _head(self):
        st, h, _ = _request(self._path, "HEAD")
        return int(h.get("Content-Length", 0)) if st == 200 else -1

    def __len__(self):
        # Called right after the writer closes, to record file_size_in_bytes.
        # Use the local count to avoid a second round trip per data file.
        if self._written is not None:
            return self._written
        n = self._head()
        if n < 0:
            raise FileNotFoundError(self.location)
        return n

    def exists(self):
        return self._written is not None or self._head() >= 0

    def to_input_file(self):
        return PelicanInputFile(self.location)

    def create(self, overwrite=False):
        if not overwrite and self.exists():
            raise FileExistsError(self.location)

        def _record(n):
            self._written = n

        return _OutputStream(self._path, _record)


class PelicanFileIO(FileIO):
    """The FileIO pyiceberg does not ship for this storage."""

    def new_input(self, location):
        return PelicanInputFile(location)

    def new_output(self, location):
        return PelicanOutputFile(location)

    def delete(self, location):
        loc = location.location if hasattr(location, "location") else location
        _request(_path_of(loc), "DELETE")


# --- helpers used by the experiment for setup/teardown, not by pyiceberg ---

def propfind(path, depth="1"):
    st, _, body = _request(path, "PROPFIND", headers={"Depth": depth})
    if st != 207:
        return st, []
    import re
    hrefs = re.findall(rb"<[dD]:href>([^<]+)</[dD]:href>", body)
    out = []
    for h in hrefs:
        href = h.decode()
        p = urlparse(href).path if href.startswith("http") else href
        if p.startswith("/sage"):
            p = p[len("/sage"):]
        if p.rstrip("/") != path.rstrip("/"):
            out.append(p)
    return st, out


def rmtree(path):
    """Recursively delete a prefix. Returns the number of objects removed."""
    st, kids = propfind(path)
    if st != 207:
        return 0
    n = 0
    for k in kids:
        sub_st, sub_kids = propfind(k)
        if sub_st == 207 and sub_kids:
            n += rmtree(k)
        else:
            if _request(k, "DELETE")[0] in (200, 204):
                n += 1
    _request(path, "DELETE")
    return n
