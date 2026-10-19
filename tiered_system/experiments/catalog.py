#!/usr/bin/env python
"""
The smallest catalog that does the jobs the directory tree cannot.

exp10 showed compaction preserves query results. exp11 showed a correction
file is picked up but nothing can express "this record supersedes that one".
Those two gaps -- safe replacement and supersession -- are the whole reason
plans/1_draft.md section 7 wants a catalog, and the reason the Iceberg
question exists at all.

This is a deliberate attempt to find the floor: if a few hundred lines covers
the required behaviour, the Iceberg question is about operational maturity
rather than capability. If this sprawls, that is the answer instead.

Design, matching section 7's table:

  A snapshot is an immutable list of live files plus their statistics.
  Committing writes a new snapshot file and then atomically swaps a pointer.
  A reader resolves the pointer once and holds that snapshot for its lifetime,
  so compaction can publish a replacement mid-query without disturbing it.

The pointer swap is the only atomicity this needs. On a POSIX filesystem that
is os.replace. What it costs on Pelican is exactly the open question, and is
deliberately isolated in CatalogStore so the answer can be swapped in.

Supersession is expressed by generation: each file entry carries the interval
it covers and a generation number. A later generation covering the same
interval replaces an earlier one. That is enough for corrections and for
compaction, and is much weaker than Iceberg's row-level deletes.
"""
import json
import os
import tempfile
import time
import uuid


class CatalogStore:
    """Filesystem catalog store. The Pelican port replaces exactly this class.

    Needs: atomic pointer swap, read a small object, write a small object.
    Notably does NOT need conditional writes, listing, or atomic rename of
    data files -- data files are immutable and uniquely named.
    """

    def __init__(self, root):
        self.root = root
        os.makedirs(os.path.join(root, "_catalog", "snapshots"), exist_ok=True)

    def _ptr(self):
        return os.path.join(self.root, "_catalog", "CURRENT")

    def put_snapshot(self, snap):
        sid = snap["snapshot_id"]
        p = os.path.join(self.root, "_catalog", "snapshots", f"{sid}.json")
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            json.dump(snap, f)
        os.replace(tmp, p)  # durable before anyone can see it
        return p

    def get_snapshot(self, sid):
        with open(os.path.join(self.root, "_catalog", "snapshots", f"{sid}.json")) as f:
            return json.load(f)

    def commit(self, snapshot_id, expected_parent=None):
        """Atomically advance the pointer. The one primitive this design needs."""
        cur = self.current_id()
        if expected_parent is not None and cur != expected_parent:
            raise ConcurrentUpdate(f"pointer moved: expected {expected_parent}, found {cur}")
        tmp = self._ptr() + f".{uuid.uuid4().hex}.tmp"
        with open(tmp, "w") as f:
            f.write(snapshot_id)
        os.replace(tmp, self._ptr())

    def current_id(self):
        try:
            with open(self._ptr()) as f:
                return f.read().strip()
        except FileNotFoundError:
            return None


class ConcurrentUpdate(Exception):
    pass


class Catalog:
    def __init__(self, root):
        self.root = root
        self.store = CatalogStore(root)

    # ---- writing ----

    def _new_snapshot(self, files, parent, op):
        return {
            "snapshot_id": f"{int(time.time()*1000)}-{uuid.uuid4().hex[:8]}",
            "parent_id": parent,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "operation": op,
            "files": files,
        }

    def add_files(self, entries, op="append"):
        """Publish new files. Each entry: path, date, rows, size, stats."""
        parent = self.store.current_id()
        live = dict(self._live_map())
        for e in entries:
            e = dict(e)
            e.setdefault("generation", 0)
            live[e["path"]] = e
        snap = self._new_snapshot(sorted(live.values(), key=lambda x: x["path"]), parent, op)
        self.store.put_snapshot(snap)
        self.store.commit(snap["snapshot_id"], expected_parent=parent)
        return snap["snapshot_id"]

    def replace(self, superseded_paths, new_entries):
        """Compaction: swap files for their replacements in one atomic step.

        This is the operation the directory tree cannot do safely. Until the
        pointer moves, readers see the old files; after, the new ones. There
        is no window where both or neither are visible.
        """
        parent = self.store.current_id()
        live = dict(self._live_map())
        gen = 1 + max([live[p]["generation"] for p in superseded_paths if p in live] or [0])
        for p in superseded_paths:
            live.pop(p, None)
        for e in new_entries:
            e = dict(e)
            e["generation"] = gen
            live[e["path"]] = e
        snap = self._new_snapshot(sorted(live.values(), key=lambda x: x["path"]), parent, "replace")
        snap["superseded"] = list(superseded_paths)
        self.store.put_snapshot(snap)
        self.store.commit(snap["snapshot_id"], expected_parent=parent)
        return snap["snapshot_id"]

    # ---- reading ----

    def _live_map(self):
        sid = self.store.current_id()
        if not sid:
            return {}
        return {e["path"]: e for e in self.store.get_snapshot(sid)["files"]}

    def pin(self):
        """A reader resolves the pointer once and holds this view."""
        return Snapshot(self, self.store.current_id())

    def garbage(self):
        """Files on disk that the current snapshot does not reference.

        Deletion is deliberately NOT automatic: section 7 requires superseded
        files to outlive readers that pinned an older snapshot.
        """
        live = set(self._live_map())
        on_disk = set()
        for d in os.listdir(self.root):
            if not d.startswith("date="):
                continue
            for f in os.listdir(os.path.join(self.root, d)):
                if f.endswith(".parquet"):
                    on_disk.add(os.path.join(d, f))
        return sorted(on_disk - live)


class Snapshot:
    """An immutable view. Planning happens here, with no file opens."""

    def __init__(self, cat, sid):
        self.sid = sid
        self.root = cat.root
        self.files = cat.store.get_snapshot(sid)["files"] if sid else []

    def plan(self, start=None, end=None, filters=None):
        """Select candidate files from statistics alone.

        Returns absolute paths. Matching rows still come from the data files --
        the manifest only rejects files that cannot contain a match.
        """
        filters = filters or {}
        out = []
        for e in self.files:
            if start and e.get("date") and e["date"] < start[:10]:
                continue
            if end and e.get("date") and e["date"] > end[:10]:
                continue
            if not self._stats_allow(e, filters):
                continue
            out.append(os.path.join(self.root, e["path"]))
        return out

    @staticmethod
    def _stats_allow(entry, filters):
        for col, val in filters.items():
            if any(ch in val for ch in "*|"):
                continue  # regex: statistics cannot reject
            groups = entry.get("groups") or [entry.get("stats", {})]
            hit = False
            for g in groups:
                s = (g.get("stats") or g).get(col)
                if not s or s.get("min") is None:
                    hit = True
                    break
                if s["min"] <= val <= s["max"]:
                    hit = True
                    break
            if not hit:
                return False
        return True
