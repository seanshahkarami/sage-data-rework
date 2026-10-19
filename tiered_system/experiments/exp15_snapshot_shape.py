#!/usr/bin/env python
"""
Experiment 15: how big is the snapshot really, and what shape keeps it small?

exp14's synthetic projection gave 39 MB for today's archive -- alarming, and
the main quantitative argument for Iceberg's split-manifest design. But that
assumed 40 row groups per file each carrying full min/max strings for three
columns. exp09 measured 80 KB for 242 *real* files.

The difference is entirely in what you store per row group. This measures the
real thing at three levels of detail, on real files, so the Iceberg scale
argument can be judged against a number rather than a guess.

Levels:
  full      every column's stats, per row group (what exp14 projected)
  pruning   only the columns that actually reject files: time, plugin
  file      per-file min/max only, no per-group detail

Usage: python exp15_snapshot_shape.py <archive-root> [--days N]
"""
import gzip
import json
import os
import sys
import time

import pyarrow.parquet as pq

PRUNE_COLS = ("time", "plugin")
ALL_COLS = ("time", "name", "plugin", "vsn", "node", "host", "task", "job", "zone")


def build(root, days, level):
    out = []
    for d in days:
        ddir = os.path.join(root, d)
        for fn in sorted(os.listdir(ddir)):
            if not fn.endswith(".parquet"):
                continue
            p = os.path.join(ddir, fn)
            md = pq.ParquetFile(p).metadata
            cols = ALL_COLS if level == "full" else PRUNE_COLS
            e = {"path": f"{d}/{fn}", "date": d.replace("date=", ""),
                 "rows": md.num_rows, "size": os.path.getsize(p)}
            if level == "file":
                st = {}
                for g in range(md.num_row_groups):
                    rg = md.row_group(g)
                    for c in range(rg.num_columns):
                        col = rg.column(c)
                        if col.path_in_schema in cols and col.is_stats_set and col.statistics.has_min_max:
                            s = col.statistics
                            cur = st.setdefault(col.path_in_schema, [str(s.min), str(s.max)])
                            cur[0] = min(cur[0], str(s.min))
                            cur[1] = max(cur[1], str(s.max))
                e["stats"] = st
            else:
                groups = []
                for g in range(md.num_row_groups):
                    rg = md.row_group(g)
                    st = {}
                    for c in range(rg.num_columns):
                        col = rg.column(c)
                        if col.path_in_schema in cols and col.is_stats_set and col.statistics.has_min_max:
                            s = col.statistics
                            st[col.path_in_schema] = [str(s.min), str(s.max)]
                    groups.append([rg.num_rows, st])
                e["groups"] = groups
            out.append(e)
    return out


def main():
    root = os.path.abspath(sys.argv[1])
    n = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 60
    days = sorted(d for d in os.listdir(root) if d.startswith("date="))[:n]
    nfiles = sum(len([f for f in os.listdir(os.path.join(root, d)) if f.endswith(".parquet")])
                 for d in days)

    print(f"{len(days)} real days, {nfiles} files (archive today is 4,564)\n")
    print(f"{'level':10s} {'bytes':>12s} {'gzipped':>11s} {'per file':>10s} {'-> 4,564 files':>16s} {'gz':>11s}")
    rows = []
    for level in ("full", "pruning", "file"):
        ents = build(root, days, level)
        blob = json.dumps(ents, separators=(",", ":")).encode()
        gz = gzip.compress(blob, 6)
        per = len(blob) / nfiles
        print(f"{level:10s} {len(blob):>12,} {len(gz):>11,} {per:>9,.0f}B "
              f"{per*4564:>15,.0f}B {len(gz)/nfiles*4564:>10,.0f}B")
        rows.append({"level": level, "bytes": len(blob), "gz": len(gz),
                     "per_file": round(per), "projected_4564": round(per*4564),
                     "projected_4564_gz": round(len(gz)/nfiles*4564)})

    # does the cheap form still prune as well?
    print()
    ents_full = build(root, days, "full")
    ents_file = build(root, days, "file")
    probe = None
    for e in ents_file:
        s = e["stats"].get("plugin")
        if s:
            probe = s[0]
            break

    def rejects(ents, col, val, keyed):
        r = 0
        for e in ents:
            if keyed == "file":
                s = e["stats"].get(col)
                hit = (not s) or (s[0] <= val <= s[1])
            else:
                hit = any((lambda st: (col not in st) or (st[col][0] <= val <= st[col][1]))(g[1])
                          for g in e["groups"])
            if not hit:
                r += 1
        return r

    for val, lbl in [("zzz-nonexistent", "nonexistent plugin"), (probe, "a real plugin")]:
        if val is None:
            continue
        rf = rejects(ents_full, "plugin", val, "groups")
        rl = rejects(ents_file, "plugin", val, "file")
        print(f"files rejected for {lbl:20s} full={rf}/{nfiles}  file-level={rl}/{nfiles}")

    with open(os.path.dirname(os.path.abspath(__file__)) + "/results/exp15_snapshot_shape.json", "w") as f:
        json.dump({"days": len(days), "files": nfiles, "levels": rows}, f, indent=2)


if __name__ == "__main__":
    main()
