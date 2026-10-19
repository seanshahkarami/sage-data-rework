"""exp23b: can third-party readers consume what the Go writer produces?

The NDP question is a READER question. SAGE would write with iceberg-go;
NDP users would read with whatever they already have -- DuckDB in a
notebook, pyiceberg in a script, Spark/Trino at the far end. So "Iceberg
makes the data self-describing" is only worth anything if the readers a
consumer actually reaches for can open it.

Two fixtures are compared so the result is diagnostic rather than a bare
failure (write them with:
  cd icebergo && ICEBERGO_FIXTURES=/tmp/ndpfix go test -tags reader \
      -run TestInteropFixtures ./...):

  v3_ns  format-version 3, timestamptz_ns  -- what the archive needs
  v2_us  format-version 2, timestamptz     -- the conventional choice

If v2_us reads and v3_ns does not, the blocker is the nanosecond type and
the format version, not Iceberg or the Go writer. That distinction decides
whether nanoseconds are affordable, so it is worth isolating rather than
reporting "DuckDB failed".

Run:  python -I ndp_interop.py /tmp/ndpfix
"""

import glob
import json
import os
import sys

fails = 0


def check(name, ok, detail=""):
    global fails
    status = "PASS" if ok else "FAIL"
    if not ok:
        fails += 1
    print(f"[{status}] {name}")
    if detail:
        print(f"       {detail}")


def latest_metadata(table_dir):
    """Resolve the current metadata file the way a reader must.

    Prefers version-hint.text -- the pointer the filesystem catalog
    publishes -- and falls back to lexical order only to keep the probe
    running on fixtures that lack one.
    """
    md = os.path.join(table_dir, "metadata")
    hint = os.path.join(md, "version-hint.text")
    if os.path.exists(hint):
        with open(hint) as fh:
            v = fh.read().strip()
        cand = os.path.join(md, f"v{v}.metadata.json")
        if os.path.exists(cand):
            return cand, f"via version-hint.text -> v{v}"
    files = sorted(glob.glob(os.path.join(md, "*.metadata.json")))
    return (files[-1], "via filename ordering (no hint)") if files else (None, "none")


def read_schema_plainly(meta_path):
    """What a consumer learns from the metadata JSON with no Iceberg library.

    This is the floor: even a reader with nothing but `json` can describe
    the dataset. It is the part of the "self-describing" claim that does
    not depend on anyone's client support.
    """
    with open(meta_path) as fh:
        meta = json.load(fh)
    cur = meta.get("current-schema-id", 0)
    schema = next((s for s in meta["schemas"] if s.get("schema-id") == cur), meta["schemas"][0])
    return meta, schema


def main(root):
    print("Iceberg reader interop: what can a third party open?\n")

    for name in ("v2_us", "v3_ns"):
        tdir = os.path.join(root, name, "sage", "obs")
        if not os.path.isdir(tdir):
            print(f"-- {name}: fixture missing at {tdir}; skipping\n")
            continue
        print(f"-- {name} " + "-" * 50)

        meta_path, how = latest_metadata(tdir)
        check(f"{name}: current metadata resolved from storage alone",
              meta_path is not None, how)
        if not meta_path:
            continue

        meta, schema = read_schema_plainly(meta_path)
        cols = [f"{f['name']}:{f['type']}" for f in schema["fields"]]
        check(f"{name}: schema readable with the stdlib json module alone",
              len(cols) == 2,
              f"format-version={meta['format-version']}, columns: {', '.join(cols)}")

        snaps = meta.get("snapshots", [])
        rows = snaps[-1].get("summary", {}).get("total-records") if snaps else None
        check(f"{name}: row count available without reading data", rows is not None,
              f"total-records={rows}, snapshots={len(snaps)}")

        # pyiceberg: the reference Python client.
        try:
            from pyiceberg.table import StaticTable
            st = StaticTable.from_metadata(meta_path)
            n = len(st.scan().to_arrow())
            check(f"{name}: pyiceberg StaticTable reads it (no catalog)", n == 10,
                  f"{n} rows")
        except Exception as e:
            check(f"{name}: pyiceberg StaticTable reads it (no catalog)", False,
                  f"{type(e).__name__}: {str(e)[:160]}")

        # DuckDB: what an NDP notebook user most likely reaches for.
        try:
            import duckdb
            con = duckdb.connect()
            con.execute("INSTALL iceberg")
            con.execute("LOAD iceberg")
            n = con.execute(
                "SELECT count(*) FROM iceberg_scan(?)", [meta_path]
            ).fetchone()[0]
            check(f"{name}: DuckDB iceberg_scan reads it", n == 10, f"{n} rows")
        except Exception as e:
            check(f"{name}: DuckDB iceberg_scan reads it", False,
                  f"{type(e).__name__}: {str(e)[:160]}")

        # The fallback every reader has: the Parquet files are plain Parquet.
        try:
            import pyarrow.parquet as pq
            data = sorted(glob.glob(os.path.join(tdir, "data", "*.parquet")))
            tot = sum(pq.ParquetFile(p).metadata.num_rows for p in data)
            check(f"{name}: data files are plain Parquet, readable directly", tot == 10,
                  f"{len(data)} file(s), {tot} rows, no Iceberg client involved")
        except Exception as e:
            check(f"{name}: data files are plain Parquet, readable directly", False,
                  f"{type(e).__name__}: {str(e)[:160]}")
        print()

    print(f"{fails} check(s) failed")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/ndpfix"))
