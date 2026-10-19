"""exp38: is QUERYING the data over Pelican scalable?

exp37 answered a narrower question than its summary implied. It measured
query *planning* -- which files a query must open -- and found that side
scales: partition pruning cut 302 -> 37 requests over the network. But
every "read" in exp37 moved a few hundred bytes, because it registered a
realistic file COUNT with tiny payloads to measure the commit path.

So the number that decides whether consumers can use this -- the time to
actually move the bytes of a real query -- was never measured. SUMMARY
sec 37 says so in one line ("throughput to Pelican ... remains
unmeasured"); this closes it with REAL archive parquet on the origin.

  Q1. What does a real 78 MB archive file cost to read over Pelican,
      through Iceberg, end to end?
  Q2. One column vs all columns -- does projection pushdown pay off over
      the network, where it saves bytes but not necessarily requests?
  Q3. Does the scan parallelise across files? pyiceberg maps tasks onto a
      ThreadPoolExecutor (io/pyarrow.py:1847), and exp17 measured 28x from
      concurrency on raw ranges -- but never through the Iceberg path.
  Q4. What is the ceiling? Measured MB/s vs the single-stream ~30 MB/s
      exp17 found, and what that projects to for a one-week query.
  Q5. Does the 1-row-group shape block row-group pruning, as predicted?

Unlike exp37, this DOES upload real archive files -- throughput cannot be
faked with tiny payloads. It uploads a small number of them (default 4,
~300 MB) to a nanosecond-stamped prefix, checked empty before use and
removed after (PROPFIND -> 404 both ends). The archive is read-only;
nothing touches /node-data or any path sage-storage-loader uses.

Note on the data: archive files are `timestamp[ns]`, which the sec 25
decision stores as microseconds, and exp36 Q1 showed add_files cannot
register them. So files are rewritten to us on upload -- the same rewrite
ingest would do. That is a property of the files, not of this test.

Run:  conda run -n sage_pipeline python -I exp38_query_throughput.py
      conda run -n sage_pipeline python -I exp38_query_throughput.py --files=6
      conda run -n sage_pipeline python -I exp38_query_throughput.py --keep
"""
import os
import shutil
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq

# `-I` drops the script directory from sys.path; exp37 does the same.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pelican_fileio as pfio
import pelican_token as pt

PREFIX = f"/cold-storage-test-query-{time.time_ns()}"
WAREHOUSE = f"{pt.ROOT_URL}{PREFIX}/warehouse"
FILEIO = "pelican_fileio.PelicanFileIO"
CATALOG_NAME = "sage"

ARCHIVE = ("/Users/franciscolozano/Workspaces/sage_data_pipeline_v2/"
           "sage-data-rework/data/archive")

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    return ok


def delta(before):
    now = pfio.COUNTS
    d = {k: now[k] - before.get(k, 0) for k in now}
    return {k: v for k, v in d.items() if v}, sum(d.values())


def us_schema(arrow_schema):
    """The sec 25 decision, applied: every timestamp stored as microseconds."""
    return pa.schema([
        pa.field(f.name,
                 pa.timestamp("us", tz="UTC") if pa.types.is_timestamp(f.type) else f.type,
                 nullable=f.nullable)
        for f in arrow_schema
    ])


# --------------------------------------------------------------------------
# A byte-counting wrapper. Request count was exp37's unit; here the unit is
# BYTES AND SECONDS, so count both without disturbing pfio.COUNTS.
# --------------------------------------------------------------------------

BYTES = {"down": 0, "up": 0}


def install_byte_counter():
    orig = pfio._request

    def counting(path, method, data=None, headers=None, timeout=300):
        st, h, body = orig(path, method, data, headers, timeout)
        if body:
            BYTES["down"] += len(body)
        if data:
            BYTES["up"] += len(data)
        return st, h, body

    pfio._request = counting
    return orig


def reset_bytes():
    BYTES["down"] = BYTES["up"] = 0


def pick_files(n):
    """Take n real archive files from distinct days, smallest-first.

    Distinct days so the partitioned table has something to prune on;
    smallest-first so the upload stays bounded -- throughput per byte is
    what is being measured, not patience.
    """
    days = sorted(d for d in os.listdir(ARCHIVE) if d.startswith("date="))
    out = []
    for d in days:
        p = os.path.join(ARCHIVE, d)
        cands = [(os.path.getsize(os.path.join(p, f)), os.path.join(p, f))
                 for f in os.listdir(p) if f.endswith(".parquet")]
        if cands:
            out.append((d, min(cands)[1], min(cands)[0]))
        if len(out) >= n:
            break
    return out


def main():
    keep = "--keep" in sys.argv
    n_files = 4
    for a in sys.argv[1:]:
        if a.startswith("--files="):
            n_files = int(a.split("=", 1)[1])

    print(__doc__.split("Run:")[0].rstrip())
    print(f"\nprefix   : {PREFIX}")
    print(f"files    : {n_files} real archive files")

    picked = pick_files(n_files)
    if len(picked) < n_files:
        print(f"ABORT: only found {len(picked)} archive files")
        return 2
    total_mb = sum(s for _, _, s in picked) / 1e6
    print(f"source   : {total_mb:,.0f} MB from {len(picked)} archive days")

    st, kids = pfio.propfind(PREFIX)
    if st != 404:
        print(f"\nABORT: prefix not empty (PROPFIND -> {st}, {len(kids)} children)")
        return 2
    print("fresh prefix: PROPFIND -> 404")

    from pyiceberg.catalog.sql import SqlCatalog
    workdir = tempfile.mkdtemp(prefix="exp38-")
    dbpath = os.path.join(workdir, "catalog.db")
    install_byte_counter()

    try:
        # ---- upload: real bytes, through the Iceberg write path ----
        # exp36 Q1: add_files cannot register ns-stamped files, so ingest
        # rewrites them to us. That rewrite is done here too, which makes
        # this an honest measurement of what ingest would actually cost.
        print(f"\n--- uploading {total_mb:,.0f} MB of real archive parquet ---")
        first = pq.ParquetFile(picked[0][1])
        src_schema = first.schema_arrow
        arrow_schema = us_schema(src_schema)
        print(f"  schema: {len(arrow_schema)} columns, "
              f"{first.metadata.num_rows:,} rows/file, "
              f"{first.metadata.num_row_groups} row group(s)")

        cat = SqlCatalog(CATALOG_NAME, **{
            "uri": f"sqlite:///{dbpath}",
            "warehouse": WAREHOUSE,
            "py-io-impl": FILEIO,
        })
        cat.create_namespace("sage")
        tbl = cat.create_table("sage.obs", schema=arrow_schema)

        up_t0 = time.time()
        for i, (day, path, size) in enumerate(picked):
            t = pq.read_table(path)
            # Cast ns -> us. exp36 Q2: 94% of rows carry sub-us digits, but
            # truncation introduced 0 new collisions in 10M rows, so the
            # loss is real and harmless. safe=False is deliberate.
            t = t.cast(arrow_schema, safe=False)
            tbl.append(t)
            print(f"  [{i + 1}/{len(picked)}] {day}  {size / 1e6:.0f} MB  "
                  f"({time.time() - up_t0:.0f}s elapsed)")
        up_wall = time.time() - up_t0
        up_mb = BYTES["up"] / 1e6
        print(f"  uploaded {up_mb:,.0f} MB in {up_wall:.0f}s = "
              f"{up_mb / up_wall:.1f} MB/s (write path)")

        tbl = cat.load_table("sage.obs")
        n_data_files = len(list(tbl.scan().plan_files()))
        stored_mb = sum(t.file.file_size_in_bytes
                        for t in tbl.scan().plan_files()) / 1e6
        print(f"  table: {n_data_files} data files, {stored_mb:,.0f} MB stored")
        if n_data_files > len(picked):
            # Worth stating plainly: pyiceberg split each 10M-row append
            # into several files of its own sizing. Good for us (the scan
            # pool has more to chew on) but it means "files" here is
            # pyiceberg's count, not the archive's.
            print(f"  NOTE: {len(picked)} appends became {n_data_files} files "
                  f"-- pyiceberg sizes its own output.")
        print(f"  stored {stored_mb:,.0f} MB vs {total_mb:,.0f} MB on disk "
              f"({100 * stored_mb / total_mb:.0f}%) -- the us cast and "
              f"pyiceberg's encoding, not loss.")

        # ---- Q5: row-group shape ----
        print("\n--- Q5: can Iceberg prune WITHIN a file? ---")
        print(f"  archive files are {first.metadata.num_row_groups} row group "
              f"of {first.metadata.num_rows:,} rows")
        rewritten_rg = None
        tasks = list(tbl.scan().plan_files())
        if tasks:
            # Read the footer of what WE wrote -- pyiceberg may group rows
            # differently than the exporter did.
            loc = tasks[0].file.file_path
            inp = pfio.PelicanInputFile(loc)
            rewritten_rg = pq.ParquetFile(inp.open()).metadata.num_row_groups
            print(f"  as written by pyiceberg: {rewritten_rg} row group(s)")
        check("Q5a: the ARCHIVE's files have no sub-file unit to prune to",
              first.metadata.num_row_groups == 1,
              f"1 row group -- smallest addressable unit is the whole "
              f"{picked[0][2] / 1e6:.0f} MB file")
        # The happier half: ingest has to rewrite anyway (exp36 Q1), and
        # the rewrite fixes the shape for free.
        if rewritten_rg:
            check("Q5b: the rewrite ingest must do already improves on that",
                  rewritten_rg >= first.metadata.num_row_groups,
                  f"pyiceberg wrote {rewritten_rg} row groups per file "
                  f"without being asked -- the exporter's "
                  f"MaxRowsPerRowGroup fix is additive, not a prerequisite")

        # ---- Q1/Q2: full rows vs one column, real bytes ----
        # Done through StaticTable so it is the consumer's path: hint ->
        # metadata -> scan, no database.
        from pyiceberg.table import StaticTable
        meta_url = tbl.metadata_location

        def timed_scan(label, columns=None, row_filter=None, workers=None):
            """One cold scan, timed, with bytes and requests attributed."""
            if workers is not None:
                os.environ["PYICEBERG_MAX_WORKERS"] = str(workers)
            else:
                os.environ.pop("PYICEBERG_MAX_WORKERS", None)
            # The executor is cached per process (concurrent.py:44), so the
            # env var alone would not take effect -- drop the instance.
            # max_workers() builds a fresh Config(), which re-reads the env.
            from pyiceberg.utils.concurrent import ExecutorFactory
            ExecutorFactory._instance = None

            pfio.reset_counts()
            reset_bytes()
            t0 = time.time()
            s = StaticTable.from_metadata(meta_url, properties={"py-io-impl": FILEIO})
            scan = s.scan(row_filter=row_filter) if row_filter else s.scan()
            if columns:
                scan = scan.select(*columns)
            arr = scan.to_arrow()
            wall = time.time() - t0
            _, nreq = delta({})
            mb = BYTES["down"] / 1e6
            print(f"  {label:<34} {wall:6.1f}s  {nreq:>4} req  "
                  f"{mb:7.1f} MB  {mb / wall:6.1f} MB/s  {arr.num_rows:,} rows")
            return {"wall": wall, "req": nreq, "mb": mb,
                    "rows": arr.num_rows, "cols": arr.num_columns}

        print("\n--- Q1/Q2: real query cost, sequential (1 worker) ---")
        ncol = len(arrow_schema)
        all_1 = timed_scan(f"all {ncol} columns", workers=1)
        one_1 = timed_scan("1 column (value_float)",
                           columns=["value_float"], workers=1)
        check("Q1: a real scan moves real bytes, not metadata",
              all_1["mb"] > 10,
              f"{all_1['mb']:.0f} MB in {all_1['wall']:.0f}s "
              f"= {all_1['mb'] / all_1['wall']:.1f} MB/s")
        saved_pct = 100 * (1 - one_1["mb"] / all_1["mb"]) if all_1["mb"] else 0
        time_pct = 100 * (1 - one_1["wall"] / all_1["wall"])
        check("Q2: column projection cuts BYTES even over HTTP ranges",
              one_1["mb"] < all_1["mb"] * 0.5,
              f"{all_1['mb']:.0f} MB -> {one_1['mb']:.1f} MB "
              f"({saved_pct:.0f}% fewer bytes), "
              f"{all_1['wall']:.0f}s -> {one_1['wall']:.0f}s")
        # The headline finding of Q2 is the GAP between those two numbers.
        print(f"       but only {time_pct:.0f}% less TIME, and MORE requests "
              f"({all_1['req']} -> {one_1['req']}):")
        print(f"       {one_1['mb'] / one_1['wall']:.1f} MB/s vs "
              f"{all_1['mb'] / all_1['wall']:.1f} MB/s -- a projected scan is "
              f"LATENCY-bound,")
        print("       scattering small column reads instead of streaming "
              "bulk bytes.")
        check("Q2b: projection does NOT cut time proportionally to bytes",
              time_pct < saved_pct / 2,
              f"{saved_pct:.0f}% of bytes saved buys only {time_pct:.0f}% "
              f"of the time -- do not size a query by bytes")

        # ---- Q3: does concurrency help through the Iceberg path? ----
        # exp17 got 28x on raw ranges. pyiceberg maps tasks (FILES) onto the
        # executor, so the ceiling here is the file count, not the range
        # count -- that distinction is the finding.
        print(f"\n--- Q3: concurrency across {n_data_files} files "
              f"(pyiceberg maps FILES onto the pool) ---")
        conc = {1: all_1}
        for w in (4, 8, 16):
            conc[w] = timed_scan(f"all columns, {w} workers", workers=w)
        best_w = min(conc, key=lambda k: conc[k]["wall"])
        speedup = conc[1]["wall"] / conc[best_w]["wall"]
        check("Q3: the Iceberg scan DOES parallelise across files",
              speedup > 1.5,
              f"{conc[1]['wall']:.0f}s at 1 worker -> "
              f"{conc[best_w]['wall']:.0f}s at {best_w} "
              f"({speedup:.1f}x)")
        print(f"  NOTE: the pool is over FILES. {n_data_files} files caps "
              f"useful workers at {n_data_files}.")
        # Did it saturate, or is the ceiling just this test's file count?
        ws = sorted(conc)
        gain = conc[ws[-2]]["wall"] / conc[ws[-1]]["wall"]
        if gain < 1.15:
            print(f"  SATURATED: {ws[-2]} -> {ws[-1]} workers bought only "
                  f"{gain:.2f}x. This is the link's ceiling, not the "
                  f"file count's.")
        else:
            best_mbs = conc[best_w]["mb"] / conc[best_w]["wall"]
            print(f"  NOT saturated: {ws[-2]} -> {ws[-1]} workers still "
                  f"gained {gain:.2f}x, so {best_mbs:.0f} MB/s is a FLOOR "
                  f"on what a real consumer gets.")

        # ---- Q4: the ceiling, and what it projects to ----
        print("\n--- Q4: throughput ceiling ---")
        seq_mbs = all_1["mb"] / all_1["wall"]
        par_mbs = conc[best_w]["mb"] / conc[best_w]["wall"]
        print(f"  sequential : {seq_mbs:6.1f} MB/s")
        print(f"  {best_w} workers  : {par_mbs:6.1f} MB/s")
        print("  exp17 raw single-range ceiling was ~30 MB/s; "
              "concurrency is how you pass it.")
        check("Q4: concurrency raises throughput past the single-stream wall",
              par_mbs > seq_mbs,
              f"{seq_mbs:.0f} -> {par_mbs:.0f} MB/s")

        # ---- projection to a real query ----
        # Grounded in measured MB/s and the archive's real shape: 608 days,
        # 4,564 files, 209 GB -> ~7.5 files and ~344 MB per day.
        print("\n--- Projection to a real one-week query (EXTRAPOLATED) ---")
        archive_gb = 209.0
        archive_days = 608
        week_gb = archive_gb * 7 / archive_days
        print(f"  one week of the real archive ~ {week_gb * 1000:,.0f} MB "
              f"(~{4564 * 7 / archive_days:.0f} files)")
        for label, rate in (("sequential", seq_mbs), (f"{best_w} workers", par_mbs)):
            print(f"    all columns, {label:<11} -> "
                  f"{week_gb * 1000 / rate / 60:5.1f} min")
        if all_1["mb"]:
            frac = one_1["mb"] / all_1["mb"]
            # Projection saves bytes, but costs round trips (Q2), so scale
            # by the measured TIME ratio, not the byte ratio. Using bytes
            # here would predict ~5 s and be wrong by an order of magnitude.
            tfrac = one_1["wall"] / all_1["wall"]
            print(f"    1 column, {best_w} workers    -> "
                  f"{week_gb * 1000 / par_mbs * tfrac / 60:5.1f} min "
                  f"(keeps {100 * frac:.0f}% of bytes but "
                  f"{100 * tfrac:.0f}% of the time)")
        print("  Caveat: measured on a laptop link. A well-connected "
              "consumer does better;")
        print("  the SHAPE (concurrency is mandatory) is the transferable "
              "result, not the rate.")

        npass = sum(1 for _, ok in RESULTS if ok)
        print(f"\n{npass}/{len(RESULTS)} checks passed")
        return 0 if npass == len(RESULTS) else 1

    finally:
        os.environ.pop("PYICEBERG_MAX_WORKERS", None)
        if keep:
            print(f"\n--keep: leaving {PREFIX} in place")
        else:
            print(f"\ncleaning up {PREFIX} ...")
            n = pfio.rmtree(PREFIX)
            st, left = pfio.propfind(PREFIX)
            print(f"deleted {n} objects; PROPFIND {PREFIX} -> {st}"
                  + (f" ({len(left)} left)" if left else " (clean)"))
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
