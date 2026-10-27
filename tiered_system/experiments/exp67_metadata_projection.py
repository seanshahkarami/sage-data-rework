"""Measure metadata projection on the actual DuckDB 2.0 Go streaming path.

Only local archive reads. SQL/IPC/spill use isolated temporary directories.
The SQL probe exports current Go statements; the native probe executes supplied
SQL. Diagnostic ablations change payloads, so their elapsed times are not speedup
claims. Baseline/candidate must preserve full-row signatures and public schema.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp66_duckdb20_evaluation import run_probe, signature, sql_quote, validate_version

MARKER = "iceberg_scan('__SAGE_METADATA__')"
TAGS = ["plugin", "vsn", "node", "host", "task", "job", "zone"]


def fixture(path):
    """Every tag-presence mask, mixed values and raw/promoted collisions."""
    maps = [None, {}, {"sensor": "中文\"\\\n{}", "units": "C"},
            {"plugin": "raw", "vsn": "raw"}, {"job": None, "zone": "raw"},
            {"sensor": None, "VSN": "case sensitive"}, {"x": "}"}, {"x": "{"}]
    n = 128*len(maps)
    columns = dict(time=pa.array([1740787200000000000+i*1001 for i in range(n)], type=pa.timestamp("ns", tz="UTC")),
        name=pa.array(["test"]*n), value_float=pa.array([i/10 if i%4==0 else None for i in range(n)], type=pa.float64()),
        value_int=pa.array([9007199254740993+i if i%4==1 else None for i in range(n)], type=pa.int64()),
        value_str=pa.array(["str\"\\\n中文" if i%4==2 else None for i in range(n)], type=pa.string()),
        meta=pa.array([None if maps[i%len(maps)] is None else list(maps[i%len(maps)].items()) for i in range(n)],
                      type=pa.map_(pa.string(), pa.string())))
    for bit, tag in enumerate(TAGS):
        columns[tag] = pa.array([None if not ((i//len(maps)) & (1<<bit)) else
                                ("" if i%5==0 else tag+"\"\\\n中文") for i in range(n)], type=pa.string())
    pq.write_table(pa.table(columns), path)
    return n


def native_table(binary, sql, memory, root):
    root.mkdir()
    p = root/"query.sql"
    p.write_text(sql)
    env = dict(os.environ, COLD_EVAL_DIR=str(root), COLD_EVAL_SQL=str(p), COLD_EVAL_MEMORY=memory,
               COLD_EVAL_ASYNC_THREADS="8", COLD_EVAL_PROFILE="0")
    monitored([str(binary), "-test.run=^TestColdDuckDBEvaluationProbe$", "-test.count=1"], env)
    report = json.loads((root/"run.json").read_text())
    validate_version("2.0.0", report)
    with (root/"body.arrows").open("rb") as f:
        return ipc.open_stream(f).read_all()


def concat_reference(meta):
    """Rejected exploratory candidate: splice serialized object bodies."""
    guard, rest = meta.split(" THEN ", 1)
    fallback, _ = rest.split(" ELSE ", 1)
    raw = "to_json(COALESCE(meta, MAP {}::MAP(VARCHAR, VARCHAR)))::VARCHAR"
    parts = [f"NULLIF(substr({raw},2,length({raw})-2),'')"]
    parts += [f"CASE WHEN {t} IS NOT NULL THEN '\"{t}\":' || to_json({t})::VARCHAR END" for t in TAGS]
    return guard+" THEN "+fallback+" ELSE '{' || concat_ws(',', "+",".join(parts)+") || '}' END"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-probe", type=Path, required=True)
    parser.add_argument("--sql-probe", type=Path, required=True)
    parser.add_argument("--archive-day", type=Path, default=HERE.parents[1]/"data/archive/date=2025-03-01")
    parser.add_argument("--memory", default="3GB")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--match", default="", help="optional case-name substring for a targeted rerun")
    parser.add_argument("--stress-rows", type=int, default=2000000)
    parser.add_argument("--include-rejected", action="store_true")
    parser.add_argument("--output", type=Path, default=HERE/"results/exp67_metadata_projection.json")
    args = parser.parse_args()
    measured_cases = [name for name in ("minute", "hour", "hour_all_names", "day", "stress") if args.match in name]
    if not measured_cases:
        parser.error("no matching cases")
    if args.reps < 1 or args.stress_rows < 1:
        parser.error("positive repetitions/rows required")
    native, probe = args.native_probe.resolve(), args.sql_probe.resolve()
    files = sorted(args.archive_day.glob("*.parquet"))
    if not native.is_file() or not probe.is_file() or not files:
        parser.error("existing probes and archive Parquet required")
    q = dict(start="2025-03-01T00:00:00Z", end="2025-03-02T00:00:00Z", filter={"name":"wxt.env.temp", "vsn":"W0A0"})
    cases = [dict(Name="day", Query=q), dict(Name="minute", Query=dict(q,end="2025-03-01T00:01:00Z")),
             dict(Name="hour", Query=dict(q,end="2025-03-01T01:00:00Z")),
             dict(Name="hour_all_names", Query=dict(q,end="2025-03-01T01:00:00Z",filter={"vsn":"W0A0"})),
             dict(Name="stress", Query=dict(start=q["start"],end=q["end"])),
             dict(Name="semantics", Query=dict(start=q["start"],end=q["end"])),
             dict(Name="head", Query=dict(start=q["start"],end=q["end"],head=1)),
             dict(Name="tail", Query=dict(start=q["start"],end=q["end"],tail=1))]
    out = dict(scope="current DuckDB 2.0 native streaming + Zstd, local files only; no Pelican or VM rerun",
               native_probe_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),
               sql_probe_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(),
               memory_limit=args.memory, primary_timings="unprofiled, checksums outside native timer, rotated mode order",
               results=[], summaries=[], diagnostics=[], semantic_checks=[])
    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(out, indent=2)+"\n")
    with tempfile.TemporaryDirectory(prefix="sage-exp67-") as directory:
        root = Path(directory)
        p = root/"queries.json"
        p.write_text(json.dumps(cases))
        monitored([str(probe), "-test.run=^TestColdProjectionProbe$", "-test.count=1"],
                  dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(p)))
        scan = "read_parquet(["+",".join(sql_quote(p.resolve()) for p in files)+"], hive_partitioning=false)"
        fixture_path = root/"semantics.parquet"
        count = fixture(fixture_path)
        for case in ("semantics", "head", "tail"):
            tables = [native_table(native, (root/f"{case}-{mode}.sql").read_text().replace(MARKER,"read_parquet("+sql_quote(fixture_path)+")"),
                                   args.memory,root/f"fixture-{case}-{mode}") for mode in ("baseline","optimized")]
            if not tables[0].equals(tables[1]):
                raise AssertionError(f"exact Arrow table mismatch: {case}")
            if case=="semantics":
                if len(tables[0]) != count:
                    raise AssertionError("semantic fixture lost rows")
                stamps = tables[0].column("timestamp").cast(pa.int64()).to_numpy()
                if not np.any(stamps % 1000):
                    raise AssertionError("nanosecond precision lost")
            out["semantic_checks"].append(dict(case=case,rows=len(tables[0]),exact_table_equal=True))
        n = args.stress_rows
        stress = f"""(SELECT TIMESTAMPTZ '2025-03-01' + ({n}-i)*INTERVAL '1 microsecond' AS time,
        'wxt.env.temp' AS name,i*0.1+0.125 AS value_float,NULL::BIGINT AS value_int,NULL::VARCHAR AS value_str,
        MAP {{'sensor':'vaisala','unique':repeat(md5(i::VARCHAR),4)}} AS meta,
        'test:1.0' AS plugin,'W0A0' AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
        NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM range({n}) t(i))"""
        for case in measured_cases:
            source = stress if case=="stress" else scan
            sqls = {mode:(root/f"{case}-{mode}.sql").read_text().replace(MARKER,source) for mode in ("baseline","optimized")}
            reference = None
            for rep in range(args.reps):
                for mode in (("baseline","optimized") if rep%2==0 else ("optimized","baseline")):
                    run = run_probe(native,"2.0.0",sqls[mode],args.memory,8,False)
                    if reference is None:
                        reference = signature(run)
                    if signature(run) != reference:
                        raise AssertionError(f"content/schema/order mismatch: {case} {mode}")
                    out["results"].append(dict(case=case,mode=mode,repetition=rep+1,**run))
                    save()
                    print(f"{case} {mode}: rows={run['rows']} first={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s",flush=True)
            for mode in ("baseline","optimized"):
                runs=[r for r in out["results"] if r["case"]==case and r["mode"]==mode]
                out["summaries"].append(dict(case=case,mode=mode,rows=runs[0]["rows"],
                    total_seconds=statistics.median(r["total_seconds"] for r in runs),
                    first_batch_seconds=statistics.median(r["first_batch_seconds"] for r in runs),
                    peak_process_rss_bytes=max(r["peak_process_rss_bytes"] for r in runs)))
            if case in ("day","stress"):
                for mode in ("baseline","optimized","without_meta","without_value"):
                    sql=(root/f"{case}-{mode}.sql").read_text().replace(MARKER,source)
                    run=run_probe(native,"2.0.0",sql,args.memory,8,True)
                    if mode in ("baseline","optimized") and signature(run)!=reference:
                        raise AssertionError("profiled content differs")
                    out["diagnostics"].append(dict(case=case,mode=mode,**run))
                if args.include_rejected:
                    meta=(root/f"{case}-meta_before.sql").read_text()
                    for mode,expr in (("rejected_concat",concat_reference(meta)),("rejected_correlated_cache","(SELECT "+meta+")")):
                        sql=sqls["baseline"].replace(meta,expr)
                        run=run_probe(native,"2.0.0",sql,args.memory,8,True)
                        if signature(run)!=reference:
                            raise AssertionError("exploratory candidate changed content")
                        out["diagnostics"].append(dict(case=case,mode=mode,**run))
            save()
        cardinality_sql=(root/"day-cardinality.sql").read_text().replace(MARKER,scan)
        table=native_table(native,cardinality_sql,args.memory,root/"cardinality")
        out["filtered_day_metadata_cardinality"]=json.loads(table.column("value")[0].as_py())
        save()
    print("Report:",args.output)


if __name__=="__main__":
    main()
