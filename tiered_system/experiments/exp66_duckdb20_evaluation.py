"""Compare the actual Go DuckDB streaming path across two pinned engines.

Build both test binaries with -tags=duckdb_arrow. --binary VERSION=PATH must
name the engine actually linked into that binary. Source archive is read-only;
SQL, IPC and spill use isolated local temporary directories. No remote access.
Primary timings exclude profiling and checksums. --profiles adds separate runs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp63_cold_execution import checksums, MARKER
from exp65_cold_query_profile import summarize_duckdb


def sql_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def signature(run):
    return {key: run[key] for key in ("rows", "schema", "row_hash_sum", "row_hash_xor")}


def validate_version(label, report):
    actual = report["engine"]["version"].removeprefix("v")
    if actual != label:
        raise ValueError(f"binary labeled {label} actually links DuckDB {actual}")
    if report.get("compression") != "zstd":
        raise ValueError("comparison requires the production Zstd codec")


def run_probe(binary, version, sql, memory, async_threads, profiled):
    with tempfile.TemporaryDirectory(prefix="sage-exp66-run-") as directory:
        root = Path(directory)
        statement = root/"query.sql"
        statement.write_text(sql)
        env = dict(os.environ, COLD_EVAL_DIR=str(root), COLD_EVAL_SQL=str(statement),
                   COLD_EVAL_MEMORY=memory, COLD_EVAL_ASYNC_THREADS=str(async_threads),
                   COLD_EVAL_PROFILE="1" if profiled else "0")
        _, peak = monitored([str(binary), "-test.run=^TestColdDuckDBEvaluationProbe$", "-test.count=1"], env)
        report = json.loads((root/"run.json").read_text())
        validate_version(version, report)
        content = checksums(root/"body.arrows")
        if report["rows"] != content["rows"]:
            raise ValueError("native writer count differs from decoded IPC")
        report.update(content, peak_process_rss_bytes=peak, version=version)
        if profiled:
            native = list(root.glob("*.duckdb.json"))
            if len(native) != 1 or not native[0].stat().st_size:
                raise ValueError("native profile missing")
            report["duckdb"] = summarize_duckdb(json.loads(native[0].read_text()))
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, help="repeat VERSION=/absolute/test/binary")
    parser.add_argument("--archive-day", type=Path, default=HERE.parents[1]/"data/archive/date=2025-03-01")
    parser.add_argument("--memory", default="512MB")
    parser.add_argument("--async-threads", type=int, default=8)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--stress-rows", type=int, default=2000000)
    parser.add_argument("--profiles", action="store_true")
    parser.add_argument("--output", type=Path, default=HERE/"results/exp66_duckdb20_evaluation.json")
    args = parser.parse_args()
    binaries = {}
    for item in args.binary:
        version, sep, path = item.partition("=")
        if not sep or version in binaries or not Path(path).is_file():
            parser.error("unique VERSION=existing-binary required")
        binaries[version] = Path(path).resolve()
    if len(binaries) < 2 or args.reps < 1 or args.stress_rows < 1 or not 1 <= args.async_threads <= 256:
        parser.error("two versions and positive repetitions/rows/async threads required")
    files = sorted(args.archive_day.glob("*.parquet"))
    if not files:
        parser.error("archive day contains no Parquet files")
    query = dict(start="2025-03-01T00:00:00Z", end="2025-03-02T00:00:00Z",
                 filter={"name":"wxt.env.temp", "vsn":"W0A0"})
    cases = [dict(Name="day", Query=query),
             dict(Name="minute", Query=dict(query, end="2025-03-01T00:01:00Z")),
             dict(Name="stress", Query=dict(start=query["start"], end=query["end"]))]
    out = dict(scope="local Go native streaming + Zstd; no Iceberg/Pelican/network", results=[], summaries=[],
               primary_timings="profiling disabled; checksum validation after timing; native DB initialization excluded",
               memory_scope="whole native subprocess RSS; sampled, not a hard allocation bound", memory_limit=args.memory,
               binary_sha256={v:hashlib.sha256(p.read_bytes()).hexdigest() for v,p in binaries.items()})
    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(out, indent=2)+"\n")
    with tempfile.TemporaryDirectory(prefix="sage-exp66-sql-") as directory:
        root = Path(directory)
        query_file = root/"queries.json"
        query_file.write_text(json.dumps(cases))
        env = dict(os.environ, COLD_EXECUTION_PROBE_DIR=str(root), COLD_EXECUTION_QUERIES=str(query_file))
        # Generate the same response SQL for both versions, from the candidate.
        monitored([str(list(binaries.values())[-1]), "-test.run=^TestColdExecutionProbe$", "-test.count=1"], env)
        scan = "read_parquet(["+",".join(sql_quote(p.resolve()) for p in files)+"], hive_partitioning=false)"
        n = args.stress_rows
        stress = f"""(SELECT TIMESTAMPTZ '2025-03-01' + ({n}-i)*INTERVAL '1 microsecond' AS time,
            'wxt.env.temp' AS name, i*0.1+0.125 AS value_float, NULL::BIGINT AS value_int,
            NULL::VARCHAR AS value_str, MAP {{'sensor':'vaisala','unique':repeat(md5(i::VARCHAR),4)}} AS meta,
            'test:1.0' AS plugin,'W0A0' AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
            NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM range({n}) t(i))"""
        for case in ("minute", "day", "stress"):
            sql = (root/f"{case}-optimized.sql").read_text().replace(MARKER, stress if case=="stress" else scan)
            reference = None
            versions = list(binaries)
            # Rotate which engine runs first to reduce ordering/cache bias.
            for rep in range(args.reps):
                order = versions[rep%len(versions):]+versions[:rep%len(versions)]
                for version in order:
                    run = run_probe(binaries[version], version, sql, args.memory, args.async_threads, False)
                    if reference is None:
                        reference = signature(run)
                    if signature(run) != reference:
                        raise ValueError(f"schema/content mismatch: {case} DuckDB {version}")
                    out["results"].append(dict(case=case, repetition=rep+1, **run))
                    save()
                    print(f"{case} {version}: rows={run['rows']} first={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s", flush=True)
            if args.profiles:
                for version in versions:
                    run = run_probe(binaries[version], version, sql, args.memory, args.async_threads, True)
                    if signature(run) != reference:
                        raise ValueError("profiled query content differs")
                    out["results"].append(dict(case=case, repetition="diagnostic", **run))
                    save()
            for version in versions:
                runs = [r for r in out["results"] if r["case"]==case and r["version"]==version and not r["profiled"]]
                out["summaries"].append(dict(case=case, version=version, content_schema_order_checks_passed=True,
                    rows=runs[0]["rows"], total_seconds=statistics.median(r["total_seconds"] for r in runs),
                    first_batch_seconds=statistics.median(r["first_batch_seconds"] for r in runs),
                    peak_process_rss_bytes=max(r["peak_process_rss_bytes"] for r in runs)))
            save()
    print("Report:", args.output)


if __name__ == "__main__":
    main()
