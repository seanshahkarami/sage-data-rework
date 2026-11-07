"""Summarize opt-in cold Arrow profiles copied from the API container.

Reads local files only. Backend elapsed phases and DuckDB operator work are
reported separately: parallel operator timings are not additive wall time.
Raw profiles contain query SQL/warehouse URLs; the summary omits SQL/filters.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path

ROOT_METRICS = (
    "latency", "cpu_time", "rows_returned", "cumulative_rows_scanned",
    "total_bytes_read", "total_bytes_written", "system_peak_buffer_memory",
    "system_peak_temp_dir_size", "blocked_thread_time",
)
NOTES = [
    "Backend *_seconds fields are elapsed seconds; *_since_backend_start and lifetime are overlapping markers, not additive phases. metadata_discovery_rows and metadata_reuse_applied are counts/flags.",
    "Backend lifetime includes consumer pacing/IPC/compression/socket backpressure; fetch/conversion do not measure all response encoding work.",
    "DuckDB operator seconds describe operator work across threads, may include I/O waits, and must not be summed with backend phases or interpreted as pure CPU.",
    "prepare_seconds includes binding/planning and may include Iceberg metadata HTTP reads; scan work may include Pelican waits and decompression.",
    "DuckDB byte counters are engine-reported, not a complete measurement of Pelican HTTP traffic; zero does not prove no reads.",
    "Only complete queries with a valid DuckDB profile are eligible for bottleneck analysis. Profiling itself can add overhead.",
    "Uniform metadata discovery is timed separately and excluded from the main native profile; use metadata_discovery_seconds and full HTTP elapsed time to include its cost.",
]


def summarize_duckdb(profile):
    """Normalize legacy flat profiles and DuckDB 2.0 grouped JSON metrics.

    2.0 layout follows docs/2.0/dev/metrics. Keep numeric groups as well as
    comparable names, and never substitute cumulative rows for returned rows.
    """
    modern = isinstance(profile.get("operator"), list)
    roots = profile.get("operator") if modern else profile.get("children")
    if not isinstance(roots, list):
        raise ValueError("missing recognized DuckDB operator tree")
    operators = []
    def visit(node, depth=0):
        name = node.get("type", "unknown") if modern else node.get("operator_name", node.get("name", "unknown"))
        timing = node.get("timing") if modern else node.get("operator_timing")
        if timing is not None:
            operators.append(dict(name=name.strip(), depth=depth, seconds=timing,
                rows=node.get("intermediate_rows") if modern else node.get("operator_cardinality"),
                rows_scanned=node.get("rows_scanned") if modern else node.get("operator_rows_scanned"),
                **({"row_groups_scanned": node["row_groups_scanned"]} if "row_groups_scanned" in node else {})))
        for child in node.get("children", []):
            visit(child, depth+1)
    for child in roots:
        visit(child)
    groups = defaultdict(float)
    for op in operators:
        name = op["name"].upper()
        group = "scan" if any(s in name for s in ("SCAN", "READ_PARQUET")) else "sort" if any(s in name for s in ("ORDER_BY", "TOP_N")) else "projection" if "PROJECTION" in name else "other"
        groups[group] += op["seconds"]
    scoped = {}
    if modern:
        for group in ("query", "system", "io", "parser", "planner", "optimizer", "physical_planner"):
            for key, value in profile.get(group, {}).items():
                if isinstance(value, (int, float)):
                    scoped[f"{group}.{key}"] = value
        mapping = dict(latency="query.total_time", cpu_time="query.cpu_time",
            cumulative_rows_scanned="query.total_rows_scanned", total_bytes_read="io.total_bytes_read",
            total_bytes_written="io.total_bytes_written", system_peak_buffer_memory="system.peak_buffer_memory",
            system_peak_temp_dir_size="system.peak_temp_dir_size", blocked_thread_time="system.blocked_thread_time")
        metrics = {key: scoped[value] for key, value in mapping.items() if value in scoped}
        detailed = {key: value for key, value in scoped.items()
                    if key.startswith(("parser.", "planner.", "optimizer.", "physical_planner."))}
    else:
        metrics = {k: profile[k] for k in ROOT_METRICS if k in profile}
        detailed = {key: value for key, value in profile.items()
                    if key.startswith(("planner", "optimizer", "physical_planner"))
                    and isinstance(value, (int, float))}
    return dict(format="duckdb_2_grouped" if modern else "duckdb_1_flat", metrics=metrics,
        scoped_numeric_metrics=scoped, detailed_planning_seconds=detailed,
        operator_work_seconds=dict(groups), operators=sorted(operators, key=lambda op: op["seconds"], reverse=True))


def load_timing(path):
    timing = json.loads(path.read_text())
    name = timing.get("duckdb_file", "")
    # Accept only a paired file in this directory, even for hand-edited inputs.
    if not name or Path(name).name != name or "/" in name or "\\" in name:
        raise ValueError(f"invalid paired profile name in {path}")
    result = dict(source=str(path), status=timing["status"], rows=timing["rows"],
        batches=timing["batches"], phases=timing["phases"], eligible=False)
    if timing.get("engine_version"):
        result["engine_version"] = timing["engine_version"]
    paired = path.with_name(name)
    if not timing.get("duckdb_profile_present"):
        result["profile_error"] = "native profiler did not produce a nonempty file"
        return result
    try:
        profile = json.loads(paired.read_text())
        result["duckdb"] = summarize_duckdb(profile)
        result["eligible"] = timing["status"] == "complete"
    except (OSError, ValueError, TypeError) as exc:
        result["profile_error"] = str(exc)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="isolated profile folder copied from container")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--raw", action="store_true", help="DuckDB-only local validation; no Go phase measurements")
    args = parser.parse_args()
    if args.raw:
        queries = [dict(source=str(p), status="duckdb_only", eligible=False,
                        duckdb=summarize_duckdb(json.loads(p.read_text())))
                   for p in sorted(args.directory.rglob("*.duckdb.json")) if p.stat().st_size]
    else:
        queries = [load_timing(p) for p in sorted(args.directory.rglob("*.timing.json"))]
    if not queries:
        parser.error("no matching profiles found")
    scope = "local DuckDB-only validation; no native Go phases or VM bottleneck conclusion" if args.raw else "cold query elapsed phases and DuckDB operator work"
    output = dict(scope=scope, notes=NOTES, queries=queries)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2)+"\n")
    for q in queries:
        phases = q.get("phases", {})
        print(f"{Path(q['source']).name}: {q['status']}, eligible={q['eligible']}, rows={q.get('rows', '?')}")
        for key in ("hint_http_seconds", "arrow_queue_wait_seconds", "arrow_engine_init_seconds", "prepare_seconds",
                    "execute_pending_seconds", "first_batch_since_backend_start_seconds", "fetch_chunks_seconds",
                    "arrow_conversion_seconds", "native_eof_since_backend_start_seconds", "total_backend_lifetime_seconds"):
            if key in phases:
                print(f"  {key}: {phases[key]:.6f}")
        for op in q.get("duckdb", {}).get("operators", [])[:5]:
            print(f"  operator work {op['name']}: {op['seconds']:.6f} s, rows={op['rows']}, scanned={op['rows_scanned']}")
        if "profile_error" in q:
            print("  "+q["profile_error"])
    print("Operator work is not additive wall time or pure CPU. Full report:", args.output)


if __name__ == "__main__":
    main()
