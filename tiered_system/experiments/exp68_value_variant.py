"""Compare split typed values with one stored Parquet VARIANT on DuckDB 2.0.

Read-only archive input; all rewritten Parquet, spill and IPC use isolated local
temporary directories. No Iceberg/Pelican access or production changes. Export
current response SQL from TestColdProjectionProbe; change only value plumbing.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp66_duckdb20_evaluation import run_probe, signature, sql_quote
from exp67_metadata_projection import fixture, native_table, MARKER

VALUE_JSON = """CASE
                WHEN value_float IS NOT NULL THEN to_json(value_float)::VARCHAR
                WHEN value_int IS NOT NULL THEN to_json(value_int)::VARCHAR
                ELSE to_json(value_str)::VARCHAR
            END"""
VALUE_INPUT = """value_float::DOUBLE AS value_float,
            value_int::BIGINT AS value_int,
            value_str::VARCHAR AS value_str,"""
VALUE_VARIANT = """CASE WHEN value_float IS NOT NULL THEN value_float::VARIANT
 WHEN value_int IS NOT NULL THEN value_int::VARIANT ELSE value_str::VARIANT END"""


def variant_sql(sql):
    if sql.count(VALUE_JSON) != 1 or sql.count(VALUE_INPUT) != 1:
        raise ValueError("production value SQL changed; inspect before adapting benchmark")
    return sql.replace(VALUE_JSON, "to_json(stored_value)::VARCHAR").replace(
        VALUE_INPUT, "value AS stored_value,")


def write_sql(source, path, variant):
    projection = ("* EXCLUDE(value_float,value_int,value_str), " + VALUE_VARIANT + " AS value") if variant else "*"
    return f"""WITH written AS (COPY (SELECT {projection} FROM {source}
 ORDER BY time,name) TO {sql_quote(path)}
 (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 122880))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'written'::VARCHAR AS name,
 Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM written"""


def file_info(path):
    f = pq.ParquetFile(path)
    return dict(bytes=path.stat().st_size, rows=f.metadata.num_rows,
                row_group_rows=[f.metadata.row_group(i).num_rows for i in range(f.metadata.num_row_groups)],
                physical_schema=str(f.schema),
                column_compressed_bytes={f.schema.column(i).path: sum(
                    f.metadata.row_group(j).column(i).total_compressed_size
                    for j in range(f.metadata.num_row_groups)) for i in range(len(f.schema))})


def semantic_fixture(path):
    fixture(path)
    table = pq.read_table(path)
    # Includes simultaneous nonnull columns to verify historical precedence.
    values = [(1.25,None,None), (-0.0,None,None), (float('nan'),None,None),
              (float('inf'),None,None), (-float('inf'),None,None),
              (1e-300,None,None), (1e300,None,None),
              (None,9007199254740993,None), (None,-9223372036854775808,None),
              (None,9223372036854775807,None), (None,None,'123'),
              (None,None,'true'), (None,None,''), (None,None,'中文"\\\n'),
              (None,None,None), (1.25,123,'ignored'), (None,123,'ignored')]
    for j, (name, dtype) in enumerate(zip(('value_float','value_int','value_str'),
                                        (pa.float64(),pa.int64(),pa.string()))):
        table = table.set_column(table.schema.get_field_index(name), name,
                                pa.array([values[i % len(values)][j] for i in range(len(table))], type=dtype))
    pq.write_table(table,path)
    return len(table)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-probe', type=Path, required=True)
    parser.add_argument('--sql-probe', type=Path, required=True)
    parser.add_argument('--archive-day', type=Path, default=HERE.parents[1]/'data/archive/date=2025-03-01')
    parser.add_argument('--memory', default='3GB')
    parser.add_argument('--reps', type=int, default=3)
    parser.add_argument('--stress-rows', type=int, default=2000000)
    parser.add_argument('--match', default='')
    parser.add_argument('--output', type=Path, default=HERE/'results/exp68_value_variant.json')
    args = parser.parse_args()
    native, exporter = args.native_probe.resolve(), args.sql_probe.resolve()
    files = sorted(args.archive_day.glob('*.parquet'))
    if not native.is_file() or not exporter.is_file() or not files or args.reps < 1 or args.stress_rows < 1:
        parser.error('existing probes/archive and positive repetitions/rows required')
    case_names = [x for x in ('minute','hour','day','mixed_hour','stress') if args.match in x]
    if not case_names:
        parser.error('no matching cases')
    report = dict(scope='DuckDB 2.0 native Go streaming + Zstd IPC, local rewritten Parquet only; no Iceberg/network',
                  memory_limit=args.memory, reps=args.reps, results=[], summaries=[], profiles=[], datasets={},
                  native_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),
                  exporter_sha256=hashlib.sha256(exporter.read_bytes()).hexdigest(),
                  primary_timings='unprofiled, alternating layout order, checksums and rewrite excluded; warm OS cache, fresh engine each run',
                  parquet_settings=dict(compression='zstd',level=3,row_group_size=122880,order_by=['time','name']),
                  limitations=['single local file per dataset; not HTTP/Iceberg or month scale',
                               'rewrites include all columns; no direct VARIANT wire schema'])
    def save():
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    with tempfile.TemporaryDirectory(prefix='sage-exp68-') as directory:
        root = Path(directory)
        query = dict(start='2025-03-01T00:00:00Z',end='2025-03-02T00:00:00Z',filter={'name':'wxt.env.temp','vsn':'W0A0'})
        cases = [dict(Name='day',Query=query),
                 dict(Name='minute',Query=dict(query,end='2025-03-01T00:01:00Z')),
                 dict(Name='hour',Query=dict(query,end='2025-03-01T01:00:00Z')),
                 dict(Name='mixed_hour',Query=dict(query,end='2025-03-01T01:00:00Z',filter={'vsn':'W0A0'})),
                 dict(Name='stress',Query=dict(start=query['start'],end=query['end'])),
                 dict(Name='semantics',Query=dict(start=query['start'],end=query['end']))]
        (root/'cases.json').write_text(json.dumps(cases))
        monitored([str(exporter),'-test.run=^TestColdProjectionProbe$','-test.count=1'],
                  dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(root/'cases.json')))
        templates = {c['Name']:(root/(c['Name']+'-optimized.sql')).read_text() for c in cases}
        sources = {}
        archive = 'read_parquet(['+','.join(sql_quote(p.resolve()) for p in files)+'], hive_partitioning=false)'
        sources['day'] = f"(SELECT * FROM {archive} WHERE name='wxt.env.temp' AND vsn='W0A0')"
        sources['mixed_hour'] = f"(SELECT * FROM {archive} WHERE vsn='W0A0' AND CAST(time AS TIMESTAMPTZ_NS) >= CAST(TIMESTAMPTZ '2025-03-01 00:00:00+00' AS TIMESTAMPTZ_NS) AND CAST(time AS TIMESTAMPTZ_NS) < CAST(TIMESTAMPTZ '2025-03-01 01:00:00+00' AS TIMESTAMPTZ_NS))"
        n = args.stress_rows
        sources['stress'] = f"""(SELECT TIMESTAMPTZ '2025-03-01 00:00:00+00' + i*INTERVAL '1 microsecond' AS time,
 'stress' AS name,CASE WHEN i%3=0 THEN i*0.1+0.125 END::DOUBLE AS value_float,
 CASE WHEN i%3=1 THEN 9007199254740993+i END::BIGINT AS value_int,
 CASE WHEN i%3=2 THEN 'str-'||md5(i::VARCHAR) END::VARCHAR AS value_str,
 MAP {{'sensor':'test','unique':repeat(md5(i::VARCHAR),4)}} AS meta,
 'test:1.0' AS plugin,'W0A0' AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM range({n}) t(i))"""
        path = root/'semantics.parquet'
        expected_count = semantic_fixture(path)
        sources['semantics'] = f'read_parquet({sql_quote(path)})'
        needed = {'semantics'} | {('day' if c in ('minute','hour','day') else c) for c in case_names}
        datasets = {}
        for dataset in sorted(needed):
            datasets[dataset] = {}
            for mode in ('split','variant'):
                path = root/f'{dataset}-{mode}.parquet'
                native_table(native,write_sql(sources[dataset],path,mode=='variant'),args.memory,root/f'write-{dataset}-{mode}')
                datasets[dataset][mode] = path
                report['datasets'].setdefault(dataset,{})[mode] = file_info(path)
            infos = report['datasets'][dataset]
            if infos['split']['row_group_rows'] != infos['variant']['row_group_rows']:
                raise AssertionError('row group layout differs')
            print(dataset,'bytes', {m:i['bytes'] for m,i in infos.items()},flush=True)
            save()
        # Compare original response against both persisted layouts exactly.
        expected = native_table(native,templates['semantics'].replace(MARKER,sources['semantics']),args.memory,root/'expected')
        semantic = {}
        for mode,path in datasets['semantics'].items():
            sql = templates['semantics'] if mode=='split' else variant_sql(templates['semantics'])
            table = native_table(native,sql.replace(MARKER,f'read_parquet({sql_quote(path)})'),args.memory,root/f'semantic-{mode}')
            semantic[mode] = dict(rows=len(table),exact_public_table_equal=table.equals(expected))
            if len(table)!=expected_count or not table.equals(expected):
                raise AssertionError(f'exact public semantic fixture failed: {mode}')
        report['semantic_checks'] = semantic
        for case in case_names:
            dataset = 'day' if case in ('minute','hour','day') else case
            sqls = {mode:(templates[case] if mode=='split' else variant_sql(templates[case])).replace(
                MARKER,f'read_parquet({sql_quote(path)})') for mode,path in datasets[dataset].items()}
            reference = None
            for rep in range(args.reps):
                for mode in (('split','variant') if rep%2==0 else ('variant','split')):
                    run = run_probe(native,'2.0.0',sqls[mode],args.memory,8,False)
                    if reference is None: reference = signature(run)
                    if run['rows'] == 0: raise AssertionError(f'empty benchmark case: {case}')
                    if signature(run)!=reference: raise AssertionError(f'content/schema/order differs: {case} {mode}')
                    report['results'].append(dict(case=case,mode=mode,repetition=rep+1,**run))
                    save()
                    print(case,mode,f"first={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s",flush=True)
            for mode in sqls:
                runs = [r for r in report['results'] if r['case']==case and r['mode']==mode]
                report['summaries'].append(dict(case=case,mode=mode,rows=runs[0]['rows'],
                    total_seconds=statistics.median(r['total_seconds'] for r in runs),
                    first_batch_seconds=statistics.median(r['first_batch_seconds'] for r in runs),
                    peak_process_rss_bytes=max(r['peak_process_rss_bytes'] for r in runs)))
                if case in ('day','mixed_hour','stress'):
                    run = run_probe(native,'2.0.0',sqls[mode],args.memory,8,True)
                    if signature(run)!=reference: raise AssertionError('profiled result differs')
                    report['profiles'].append(dict(case=case,mode=mode,**run))
            save()
    print('Report:',args.output)


if __name__=='__main__':
    main()
