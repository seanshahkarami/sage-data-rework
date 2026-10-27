"""Execute current v3 response SQL on native DuckDB 2.0 with isolated Parquet.

This supplements, not replaces, the opt-in REST/HTTP Go integration test.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp67_metadata_projection import native_table,MARKER,fixture as split_fixture
from exp66_duckdb20_evaluation import sql_quote
sys.path.insert(0,str(HERE.parent/'demo/tools'))
from seed_cold import archive_select


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-probe',type=Path,required=True)
    parser.add_argument('--sql-probe',type=Path,required=True)
    parser.add_argument('--output',type=Path,default=HERE/'results/exp69_v3_validation.json')
    args=parser.parse_args()
    native,exporter=args.native_probe.resolve(),args.sql_probe.resolve()
    base=dict(start='2025-03-01T00:00:07.420753193Z',end='2025-03-01T00:00:08.000000003Z')
    expected={
        'all':([1740787207420753193,1740787207420753571,1740787208000000001,1740787208000000002],['21.5','9007199254740993','"123"','true']),
        'head':([1740787207420753193,1740787208000000001,1740787208000000002],['21.5','"123"','true']),
        'tail':([1740787207420753571,1740787208000000001,1740787208000000002],['9007199254740993','"123"','true']),
        'upper_ns':([1740787207420753193],['21.5']),
        'lower_ns':([1740787207420753571],['9007199254740993']),
        'empty':([],[]),
    }
    cases=[dict(Name='all',Query=base),dict(Name='head',Query=dict(base,head=1)),dict(Name='tail',Query=dict(base,tail=1)),
           dict(Name='upper_ns',Query=dict(base,end='2025-03-01T00:00:07.420753571Z')),
           dict(Name='lower_ns',Query=dict(base,start='2025-03-01T00:00:07.420753194Z',end='2025-03-01T00:00:08Z')),
           dict(Name='empty',Query=dict(base,start=base['end']))]
    report=dict(scope='native DuckDB 2.0, actual current Go v3 SQL, stored VARIANT Parquet; no REST/HTTP transport test',checks=[])
    with tempfile.TemporaryDirectory(prefix='sage-v3-sql-') as directory:
        root=Path(directory); data=root/'fixture.parquet'
        # Execute the publisher's actual ingestion SELECT, including its day
        # clip, against archive-shaped split values and nanosecond timestamps.
        import pyarrow.parquet as pq
        original=root/'archive.parquet'; split_fixture(original)
        source=pq.read_table(original)
        ingest=archive_select([original],'2025-03-01')
        conversion=native_table(native,f"SELECT CAST(time AT TIME ZONE 'UTC' AS TIMESTAMP_NS) AS timestamp,name,to_json(value)::VARCHAR AS value,to_json(meta)::VARCHAR AS meta FROM ({ingest})",'512MB',root/'ingest')
        if conversion.column('timestamp').cast('int64').to_pylist()!=source.column('time').cast('int64').to_pylist():
            raise AssertionError('ingestion changed timestamp digits or row count')
        for old,new in zip(source.to_pylist(),conversion.column('value').to_pylist()):
            expected_value=next((old[k] for k in ('value_float','value_int','value_str') if old[k] is not None),None)
            if (json.loads(new) if new is not None else None)!=expected_value:
                raise AssertionError('ingestion changed value type/content')
        report['ingestion_rows_checked']=len(conversion)
        (root/'cases.json').write_text(json.dumps(cases))
        monitored([str(exporter),'-test.run=^TestColdProjectionProbe$','-test.count=1'],
                  dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(root/'cases.json')))
        fixture="""SELECT time,name,value,MAP {}::MAP(VARCHAR,VARCHAR) AS meta,
 'test'::VARCHAR AS plugin,'W0A0'::VARCHAR AS vsn,NULL::VARCHAR AS node,NULL::VARCHAR AS host,
 NULL::VARCHAR AS task,NULL::VARCHAR AS job,NULL::VARCHAR AS zone FROM (VALUES
 (TIMESTAMPTZ_NS '2025-03-01 00:00:07.420753193+00','temperature',21.5::DOUBLE::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:07.420753571+00','temperature',9007199254740993::BIGINT::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:08.000000001+00','status','123'::VARIANT),
 (TIMESTAMPTZ_NS '2025-03-01 00:00:08.000000002+00','flag',true::VARIANT)) t(time,name,value)"""
        write=f"""WITH written AS (COPY ({fixture}) TO {sql_quote(data)} (FORMAT PARQUET,COMPRESSION ZSTD))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'write'::VARCHAR AS name,Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM written"""
        native_table(native,write,'512MB',root/'write')
        for case in cases:
            sql=(root/(case['Name']+'-optimized.sql')).read_text().replace(MARKER,'read_parquet('+sql_quote(data)+')')
            table=native_table(native,sql,'512MB',root/case['Name'])
            stamps=table.column('timestamp').cast('int64').to_pylist(); values=table.column('value').to_pylist()
            if (stamps,values)!=expected[case['Name']]: raise AssertionError((case['Name'],stamps,values))
            if table.column_names!=['timestamp','name','value','meta']: raise AssertionError('public schema changed')
            for meta in table.column('meta').to_pylist():
                if json.loads(meta)!={'plugin':'test','vsn':'W0A0'}: raise AssertionError('metadata changed')
            report['checks'].append(dict(case=case['Name'],rows=len(table),timestamps_ns=stamps,values=values,passed=True))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__': main()
