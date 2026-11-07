"""Prepare and validate a tiny actual local Iceberg v3 aggregation fixture.

Writes only a fresh private local directory; no REST/Pelican/Influx service or
production API changes. Generates an opt-in Go HTTP/lease probe manifest.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent))
import exp86_aggregation_semantics as base
import exp91_variant_encoding as uint_encoding

TAGS=('plugin','vsn','node','host','task','job','zone')
FIELD_IDS="{'time':1,'name':2,'plugin':3,'vsn':4,'node':5,'host':6,'task':7,'job':8,'zone':9,'meta':{__duckdb_field_id:10,key:12,value:13},'value':11}"


def full_meta():
    promoted='map_from_entries(list_filter(list_zip(['+','.join(base.quote(x) for x in TAGS)+'],['+','.join(TAGS)+']),lambda x:x[2] IS NOT NULL))'
    return 'map_from_entries(list_sort(map_entries(map_concat(meta,'+promoted+'))))'


def input_cases(integer_path,baseline_path,exclude_unsigned=False):
    integer=json.loads(integer_path.read_text())
    if integer.get('experiment')!='exp88' or not integer.get('completed') or integer.get('summary',{}).get('FAIL')!=0:raise ValueError('passing exp88 report required')
    rows=json.loads(integer_path.with_name('fixture.json').read_text())
    cases=[dict(case=dict(x['case'],id='integer-'+x['case']['id']),expected=x['flux']['rows']) for x in integer['results']]
    if baseline_path:
        baseline=json.loads(baseline_path.read_text())
        if baseline.get('experiment')!='exp86' or not baseline.get('completed') or not baseline.get('raw_fixture_verified'):raise ValueError('completed original exp86 report required')
        rows.extend(r for r in json.loads(baseline_path.with_name('fixture.json').read_text()) if r['name']!='exp86.mixed')
        for entry in baseline['results']:
            if entry['case'].get('oracle_only') or entry['case']['name']=='exp86.mixed' or entry['flux']['status']!='success':continue
            cases.append(dict(case=dict(entry['case'],id='baseline-'+entry['case']['id']),expected=entry['flux']['rows']))
    for row in rows:
        if type(row['time']) is not int:raise ValueError('integer timestamp required')
    if len({x['case']['id'] for x in cases})!=len(cases):raise ValueError('duplicate case id')
    for item in cases:
        if not re.fullmatch(r'[A-Za-z0-9_-]+',item['case']['id']):raise ValueError('unsafe case id')
    excluded=[]
    if exclude_unsigned:
        names={r['name'] for r in rows if r['kind']=='uint'}
        excluded=[x['case']['id'] for x in cases if x['case']['name'] in names]
        rows=[r for r in rows if r['name'] not in names]
        cases=[x for x in cases if x['case']['name'] not in names]
    return rows,cases,excluded


def raw_source(metadata,case,placement,hot,unsigned_encoding='native'):
    value=uint_encoding.decode_value() if unsigned_encoding=='decimal20' else 'value'
    cold='SELECT CAST(time AT TIME ZONE \'UTC\' AS TIMESTAMP_NS) AS t,name,'+full_meta()+' AS meta,'+value+' AS value FROM iceberg_scan('+base.quote(metadata)+')'
    if placement=='cold_only':return cold
    cold+=' WHERE epoch_ns(time)<'+str(case['border'])
    types=[('int','BIGINT'),('uint','UBIGINT'),('float','DOUBLE'),('bool','BOOLEAN'),('string','VARCHAR')]
    value='CASE kind '+' '.join('WHEN '+base.quote(k)+' THEN text::'+t+'::VARIANT' for k,t in types)+' END'
    return cold+' UNION ALL SELECT make_timestamp_ns(time_ns),name,map_from_entries(list_sort(map_entries(meta))),'+value+' FROM read_parquet('+base.quote(hot)+')'


def response_sql(case,source):
    inner=base.aggregate_sql(case,'source',integer_policy='flux64').rstrip(';')
    return 'WITH source AS ('+source+') SELECT make_timestamp_ns(time) AS timestamp,name,value,meta FROM ('+inner+') ORDER BY timestamp,name,meta;'


def run(args):
    if args.exclude_unsigned and args.unsigned_encoding!='native':raise ValueError('unsigned encoding and unsigned exclusion are mutually exclusive')
    root=Path(args.run_dir).resolve();integer=Path(args.integer_report).resolve()
    baseline=Path(args.baseline_report).resolve() if args.baseline_report else None
    if root.exists() or any('node-data' in p.parts for p in [root,integer,*([baseline] if baseline else [])]):raise ValueError('fresh isolated preparation and sources required')
    rows,cases,excluded=input_cases(integer,baseline,args.exclude_unsigned)
    if args.unsigned_encoding=='decimal20':rows.extend(uint_encoding.boundary_rows())
    root.mkdir(parents=True,mode=0o700);args._owns_root=True;(root/'data').mkdir();(root/'queries').mkdir()
    duck=base.Duck(args.duckdb,root);version=duck.version()
    if args.unsigned_encoding=='decimal20':uint_encoding.check_invalid_values(duck)
    (root/'fixture.json').write_text(json.dumps(rows,indent=2)+'\n')
    groups={}
    for row in rows:groups.setdefault((row['time']//86_400_000_000_000,row['meta']['plugin']),[]).append(row)
    files=[]
    for index,(_,group) in enumerate(sorted(groups.items())):
        path=root/'data'/f'{index:03d}.parquet'
        stored_value=uint_encoding.encode_value() if args.unsigned_encoding=='decimal20' else 'value'
        projection='t::TIMESTAMPTZ_NS AS time,name,'+','.join('meta['+base.quote(x)+'] AS '+x for x in TAGS)+',map_from_entries(list_filter(map_entries(meta),lambda x:x.key NOT IN ('+','.join(base.quote(x) for x in TAGS)+'))) AS meta,'+stored_value+' AS value'
        sql="SET TimeZone='UTC';"+base.source_sql(group)+'COPY (SELECT '+projection+' FROM full_raw ORDER BY plugin,vsn,time) TO '+base.quote(path)+' (FORMAT PARQUET,COMPRESSION ZSTD,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 262144,FIELD_IDS '+FIELD_IDS+');'
        duck.run(sql,'write-'+str(index));files.append(str(path))
    writer_input=dict(root=str(root/'warehouse'),tables=[dict(name='full',files=files)])
    if args.unsigned_encoding=='decimal20':writer_input['value_encoding']=uint_encoding.ENCODING
    input_path=root/'fixture-input.json';input_path.write_text(json.dumps(writer_input,indent=2)+'\n')
    result=subprocess.run([args.fixture_writer,str(input_path)],text=True,capture_output=True,timeout=120)
    (root/'writer.stdout').write_text(result.stdout);(root/'writer.stderr').write_text(result.stderr)
    if result.returncode:raise RuntimeError('fixture writer failed; see writer.stderr')
    metadata=json.loads((root/'warehouse'/'tables.json').read_text())['full']
    actual=Path(metadata.removeprefix('file://')).resolve()
    if not actual.is_relative_to(root/'warehouse'):raise ValueError('metadata outside private fixture')
    header=json.loads(actual.read_text());schema=header['schemas'][-1]
    uint_encoding.check_marker(header,args.unsigned_encoding)
    fields={x['name']:x for x in schema['fields']}
    if header['format-version']!=3 or fields['value']['id']!=11 or fields['value']['type']!='variant' or fields['time']['type']!='timestamptz_ns':raise ValueError('Iceberg schema/version differs')
    # Prove all original values, kinds, nanoseconds and full tags survived publication.
    raw='SELECT epoch_ns(t) AS time,name,to_json(meta)::VARCHAR AS meta,'+'CASE '+base.kind_sql()+' '+' '.join('WHEN '+base.quote(k)+' THEN to_json(value::'+t+')::VARCHAR' for k,t in [('int','BIGINT'),('uint','UBIGINT'),('float','DOUBLE'),('bool','BOOLEAN'),('string','VARCHAR')])+' END AS value,'+base.kind_sql()+' AS kind FROM ('+raw_source(metadata,{},'cold_only',None,args.unsigned_encoding)+')'
    observed,_=duck.run('LOAD iceberg;'+raw,'raw-iceberg')
    expected=[dict(time=r['time'],name=r['name'],meta=r['meta'],kind=r['kind'],value=r['value']) for r in rows]
    difference=base.compare(expected,base.duck_records(observed))
    if difference:raise AssertionError('raw Iceberg mismatch: '+difference)
    manifest=dict(unsigned_encoding=args.unsigned_encoding,excluded_unsigned_cases=excluded,scope='Experimental precompiled SQL + actual Iceberg + production HTTP encoders and read-grant wrapper; production aggregation routing/hot fetch not enabled',
        metadata=metadata,root=str(root),cases=[])
    report=dict(experiment='exp89',unsigned_encoding=args.unsigned_encoding,duckdb_version=version,raw_rows_verified=len(rows),metadata=metadata,excluded_unsigned_cases=excluded,
        integer_report_sha256=hashlib.sha256(integer.read_bytes()).hexdigest(),
        compiler_sha256=hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest(),
        encoding_sha256=hashlib.sha256(Path(uint_encoding.__file__).read_bytes()).hexdigest(),
        scope='Actual local Iceberg v3 CLI checks; Go HTTP/lease probe not yet executed',results=[],completed=False)
    if baseline:report['baseline_report_sha256']=hashlib.sha256(baseline.read_bytes()).hexdigest()
    save=lambda:(root/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    save()
    for item in cases:
        case=item['case']
        for placement in ('cold_only','split'):
            stem=case['id']+'-'+placement
            hot=root/'data'/(stem+'-hot.parquet')
            if placement=='split':
                setup,_=base.placement_sql(rows,case,hot.parent,'split')
                generated=hot.parent/(case['id']+'-hot.parquet')
                # placement_sql owns a unique per-case path. The cold-only pass
                # never writes it; no previous preparation is adopted.
                duck.run(setup,'stage-'+stem);hot=generated
            source=raw_source(metadata,case,placement,hot,args.unsigned_encoding)
            typed='WITH source AS ('+source+') SELECT * FROM ('+base.aggregate_sql(case,'source',integer_policy='flux64').rstrip(';')+')'
            typed_rows,_=duck.run('LOAD iceberg;'+typed,'typed-'+stem)
            typed_difference=base.compare(item['expected'],base.duck_records(typed_rows))
            statement=response_sql(case,source)
            sql_path=root/'queries'/(stem+'.sql');sql_path.write_text(statement)
            comparison='WITH response AS ('+statement.rstrip(';')+') SELECT epoch_ns(timestamp) AS time,name,meta,value,\'other\' AS kind FROM response'
            out,seconds=duck.run('LOAD iceberg;'+comparison,'scan-'+stem)
            actual_rows=base.duck_records(out)
            # Classify the actual JSON scalar, never borrow a type from the
            # expected result. This catches integer-to-float serialization.
            for record in actual_rows:
                value=record['value']
                record['kind']=('other' if value is None else 'bool' if isinstance(value,bool)
                    else 'int' if isinstance(value,int) else 'float' if isinstance(value,float) else 'string')
            # Public JSON has a single integer kind. Check signedness above at
            # the typed SQL boundary, then independently classify public values.
            public_expected=[dict(r,kind='int' if r['kind']=='uint' else r['kind']) for r in item['expected']]
            difference=base.compare(public_expected,actual_rows)
            report['results'].append(dict(case=stem,passed=difference is None and typed_difference is None,difference=difference,typed_difference=typed_difference,seconds=seconds))
            request=dict(start=base.stamp(case['start']),end=base.stamp(case['end']),filter={'name':case['name']},experimental_func=case['function'])
            if case['window']:request['experimental_window']=case['window']
            for limit in ('head','tail'):
                if limit in case:request[limit]=case[limit]
            manifest['cases'].append(dict(id=stem,request=request,sql=str(sql_path),
                border=base.stamp(case['end'] if placement=='cold_only' else case['border']),expected=item['expected']))
            save()
    report.update(completed=True,summary=dict(total=len(report['results']),passed=sum(x['passed'] for x in report['results'])))
    save();(root/'http-manifest.json').write_text(json.dumps(manifest,indent=2,allow_nan=False)+'\n')
    print(json.dumps(report['summary'],indent=2));print('Go HTTP manifest:',root/'http-manifest.json')
    return 0 if all(x['passed'] for x in report['results']) else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--unsigned-encoding',choices=('native','decimal20'),default='native',help='explicit experimental table-marked uint64 encoding; native baseline unchanged')
    parser.add_argument('--exclude-unsigned',action='store_true',help='explicitly limit fixture to series without uint inputs; record all exclusions')
    parser.add_argument('--duckdb',default='duckdb');parser.add_argument('--fixture-writer',required=True)
    parser.add_argument('--integer-report',required=True);parser.add_argument('--baseline-report');parser.add_argument('--run-dir',required=True)
    args=parser.parse_args();args._owns_root=False
    try: status=run(args)
    except Exception as error:
        if args._owns_root:
            path=Path(args.run_dir).resolve()/'failure.json'
            with path.open('x') as out:json.dump(dict(completed=False,error_type=type(error).__name__,error=str(error)[-4000:]),out,indent=2)
        raise
    raise SystemExit(status)


if __name__=='__main__':main()
