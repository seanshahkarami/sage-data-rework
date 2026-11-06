"""Decode immutable exp89 HTTP bodies through the actual Sage client.

No network/native engine/warehouse operations. Value comparisons distinguish
harmless dataframe dtype changes from integer precision loss. Never overwrites
an existing report or changes the client implementation.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys

HERE=Path(__file__).resolve().parent


def scalar(value):
    if hasattr(value,'item'):value=value.item()
    if value is None or isinstance(value,float) and math.isnan(value):return None
    return value


def compare_values(expected,actual):
    actual=scalar(actual)
    if expected is None:return None if actual is None else 'null changed'
    if isinstance(expected,int) and not isinstance(expected,bool):
        if not isinstance(actual,(int,float)) or isinstance(actual,bool) or actual!=expected:
            return 'integer precision/value changed'
        return None
    if isinstance(expected,float):
        if not isinstance(actual,(int,float)) or isinstance(actual,bool) or not math.isclose(expected,actual,rel_tol=1e-12,abs_tol=1e-10):return 'floating value changed'
        return None
    return None if expected==actual else 'scalar value changed'


def records(frame,pd):
    output=[]
    for i in range(len(frame)):
        metadata={}
        for col in frame.columns:
            if col.startswith('meta.'):
                value=frame[col].iloc[i]
                if not pd.isna(value):metadata[col[5:]]=str(value)
        output.append(dict(time=int(pd.Timestamp(frame['timestamp'].iloc[i]).value),name=str(frame['name'].iloc[i]),
            meta=metadata,value=None if pd.isna(frame['value'].iloc[i]) else scalar(frame['value'].iloc[i])))
    return output


def compare_rows(expected,actual):
    key=lambda r:(r['time'],r['name'],json.dumps(r['meta'],sort_keys=True))
    a,b=sorted(expected,key=key),sorted(actual,key=key)
    if len(a)!=len(b):return [dict(reason='row count changed',expected=len(a),actual=len(b))],0
    differences=[];integer_to_float=0
    for x,y in zip(a,b):
        if key(x)!=key(y):differences.append(dict(reason='identity/time/metadata changed',expected=x,actual=y));continue
        if isinstance(x['value'],int) and isinstance(y['value'],float):integer_to_float+=1
        difference=compare_values(x['value'],y['value'])
        if difference:differences.append(dict(reason=difference,time=x['time'],expected=x['value'],actual=y['value']))
    return differences,integer_to_float


def run(args):
    manifest=Path(args.manifest).resolve();responses=Path(args.responses).resolve();out=Path(args.output).resolve()
    source=Path(args.client_source).resolve()
    if any('node-data' in p.parts for p in (manifest,responses,out,source)) or out.exists():raise ValueError('isolated inputs and a new output file required')
    sys.path.insert(0,str(source))
    import pandas as pd
    import pyarrow as pa
    from sage_data_client import _arrow,_ndjson,_values
    if not Path(_arrow.__file__).resolve().is_relative_to(source):raise ValueError('wrong client checkout imported')
    raw=manifest.read_bytes();spec=json.loads(raw)
    report=dict(experiment='exp90',scope='Saved exp89 HTTP bodies through current Python decoders; no network or production changes',
        manifest_sha256=hashlib.sha256(raw).hexdigest(),pandas_version=pd.__version__,pyarrow_version=pa.__version__,
        client_sha256={m.__name__:hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest() for m in (_arrow,_ndjson,_values)},results=[])
    for case in spec['cases']:
        if not re.fullmatch(r'[A-Za-z0-9_-]+',case['id']):raise ValueError('unsafe case id')
        for format in ('ndjson','arrow'):
            path=responses/(case['id']+'.'+format)
            with path.open('rb') as file:frame=_arrow._load_arrow(file) if format=='arrow' else _ndjson.load(file)
            differences,coercions=compare_rows(case['expected'],records(frame,pd))
            report['results'].append(dict(case=case['id'],format=format,passed=not differences,
                value_dtype=str(frame['value'].dtype),integer_to_float_rows=coercions,differences=differences,
                response_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    report['summary']=dict(total=len(report['results']),passed=sum(x['passed'] for x in report['results']),
        failed=sum(not x['passed'] for x in report['results']),
        integer_precision_failures=sum(d['reason']=='integer precision/value changed' for x in report['results'] for d in x['differences']))
    out.parent.mkdir(parents=True,exist_ok=True)
    with out.open('x') as f:json.dump(report,f,indent=2,allow_nan=False);f.write('\n')
    print(json.dumps(report['summary'],indent=2))
    return 1 if report['summary']['failed'] else 0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',required=True);parser.add_argument('--responses',required=True);parser.add_argument('--output',required=True)
    parser.add_argument('--client-source',default=str(HERE.parents[2]/'sage-data-client/src'))
    raise SystemExit(run(parser.parse_args()))


if __name__=='__main__':main()
