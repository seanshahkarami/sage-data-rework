"""Read-only wire and Sage-client compatibility check for the adopted warehouse.

Fetch each format once, validate the actual public wire body, then replay that
same body through the workspace client's public query entry point. No database,
Docker, catalog, or storage mutations. Not a performance benchmark.
"""
import argparse
from email.message import Message
import hashlib
import importlib
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

import pandas as pd

HERE=Path(__file__).resolve().parent
WORKSPACE=HERE.parents[2]
sys.path.insert(0,str(HERE))
sys.path.insert(0,str(WORKSPACE/'sage-data-client/src'))
from exp73_plugin_live_compare import receive,validate_body,cases

client=importlib.import_module('sage_data_client.query')


def client_frame(path,query,fmt):
    # Replay the exact fetched wire bytes, including the real MIME/codec
    # contract already checked by receive(). The public client still performs
    # its own request construction and response-format dispatch.
    class Response:
        def __init__(self):
            self.body=path.open('rb');self.headers=Message()
            self.headers['Content-Type']='application/vnd.apache.arrow.stream' if fmt=='arrow' else 'application/x-ndjson'
            if fmt=='arrow':self.headers['X-Arrow-Compression']='zstd'
        def __enter__(self):return self
        def __exit__(self,*args):self.body.close()
        def read(self,n=-1):return self.body.read(n)
        def readable(self):return True
        @property
        def closed(self):return self.body.closed
        def __iter__(self):return iter(self.body)
    def replay(request):
        sent=json.loads(request.data)
        for bound in ('start','end'):
            if bound in query and pd.to_datetime(sent[bound],utc=True).value!=pd.to_datetime(query[bound],utc=True).value:
                raise ValueError('client changed the '+bound+' request instant')
        return Response()
    with patch.object(client,'urlopen',side_effect=replay):
        return client.query(endpoint='http://captured-response/api/v1/query',**query)


def frame_signature(frame):
    """Compare decoded frames regardless of row ties and metadata column order."""
    required={'timestamp','name','value'}
    if not required<=set(frame) or any(c not in required and not c.startswith('meta.') for c in frame):
        raise ValueError('unexpected dataframe schema')
    if str(frame.timestamp.dt.tz)!='UTC':raise ValueError('client timestamp timezone is not UTC')
    if not frame.timestamp.is_monotonic_increasing:raise ValueError('client timestamp ordering changed')
    # pandas hashing normalizes both frames to the same Python-backed columns,
    # but retains the value column's inferred dtype to catch type regressions.
    normalized=frame.reindex(columns=sorted(frame.columns)).copy()
    normalized['timestamp']=pd.to_datetime(frame.timestamp).astype('datetime64[ns, UTC]')
    for column in normalized:
        if column!='timestamp':normalized[column]=normalized[column].astype(object).where(normalized[column].notna(),None)
    hashes=pd.util.hash_pandas_object(normalized,index=False).to_numpy()
    import numpy as np
    return dict(rows=len(frame),columns=sorted(frame.columns),value_dtype=str(frame.value.dtype),
                hash_sum=str(int(hashes.sum(dtype=np.uint64))),hash_xor=str(int(np.bitwise_xor.reduce(hashes,initial=np.uint64(0)))))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint',required=True)
    p.add_argument('--output',type=Path,required=True,help='fresh result filename')
    p.add_argument('--include-day',action='store_true',help='also decode the full March 1 series day')
    a=p.parse_args()
    if a.output.exists():p.error('use a fresh output file')
    selected=[('2025-03-01_series_minute',dict(cases('unused'))['series_minute']),
              ('2025-03-01_series_hour',dict(cases('unused'))['series_hour'])]
    for day in ('2025-03-31','2025-04-01','2025-04-30'):
        query=dict(selected[0][1],start=day+'T00:00:00Z',end=day+'T00:01:00Z')
        selected.append((day+'_series_minute',query))
    base=selected[0][1]
    selected += [('head',dict(base,head=2)),('tail',dict(base,tail=2)),
                 ('exclusive_nanosecond_bound',dict(base,end='2025-03-01T00:00:00.026403195Z')),
                 ('empty',dict(base,filter={'name':'__sage_v3_compatibility_absent__','vsn':'W0A0'}))]
    if a.include_day:selected.append(('series_day',dict(cases('unused'))['series_day']))
    report=dict(status='running',endpoint=a.endpoint,scope='live HTTP Arrow/NDJSON wire validation plus public workspace-client replay of the same bodies',
                client_source=str(Path(client.__file__).resolve()),client_sha256=hashlib.sha256(Path(client.__file__).read_bytes()).hexdigest(),
                client_files_sha256={path.name:hashlib.sha256(path.read_bytes()).hexdigest()
                                     for path in sorted(Path(client.__file__).parent.glob('*.py'))},results=[])
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        with tempfile.TemporaryDirectory(prefix='sage-v3-client-') as directory:
            path=Path(directory)/'body'
            for name,query in selected:
                wire_reference=frame_reference=None
                for fmt in ('arrow','ndjson'):
                    received=receive(a.endpoint,query,fmt,path,1800)
                    wire=validate_body(path,fmt)
                    if received['rows']!=wire['rows']:raise ValueError('wire row count differs')
                    if wire_reference is None:wire_reference=wire
                    if wire!=wire_reference:raise ValueError(name+': Arrow/NDJSON wire mismatch')
                    frame=client_frame(path,query,fmt)
                    signature=frame_signature(frame)
                    if len(frame)!=wire['rows']:raise ValueError('client row count differs')
                    if frame_reference is None:frame_reference=signature
                    if signature!=frame_reference:raise ValueError(name+': Arrow/NDJSON client mismatch')
                    if name=='empty' and len(frame):raise ValueError('empty query returned rows')
                    if name!='empty' and not len(frame):raise ValueError('missing expected warehouse data')
                    if name=='2025-03-01_series_minute':
                        if len(frame)!=698 or frame.timestamp.iloc[0].value!=1740787200026403194 or frame.value.iloc[0]!=11.7:
                            raise ValueError('published warehouse reference row changed')
                    if name=='exclusive_nanosecond_bound' and (len(frame)!=1 or frame.timestamp.iloc[0].value!=1740787200026403194):
                        raise ValueError('exclusive nanosecond bound changed')
                    report['results'].append(dict(case=name,format=fmt,query=query,wire=wire,client=signature,dtypes={c:str(frame[c].dtype) for c in frame}));save()
                    print(name,fmt,len(frame),'passed',flush=True)
        report['status']='complete';save()
    except Exception as exc:
        report['status']='failed';report['error']=str(exc);save();raise


if __name__=='__main__':main()
