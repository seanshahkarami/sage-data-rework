import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

from exp81_v3_client_compatibility import client_frame,frame_signature


class TestV3ClientCompatibility(unittest.TestCase):
    def test_public_client_replay_preserves_mixed_types_nanos_and_metadata(self):
        values=[21.5,9007199254740993,'123',True,None]
        stamps=[1740787200026403194+i for i in range(len(values))]
        table=pa.table({'timestamp':pa.array(stamps,type=pa.timestamp('ns')),
                        'name':['test']*5,'value':[json.dumps(v) for v in values],
                        'meta':['{"plugin":"p","vsn":"W0A0"}']*5})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'body'
            with path.open('wb') as stream,ipc.new_stream(stream,table.schema,options=ipc.IpcWriteOptions(compression='zstd')) as writer:writer.write_table(table,max_chunksize=2)
            query={'start':'2025-03-01T00:00:00Z','end':'2025-03-02T00:00:00Z'}
            arrow=client_frame(path,query,'arrow')
            rows=[dict(timestamp=pd.Timestamp(stamp,tz='UTC').isoformat(),name='test',value=value,meta={'plugin':'p','vsn':'W0A0'}) for stamp,value in zip(stamps,values)]
            path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
            ndjson=client_frame(path,query,'ndjson')
            self.assertEqual(frame_signature(arrow),frame_signature(ndjson))
            for frame in (arrow,ndjson):
                self.assertEqual([stamp.value for stamp in frame.timestamp],stamps)
                self.assertEqual(frame.value.tolist(),values)
                self.assertEqual([type(v) for v in frame.value], [type(v) for v in values])
                self.assertEqual(frame['meta.vsn'].tolist(),['W0A0']*5)

    def test_empty_response_contract(self):
        table=pa.table({'timestamp':pa.array([],type=pa.timestamp('ns')),'name':pa.array([],type=pa.string()),
                        'value':pa.array([],type=pa.string()),'meta':pa.array([],type=pa.string())})
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'body'
            with path.open('wb') as stream,ipc.new_stream(stream,table.schema) as writer:writer.write_table(table)
            query={'start':'2025-03-01T00:00:00Z'}
            arrow=client_frame(path,query,'arrow');path.write_bytes(b'')
            ndjson=client_frame(path,query,'ndjson')
            self.assertEqual(frame_signature(arrow),frame_signature(ndjson))


if __name__=='__main__':unittest.main()
