from email.message import Message
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.ipc as ipc

from exp73_plugin_live_compare import receive,validate_body,public_schema,full_warehouse_cases,full_warehouse_coverage,validate_candidate_audit


class TestLiveLayoutComparison(unittest.TestCase):
    def full_audits(self):
        total=8_457_186_954;day_ns=86_400_000_000_000
        files=[dict(path=str(day),rows=total//61+(day-20148<total%61),
                    min_timestamp_ns=day*day_ns,max_timestamp_ns=(day+1)*day_ns-1)
               for day in range(20148,20209)]
        base=dict(layout='time',value_type='variant',value_field_id=11,rows=total,snapshots=61,
                  min_timestamp_ns=files[0]['min_timestamp_ns'],max_timestamp_ns=files[-1]['max_timestamp_ns'],file_time_ranges=files)
        candidate=dict(base,layout='plugin-vsn-time',mixed_plugin_groups=0,ordering_checked=True,file_row_limit=10_000_000,
                       batch_order='time',time_batch_ranges_checked=True,row_group_row_limit=262144,
                       max_row_group_rows=262144,bounded_plugin_groups=True,plugin_pure_groups=True)
        return base,candidate

    def test_full_coverage_checks_every_day_and_retained_policy(self):
        import copy
        base,candidate=self.full_audits()
        validate_candidate_audit(candidate,full_warehouse=True)
        self.assertEqual(full_warehouse_coverage(base,candidate)['days'],61)
        with self.assertRaisesRegex(ValueError,'March-1-only'):validate_candidate_audit(candidate)
        changed=copy.deepcopy(candidate)
        changed['file_time_ranges'][0]['rows']+=1;changed['file_time_ranges'][1]['rows']-=1
        with self.assertRaisesRegex(ValueError,'daily row counts'):full_warehouse_coverage(base,changed)
        changed=copy.deepcopy(candidate);changed['file_time_ranges'].pop()
        with self.assertRaisesRegex(ValueError,'all 61 days'):full_warehouse_coverage(base,changed)
        with self.assertRaisesRegex(ValueError,'retained'):full_warehouse_coverage(base,dict(candidate,row_group_row_limit=100000))
        changed=copy.deepcopy(candidate);changed['file_time_ranges'][0]['max_timestamp_ns']+=1
        with self.assertRaisesRegex(ValueError,'single-day'):full_warehouse_coverage(base,changed)

    def test_full_suite_samples_both_months_and_crosses_boundary(self):
        from datetime import datetime
        selected=dict(full_warehouse_cases('plugin'))
        self.assertEqual(len(selected),39)
        for day in ('2025-03-01','2025-03-31','2025-04-01','2025-04-30'):
            self.assertEqual(selected[day+'_plugin_hour']['filter'],{'plugin':'plugin'})
            self.assertEqual(selected[day+'_series_day']['start'][:10],day)
        march=selected['2025-03_series_month'];april=selected['2025-04_series_month']
        self.assertEqual(march['end'],april['start'])
        self.assertEqual((datetime.fromisoformat(april['end'])-datetime.fromisoformat(april['start'])).days,30)
        boundary=selected['month_boundary_fleet_hour']
        self.assertNotEqual(boundary['start'][:7],boundary['end'][:7])

    def test_arrow_http_body_is_spooled_and_counted_without_retaining_batches(self):
        schema=pa.schema([pa.field('timestamp',pa.timestamp('ns')),pa.field('name',pa.string()),
                          pa.field('value',pa.string()),pa.field('meta',pa.string())])
        table=pa.Table.from_arrays([pa.array([1740787200026403194],type=pa.timestamp('ns')),
                                    pa.array(['test']),pa.array(['9007199254740993']),pa.array(['{}'])],schema=schema)
        sink=pa.BufferOutputStream()
        with ipc.new_stream(sink,schema,options=ipc.IpcWriteOptions(compression='zstd')) as writer:writer.write_table(table)
        payload=sink.getvalue().to_pybytes()
        class Response(io.BytesIO):pass
        response=Response(payload);response.headers=Message()
        response.headers['Content-Type']='application/vnd.apache.arrow.stream'
        response.headers['X-Arrow-Compression']='zstd'
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'body'
            result=receive('http://test',{},'arrow',path,30,lambda *a,**k:response)
            self.assertEqual(result['rows'],1);self.assertEqual(result['bytes'],len(payload))
            self.assertEqual(path.read_bytes(),payload)
            self.assertEqual(validate_body(path,'arrow')['rows'],1)

    def test_both_formats_preserve_large_ints_nanos_and_metadata_keys(self):
        stamps=[1740787200026403194,1740787200026403195,1740787200026403196]
        values=['9007199254740993','"123"','true']
        table=pa.table({'timestamp':pa.array(stamps,type=pa.timestamp('ns')),'name':['test']*3,
                        'value':values,'meta':['{"z":"last","a":"first"}']*3})
        public_schema(table.schema)
        with tempfile.TemporaryDirectory() as directory:
            arrow=Path(directory)/'body.arrow';ndjson=Path(directory)/'body.json'
            with arrow.open('wb') as stream,ipc.new_stream(stream,table.schema,options=ipc.IpcWriteOptions(compression='zstd')) as writer:
                writer.write_table(table)
            ndjson.write_text('\n'.join(json.dumps(dict(timestamp=f'2025-03-01T00:00:00.026403{194+i}Z',name='test',
                              value=json.loads(value),meta={'a':'first','z':'last'})) for i,value in enumerate(values))+'\n')
            self.assertEqual(validate_body(arrow,'arrow'),validate_body(ndjson,'ndjson'))
            self.assertEqual(validate_body(arrow,'arrow')['rows'],3)
            with self.assertRaises(ValueError):public_schema(table.schema.remove(2))

    def test_first_ndjson_line_is_measured_before_large_buffer_read(self):
        clock={'now':0}
        class Response(io.BytesIO):
            headers=Message()
            def readline(self,*args):clock['now']=1;return super().readline(*args)
            def read(self,*args):clock['now']=10;return super().read(*args)
        with tempfile.TemporaryDirectory() as directory:
            response=Response(b'{"a":1}\n{"b":2}\n')
            with patch('exp73_plugin_live_compare.time.perf_counter',side_effect=lambda:clock['now']):
                result=receive('http://test',{},'ndjson',Path(directory)/'body',30,lambda *a,**k:response)
            self.assertEqual(result['rows'],2)
            self.assertEqual(result['first_result_seconds'],1)
            self.assertEqual(result['total_seconds'],10)

    def test_validation_rejects_timestamp_disorder(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'body'
            path.write_text('\n'.join(json.dumps(dict(timestamp='2025-03-01T00:00:00.'+n+'Z',name='x',value=1,meta={}))
                                      for n in ['000000002','000000001'])+'\n')
            with self.assertRaisesRegex(ValueError,'ordering'):validate_body(path,'ndjson')


if __name__=='__main__':unittest.main()
