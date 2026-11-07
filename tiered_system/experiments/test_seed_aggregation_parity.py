"""Native-kind seed preflight and loopback-only mutation guards, without services."""
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('parity_seed',Path(__file__).with_name('seed_aggregation_parity.py'))
e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)


class SeedTests(unittest.TestCase):
    def test_nonlocal_or_credential_urls_are_rejected_before_any_io(self):
        for url in ('https://data.sagecontinuum.org','http://user:password@localhost:8086','file://localhost/tmp'):
            with self.subTest(url=url),patch.object(e.base,'Influx') as client:
                with self.assertRaises(ValueError):e.run(SimpleNamespace(fixture_root='/not-read',run_dir='/not-created',influx_url=url,org='fixture'))
                client.assert_not_called()

    def exercise(self,annotation):
        with TemporaryDirectory() as directory:
            root=Path(directory);fixture=root/'fixture';fixture.mkdir();out=root/'out'
            (fixture/'report.json').write_text(json.dumps(dict(completed=True,summary=dict(passed=1,total=1))))
            (fixture/'http-manifest.json').write_text(json.dumps(dict(cases=[dict(request=dict(filter=dict(name='counter')))])))
            row=dict(time=1740787200000000194,name='counter',meta={'vsn':'A'},kind='uint',value=6)
            (fixture/'fixture.json').write_text(json.dumps([row,dict(row,name='unqueried-control')]))
            writes=[]
            class Client:
                def __init__(self,*args):pass
                def request(self,path):return b'{"version":"v2.7.7"}'
                def setup(self,rows):writes.extend(rows);return 'id'
                def query(self,flux):return '#datatype,string,long,dateTime:RFC3339,'+annotation+',string,string,string\n,result,table,_time,_value,_field,_measurement,vsn\n,,0,2025-03-01T00:00:00.000000194Z,6,value,counter,A\n'
            args=SimpleNamespace(fixture_root=str(fixture),run_dir=str(out),influx_url='http://127.0.0.1:18096',org='fixture')
            with patch.object(e.base,'Influx',Client),patch.dict(e.os.environ,{'INFLUX_TOKEN':'fixture-only'}):
                if annotation=='unsignedLong':self.assertEqual(e.run(args),0)
                else:
                    with self.assertRaises(ValueError):e.run(args)
            self.assertEqual(writes,[row])
            return json.loads((out/'report.json').read_text())

    def test_same_number_with_changed_native_kind_cannot_pass(self):
        report=self.exercise('long')
        self.assertFalse(report['completed'])
        self.assertIn('value kind differs',report['raw_difference'])

    def test_verified_seed_keeps_native_unsigned_kind(self):
        report=self.exercise('unsignedLong')
        self.assertTrue(report['completed'])
        self.assertEqual(report['raw_rows_verified'],1)
        self.assertTrue(report['bucket'].startswith('agg-exp86-'))
