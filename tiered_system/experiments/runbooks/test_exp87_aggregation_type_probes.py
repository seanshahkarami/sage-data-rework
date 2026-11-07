"""No HTTP, Docker, or native engine calls; live results remain a separate gate."""
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError

SPEC=importlib.util.spec_from_file_location('exp87',Path(__file__).with_name('exp87_aggregation_type_probes.py'))
e=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(e)


class ProbeTests(unittest.TestCase):
    def test_probe_ids_unique_and_all_execution_shapes_present(self):
        probes=e.probes('agg-exp86-'+'a'*32,'agg-exp87-'+'b'*32)
        self.assertEqual(len(probes),331)
        self.assertEqual(len({x['id'] for x in probes}),len(probes))
        sources={x['source'] for x in probes}
        self.assertTrue({'storage','storage-native','array-native','array-float','array-union','storage-map-native','storage-float'}<=sources)

    def test_read_only_mode_has_no_new_bucket_references(self):
        probes=e.probes('agg-exp86-'+'a'*32,None)
        self.assertTrue(probes)
        self.assertFalse(any('agg-exp87-' in x['flux'] for x in probes))
        self.assertFalse(any(x['source']=='storage-native' for x in probes))

    def test_original_source_client_refuses_mutation(self):
        client=e.ReadOnlyInflux('http://localhost:18096','secret','waggle','agg-exp86-'+'a'*32)
        for path,body in [('/api/v2/buckets',{}),('/api/v2/write?org=waggle',b'point'),('/api/v2/delete',{}),('/api/v2/buckets/id',{})]:
            with self.subTest(path=path),self.assertRaises(ValueError):client.request(path,body)
        with patch.object(e.base.Influx,'request',return_value=b'{}') as call:
            client.query('fixture')
            self.assertIn('/api/v2/query?',call.call_args.args[0])

    def test_variant_identity_and_insertion_order_are_distinct(self):
        rows=e.variants()
        self.assertEqual(len(rows),16)
        forward=[r for r in rows if r['name']=='exp87.int-then-float']
        reverse=[r for r in rows if r['name']=='exp87.late-written-first']
        self.assertEqual([(r['time'],r['kind'],r['value']) for r in forward],
                         [(r['time'],r['kind'],r['value']) for r in reversed(reverse)])
        self.assertTrue(all(r['name'].startswith('exp87.') for r in rows))

    def test_arrays_keep_large_unsigned_literals_as_strings(self):
        flux=e.array_source(e.arithmetic_cases()[0])
        self.assertIn('uint(v: "18446744073709551612")',flux)
        self.assertIn('_start',flux)
        self.assertIn('group(columns:',flux)
        self.assertIn('float(v:',e.array_source(e.arithmetic_cases()[0],True))

    def test_mixed_union_compares_order_and_conversion_before_group(self):
        a=e.mixed_array_source('int',True)
        b=e.mixed_array_source('float',True)
        self.assertLess(a.index('int(v:'),a.index('float(v:'))
        self.assertLess(b.index('float(v:'),b.index('int(v:'))
        self.assertNotIn('group(',a)
        probes=e.probes('agg-exp86-'+'a'*32,None)
        converted=next(p for p in probes if p['id']=='array-mixed-int-converted-mean')['flux']
        self.assertLess(converted.index('toFloat()'),converted.index('group('))

    def test_arithmetic_sql_preserves_exact_wide_and_wrapped_sums(self):
        duck=Mock()
        outputs=[]
        for case in e.arithmetic_cases():
            wide=sum(case['values']);mod=1<<64;sign=1<<63
            wrapped=wide%mod if case['kind']=='uint' else (wide+sign)%mod-sign
            outputs.append(([dict(exact_sum=str(wide),wrapped_sum=str(wrapped),n=2)],0))
        duck.run.side_effect=outputs
        self.assertEqual(len(e.duck_checks(duck)),5)
        for call in duck.run.call_args_list:
            sql=call.args[0]
            self.assertIn('sum(value::HUGEINT)',sql)
            self.assertIn('::VARCHAR AS exact_sum',sql)

    def test_transport_failure_aborts_instead_of_becoming_a_probe_result(self):
        query={'id':'probe','flux':'fixture','parse_time':True}
        with TemporaryDirectory() as root:
            client=Mock();client.query.side_effect=URLError('unreachable')
            with self.assertRaises(URLError):e.execute_probe(client,query,Path(root))
            client.query.side_effect=RuntimeError('Influx HTTP 401: unauthorized')
            with self.assertRaises(RuntimeError):e.execute_probe(client,query,Path(root))
            client.query.side_effect=RuntimeError('Influx HTTP 400: unsupported input type')
            result=e.execute_probe(client,query,Path(root))
            self.assertEqual(result['status'],'error')
            self.assertEqual(result['error_category'],'input_type')

    def test_original_reference_required_for_live_mode(self):
        with TemporaryDirectory() as directory:
            args=SimpleNamespace(run_dir=str(Path(directory)/'new'),source_report=None,local_only=False)
            with self.assertRaisesRegex(ValueError,'source-report'):e.run(args)
            self.assertFalse(Path(args.run_dir).exists())


if __name__=='__main__':unittest.main()
