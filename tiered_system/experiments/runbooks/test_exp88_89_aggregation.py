"""Offline oracle/preparation guards; native/HTTP gates have separate reports."""
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

HERE=Path(__file__).resolve().parent

def load(name):
 spec=importlib.util.spec_from_file_location(name,HERE/(name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

base=load('exp86_aggregation_semantics');replay=load('exp88_flux_integer_replay');fixture=load('exp89_aggregation_fixture')

class CandidateTests(unittest.TestCase):
 def test_policy_is_explicit_and_baseline_is_retained(self):
  case=next(c for c in base.cases() if c['id']=='signed-overflow')
  strict=base.aggregate_sql(case,'source')
  compatible=base.aggregate_sql(case,'source',integer_policy='flux64')
  self.assertNotIn('% 18446744073709551616',strict)
  self.assertIn('% 18446744073709551616',compatible)
  self.assertIn('::HUGEINT',compatible)
  with self.assertRaises(ValueError):base.aggregate_sql(case,'source',integer_policy='unknown')

 def test_wrapped_mean_divides_after_integer_reduction(self):
  value=base.flux_integer_value('mean','uint','UBIGINT')
  self.assertLess(value.index('% 18446744073709551616'),value.index('::DOUBLE'))
  self.assertIn('NULLIF(count(',value)

 def test_unsigned_fixture_exclusion_is_recorded_not_coerced(self):
  with TemporaryDirectory() as directory:
   p=Path(directory)/'report.json'
   rows=[dict(name='t',time=1,kind='uint',value=18446744073709551612,meta={})]
   p.with_name('fixture.json').write_text(json.dumps(rows))
   p.write_text(json.dumps(dict(experiment='exp88',completed=True,summary={'FAIL':0},results=[dict(case={'id':'case','name':'t'},flux={'rows':[]})])))
   source,cases,excluded=fixture.input_cases(p,None,True)
   self.assertEqual((source,cases,excluded),([],[],['integer-case']))
   source,cases,excluded=fixture.input_cases(p,None,False)
   self.assertEqual(source,rows);self.assertEqual(excluded,[])

 def test_failed_reference_cannot_be_used_as_an_oracle(self):
  with TemporaryDirectory() as directory:
   p=Path(directory)/'report.json';p.write_text(json.dumps(dict(experiment='exp88',completed=True,summary={'FAIL':1})))
   with self.assertRaises(ValueError):fixture.input_cases(p,None)

 def test_public_projection_is_four_fields_without_epoch_or_kind_columns(self):
  case=next(c for c in base.cases() if c['id']=='mean-whole')
  sql=fixture.response_sql(case,'SELECT * FROM input')
  self.assertIn('SELECT make_timestamp_ns(time) AS timestamp,name,value,meta FROM',sql)

 def test_cold_predicate_precedes_hot_union(self):
  case=next(c for c in base.cases() if c['id']=='mean-whole')
  sql=fixture.raw_source('file:///tmp/fixture.metadata.json',case,'split',Path('/tmp/hot.parquet'))
  self.assertLess(sql.index('WHERE epoch_ns(time)<'),sql.index('UNION ALL'))
  self.assertIn('map_from_entries(list_sort',sql)
  self.assertIn('::UBIGINT::VARIANT',sql)

 def test_field_ids_match_the_published_variant_contract(self):
  self.assertIn("'time':1",fixture.FIELD_IDS);self.assertIn("'value':11",fixture.FIELD_IDS)
  self.assertIn('key:12,value:13',fixture.FIELD_IDS)

if __name__=='__main__':unittest.main()
