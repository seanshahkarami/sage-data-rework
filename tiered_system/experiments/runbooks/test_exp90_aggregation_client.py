"""Precision checks must not compare numpy scalars with implicit float coercion."""
import importlib.util
from pathlib import Path
import unittest

SPEC=importlib.util.spec_from_file_location('exp90',Path(__file__).with_name('exp90_aggregation_client.py'))
e=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(e)

class ClientProbeTests(unittest.TestCase):
 def test_largest_signed_int_float_rounding_is_detected(self):
  self.assertEqual(e.compare_values((1<<63)-1,float((1<<63)-1)),'integer precision/value changed')
  self.assertIsNone(e.compare_values(-(1<<63),float(-(1<<63))))
 def test_harmless_dtype_changes_are_not_precision_failures(self):
  self.assertIsNone(e.compare_values(10,10.0))
  self.assertIsNone(e.compare_values(None,float('nan')))
  self.assertIsNotNone(e.compare_values(1,True))
  self.assertIsNotNone(e.compare_values(1.0,True))
 def test_numpy_style_scalars_are_converted_before_comparison(self):
  class Scalar:
   def item(self):return float((1<<63)-1)
  self.assertIsNotNone(e.compare_values((1<<63)-1,Scalar()))
 def test_identity_and_precision_failures_are_separate(self):
  row=dict(time=194,name='t',meta={'sensor':'a'},value=(1<<63)-1)
  bad=dict(row,value=float((1<<63)-1))
  failures,coercions=e.compare_rows([row],[bad])
  self.assertEqual(failures[0]['reason'],'integer precision/value changed');self.assertEqual(coercions,1)
  failures,_=e.compare_rows([row],[dict(row,time=195)])
  self.assertEqual(failures[0]['reason'],'identity/time/metadata changed')

if __name__=='__main__':unittest.main()
