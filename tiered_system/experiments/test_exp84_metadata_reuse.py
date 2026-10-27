import unittest

from exp84_metadata_reuse import uniform_statement,META_MARKER


class TestUniformMetadataSelection(unittest.TestCase):
    def test_only_exactly_one_tuple_selects_uniform_path(self):
        template='SELECT '+META_MARKER+'::VARCHAR AS meta'
        for metadata in ([],['{}','{}'],['{}','{"other":"value"}']):
            self.assertEqual(uniform_statement('baseline',template,metadata),('baseline','fallback'))
        raw='{"z":"it\'s \\\\ 中文","a":null}'
        statement,path=uniform_statement('baseline',template,[raw])
        self.assertEqual(path,'uniform')
        self.assertEqual(statement,"SELECT '"+raw.replace("'","''")+"'::VARCHAR AS meta")

    def test_non_object_and_wrong_template_fail_closed(self):
        for raw in ('null','[]','"text"'):
            with self.assertRaises(ValueError):uniform_statement('base',META_MARKER,[raw])
        with self.assertRaises(ValueError):uniform_statement('base','no marker',['{}'])
        with self.assertRaises(ValueError):uniform_statement('base',META_MARKER+META_MARKER,['{}'])


if __name__=='__main__':unittest.main()
