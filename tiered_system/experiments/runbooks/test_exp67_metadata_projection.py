from pathlib import Path
import tempfile
import unittest

import pyarrow.parquet as pq

from exp67_metadata_projection import TAGS, fixture


class FixtureTests(unittest.TestCase):
    def test_covers_all_presence_masks_and_nanoseconds(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"fixture.parquet"
            count=fixture(path)
            table=pq.read_table(path)
            self.assertEqual(count,1024)
            seen={sum((row[tag] is not None)*(1<<bit) for bit,tag in enumerate(TAGS))
                  for row in table.select(TAGS).to_pylist()}
            self.assertEqual(seen,set(range(128)))
            self.assertEqual(table.schema.field("time").type.unit,"ns")
            self.assertTrue(any(x=="" for x in table.column("plugin").to_pylist()))
            self.assertIn(None,table.column("meta").to_pylist())


if __name__=="__main__":
    unittest.main()
