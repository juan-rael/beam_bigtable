import logging
import unittest

from apache_beam.io.iobase import SourceBundle
from beam_bigtable.bigtable import BigtableReadConfiguration
from beam_bigtable.bigtable import ReadFromBigtable
from google.cloud.bigtable.row_set import RowRange
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

class BigtableIOReadIT(unittest.TestCase):
  def setUp(self):
    self.project_id = 'grass-clump-479'
    self.instance_id = 'endurance'
    self.table_id = 'perf1DFN4UF2'
    config = BigtableReadConfiguration(self.project_id, self.instance_id, self.table_id)
    self.read_from_bigtable = ReadFromBigtable(config)
    self.get_sample_row_keys = self.read_from_bigtable.get_sample_row_keys()
    
  def test_bigtable_sub_range(self):
    list_range_tracker = [
      LexicographicKeyRangeTracker(b'user0038', b'user0163'),
      LexicographicKeyRangeTracker(b'user0163', b'user04'),
      LexicographicKeyRangeTracker(b'user04', b'user054'),
      LexicographicKeyRangeTracker(b'user054', b'user083'),
      LexicographicKeyRangeTracker(b'user083', b'user105'),
      LexicographicKeyRangeTracker(b'user105', b'user127'),
      LexicographicKeyRangeTracker(b'user127', b'user162'),
      LexicographicKeyRangeTracker(b'user162', b'user183'),
      LexicographicKeyRangeTracker(b'user183', b'user2'),
    ]
    for range_tracker in list_range_tracker:
      assert (len(list(self.read_from_bigtable.split_key_range_into_bundle_sized_sub_ranges(805306368, 201326592, range_tracker))) == 4)
  def test_bigtable_split_range(self):
    list_row_range = [
      RowRange(start_key=b'user0038', end_key=b'user0163'),
      RowRange(start_key=b'user0163', end_key=b'user04'),
      RowRange(start_key=b'user04', end_key=b'user054'),
      RowRange(start_key=b'user054', end_key=b'user083'),
      RowRange(start_key=b'user083', end_key=b'user105'),
      RowRange(start_key=b'user105', end_key=b'user127'),
      RowRange(start_key=b'user127', end_key=b'user162'),
      RowRange(start_key=b'user162', end_key=b'user183'),
      RowRange(start_key=b'user183', end_key=b'user2'),
    ]
    for row_range in list_row_range:
      for split_range in self.read_from_bigtable.split_range_size(402653184, self.get_sample_row_keys, row_range):
        assert isinstance(split_range, SourceBundle)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()