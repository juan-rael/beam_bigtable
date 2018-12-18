from __future__ import absolute_import
import argparse
import logging
import re

import apache_beam as beam
from past.builtins import unicode
from apache_beam.io import iobase
from google.cloud import bigtable
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle

from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.transforms.display import DisplayDataItem
from beam_bigtable.bigtable import BigtableReadConfiguration

from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

from beam_bigtable.bigtable import ReadFromBigtable

from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange


project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

class RReadFromBigtable(ReadFromBigtable):
	def __init__(self, beam_options):
		super(RReadFromBigtable, self).__init__(beam_options)
	def getTable(self):
		return self._getTable()
	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		if self.beam_options.row_set is not None:
			sample_rows_ranges = self.beam_options.row_set.row_ranges
			sample_rows_keys = self.beam_options.row_set.row_keys
			for sample_row_key in sample_rows_keys:
				yield iobase.SourceBundle(1,self,sample_row_key,sample_row_key)
			for sample_row_key in sample_rows_ranges:
				yield iobase.SourceBundle(1,self,sample_row_key.start_key,sample_row_key.end_key)
		else:
			suma = 0
			last_offset = 0
			current_size = 0
			start_key = b''
			sample_row_keys = self._getTable().sample_row_keys()
			for sample_row_key in sample_row_keys:
				current_size = sample_row_key.offset_bytes-last_offset
				if suma >= desired_bundle_size:
					yield iobase.SourceBundle(suma,self,start_key,sample_row_key.row_key)
					start_key = sample_row_key.row_key

					suma = 0
				suma += current_size
				last_offset = sample_row_key.offset_bytes
			
row_set = RowSet()
row_set.add_row_range(RowRange(start_key=b'user163', end_key=b'user76',start_inclusive=True,end_inclusive=True))
row_set.add_row_range(RowRange(start_key=b'user887', end_key=b'user945',start_inclusive=True,end_inclusive=True))
row_set.add_row_key('817')

#config = BigtableReadConfiguration(project_id, instance_id, table_id, row_set=row_set)
config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)

size = long(402653184)
#size = long(805306368)
#size = long(1610612736)
#size = long(2415919104)
for i in read_from_bigtable.split(size):
	print( i )