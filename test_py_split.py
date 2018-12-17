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
	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		# TODO: Check where to put RowSet, because, can't set start_key and end_key with row_set.
		if self.beam_options.row_set is not None:
			for row_range in self.beam_options.row_set.row_ranges:
				yield iobase.SourceBundle(1,self,row_range.start_key,row_range.end_key)
		else:
			sample_row_keys = self._getTable().sample_row_keys()
			start_key = b''
			suma = long(0)
			for sample_row_key in sample_row_keys:
				tmp = suma + desired_bundle_size
				if tmp <= sample_row_key.offset_bytes:
					yield iobase.SourceBundle(1,self,start_key,sample_row_key.row_key)
					start_key = sample_row_key.row_key
					suma += desired_bundle_size
			if start_key != b'':
				yield iobase.SourceBundle(1,self,start_key,b'')

row_set = RowSet()
row_set.add_row_range(RowRange(start_key=b'127', end_key=b'384',start_inclusive=True,end_inclusive=True))
row_set.add_row_range(RowRange(start_key=b'646', end_key=b'701',start_inclusive=True,end_inclusive=True))

config = BigtableReadConfiguration(project_id, instance_id, table_id, row_set=row_set)
read_from_bigtable = RReadFromBigtable(config)

size = read_from_bigtable.estimate_size()
for i in read_from_bigtable.split(size):
	print( i )