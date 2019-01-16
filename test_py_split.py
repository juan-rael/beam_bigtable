from __future__ import absolute_import

import sys
reload(sys)


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
import math
from google.cloud._helpers import _to_bytes

import copy

project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

class RReadFromBigtable(ReadFromBigtable):
	def getTable(self):
		return self._getTable()
	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		sample_row_keys = self.get_sample_row_keys()
		suma = 0
		last_offset = 0
		current_size = 0

		start_key = b''
		end_key = b''

		for sample_row_key in sample_row_keys:
			current_size = sample_row_key.offset_bytes-last_offset
			if suma >= desired_bundle_size:
				end_key = sample_row_key.row_key
				yield iobase.SourceBundle(suma, self, start_key, end_key)
				#for fraction in self.range_split_fraction(suma, desired_bundle_size, start_key, end_key):
				#	yield fraction
				start_key = sample_row_key.row_key

				suma = 0
			suma += current_size
			last_offset = sample_row_key.offset_bytes
	
	def split_key_range_into_bundle_sized_sub_ranges(self, sample_size_bytes, desired_bundle_size, ranges):
		last_key = copy.deepcopy(ranges.stop_position())
		s = ranges.start_position()
		e = ranges.stop_position()

		split_ = float(desired_bundle_size) / float(sample_size_bytes)
		split_count = int( math.ceil( sample_size_bytes / desired_bundle_size ) )

		for i in range(1,split_count):
			estimate_position = ((i) * split_)
			#print( estimate_position )
			position = LexicographicKeyRangeTracker.fraction_to_position(estimate_position, ranges.start_position(), ranges.stop_position())
			e = position
			yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
			s = position
		if not s == last_key:
			yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
#size = read_from_bigtable.estimate_size()

#ranges = RowRange(start_key=b'user0038', end_key=b'user1269970')
#ranges = RowRange(start_key=b'user0038', end_key=b'user2')
#desired_bundle_size = 402653184
#sample_row_keys = read_from_bigtable.get_sample_row_keys()

#split_size = read_from_bigtable.split_range_size(desired_bundle_size, sample_row_keys, ranges)


current_size = 805306368
desired_bundle_size = 201326592
#desired_bundle_size = 402653184
start_ = ''
end_ = 'user04'
start = ''
end = None

a = []

for i in range(1, 10, 1):
	range_tracker = LexicographicKeyRangeTracker(start_, end_)
	pos = LexicographicKeyRangeTracker.fraction_to_position(float(i)/10, start_, end_)
	split = range_tracker.try_split(pos)
	end = split[0]
	a.append(LexicographicKeyRangeTracker(start, end))
	start = split[0]
if not start == end_:
	a.append(LexicographicKeyRangeTracker(start, end_))

for i in a:
	read_r = read_from_bigtable.getTable().read_rows(start_key=i.start_position(), end_key=i.stop_position())
	print(i.start_position(),i.stop_position())
	for row in read_r:
		print("\t" + row.row_key)
	print("+++")

#read_r = read_from_bigtable.getTable().read_rows(start_key=b'user0000000',end_key=b'user0000009')
#for row in read_r:
#	print("\t" + row.row_key)