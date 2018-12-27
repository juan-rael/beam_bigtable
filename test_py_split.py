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

def compare(a, b):
    return ((a > b) - (a < b))

def frange(start, stop, step):
	i = start
	while i < stop:
		yield i
		i += step

def checkArgument(condition, message, *args):
	pass
class RowRanges(RowRange):
	def overlaps(self, other):
		return self.ends_after_key(other.start_key) and other.ends_after_key(self.start_key)
	def ends_after_key(self, key):
		return self.end_key == b'' or compare(key,self.end_key)
	def contains_key(self, key):
		return compare(key, self.start_key) and self.ends_after_key(key)
class RReadFromBigtable(ReadFromBigtable):
	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		sample_row_keys = self._getTable().sample_row_keys()
		start_key = b'user0038'
		end_key = b'user0163'
		for i in self.range_split_fraction(805306368, 402653184, start_key, end_key):
			print( "Range: " + str(i.start_position) + "|" + str(i.stop_position))
			print( "+++++++" )
	def range_split_fraction(self, current_size, desired_bundle_size, start_key, end_key):
		range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
		return self.split_key_range_into_bundle_sized_sub_ranges(current_size, desired_bundle_size, range_tracker)
	def split_key_range_into_bundle_sized_sub_ranges(self, sample_size_bytes, desired_bundle_size, ranges):
		last_key = copy.deepcopy(ranges.stop_position())
		s = ranges.start_position()
		e = ranges.stop_position()

		split_ = float(desired_bundle_size) / float(sample_size_bytes)
		split_count = int( math.ceil( sample_size_bytes / desired_bundle_size ) )

		for i in range(split_count):
			estimate_position = ((i+1) * split_)
			position = LexicographicKeyRangeTracker.fraction_to_position(estimate_position, ranges.start_position(), ranges.stop_position())
			e = position
			yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
			s = position
		yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )
project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
size = read_from_bigtable.estimate_size()
# a = read_from_bigtable.range_split_fraction(805306368, 402653184, b'user0038', b'user0163')
for i in read_from_bigtable.range_split_fraction(805306368, 201326592, b'user0038', b'user0163'):
	print( str(i.start_position) + "|" + str(i.stop_position))
	range_tracker = LexicographicKeyRangeTracker(i.start_position,i.stop_position)
	for row in read_from_bigtable.read(range_tracker):
		print( row.row_key )