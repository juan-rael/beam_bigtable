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
		
		for i in self.split_ranges_fractions(desired_bundle_size, start_key, end_key):
			ranges = LexicographicKeyRangeTracker(i.start_position, i.stop_position )
			read_rows = self._getTable().read_rows(
				start_key=ranges.start_position(),
				end_key=ranges.stop_position()
			)
			for row in read_rows:
				print( row.row_key )
			print( "+++++++" )
	def split_ranges_fractions(self, desired_bundle_size, start_key, end_key):
		s = start_key
		e = end_key

		range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
		for i in frange(0, 1, 0.1):
			
			position = range_tracker.fraction_to_position(i, start_key, end_key)
			if position is not start_key and position is not end_key:
				splited = range_tracker.try_split(position)
				e = splited[0]
				yield iobase.SourceBundle(1, self, s, e )
				s = splited[0]
project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
size = read_from_bigtable.estimate_size()
read_from_bigtable.split(size)
