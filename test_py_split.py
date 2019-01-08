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
	def get_sample_row(self):
		return self._getTable().sample_row_keys()
	def range_size(self):
		pass
	def split_range_size(self, desired_bundle_size_bytes, sample_row_keys, range_):
		last_end_key = b''
		last_offset = 0
		splits = []
		for response in sample_row_keys:
			response_end_key = response.row_key
			response_offset = response.offset_bytes

			split_start_key = last_end_key
			if split_start_key < range_.start_key:
				split_start_key = range_.start_key
			
			split_end_key = response_end_key
			if not range_.contains_key(split_end_key):
				split_end_key = range_.end_key
			
			sample_size_bytes = response_offset - last_offset
			sub_splits = self.range_split_fraction(sample_size_bytes,
							   desired_bundle_size_bytes,
							   split_start_key,
							   split_end_key)
			splits.extend(sub_splits)
			last_end_key = response_end_key
			last_offset = response_offset
		
		return splits
project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
size = read_from_bigtable.estimate_size()

ranges = RowRanges(start_key=b'user0038', end_key=b'user0165573')
desired_bundle_size = 402653184
sample_row_keys = read_from_bigtable.get_sample_row()
split_size = read_from_bigtable.split_range_size(desired_bundle_size, sample_row_keys, ranges)

for i in split_size:
	print( i )