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
		return self.start_key <= key and self.end_key >= key
class RReadFromBigtable(ReadFromBigtable):
	def getTable(self):
		return self._getTable()
	def get_sample_row(self):
		return self._getTable().sample_row_keys()
	
	def split_range_size(self, desired_bundle_size_bytes, sample_row_keys, range_):
		prev = None
		start, end, size = None, None, 0
		range_all_split = []
		l = 0
		for sample_row in sample_row_keys:
			current = sample_row.offset_bytes - l
			if sample_row.row_key == b'':
				continue
			
			if range_.contains_key(sample_row.row_key):
				if start is not None:
					end = sample_row.row_key
					yield {"start": start, "end": end, "size": current}
				start = sample_row.row_key
			l = sample_row.offset_bytes
project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
size = read_from_bigtable.estimate_size()

#ranges = RowRanges(start_key=b'user0038', end_key=b'user1269970')
ranges = RowRanges(start_key=b'user0038', end_key=b'user2')
desired_bundle_size = 402653184
sample_row_keys = read_from_bigtable.get_sample_row()
split_size = read_from_bigtable.split_range_size(desired_bundle_size, sample_row_keys, ranges)

for i in split_size:
	print( i )