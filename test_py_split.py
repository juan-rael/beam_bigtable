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
	def split_range_size(self, start_key, end_key):
		prev = None	
		size = None
		start,end,suma = None,None,None
		l = 0
		lexi = None
		for i in self.get_sample_row():
			current = i.offset_bytes - l
			l = i.offset_bytes
			if i.row_key > start_key:
				if start is None:
					start = prev
					suma = 0
			if suma is not None:
				suma += current
			if i.row_key > end_key:
				if end is None:
					if end_key > prev:
						print(prev, end_key, i.row_key)
						lexi = LexicographicKeyRangeTracker.position_to_fraction(end_key, start=prev, end=i.row_key)
						print(lexi)

						end = end_key
					else:
						end = prev
					break
			prev = i.row_key
		print( start )
		print( end )
		return size

project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
size = read_from_bigtable.estimate_size()
# a = read_from_bigtable.range_split_fraction(805306368, 402653184, b'user0038', b'user0163')
#for i in read_from_bigtable.range_split_fraction(805306368, 805306368, b'user0038', b'user0163'):
#	print( str(i.start_position) + "|" + str(i.stop_position))
#	range_tracker = LexicographicKeyRangeTracker(i.start_position,i.stop_position)
#	for row in read_from_bigtable.read(range_tracker):
#		print( row.row_key )
start_key = b'user0038'
end_key = b'user0165573'
current_size = 0
print( read_from_bigtable.split_range_size(start_key, end_key) )