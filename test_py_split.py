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
	pass

	


config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = RReadFromBigtable(config)
#size = read_from_bigtable.estimate_size()

#ranges = RowRange(start_key=b'user0038', end_key=b'user1269970')
#ranges = RowRange(start_key=b'user0038', end_key=b'user2')
#desired_bundle_size = 402653184
#sample_row_keys = read_from_bigtable.get_sample_row_keys()

#split_size = read_from_bigtable.split_range_size(desired_bundle_size, sample_row_keys, ranges)
array = [
	
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
for a in array:
	for sub_ranges in read_from_bigtable.split_key_range_into_bundle_sized_sub_ranges(805306368, 201326592, a):
		print( sub_ranges)
	print( "++" )