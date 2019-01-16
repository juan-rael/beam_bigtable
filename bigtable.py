import copy
import math
import logging
import apache_beam as beam
from google.cloud import bigtable
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker


class ReadBigtableOptions(PipelineOptions):
	""" Create the Pipeline Options to set ReadBigtable/WriteBigtable.
	You can create and use this class in the Template, with a certainly steps.
	"""
	@classmethod
	def _add_argparse_args(cls, parser):
		super(ReadBigtableOptions, cls)._add_argparse_args(parser)
		parser.add_argument('--instance', required=True )
		parser.add_argument('--table', required=True )

class ReadFromBigtable(iobase.BoundedSource):
	""" Bigtable apache beam read source
	:type split_keys: dict
						'~end_key'}`.
	:type beam_options:
	class:`~bigtable_configuration.BigtableReadConfiguration`
	:param beam_options: Class
	`~bigtable_configuration.BigtableReadConfiguration`.
	"""
	def __init__(self, beam_options):
		super(ReadFromBigtable, self).__init__()
		self.beam_options = beam_options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')
	def _getTable(self):
		if self.table is None:
			options = self.beam_options
			client = bigtable.Client(
				project=options.project_id,
				credentials=self.beam_options.credentials)
			instance = client.instance(options.instance_id)
			self.table = instance.table(options.table_id)
		return self.table

	def __getstate__(self):
		return self.beam_options

	def __setstate__(self, options):
		self.beam_options = options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')

	def estimate_size(self):
		size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
		return size
	
	def get_sample_row_keys(self):
		return self._getTable().sample_row_keys()
	
	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		sample_row_keys = self.get_sample_row_keys()

		if self.beam_options.row_set is not None:
			for sample_row_key in self.beam_options.row_set.row_ranges:
				for row_split in self.split_range_size(desired_bundle_size, sample_row_keys, sample_row_key):
					yield row_split
		else:
			suma = 0
			last_offset = 0
			current_size = 0

			start_key = b''
			end_key = b''

			for sample_row_key in sample_row_keys:
				current_size = sample_row_key.offset_bytes-last_offset
				if suma >= desired_bundle_size:
					end_key = sample_row_key.row_key
					for fraction in self.range_split_fraction(suma, desired_bundle_size, start_key, end_key):
						yield fraction
					start_key = sample_row_key.row_key

					suma = 0
				suma += current_size
				last_offset = sample_row_key.offset_bytes

	def split_range_size(self, desired_bundle_size_bytes, sample_row_keys, range_):
		start, end = None, None
		l = 0
		for sample_row in sample_row_keys:
			current = sample_row.offset_bytes - l
			if sample_row.row_key == b'':
				continue

			if range_.start_key <= sample_row.row_key and range_.end_key >= sample_row.row_key:
				if start is not None:
					end = sample_row.row_key
					range_tracker = LexicographicKeyRangeTracker(start, end)

					for split_key_range in self.split_key_range_into_bundle_sized_sub_ranges(current, desired_bundle_size_bytes, range_tracker):
						yield split_key_range
				start = sample_row.row_key
			l = sample_row.offset_bytes

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
		if not s == last_key:
			yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

	def get_range_tracker(self, start_position, stop_position):
		return LexicographicKeyRangeTracker(start_position, stop_position)

	def read(self, range_tracker):
		if not (range_tracker.start_position() == None):
			if not range_tracker.try_claim(range_tracker.start_position()):
				# there needs to be a way to cancel the request.
				return
		read_rows = self._getTable().read_rows(start_key=range_tracker.start_position(),
			end_key=range_tracker.stop_position(),
			filter_=self.beam_options.filter_)

		for row in read_rows:
			self.read_row.inc()
			yield row

	def display_data(self):
		ret = {
			'projectId': DisplayDataItem(self.beam_options.project_id, label='Bigtable Project Id', key='projectId'),
			'instanceId': DisplayDataItem(self.beam_options.instance_id, label='Bigtable Instance Id',key='instanceId'),
			'tableId': DisplayDataItem(self.beam_options.table_id, label='Bigtable Table Id', key='tableId'),
		}
		if self.beam_options.row_set is not None:
			i = 0
			for value in self.beam_options.row_set.row_keys:
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
			for (i,value) in enumerate(self.beam_options.row_set.row_ranges):
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value.get_range_kwargs()), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
		if self.beam_options.filter_ is not None:
			for (i,value) in enumerate(self.beam_options.filter_.filters):
				ret['rowFilter{}'.format(i)] = DisplayDataItem(str(value.to_pb()), label='Bigtable Row Filter {}'.format(i), key='rowFilter{}'.format(i))
		return ret

class BigtableConfiguration(object):
	""" Bigtable configuration variables.

	:type project_id: :class:`str` or :func:`unicode <unicode>`
	:param project_id: (Optional) The ID of the project which owns the
						instances, tables and data. If not provided, will
						attempt to determine from the environment.

	:type instance_id: str
	:param instance_id: The ID of the instance.

	:type table_id: str
	:param table_id: The ID of the table.
	"""

	def __init__(self, project_id, instance_id, table_id):
		self.project_id = project_id
		self.instance_id = instance_id
		self.table_id = table_id
		self.credentials = None

class BigtableReadConfiguration(BigtableConfiguration):
	""" Bigtable read configuration variables.

	:type configuration: :class:`~BigtableConfiguration`
	:param configuration: class:`~BigtableConfiguration`

	:type row_set: :class:`row_set.RowSet`
	:param row_set: (Optional) The row set containing multiple row keys and
					row_ranges.

	:type filter_: :class:`.RowFilter`
	:param filter_: (Optional) The filter to apply to the contents of the
					specified row(s). If unset, reads every column in
					each row.
	"""

	def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
		super(BigtableReadConfiguration, self).__init__(project_id, instance_id, table_id)
		self.row_set = row_set
		self.filter_ = filter_