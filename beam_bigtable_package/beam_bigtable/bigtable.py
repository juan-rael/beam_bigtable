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
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

class WriteToBigtable(beam.DoFn):
	""" Creates the connector can call and add_row to the batcher using each
	row in beam pipe line

	:type beam_options: class:`~bigtable_configuration.BigtableConfiguration`
	:param beam_options: Class `~bigtable_configuration.BigtableConfiguration`.

	:type flush_count: int
	:param flush_count: (Optional) Max number of rows to flush. If it
	reaches the max number of rows it calls finish_batch() to mutate the
	current row batch. Default is FLUSH_COUNT (1000 rows).

	:type max_mutations: int
	:param max_mutations: (Optional)  Max number of row mutations to flush.
	If it reaches the max number of row mutations it calls finish_batch() to
	mutate the current row batch. Default is MAX_MUTATIONS (100000 mutations).

	:type max_row_bytes: int
	:param max_row_bytes: (Optional) Max number of row mutations size to
	flush. If it reaches the max number of row mutations size it calls
	finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
	(5 MB).

	:type app_profile_id: str
	:param app_profile_id: (Optional) The unique name of the AppProfile.
	"""

	def __init__(self, beam_options):
		super(WriteToBigtable, self).__init__(beam_options)
		self.beam_options = beam_options
		self.client = None
		self.instance = None
		self.table = None
		self.batcher = None
		self._app_profile_id = self.beam_options.app_profile_id
		self.flush_count = self.beam_options.flush_count
		self.max_row_bytes = self.beam_options.max_row_bytes
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def start_bundle(self):
		if self.beam_options.credentials is None:
			self.client = bigtable.Client(project=self.beam_options.project_id,
										  admin=True)
		else:
			self.client = bigtable.Client(
				project=self.beam_options.project_id,
				credentials=self.beam_options.credentials,
				admin=True)
		self.instance = self.client.instance(self.beam_options.instance_id)
		self.table = self.instance.table(self.beam_options.table_id,
										 self._app_profile_id)

		self.batcher = MutationsBatcher(
			self.table, flush_count=self.flush_count,
			max_row_bytes=self.max_row_bytes)
		self.written = Metrics.counter(self.__class__, 'Written Row')

	def process(self, row):
		self.written.inc()
		self.batcher.mutate(row)

	def finish_bundle(self):
		return self.batcher.flush()

	def display_data(self):
		return {
			'projectId': DisplayDataItem(self.beam_options.project_id, label='Bigtable Project Id'),
			'instanceId': DisplayDataItem(self.beam_options.instance_id, label='Bigtable Instance Id'),
			'tableId': DisplayDataItem(self.beam_options.table_id, label='Bigtable Table Id'),
			'bigtableOptions': DisplayDataItem(str(self.beam_options), label='Bigtable Options', key='bigtableOptions'),
		}

	
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
			'bigtableOptions': DisplayDataItem(str(self.beam_options), label='Bigtable Options', key='bigtableOptions'),
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

class BigtableWriteConfiguration(BigtableConfiguration):
	"""
	:type flush_count: int
	:param flush_count: (Optional) Max number of rows to flush. If it
	reaches the max number of rows it calls finish_batch() to mutate the
	current row batch. Default is FLUSH_COUNT (1000 rows).
	:type max_mutations: int
	:param max_mutations: (Optional)  Max number of row mutations to flush.
	If it reaches the max number of row mutations it calls finish_batch() to
	mutate the current row batch. Default is MAX_MUTATIONS (100000 mutations).
	:type max_row_bytes: int
	:param max_row_bytes: (Optional) Max number of row mutations size to
	flush. If it reaches the max number of row mutations size it calls
	finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
	(5 MB).
	:type app_profile_id: str
	:param app_profile_id: (Optional) The unique name of the AppProfile.
	"""

	def __init__(self, project_id, instance_id, table_id, flush_count=None, max_row_bytes=None,
				 app_profile_id=None):
		super(BigtableWriteConfiguration, self).__init__(project_id, instance_id, table_id)
		self.flush_count = flush_count
		self.max_row_bytes = max_row_bytes
		self.app_profile_id = app_profile_id

	def __str__(self):
		import json
		return json.dumps({
			'project_id': self.project_id,
			'instance_id': self.instance_id,
			'table_id': self.table_id,
		})
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
	def __str__(self):
		import json
		row_set = []
		filters = ""
		if self.filter_ is not None:
			filters = str(self.filter_.to_pb())
		if self.row_set is not None:
			row_set = self.row_set.row_keys
			for r in self.row_set.row_ranges:
				row_set.append(r.get_range_kwargs())
		return json.dumps({
			'project_id': self.project_id,
			'instance_id': self.instance_id,
			'table_id': self.table_id,
			'row_set': row_set,
			'filter_': filters
		})
