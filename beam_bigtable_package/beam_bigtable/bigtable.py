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
  def __init__(self, beam_options):
    super(ReadFromBigtable, self).__init__()
    self.beam_options = beam_options
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'read')

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

  def split(self,
            desired_bundle_size,
            start_position=None,
            stop_position=None):
    sample_row_keys = self.get_sample_row_keys()

    suma = 0
    last_offset = 0
    current_size = 0

    start_key = b''
    end_key = b''

    if self.beam_options.row_set is not None:
      ranges = sample_row_keys
    else:
      ranges = self.beam_options.row_set.row_ranges

    for sample_row_key in ranges:
      if self.beam_options.row_set is None:
        current_size = sample_row_key.offset_bytes-last_offset
        if suma >= desired_bundle_size:
          end_key = sample_row_key.row_key
          for fraction in self.range_split_fraction(suma,
                                                    desired_bundle_size,
                                                    start_key,
                                                    end_key):
            yield fraction
          start_key = sample_row_key.row_key

          suma = 0
        suma += current_size
        last_offset = sample_row_key.offset_bytes
      else:
        for row_split in self.split_range_size(desired_bundle_size,
                                               sample_row_keys,
                                               sample_row_key):
          yield row_split

  def split_range_size(self,
                       desired_size,
                       sample_row_keys,
                       range_):
    """Split Ranges in desired_bundle_size"""
    start, end = None, None
    l = 0
    for sample_row in sample_row_keys:
      current = sample_row.offset_bytes - l
      if sample_row.row_key == b'':
        continue

      if(range_.start_key <= sample_row.row_key and
         range_.end_key >= sample_row.row_key):
        if start is not None:
          end = sample_row.row_key
          range_tracker = LexicographicKeyRangeTracker(start, end)

          split_subranges = self.split_key_range_subranges(current,
                                                           desired_size,
                                                           range_tracker)
          for split_key_range in split_subranges:
            yield split_key_range
        start = sample_row.row_key
      l = sample_row.offset_bytes

  def range_split_fraction(self,
                           current_size,
                           desired_bundle_size,
                           start_key,
                           end_key):
    range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
    return self.split_key_range_subranges(current_size,
                                          desired_bundle_size,
                                          range_tracker)

  def split_key_range_subranges(self,
                                sample_size_bytes,
                                desired_bundle_size,
                                ranges):
    last_key = copy.deepcopy(ranges.stop_position())
    s = ranges.start_position()
    e = ranges.stop_position()

    split_ = float(desired_bundle_size) / float(sample_size_bytes)
    split_count = int(math.ceil(sample_size_bytes / desired_bundle_size))

    for i in range(split_count):
      estimate_position = ((i+1) * split_)
      range_tracker = LexicographicKeyRangeTracker(ranges.start_position(),
                                                   ranges.stop_position())
      position = range_tracker.fraction_to_position(estimate_position)
      e = position
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
      s = position
    if not s == last_key:
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key)

  def read(self, range_tracker):
    get_table = self._getTable()
    if range_tracker.start_position() is not None:
      if not range_tracker.try_claim(range_tracker.start_position()):
        # there needs to be a way to cancel the request.
        return

    read_rows = get_table.read_rows(start_key=range_tracker.start_position(),
                                    end_key=range_tracker.stop_position(),
                                    filter_=self.beam_options.filter_)

    for row in read_rows:
      self.read_row.inc()
      yield row
  def get_range_tracker(self, start_position, stop_position):
    return LexicographicKeyRangeTracker(start_position, stop_position)

  def display_data(self):
    ret = {'projectId': DisplayDataItem(self.beam_options.project_id,
                                        label='Bigtable Project Id',
                                        key='projectId'),
           'instanceId': DisplayDataItem(self.beam_options.instance_id,
                                         label='Bigtable Instance Id',
                                         key='instanceId'),
           'tableId': DisplayDataItem(self.beam_options.table_id,
                                      label='Bigtable Table Id',
                                      key='tableId')}
    if self.beam_options.row_set is not None:
      i = 0
      for value in self.beam_options.row_set.row_keys:
        label = 'Bigtable Row Set {}'.format(i)
        key = 'rowSet{}'.format(i)
        ret[key] = DisplayDataItem(str(value),
                                   label=label,
                                   key=key)
        i = i+1
      for (i, value) in enumerate(self.beam_options.row_set.row_ranges):
        label = 'Bigtable Row Set {}'.format(i)
        key = 'rowSet{}'.format(i)
        ret[key] = DisplayDataItem(str(value.get_range_kwargs()),
                                   label=label,
                                   key=key)
        i = i+1
    if self.beam_options.filter_ is not None:
      for (i, value) in enumerate(self.beam_options.filter_.filters):
        label = 'Bigtable Row Filter {}'.format(i)
        key = 'rowFilter{}'.format(i)
        ret[key] = DisplayDataItem(str(value.to_pb()),
                                   label=label,
                                   key=key)
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
