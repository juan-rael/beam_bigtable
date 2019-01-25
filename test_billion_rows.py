from __future__ import absolute_import
import argparse
import datetime
import random
import string
import copy
import uuid

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import ptransform

from google.cloud._helpers import _datetime_from_microseconds
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

from google.cloud.bigtable import row
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family
from google.cloud.bigtable import enums 


EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}

class _BigTableReadFn(iobase.BoundedSource):
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    super(self.__class__, self).__init__()
    from apache_beam.metrics import Metrics
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'row_set': row_set,
                         'filter_': filter_}
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'Read Rows')

  def _getTable(self):
    if self.table is None:
      options = self.beam_options
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    return self.table

  def get_sample_row_keys(self):
    return self._getTable().sample_row_keys()

  def split(self,
            desired_bundle_size,
            start_position=None,
            stop_position=None):

    if self.beam_options['row_set'] is not None:
      for sample_row_key in self.beam_options['row_set'].row_ranges:
        sample_row_keys = self.get_sample_row_keys()
        for row_split in self.split_range_size(desired_bundle_size,
                                               sample_row_keys,
                                               sample_row_key):
          yield row_split
    else:
      suma = 0
      last_offset = 0
      current_size = 0

      start_key = b''
      end_key = b''

      sample_row_keys = self.get_sample_row_keys()
      for sample_row_key in sample_row_keys:
        current_size = sample_row_key.offset_bytes-last_offset
        if suma >= desired_bundle_size:
          end_key = sample_row_key.row_key
          for fraction in self.range_split_fraction(suma,
                                                    desired_bundle_size,
                                                    start_key, end_key):
            yield fraction
          start_key = sample_row_key.row_key

          suma = 0
        suma += current_size
        last_offset = sample_row_key.offset_bytes

  def split_range_size(self, desired_size, sample_row_keys, range_):
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

          for split_key_range in self.split_range_sized_subranges(current,
                                                                  desired_size,
                                                                  range_tracker):
            yield split_key_range
        start = sample_row.row_key
      l = sample_row.offset_bytes

  def range_split_fraction(self,
                           current_size,
                           desired_bundle_size,
                           start_key,
                           end_key):
    range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
    return self.split_range_sized_subranges(current_size,
                                            desired_bundle_size,
                                            range_tracker)

  def fraction_to_position(self, position, range_start, range_stop):
    return LexicographicKeyRangeTracker.fraction_to_position(position,
                                                             range_start,
                                                             range_stop)

  def split_range_sized_subranges(self,
                                  sample_size_bytes,
                                  desired_bundle_size,
                                  ranges):

    last_key = copy.deepcopy(ranges.stop_position())
    s = ranges.start_position()
    e = ranges.stop_position()

    split_ = float(desired_bundle_size) / float(sample_size_bytes)
    split_count = int(math.ceil(sample_size_bytes / desired_bundle_size))

    for i in range(split_count):
      estimate_position = ((i + 1) * split_)
      position = self.fraction_to_position(estimate_position,
                                           ranges.start_position(),
                                           ranges.stop_position())
      e = position
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
      s = position
    if not s == last_key:
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

  def read(self, range_tracker):
    if range_tracker.start_position() is not None:
      if not range_tracker.try_claim(range_tracker.start_position()):
        # there needs to be a way to cancel the request.
        return
    read_rows = self._getTable().read_rows(start_key=range_tracker.start_position(),
      end_key=range_tracker.stop_position(),
      filter_=self.beam_options['filter_'])

    for row in read_rows:
      self.read_row.inc()
      yield row

  def display_data(self):
    ret = {
      'projectId': DisplayDataItem(self.beam_options['project_id'],
                                   label='Bigtable Project Id',
                                   key='projectId'),
      'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                    label='Bigtable Instance Id',
                                    key='instanceId'),
      'tableId': DisplayDataItem(self.beam_options['table_id'],
                                 label='Bigtable Table Id',
                                 key='tableId')}
    
    return ret


class GenerateRow(beam.DoFn):
  def process(self, key):
    rand = random.choice(string.ascii_letters + string.digits)
    value = ''.join(rand for i in range(30))

    direct_row = row.DirectRow(row_key=key)
    for column_id in range(3):
      direct_row.set_cell('cf1',
                          ('field%s' % column_id).encode('utf-8'),
                          value,
                          datetime.datetime.now())
      yield direct_row


class Generate(beam.PTransform):
  def __init__(self, number):
    self.number = number

  def expand(self, pbegin):
    
    return (pbegin
            | 'Create' >> beam.Create([0])
            | 'Map' >> beam.Map(lambda _: generate(self.number)))

class CreateAll():
  LOCATION_ID = "us-east1-b"
  def __init__(self, project_id, instance_id, table_id):
    from google.cloud.bigtable import enums

    self.project_id = project_id
    self.instance_id = instance_id
    self.table_id = table_id
    self.STORAGE_TYPE = enums.StorageType.HDD
    self.INSTANCE_TYPE = enums.Instance.Type.DEVELOPMENT
    self.client = Client(project=self.project_id, admin=True)


  def create_table(self):
    instance = self.client.instance(self.instance_id,
                                    instance_type=self.INSTANCE_TYPE,
                                    labels=LABELS)

    if not instance.exists():
      cluster = instance.cluster(self.cluster_id,
                                 self.LOCATION_ID,
                                 default_storage_type=self.STORAGE_TYPE)
      instance.create(clusters=[cluster])
    table = instance.table(self.table_id)

    if not table.exists():
      max_versions_rule = column_family.MaxVersionsGCRule(2)
      column_family_id = 'cf1'
      column_families = {column_family_id: max_versions_rule}
      table.create(column_families=column_families)


class _BigTableWriteFn(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line

  :type beam_options: class:`~bigtable_configuration.BigtableConfiguration`
  :param beam_options: class `~bigtable_configuration.BigtableConfiguration`
  """

  def __init__(self, project_id, instance_id, table_id):
    """
    Args:
      project_id: GCP Project of to write the Rows
      instance_id: GCP Instance to write the Rows
      table_id: GCP Table to write the `DirectRows`
    """
    super(_BigTableWriteFn, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    self.batcher = self.table.mutations_batcher()

  def process(self, row):
    self.written.inc()
    # You need to set the timestamp in the cells in this row object,
    # when we do a retry we will mutating the same object, but, with this
    # we are going to set our cell with new values.
    # Example:
    # direct_row.set_cell('cf1',
    #                     'field1',
    #                     'value1',
    #                     timestamp=datetime.datetime.now())
    self.batcher.mutate(row)

  def finish_bundle(self):
    self.batcher.flush()
    self.batcher = None

  def display_data(self):
    return {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self.beam_options['table_id'],
                                       label='Bigtable Table Id')
           }


class ReadFromBigTable(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id):
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.io.Read(_BigTableReadFn(beam_options['project_id'],
                                           beam_options['instance_id'],
                                           beam_options['table_id'])))


class WriteToBigTable(beam.PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project_id=None, instance_id=None,
               table_id=None):
    super(WriteToBigTable, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
                                          beam_options['instance_id'],
                                          beam_options['table_id'])))


class PrintKeys(beam.DoFn):
  def __init__(self):
    from apache_beam.metrics import Metrics
    self.print_row = Metrics.counter(self.__class__, 'Print Row')

  def process(self, row):
    print(row.row_key)
    self.print_row.inc()
    return [row]


def run(argv=[]):
  project_id = 'grass-clump-479'
  instance_id = 'python-write'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  guid = str(uuid.uuid4())[:8]
  #table_id = 'testmillion' + guid
  jobname = 'testmillion-' + guid
  table_id = 'testmillion5dba765d'

  argv.extend([
    '--experiments=beam_fn_api',
    '--project={}'.format(project_id),
    '--instance={}'.format(instance_id),
    '--table={}'.format(table_id),
    '--projectId={}'.format(project_id),
    '--instanceId={}'.format(instance_id),
    '--tableId={}'.format(table_id),
    '--job_name={}'.format(jobname),
    '--requirements_file=requirements.txt',
    '--runner=dataflow',
#    '--runner=direct',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.2.57.tar.gz'
  ])
  parser = argparse.ArgumentParser(argv)
  parser.add_argument('--projectId')
  parser.add_argument('--instanceId')
  parser.add_argument('--tableId')
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  create_table = CreateAll(project_id, instance_id, table_id)
  print('ProjectID:',project_id)
  print('InstanceID:',instance_id)
  print('TableID:',table_id)
  print('JobID:', jobname)
  create_table.create_table()

  row_count = 1000
  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)
  config_data = {'project_id': project_id,
                 'instance_id': instance_id,
                 'table_id': table_id}
  def _generate_mutation_data(row_count):
    for i in xrange(row_count):
      yield ("beam_key%s" % ('{0:07}'.format(i)))
  """
  p_r = (p
         | 'Create' >> beam.Create([0])
         | 'Generate Row Key' >> beam.FlatMap(lambda _: _generate_mutation_data(row_count))
         | 'Generate Direct Row' >> beam.ParDo(GenerateRow())
         | 'PrintKeys' >> beam.ParDo(PrintKeys())
         | 'Write' >> WriteToBigTable(project_id=project_id, instance_id=instance_id,
                                      table_id=table_id))
  """
  """
  p_r = (p
         | 'Read' >> ReadFromBigTable(project_id, instance_id, table_id)
         | 'PrintKeys' >> beam.ParDo(PrintKeys()))
  """
  p_r = (p
         | 'Create' >> beam.Create([0]))
  result = p.run()
  #result.wait_until_finish()

if __name__ == '__main__':
  run()
