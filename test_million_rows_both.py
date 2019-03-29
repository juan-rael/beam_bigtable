from __future__ import absolute_import
import argparse
import datetime
import math
import random
import string
import uuid



import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms import core
from apache_beam.metrics import Metrics

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable import row
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family

from beam_bigtable import BigTableSource
from beam_bigtable import WriteToBigTable


EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}


class GenerateRow(beam.DoFn):
  def __init__(self):
    self.generate_row = Metrics.counter(self.__class__, 'generate_row')

  def __setstate__(self, options):
    self.generate_row = Metrics.counter(self.__class__, 'generate_row')

  def process(self, ranges):
    for row_id in range(int(ranges[0]), int(ranges[1][0])):
      key = "beam_key%s" % ('{0:07}'.format(row_id))
      rand = random.choice(string.ascii_letters + string.digits)

      direct_row = row.DirectRow(row_key=key)
      _ = [direct_row.set_cell(
                    'cf1',
                    ('field%s' % i).encode('utf-8'),
                    ''.join(rand for _ in range(100)),
                    datetime.datetime.now()) for i in range(10)]
      self.generate_row.inc()
      yield direct_row


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


class ReadFromBigTable_Read(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id):
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pbegin):
    from apache_beam.options.pipeline_options import DebugOptions
    from apache_beam.transforms import util

    self.pipeline = pbegin.pipeline

    project_id = self.beam_options['project_id']
    instance_id = self.beam_options['instance_id']
    table_id = self.beam_options['table_id']

    debug_options = self.pipeline._options.view_as(DebugOptions)
    if debug_options.experiments and 'beam_fn_api' in debug_options.experiments:
      source = BigTableSource(project_id,
                              instance_id,
                              table_id)

      def split_source(unused_impulse):
        total_size = source.estimate_size()
        if total_size:
          # 1MB = 1 shard, 1GB = 32 shards, 1TB = 1000 shards, 1PB = 32k shards
          chunk_size = max(1 << 20, 1000 * int(math.sqrt(total_size)))
        else:
          chunk_size = 64 << 20  # 64mb
        return source.split(chunk_size)
      if isinstance(pbegin, pvalue.PBegin):
        begin_step = (pbegin
                      | core.Impulse())
      else:
        begin_step = (pbegin
                      | 'Split' >> core.FlatMap(split_source)
                      | util.Reshuffle()
                      | 'ReadSplits' >> core.FlatMap(lambda split: split.source.read(
                            split.source.get_range_tracker(
                            split.start_position, split.stop_position))))
      return (begin_step)
    else:
      # Treat Read itself as a primitive.
      return pvalue.PCollection(self.pipeline)

def run(argv=[]):
  project_id = 'grass-clump-479'
  instance_id = 'python-write-2'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  #table_id = 'testmillionb38c02c4'
  #table_id = 'testmillioned113e20'
  #table_id = 'testmillion2ee87b99'
  guid = str(uuid.uuid4())[:8]
  table_id = 'testboth' + guid
  jobname = 'testmillion-both-' + guid
  

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
    '--disk_size_gb=100',
    '--region=us-central1',
    '--runner=dataflow',
    #'--runner=directRunner',
    '--autoscaling_algorithm=NONE',
    '--num_workers=100',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\setup.py',
#    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\dist\\beam_bigtable-0.3.106.tar.gz'
#    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.30.tar.gz'
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

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  row_count = 10000
  row_limit = 100
  row_step = row_count if row_count <= row_limit else row_count/row_limit

  with beam.Pipeline(options=pipeline_options) as p:
    second_step = (p
                   | 'Ranges' >> beam.Create([(str(i),str(i+row_step)) for i in xrange(0, row_count, row_step)])
                   | 'Group' >> beam.GroupByKey()
                   | 'Generate' >> beam.ParDo(GenerateRow())
                   | 'Write' >> WriteToBigTable(project_id=project_id,
                                                instance_id=instance_id,
                                                table_id=table_id)
                   | 'BigtableFromRead' >> ReadFromBigTable_Read(project_id=project_id,
                                                                 instance_id=instance_id,
                                                                 table_id=table_id))
    count = (second_step
             | 'Count' >> beam.combiners.Count.Globally())
    row_count = 10000
    assert_that(count, equal_to([row_count]))

    result = p.run()


if __name__ == '__main__':
  run()
