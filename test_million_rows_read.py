from __future__ import absolute_import
import argparse
import datetime
import uuid

import math

from sys import platform

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms import core
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud.bigtable import Client
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

from beam_bigtable import BigTableSource


class ReadFromBigTable_Read(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id):
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pbegin):
    from apache_beam.options.pipeline_options import DebugOptions
    from apache_beam.transforms import util

    assert isinstance(pbegin, pvalue.PBegin)
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

      return (
          pbegin
          | core.Impulse()
          | 'Split' >> core.FlatMap(split_source)
          | util.Reshuffle()
          | 'ReadSplits' >> core.FlatMap(lambda split: split.source.read(
              split.source.get_range_tracker(
                  split.start_position, split.stop_position))))
    else:
      # Treat Read itself as a primitive.
      return pvalue.PCollection(self.pipeline)

def get_rows(project_id, instance_id, table_id):
  client = Client(project=project_id)
  instance = client.instance(instance_id)
  table = instance.table(table_id)
  return table.read_rows()


def run(argv=[]):

  project_id = 'grass-clump-479'
  instance_id = 'python-write-2'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  guid = str(uuid.uuid1())
  #table_id = 'testmillionb38c02c4' # 10,000,000
  #table_id = 'testmillioned113e20' # 10
  #table_id = 'testmillion2ee87b99' # 10,000
  #table_id = 'testmillion9a0b1127' # 6,000,000
  
  
  #full = ('testmillioned113e20', 10)
  #full = ('testmillion2ee87b99', 10000)
  #full = ('testmillion11daf1bf', 24543)
  #full = ('testbothae323947',    10000)
  #full = ('testmillion1c1d2c39', 781000)
  #full = ('testmillionc0a4f355', 881000)
  #full = ('testmillion9a0b1127', 6000000)
  #full = ('testmillionb38c02c4', 10000000)
  full = ('testmillione320108f', 500000000)

  table_id = full[0]
  number = full[1]
  jobname = 'read-' + str(number) + '-' + table_id + '-' + guid

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
    '--autoscaling_algorithm=NONE',
    '--num_workers=300',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\setup.py',
    '--extra_package=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\dist\\beam_bigtable-0.3.116.tar.gz'
  ])
  parser = argparse.ArgumentParser(argv)
  parser.add_argument('--projectId')
  parser.add_argument('--instanceId')
  parser.add_argument('--tableId')
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  print('ProjectID:',project_id)
  print('InstanceID:',instance_id)
  print('TableID:',table_id)
  print('JobID:', jobname)

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    second_step = (p
                   | 'BigtableFromRead' >> ReadFromBigTable_Read(project_id=project_id,
                                                                 instance_id=instance_id,
                                                                 table_id=table_id))
    count = (second_step
             | 'Count' >> beam.combiners.Count.Globally())
    row_count = number
    assert_that(count, equal_to([row_count]))
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  run()
