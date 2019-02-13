from __future__ import absolute_import
import argparse
import datetime
import uuid


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

from beam_bigtable import ReadFromBigTable


EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}


class PrintKeys(beam.DoFn):
  def __init__(self):
    from apache_beam.metrics import Metrics
    self.print_row = Metrics.counter(self.__class__.__name__, 'print_row')

  def __setstate__(self, options):
    from apache_beam.metrics import Metrics
    self.print_row = Metrics.counter(self.__class__.__name__, 'print_row')

  def process(self, row):
    self.print_row.inc()
    return [row]


def run(argv=[]):
  project_id = 'grass-clump-479'
  instance_id = 'python-write-2'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  guid = str(uuid.uuid1())
  table_id = 'testmillionb38b8c9f'
  jobname = 'read-' + table_id + '-' + guid
  

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
    '--num_workers=37',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\setup.py',
#    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\dist\\beam_bigtable-0.3.32.tar.gz'
#    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.30.tar.gz'
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

  
  config_data = {'project_id': project_id,
                 'instance_id': instance_id,
                 'table_id': table_id}
  with beam.Pipeline(options=pipeline_options) as p:
    count = (p
             | 'BigtableFromRead' >> ReadFromBigTable(project_id=project_id,
                                                      instance_id=instance_id,
                                                      table_id=table_id)
             | 'Count' >> beam.combiners.Count.Globally())
    row_count = 20000000
    assert_that(count, equal_to([row_count]))

    result = p.run()


if __name__ == '__main__':
  run()
