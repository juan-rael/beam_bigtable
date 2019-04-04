# WriteToBigTable
from __future__ import absolute_import
import argparse
import datetime
import random
import string
import uuid
from sys import platform

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC
from google.cloud.bigtable import row
from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family

from beam_bigtable import WriteToBigTable

from grpc import StatusCode

from google.api_core.retry import if_exception_type
from google.api_core.retry import Retry

from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.batcher import MutationsBatcher
from google.cloud.bigtable.table import Table



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



class PrintKeys(beam.DoFn):
  def __init__(self):
    from apache_beam.metrics import Metrics
    self.print_row = Metrics.counter(self.__class__.__name__, 'Print Row')

  def process(self, row):
    self.print_row.inc()
    return [row]


def run(argv=[]):
  project_id = 'grass-clump-479'
#  instance_id = 'python-write'
  instance_id = 'python-write-2'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  guid = str(uuid.uuid4())[:8]
  table_id = 'testmillion' + guid
  jobname = 'testmillion-write-' + guid
  
  if platform == "linux" or platform == "linux2":
    argument = {
      'setup': '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
      'extra_package': '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.38.tar.gz'
    }
  elif platform == "darwin":
    argument = {
      'setup': '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
      'extra_package': '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.38.tar.gz'
    }
  elif platform == "win32":
    argument = {
      'setup': '--setup_file=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\setup.py',
      'extra_package': '--extra_package=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\dist\\beam_bigtable-0.3.32.tar.gz'
    }
  

  argv.extend([
    '--experiments=beam_fn_api',
    '--project={}'.format(project_id),
    '--instance={}'.format(instance_id),
    '--job_name={}'.format(jobname),
    '--requirements_file=requirements.txt',
    '--disk_size_gb=50',
    '--region=us-central1',
    '--runner=dataflow',
<<<<<<< HEAD
    '--autoscaling_algorithm=NONE',
    '--num_workers=300',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\setup.py',
#    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=C:\\Users\\Juan\\Project\\python\\example_bigtable_beam\\beam_bigtable_package\\dist\\beam_bigtable-0.3.114.tar.gz'
#    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.28.tar.gz'
=======
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    argument['setup'],
    argument['extra_package'],
>>>>>>> 812ca1a7dbdcfcec67cdcfac80cd8d5ba16c8c4a
  ])
  parser = argparse.ArgumentParser(argv)
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  create_table = CreateAll(project_id, instance_id, table_id)
  print('ProjectID:',project_id)
  print('InstanceID:',instance_id)
  print('TableID:',table_id)
  print('JobID:', jobname)
  create_table.create_table()

<<<<<<< HEAD
  row_count = 285714286
  row_limit = 1000
  row_step = row_count if row_count <= row_limit else row_count/row_limit
=======
  row_count = 10
  row_step = row_count if row_count <= 10000 else row_count/10000
>>>>>>> 812ca1a7dbdcfcec67cdcfac80cd8d5ba16c8c4a
  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)
  config_data = {'project_id': project_id,
                 'instance_id': instance_id,
                 'table_id': table_id}
  
  count = (p
           | 'Ranges' >> beam.Create([(str(i),str(i+row_step)) for i in xrange(0, row_count, row_step)])
           | 'Group' >> beam.GroupByKey()
           | 'Generate' >> beam.ParDo(GenerateRow())
           | 'Write' >> WriteToBigTable(project_id=project_id,
                                        instance_id=instance_id,
                                        table_id=table_id))
  p.run()

if __name__ == '__main__':
  run()
