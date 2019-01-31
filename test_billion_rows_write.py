from __future__ import absolute_import
import argparse
import datetime
import random
import string
import uuid


import apache_beam as beam
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
from google.cloud.bigtable import enums 

from beam_bigtable import WriteToBigTable

EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}

class GenerateRow(beam.DoFn):
  def __init__(self):
    if not hasattr(self, 'generate_row'):
      self.generate_row = Metrics.counter(self.__class__.__name__, 'generate_row')

  def process(self, key):
    key = "beam_key%s" % ('{0:07}'.format(key))
    rand = random.choice(string.ascii_letters + string.digits)
    value = ''.join(rand for i in range(30))
    column_id = 1
    direct_row = row.DirectRow(row_key=key)
    direct_row.set_cell(
                'cf1',
                ('field%s' % column_id).encode('utf-8'),
                value,
                datetime.datetime.now())
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
  instance_id = 'python-write'
  DEFAULT_TABLE_PREFIX = "python-test"
  #table_id = DEFAULT_TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
  guid = str(uuid.uuid4())[:8]
  table_id = 'testmillion' + guid
  jobname = 'testmillion-write-' + guid
  

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
    '--autoscaling_algorithm=NONE',
    '--num_workers=10',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.7.tar.gz'
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

  row_count = 10000000
  row_step = row_count if row_count <= 10000 else row_count/10000
  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)
  config_data = {'project_id': project_id,
                 'instance_id': instance_id,
                 'table_id': table_id}
  
  count = (p
           | 'Ranges' >> beam.Create([(str(i),str(i+row_step)) for i in xrange(0, row_count, row_step)])
           | 'Group' >> beam.GroupByKey()
           | 'Flat' >> beam.FlatMap(lambda x: list(xrange(int(x[0]), int(x[0])+row_step)))
           | 'Generate' >> beam.ParDo(GenerateRow())
           | 'Write' >> WriteToBigTable(project_id=project_id,
                                        instance_id=instance_id,
                                        table_id=table_id))
  p.run()
#  result.wait_until_finish()

if __name__ == '__main__':
  run()
