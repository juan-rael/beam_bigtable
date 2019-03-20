from __future__ import absolute_import
import argparse
import copy
import datetime
import math
import uuid


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.metrics.metric import Metrics
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from google.cloud.bigtable import Client
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row
from google.cloud._helpers import _microseconds_from_datetime
from google.cloud._helpers import UTC

from beam_bigtable import ReadFromBigTable
from beam_bigtable import WriteToBigTable


EXISTING_INSTANCES = []
LABEL_KEY = u'python-bigtable-beam'
label_stamp = datetime.datetime.utcnow().replace(tzinfo=UTC)
label_stamp_micros = _microseconds_from_datetime(label_stamp)
LABELS = {LABEL_KEY: str(label_stamp_micros)}


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


class GenerateRow(beam.DoFn):
  def __init__(self):
    if not hasattr(self, 'generate_row'):
      self.generate_row = Metrics.counter(self.__class__.__name__, 'generate_row')

  def process(self, element):
    key = element.row_key

    direct_row = row.DirectRow(row_key=key)
    for (cf_name, cf_value) in element.cells.items():
      for (f_name, f_value) in cf_value.items():
        for i in f_value:
          direct_row.set_cell(
                cf_name,
                f_name,
                i.value,
                i.timestamp)
    self.generate_row.inc()
    yield direct_row


def run():
  argv=[]
  read_config = {
    'project_id':'grass-clump-479',
    'instance_id':'python-write',
    'table_id':'testmillion6bd104b8',
  }
  # read_config = {
  #   'project_id':'grass-clump-479',
  #   'instance_id':'endurance',
  #   'table_id':'perf1DFN4UF2',
  # }
  guid = str(uuid.uuid4())[:8]
  write_config = {
    'project_id':'grass-clump-479',
    'instance_id':'python-write',
    'table_id':'testmillion' + guid,
  }
  
  print('Read Config:', read_config)
  print('Write Config:', write_config)

  argv.extend([
    '--experiments=beam_fn_api',
    '--project={}'.format(read_config['project_id']),
    '--instance={}'.format(read_config['instance_id']),
    '--table={}'.format(read_config['table_id']),
    '--projectId={}'.format(read_config['project_id']),
    '--instanceId={}'.format(read_config['instance_id']),
    '--tableId={}'.format(read_config['table_id']),
    '--requirements_file=requirements.txt',
    '--runner=dataflow',
    '--autoscaling_algorithm=NONE',
    '--num_workers=10',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=/usr/src/app/example_bigtable_beam/beam_bigtable_package/setup.py',
    '--extra_package=/usr/src/app/example_bigtable_beam/beam_bigtable_package/dist/beam_bigtable-0.3.39.tar.gz'
  ])
  parser = argparse.ArgumentParser(argv)
  parser.add_argument('--projectId')
  parser.add_argument('--instanceId')
  parser.add_argument('--tableId')
  (known_args, pipeline_args) = parser.parse_known_args(argv)

  create_table = CreateAll(write_config['project_id'],
                           write_config['instance_id'],
                           write_config['table_id'])
  create_table.create_table()

  pipeline_options = PipelineOptions(argv)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  with beam.Pipeline(options=pipeline_options) as p:
    _ = (p
         | 'BigtableFromRead' >> ReadFromBigTable(project_id=read_config['project_id'],
                                                  instance_id=read_config['instance_id'],
                                                  table_id=read_config['table_id'])
         | 'GenerateRow' >> beam.ParDo(GenerateRow())
         | 'Write' >> WriteToBigTable(project_id=write_config['project_id'],
                                      instance_id=write_config['instance_id'],
                                      table_id=write_config['table_id']))
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
  run()
