import argparse

import apache_beam as beam

from bigtable import BigtableReadConfiguration, ReadBigtableOptions
from bigtable_read import *
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions

from google.cloud.bigtable import Client
from google.cloud.bigtable.batcher import MutationsBatcher

class PrintKeys(beam.DoFn):
  def __init__(self, *args, **kargs):
    super(PrintKeys, self).__init__(*args, **kargs)
    self.project_id = kargs['project_id']
    self.instance_id = kargs['instance_id']
    self.table_id = kargs['table_id']
    self.table = None
    self.client = None
    self.batcher = None

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.project_id)
      instance = client.instance(self.instance_id)
      self.table = instance.table(self.table_id)
    self.batcher = MutationsBatcher(self.table)


  def process(self, row):
    print(row)
    return [row]


class WriteToBigTable(beam.PTransform):
  def __init__(self, project_id=None, instance_id=None,
               table_id=None):
    super(WriteToBigTable, self).__init__()
    self.column_family_id = 'cf1'
    self.project_id = project_id
    self.instance_id = instance_id
    self.table_id = table_id
  def expand(self, pvalue):
    return (pvalue
            | 'Generate Direct Rows' >> beam.Create(['a', 'b', 'c'])
            | 'Print' >> beam.ParDo(PrintKeys(project_id=self.project_id,
                                              instance_id=self.instance_id,
                                              table_id=self.table_id)))

project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

pipeline_args = [
  '--project=' + project_id,
  '--instance=' + instance_id,
  '--table=' + table_id
]
pipeline_options = PipelineOptions(pipeline_args)
with beam.Pipeline(options=pipeline_options) as pipeline:
    _ = (pipeline
         | 'Write To BigTable' >> WriteToBigTable(project_id=project_id,
                                                  instance_id=instance_id,
                                                  table_id=table_id))