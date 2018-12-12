from __future__ import absolute_import
import argparse
import logging
import re

import apache_beam as beam
from past.builtins import unicode
from apache_beam.io import iobase
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from google.cloud import bigtable

class PrintKeys(beam.DoFn):
  def __init__(self):
    super(PrintKeys, self).__init__()
    self.printing = Metrics.counter(self.__class__, 'printing')
  def process(self, row):
    self.printing.inc()
    return [row.row_key]

def run(argv=None):
  from beam_bigtable.bigtable import BigtableReadConfiguration,ReadFromBigtable
  argv = [
    '--experiments=beam_fn_api',
    '--project=grass-clump-479',
    '--requirements_file=requirements.txt',
    '--runner=dataflow',
    '--staging_location=gs://juantest/stage',
    '--temp_location=gs://juantest/temp',
    '--setup_file=./beam_bigtable_package/setup.py',
    '--extra_package=./beam_bigtable_package/dist/beam_bigtable-0.2.3.tar.gz',
  ]
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=pipeline_options)

  config = BigtableReadConfiguration('grass-clump-479', 'cpp-integration-tests', 'table-8lzvsh0u')
  read_from_bigtable = ReadFromBigtable(config)

  counts = (
    p 
    | 'read' >> beam.io.Read(read_from_bigtable)
    | 'print' >> beam.ParDo(PrintKeys())
  )
  result = p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()