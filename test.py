from __future__ import absolute_import
import argparse
import logging
import re

import apache_beam as beam
from past.builtins import unicode
from apache_beam.io import iobase
from google.cloud import bigtable
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle

from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.transforms.display import DisplayDataItem
from beam_bigtable.bigtable import BigtableReadConfiguration
#from bigtable import BigtableReadConfiguration
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker


class PrintKeys(beam.DoFn):
	def __init__(self):
		self.printing = Metrics.counter(self.__class__, 'printing')
	def process(self, row):
		self.printing.inc()
		return [row.row_key]

def run(args):
	from beam_bigtable.bigtable import BigtableReadConfiguration,ReadFromBigtable
	#from bigtable import BigtableReadConfiguration,ReadFromBigtable

	project_id = args.project
	instance_id=args.instance
	table_id = args.table

	#argv_input = 'gs://dataflow-samples/shakespeare/kinglear.txt'
	argv = [
		'--project=grass-clump-479',
		'--requirements_file=requirements.txt',
		'--runner=dataflow',
		#'--runner=direct',
		'--staging_location=gs://juantest/stage',
		'--temp_location=gs://juantest/temp',
		'--setup_file=./beam_bigtable_package/setup.py',
		'--extra_package=./beam_bigtable_package/dist/beam_bigtable-0.2.20.tar.gz',
		'--num_workers=30'
	]
	parser = argparse.ArgumentParser()
	known_args, pipeline_args = parser.parse_known_args(argv)

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True
	p = beam.Pipeline(options=pipeline_options)

	config = BigtableReadConfiguration(project_id, instance_id, table_id)
	read_from_bigtable = ReadFromBigtable(config)

	counts = (
		p 
		| 'read' >> beam.io.Read(read_from_bigtable)
		| 'print' >> beam.ParDo(PrintKeys())
	)
	result = p.run()
	result.wait_until_finish()

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	parser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument(
		'--project',
		help='Your Cloud Platform project ID.'
	)
	parser.add_argument(
		'--instance',
		help='ID of the Cloud Bigtable instance to connect to.'
	)
	parser.add_argument(
		'--table',
		help='Table to create and destroy.'
	)
	args = parser.parse_args()
	run(args)