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

from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

from beam_bigtable.bigtable import ReadFromBigtable,ReadBigtableOptions

from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange

class PrintKeys(beam.DoFn):
	def __init__(self):
		self.printing = Metrics.counter(self.__class__, 'printing')
	def process(self, row):
		self.printing.inc()
		return [row.row_key]

def run(args):
	from beam_bigtable.bigtable import BigtableReadConfiguration

	project_id = args.project
	instance_id = args.instance
	table_id = args.table

	parser = argparse.ArgumentParser()
	(known_args, pipeline_args) = parser.parse_known_args(args)

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(ReadBigtableOptions)

	p = beam.Pipeline(options=pipeline_options)

	row_set = RowSet()
	row_set.add_row_range(RowRange(start_key=b'user354', end_key=b'user384',start_inclusive=True,end_inclusive=True))
	row_set.add_row_range(RowRange(start_key=b'user646', end_key=b'user665',start_inclusive=True,end_inclusive=True))

	#config = BigtableReadConfiguration(project_id, instance_id, table_id, row_set=row_set)
	config = BigtableReadConfiguration(project_id, instance_id, table_id)
	read_from_bigtable = ReadFromBigtable(config)

	counts = (
		p 
		| 'read' >> beam.io.Read(read_from_bigtable)
		| 'print' >> beam.ParDo(PrintKeys())
	)
	result = p.run()
	#result.wait_until_finish()

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

	parser.add_argument(
		'--requirements_file',
		help=''
	)
	
	parser.add_argument(
		'--runner',
		help=''
	)

	parser.add_argument(
		'--staging_location',
		help=''
	)

	parser.add_argument(
		'--temp_location',
		help=''
	)

	parser.add_argument(
		'--setup_file',
		help=''
	)

	parser.add_argument(
		'--extra_package',
		help=''
	)


	parser.add_argument(
		'--job_name',
		help='Job name to create.'
	)
	args = parser.parse_args()
	run(args)