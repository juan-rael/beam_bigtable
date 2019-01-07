from __future__ import absolute_import
import argparse
import logging
import re
import string
import random
import datetime



import apache_beam as beam
from past.builtins import unicode
from apache_beam.io import iobase
from google.cloud import bigtable
from apache_beam.metrics import Metrics
from google.cloud.bigtable.client import Client
from google.cloud.bigtable import row
from google.cloud.bigtable.row_set import RowSet

from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions

def _generate_mutation_data(row_index):
    row_contents = []
    value = ''.join(random.choice(string.ascii_letters +
                                  string.digits) for i in range(100))
    print(value)
    column_family_id = 'cf'
    start_index = 0
    end_index = row_index
    while start_index < end_index:
        row_values = {}
        start_index += 1
        key = "beam_key%s" % ('{0:07}'.format(start_index))
        print("key = ",key)
        row_values["row_key"] = key
        #row_values["table"] = "projects/grass-clump-479/instances/test-sangram-beam/tables/test-beam1" #'test-beam1'  # remove this
        #print row_values["table"], "____________________________-"
        row_values["row_content"] = []
        for column_id in range(10):
            row_content = {
                "column_family_id": column_family_id,
                "column_id": ('field%s' % column_id).encode('utf-8'),
                "value": value
            }
            row_values["row_content"].append(row_content)
        #print "\n",row_values["row_content"],"\n"
        #row_values["table"] = "projects/grass-clump-479/instances/test-sangram-beam/tables/test-beam1" #'test-beam1'  # remove this
        row_contents.append(row_values)

    return row_contents

class GenerateDirectRows(beam.DoFn):
    """ Generates an iterator of DirectRow object to process on beam pipeline.

    """
    def __init__(self):
		self.generate_row = Metrics.counter(self.__class__, 'generate_row')
    def process(self, row_values):
        """ Process beam pipeline using an element.

        :type row_value: dict
        :param row_value: dict: dict values with row_key and row_content having
        family, column_id and value of row.
        """
        direct_row = row.DirectRow(row_key=row_values["row_key"])

        for row_value in row_values["row_content"]:
            direct_row.set_cell(
                row_value["column_family_id"],
                row_value["column_id"],
                row_value["value"],
                datetime.datetime.now())
            self.generate_row.inc()

        yield direct_row

def run(args):
	from beam_bigtable.bigtable import BigtableWriteConfiguration,WriteToBigtable

	project_id = args.project
	instance_id=args.instance
	table_id = args.table

	argv = [
		'--project=grass-clump-479',
		'--requirements_file=requirements.txt',
		'--runner=dataflow',
		'--staging_location=gs://juantest/stage',
		'--temp_location=gs://juantest/temp',
		'--setup_file=./beam_bigtable_package/setup.py',
		'--extra_package=./beam_bigtable_package/dist/beam_bigtable-0.2.45.tar.gz',
		'--num_workers=30'
	]
	parser = argparse.ArgumentParser()
	known_args, pipeline_args = parser.parse_known_args(argv)

	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True
	p = beam.Pipeline(options=pipeline_options)

	config = BigtableWriteConfiguration(project_id, instance_id, table_id)
	write_from_bigtable = WriteToBigtable(config)

	row_count = 3
	row_values = _generate_mutation_data(row_count)
	counts = (
		p
             | 'Generate Row Values' >> beam.Create(row_values)
             | 'Generate Direct Rows' >> beam.ParDo(GenerateDirectRows())
             | 'Write to BT' >> beam.ParDo(write_from_bigtable)
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