import argparse
import datetime
import unittest
import string
import random
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.display import DisplayDataItem
#from apache_beam.runners.PipelineRunner import PipelineRunner

from google.cloud.bigtable.client import Client

from google.cloud.bigtable.row_set import RowSet
from beam_bigtable.bigtable import (BigtableConfiguration, BigtableReadConfiguration)

class PrintKeys(beam.DoFn):
    def process(self, row):
        return [row.row_key]

class BigtableBeamProcess():

    def __init__(self, PROJECT_ID, INSTANCE_ID, TABLE_ID):
        self.project_id = PROJECT_ID
        self.instance_id = INSTANCE_ID
        self.table_id = TABLE_ID

        client = Client(project=PROJECT_ID, admin=True)
        instance = client.instance(INSTANCE_ID)
        table = instance.table(TABLE_ID)

    def setUp(self):
        self.rows_to_delete = []

    def tearDown(self):
        for row in self.rows_to_delete:
            row.clear()
            row.delete()
            row.commit()

    def read_rows(self, argv=[]):
        from beam_bigtable.bigtable import ReadFromBigtable
        from apache_beam.options.pipeline_options import DebugOptions
        from apache_beam.metrics import Metrics
        from apache_beam.metrics.metric import MetricsFilter

        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        config = BigtableReadConfiguration(self.project_id, self.instance_id, self.table_id, None, None)
        read_from_bigtable = ReadFromBigtable(config)
        pipeline_options = PipelineOptions(pipeline_args)
        # pipeline_options.view_as(SetupOptions).save_main_session = True
        debug_options = pipeline_options.view_as(DebugOptions)
    
        logging.info(debug_options)

        arg_output = 'gs://juantest/results/one_output'
        with beam.Pipeline(options=pipeline_options) as p:
            get_data = (
                p 
                | 'Read Rows' >> beam.io.Read(read_from_bigtable)
                | 'Print keys' >> beam.ParDo( PrintKeys() )
            )
            get_data | 'write' >> beam.io.WriteToText( arg_output )

            result = p.run()
            result.wait_until_finish()
            if (not hasattr(result, 'has_job') or result.has_job):
                read_row_filter = MetricsFilter().with_name('read')
                query_result = result.metrics().query(read_row_filter)
                if query_result['counters']:
                    read_row_counter = query_result['counters'][0]
                    logging.info('read rows: %d', read_row_counter.commited)
def main(args):
    project_id = args.project
    instance_id=args.instance
    table_id = args.table

    my_beam = BigtableBeamProcess(project_id, instance_id, table_id)
    argv = [
        '--experiments=beam_fn_api',
    #    '--runner=direct',
        '--project=grass-clump-479',
        '--requirements_file=requirements.txt',
        '--runner=dataflow',
        '--staging_location=gs://juantest/stage',
        '--temp_location=gs://juantest/temp',
        '--setup_file=./beam_bigtable_package/setup.py',
        '--extra_package=./beam_bigtable_package/dist/beam_bigtable-0.1.40.tar.gz',
        '--template_location=gs://juantest/templates/read_bigtable'
    ]
    my_beam.read_rows(argv)


if __name__ == '__main__':
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
    main(args)