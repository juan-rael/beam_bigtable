import argparse
import datetime
import unittest
import string
import random
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
#from apache_beam.runners.PipelineRunner import PipelineRunner

from google.cloud.bigtable.client import Client

from google.cloud.bigtable.row_set import RowSet
from beam_bigtable.bigtable import (BigtableConfiguration, BigtableReadConfiguration)


def _generate_mutation_data(row_index):
    row_contents = []
    value = ''.join(random.choice(string.ascii_letters + string.digits) for i in range(100))
    logging.info('Random',value)
    column_family_id = 'cf1'
    start_index = 0
    end_index = row_index
    while start_index < end_index:
        row_values = {}
        start_index += 1
        key = "beam_key%s" % ('{0:07}'.format(start_index))
        logging.info("key = ",key)
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
        row_values["table"] = "projects/grass-clump-479/instances/quickstart-instance-php/tables/bigtable-php-table"
        row_contents.append(row_values)
        logging.info("\n",row_contents,"\n")

    return row_contents


def _generate_max_mutation_value(row_index):
    row_contents = []
    number_of_bytes = 2 * 1024 * 1024
    value = b'1' * number_of_bytes  # 2MB of 1's.
    column_family_id = 'cf'
    start_index = 0
    end_index = row_index
    while start_index < end_index:
        row_values = {}
        start_index += 1
        key = "beam_key%s" % ('{0:07}'.format(start_index))
        row_values["row_key"] = key
        row_values["row_content"] = []
        for column_id in range(1):
            row_content = {
                "column_family_id": column_family_id,
                "column_id": ('field%s' % column_id).encode('utf-8'),
                "value": value
            }
            row_values["row_content"].append(row_content)

        row_contents.append(row_values)

    return row_contents


def _get_row_range_with_row_keys(row_index):
    row_set = RowSet()
    start_index = 0
    end_index = row_index
    while start_index < end_index:
        start_index += 1
        key = "beam_key%s" % ('{0:07}'.format(start_index))
        row_set.add_row_key(key)

    return row_set


class GenerateDirectRows(beam.DoFn):
    """ Generates an iterator of DirectRow object to process on beam pipeline.

    """
    def process(self, row_values):
        from google.cloud.bigtable import row
        import datetime
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

        yield direct_row


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

    def write_to_table(self, row_count,argv=[]):
        from beam_bigtable.bigtable import WriteToBigtable

        beam_options = BigtableConfiguration(self.project_id, self.instance_id, self.table_id)
        
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        row_values = _generate_mutation_data(row_count)
        pipeline_options = PipelineOptions(pipeline_args)
        with beam.Pipeline(options=pipeline_options) as p:
            (p
             | 'Generate Row Values' >> beam.Create(row_values)
             | 'Generate Direct Rows' >> beam.ParDo(GenerateDirectRows())
             | 'Write to BT' >> beam.ParDo(WriteToBigtable(beam_options)))
        
        result = p.run()
        result.wait_until_finish()

    def write_to_table_max_mutations(self):
        from beam_bigtable.bigtable import WriteToBigtable

        beam_options = BigtableConfiguration(self.project_id, self.instance_id, self.table_id)

        row_values = _generate_mutation_data(10000000000)
        pipeline_options = PipelineOptions()
        with beam.Pipeline(options=pipeline_options) as p:
            (
             p
             | 'Generate Row Values' >> beam.Create(row_values)
             | 'Generate Direct Rows' >> beam.ParDo(GenerateDirectRows())
             | 'Write to BT' >> beam.ParDo(WriteToBigtable(beam_options))
            )



    def write_to_table_max_mutation_value(self):
        from beam_bigtable.bigtable import WriteToBigtable

        beam_options = BigtableConfiguration(self.project_id, self.instance_id, self.table_id)

        row_values = _generate_max_mutation_value(10000)
        pipeline_options = PipelineOptions()
        with beam.Pipeline(options=pipeline_options) as p:
            (p
             | 'Generate Row Values' >> beam.Create(row_values)
             | 'Generate Direct Rows' >> beam.ParDo(GenerateDirectRows())
             | 'Write to BT' >> beam.ParDo(WriteToBigtable(beam_options)))

    def read_rows(self, argv=[]):
        from beam_bigtable.bigtable import ReadFromBigtable
        from apache_beam.options.pipeline_options import DebugOptions
        
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        config = BigtableReadConfiguration(self.project_id, self.instance_id, self.table_id)
        read_from_bigtable = ReadFromBigtable(config)
        pipeline_options = PipelineOptions(pipeline_args)
        # pipeline_options.view_as(SetupOptions).save_main_session = True
        debug_options = pipeline_options.view_as(DebugOptions)
    
        logging.info(debug_options)

        arg_output = 'gs://juantest/results/one_output'
        with beam.Pipeline(options=pipeline_options) as p:
            get_data = (p 
                | 'Read Rows' >> beam.io.Read(read_from_bigtable)
                | 'Print keys' >> beam.ParDo( PrintKeys() )
            )
            get_data | 'write' >> beam.io.WriteToText( arg_output )

            result = p.run()
            result.wait_until_finish()


    def read_rows_with_row_set(self):
        from beam_bigtable.bigtable import ReadFromBigtable
        beam_options = BigtableConfiguration(self.project_id, self.instance_id, self.table_id)
        bigtable_read_configuration = BigtableReadConfiguration(
            beam_options,
            row_set=_get_row_range_with_row_keys
        )
        pipeline_options = PipelineOptions()
        with beam.Pipeline(options=pipeline_options) as p:
            rows = (p | 'Read Rows' >> beam.io.Read(ReadFromBigtable(
                bigtable_read_configuration)))



def main(args):
    project_id = args.project
    instance_id=args.instance
    table_id = args.table

    my_beam = BigtableBeamProcess(project_id, instance_id, table_id)
    
    argv = [
        '--experiments=beam_fn_api',
    #   '--runner=direct',
        '--project=grass-clump-479',
        '--requirements_file=requirements.txt',
        '--runner=dataflow',
        '--staging_location=gs://juantest/stage',
        '--temp_location=gs://juantest/temp',
        '--setup_file=./beam_bigtable/setup.py',
        '--extra_package=./beam_bigtable/dist/beam_bigtable-0.1.3.tar.gz'
    ]
    my_beam.write_to_table(20, argv)
    #my_beam.read_rows(argv)


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