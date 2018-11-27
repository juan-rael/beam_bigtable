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


import apache_beam as beam

from apache_beam.io import iobase
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from google.cloud import bigtable
from google.cloud.bigtable.batcher import MutationsBatcher

class BigtableSource(iobase.BoundedSource):
    def __init__(self, beam_options):
        self.beam_options = beam_options
        self.table = None
        self.desired_bundle_size = 0
    def _getTable(self):
        if self.table is None:
            options = self.beam_options
            client = bigtable.Client(
                project = options.project_id,
                credentials = self.beam_options.credentials
            )
            instance = client.instance( options.instance_id )
            self.table = instance.table( options.table_id )
        return self.table
    def __getstate__(self):
        return self.beam_options

    def __setstate__(self, options):
        self.beam_options = options
        self.table = None
    def estimate_size(self):
        print("ReadFromBigtable estimate_size")
        size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
        logging.info(size)
        return size
    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        print("ReadFromBigtable split")
        sample_row_keys = self._getTable().sample_row_keys()
        start_key = b''
        for sample_row_key in sample_row_keys:
            yield iobase.SourceBundle(1, self, start_key, sample_row_key.row_key)
            start_key = sample_row_key.row_key
        if start_key != b'':
           yield iobase.SourceBundle(1, self, start_key, b'')
    def get_range_tracker(self, start_position, stop_position):
        print("ReadFromBigtable get_range_tracker")
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        logging.info("ReadFromBigtable read")
        self.desired_bundle_size = max(self.estimate_size(), self.desired_bundle_size)
        split = range_tracker.try_split(range_tracker.start_position())
        if not split:
            return
        else:
            read_rows = self._getTable().read_rows(
                start_key=range_tracker.start_position(),
                end_key=range_tracker.stop_position(),
                # row_set=self.beam_options.row_set, # This needs to be handled in split
                filter_=self.beam_options.filter_
            )

            for row in read_rows:
                logging.info("yielding " + row.row_key)
                if not range_tracker.try_claim(row.row_key):
                    return
                yield row

class ReadFromBigtable(BigtableSource):
    def __init__(self, beam_options):
        super(ReadFromBigtable, self).__init__(beam_options)
class PrintKeys(beam.DoFn):
    def process(self, row):
        print(row.row_key)
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
        from apache_beam.options.pipeline_options import DebugOptions

        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        config = BigtableReadConfiguration(self.project_id, self.instance_id, self.table_id)
        pipeline_options = PipelineOptions(pipeline_args)

        debug_options = pipeline_options.view_as(DebugOptions)

        arg_output = './one_split_output'
        with beam.Pipeline(options=pipeline_options) as p:
            rows_split = (p 
                | 'Get Rows' >> beam.io.Read(ReadFromBigtable(config))
                | 'split' >> beam.FlatMap(lambda x: x)
                | 'Print keys' >> beam.ParDo(PrintKeys())
            )

            rows_split | 'write' >> beam.io.WriteToText(arg_output)
        result = p.run()
        result.wait_until_finish()
def main(args):
    project_id = args.project
    instance_id=args.instance
    table_id = args.table

    my_beam = BigtableBeamProcess(project_id, instance_id, table_id)
    argv = [
        '--runner=direct'
    ]
    my_beam.read_rows(argv)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
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
    print("Hola")
    main(args)