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
from beam_bigtable.bigtable import BigtableReadConfiguration
from apache_beam.io.iobase import SourceBundle
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.display import HasDisplayData
from google.cloud.bigtable.batcher import MutationsBatcher
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker


class PrintKeys(beam.DoFn):
	def __init__(self):
		self.printing = Metrics.counter(self.__class__, 'printing')
	def process(self, row):
		self.printing.inc()
		return [row.row_key]

class ReadFromBigtable(iobase.BoundedSource):
	""" Bigtable apache beam read source
	:type split_keys: dict
						'~end_key'}`.
	:type beam_options:
	class:`~bigtable_configuration.BigtableReadConfiguration`
	:param beam_options: Class
	`~bigtable_configuration.BigtableReadConfiguration`.
	"""
	def __init__(self, beam_options):
		super(ReadFromBigtable, self).__init__()
		self.beam_options = beam_options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')

	def _getTable(self):
		if self.table is None:
			options = self.beam_options
			client = bigtable.Client(
				project=options.project_id,
				credentials=self.beam_options.credentials)
			instance = client.instance(options.instance_id)
			self.table = instance.table(options.table_id)
		return self.table

	def __getstate__(self):
		return self.beam_options

	def __setstate__(self, options):
		self.beam_options = options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')

	def estimate_size(self):
		size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
		return size

	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		sample_row_keys = self._getTable().sample_row_keys()
		start_key = b''
		for sample_row_key in sample_row_keys:
			yield iobase.SourceBundle(1, self, start_key, sample_row_key.row_key)
			start_key = sample_row_key.row_key
		if start_key != b'':
			yield iobase.SourceBundle(1, self, start_key, b'')

	def get_range_tracker(self, start_position, stop_position):
		return LexicographicKeyRangeTracker(start_position, stop_position)

	def read(self, range_tracker):
		read_rows = self._getTable().read_rows(
			start_key=range_tracker.start_position(),
			end_key=range_tracker.stop_position(),
			filter_=self.beam_options.filter_
		)
		for row in read_rows:
			if not range_tracker.try_claim(row.row_key):
				# there needs to be a way to cancel the request.
				return
			self.read_row.inc()
			yield row
	
	def display_data(self):
		ret = {
			'projectId': DisplayDataItem(self.beam_options.project_id, label='Bigtable Project Id', key='projectId'),
			'instanceId': DisplayDataItem(self.beam_options.instance_id, label='Bigtable Instance Id',key='instanceId'),
			'tableId': DisplayDataItem(self.beam_options.table_id, label='Bigtable Table Id', key='tableId'),
			'bigtableOptions': DisplayDataItem(str(self.beam_options), label='Bigtable Options', key='bigtableOptions'),
		}
		if self.beam_options.row_set is not None:
			i = 0
			for value in self.beam_options.row_set.row_keys:
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
			for (i,value) in enumerate(self.beam_options.row_set.row_ranges):
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value.get_range_kwargs()), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
		if self.beam_options.filter_ is not None:
			for (i,value) in enumerate(self.beam_options.filter_.filters):
				ret['rowFilter{}'.format(i)] = DisplayDataItem(str(value.to_pb()), label='Bigtable Row Filter {}'.format(i), key='rowFilter{}'.format(i))
		return ret

def run(argv=None):
	from beam_bigtable.bigtable import BigtableReadConfiguration
	#argv_input = 'gs://dataflow-samples/shakespeare/kinglear.txt'
	argv = [
		'--project=grass-clump-479',
		'--requirements_file=requirements.txt',
		'--runner=dataflow',
		'--staging_location=gs://juantest/stage',
		'--temp_location=gs://juantest/temp',
		'--setup_file=./beam_bigtable_package/setup.py',
		'--extra_package=./beam_bigtable_package/dist/beam_bigtable-0.1.64.tar.gz',
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