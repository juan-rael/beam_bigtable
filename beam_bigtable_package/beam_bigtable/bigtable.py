#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""BigTable connector

This module implements writing to BigTable tables.
The default mode is to set row data to write to BigTable tables.
The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt

BigTable connector can be used as main outputs. A main output
(common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. In the example below
we created a list of rows then passed to the GeneratedDirectRows
DoFn to set the Cells and then we call the BigTableWriteFn to insert
those generated rows in the table.

  main_table = (p
                | beam.Create(self._generate())
                | WriteToBigTable(project_id,
                                  instance_id,
                                  table_id))
"""
from __future__ import absolute_import

import copy
import math

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem


try:
  from google.cloud._helpers import _microseconds_from_datetime
  from google.cloud._helpers import UTC
  from google.cloud.bigtable import row
  from google.cloud.bigtable.batcher import FLUSH_COUNT, MAX_ROW_BYTES
  from google.cloud.bigtable import Client
  from google.cloud.bigtable import column_family
  from google.cloud.bigtable import enums 
except ImportError:
  FLUSH_COUNT = 1000
  MAX_MUTATIONS = 100000

__all__ = ['WriteToBigTable','ReadFromBigTable']


class _BigTableReadFn(iobase.BoundedSource):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID
  """
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    """ Constructor of the Read Source of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
      row_set():
      filter_:
    """
    super(self.__class__, self).__init__()
    from apache_beam.metrics import Metrics
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'row_set': row_set,
                         'filter_': filter_}
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'read_row')
    self.split_chunk = Metrics.counter(self.__class__, 'split')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = {'project_id': options['project_id'],
                         'instance_id': options['instance_id'],
                         'table_id': options['table_id'],
                         'row_set': options['row_set'],
                         'filter_': options['filter_']}
    self.table = None
    self.read_row = Metrics.counter(self.__class__, 'read_row')
    self.split_chunk = Metrics.counter(self.__class__, 'split')

  def _getTable(self):
    if self.table is None:
      options = self.beam_options
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    return self.table

  def get_sample_row_keys(self):
    return self._getTable().sample_row_keys()

  def get_range_tracker(self, start_position, stop_position):
    return LexicographicKeyRangeTracker(start_position, stop_position)

  def split(self,
            desired_bundle_size,
            start_position=None,
            stop_position=None):
    print('splitRead', desired_bundle_size)
    if self.beam_options['row_set'] is not None:
      for sample_row_key in self.beam_options['row_set'].row_ranges:
        sample_row_keys = self.get_sample_row_keys()
        for row_split in self.split_range_size(desired_bundle_size,
                                               sample_row_keys,
                                               sample_row_key):
          yield row_split
          print(row_split)
          self.split_chunk.inc()
    else:
      first = [i.offset_bytes for i in self.get_sample_row_keys()][0]
      if first > desired_bundle_size:
        yield iobase.SourceBundle(desired_bundle_size, self, start_key, end_key)
      else:
        addition = 0
        last_offset = 0
        current_size = 0

        start_key = b''
        end_key = b''

        sample_row_keys = self.get_sample_row_keys()
        for sample_row_key in sample_row_keys:
          current_size = sample_row_key.offset_bytes-last_offset
          if addition >= desired_bundle_size:
            end_key = sample_row_key.row_key
            for fraction in self.range_split_fraction(addition,
                                                      desired_bundle_size,
                                                      start_key, end_key):
              yield fraction
              print(fraction)
              self.split_chunk.inc()
            start_key = sample_row_key.row_key

            addition = 0
          addition += current_size
          last_offset = sample_row_key.offset_bytes

  def split_range_size(self, desired_size, sample_row_keys, range_):
    start, end = None, None
    l = 0
    for sample_row in sample_row_keys:
      current = sample_row.offset_bytes - l
      if sample_row.row_key == b'':
        continue

      if(range_.start_key <= sample_row.row_key and
         range_.end_key >= sample_row.row_key):
        if start is not None:
          end = sample_row.row_key
          range_tracker = LexicographicKeyRangeTracker(start, end)

          for split_key_range in self.split_range_sized_subranges(current,
                                                                  desired_size,
                                                                  range_tracker):
            yield split_key_range
        start = sample_row.row_key
      l = sample_row.offset_bytes

  def range_split_fraction(self,
                           current_size,
                           desired_bundle_size,
                           start_key,
                           end_key):
    range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
    return self.split_range_sized_subranges(current_size,
                                            desired_bundle_size,
                                            range_tracker)

  def fraction_to_position(self, position, range_start, range_stop):
    return LexicographicKeyRangeTracker.fraction_to_position(position,
                                                             range_start,
                                                             range_stop)

  def split_range_sized_subranges(self,
                                  sample_size_bytes,
                                  desired_bundle_size,
                                  ranges):

    last_key = copy.deepcopy(ranges.stop_position())
    s = ranges.start_position()
    e = ranges.stop_position()

    split_ = float(desired_bundle_size) / float(sample_size_bytes)
    split_count = int(math.ceil(sample_size_bytes / desired_bundle_size))

    for i in range(split_count):
      estimate_position = ((i + 1) * split_)
      position = self.fraction_to_position(estimate_position,
                                           ranges.start_position(),
                                           ranges.stop_position())
      e = position
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
      s = position
    if not s == last_key:
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

  def estimate_size(self):
    size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
    return size
  def read(self, range_tracker):
    if range_tracker.start_position() is not None:
      if not range_tracker.try_claim(range_tracker.start_position()):
        # there needs to be a way to cancel the request.
        return
    read_rows = self._getTable().read_rows(start_key=range_tracker.start_position(),
      end_key=range_tracker.stop_position(),
      filter_=self.beam_options['filter_'])

    for row in read_rows:
      self.read_row.inc()
      yield row

  def display_data(self):
    ret = {
      'projectId': DisplayDataItem(self.beam_options['project_id'],
                                   label='Bigtable Project Id',
                                   key='projectId'),
      'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                    label='Bigtable Instance Id',
                                    key='instanceId'),
      'tableId': DisplayDataItem(self.beam_options['table_id'],
                                 label='Bigtable Table Id',
                                 key='tableId')}
    
    return ret


class ReadFromBigTable(beam.PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project_id, instance_id, table_id,
               row_set=None,
               filter_=None):
    """ The PTransform to access the Bigtable Read Source
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'row_set': row_set,
                         'filter_': filter_}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | 'ReadFromBigtable' >> beam.io.Read(_BigTableReadFn(beam_options['project_id'],
                                                                 beam_options['instance_id'],
                                                                 beam_options['table_id'],
                                                                 row_set=beam_options['row_set'],
                                                                 filter_=beam_options['filter_'])))


class _BigTableWriteFn(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID

  """

  def __init__(self, project_id, instance_id, table_id,
               flush_count=FLUSH_COUNT,
               max_row_bytes=MAX_ROW_BYTES):
    """ Constructor of the Write connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(_BigTableWriteFn, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'flush_count': flush_count,
                         'max_row_bytes': max_row_bytes}
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    flush_count = self.beam_options['flush_count']
    max_row_bytes = self.beam_options['max_row_bytes']
    self.batcher = self.table.mutations_batcher(flush_count,
                                                max_row_bytes)

  def process(self, element):
    self.written.inc()
    # You need to set the timestamp in the cells in this row object,
    # when we do a retry we will mutating the same object, but, with this
    # we are going to set our cell with new values.
    # Example:
    # direct_row.set_cell('cf1',
    #                     'field1',
    #                     'value1',
    #                     timestamp=datetime.datetime.now())
    self.batcher.mutate(element)

  def finish_bundle(self):
    self.batcher.flush()
    self.batcher = None

  def display_data(self):
    return {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self.beam_options['table_id'],
                                       label='Bigtable Table Id')
           }


class WriteToBigTable(beam.PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project_id=None, instance_id=None,
               table_id=None,flush_count=FLUSH_COUNT,
               max_row_bytes=MAX_ROW_BYTES):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(WriteToBigTable, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'flush_count': flush_count,
                         'max_row_bytes': max_row_bytes}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
                                          beam_options['instance_id'],
                                          beam_options['table_id'],
                                          beam_options['flush_count'],
                                          beam_options['max_row_bytes'])))