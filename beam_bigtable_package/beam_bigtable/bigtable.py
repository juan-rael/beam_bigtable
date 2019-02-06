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
from __future__ import division

import copy
import math

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable import Client
except ImportError:
  pass

__all__ = ['WriteToBigTable', 'ReadFromBigTable']


class _BigTableWriteFn(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID

  """

  def __init__(self, project_id, instance_id, table_id):
    """ Constructor of the Write connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(_BigTableWriteFn, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}
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
    self.batcher = self.table.mutations_batcher()

  def process(self, row):
    self.written.inc()
    # You need to set the timestamp in the cells in this row object,
    # when we do a retry we will mutating the same object, but, with this
    # we are going to set our cell with new values.
    # Example:
    # direct_row.set_cell('cf1',
    #                     'field1',
    #                     'value1',
    #                     timestamp=datetime.datetime.now())
    self.batcher.mutate(row)

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
               table_id=None):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(WriteToBigTable, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
                                          beam_options['instance_id'],
                                          beam_options['table_id'])))


class _BigTableReadFn(iobase.BoundedSource):
  """ Creates the connector to get the rows in Bigtable and using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID
    row_set(RowSet): Creating a set of row keys and row ranges.
    filter_(RowFilter): Filter to apply to cells in a row.

  """
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    """ Constructor of the Read connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'row_set': row_set,
                         'filter_': filter_}
    self.table = None
    self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')

  def _getTable(self):
    if self.table is None:
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

    if self.beam_options['row_set'] is not None:
      for sample_row_key in self.beam_options['row_set'].row_ranges:
        sample_row_keys = self.get_sample_row_keys()
        for row_split in self.split_range_size(desired_bundle_size,
                                               sample_row_keys,
                                               sample_row_key):
          yield row_split
    else:
      suma = 0
      last_offset = 0
      current_size = 0

      start_key = b''
      end_key = b''

      sample_row_keys = self.get_sample_row_keys()
      for sample_row_key in sample_row_keys:
        current_size = sample_row_key.offset_bytes-last_offset
        if suma >= desired_bundle_size:
          end_key = sample_row_key.row_key
          for fraction in self.range_split_fraction(suma,
                                                    desired_bundle_size,
                                                    start_key, end_key):
            yield fraction
          start_key = sample_row_key.row_key

          suma = 0
        suma += current_size
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

          for split_key_range in self.split_range_subranges(current,
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
    return self.split_range_subranges(current_size,
                                      desired_bundle_size,
                                      range_tracker)

  def fraction_to_position(self, position, range_start, range_stop):
    return LexicographicKeyRangeTracker.fraction_to_position(position,
                                                             range_start,
                                                             range_stop)

  def split_range_subranges(self,
                            sample_size_bytes,
                            desired_bundle_size,
                            ranges):

    last_key = copy.deepcopy(ranges.stop_position())
    s = ranges.start_position()
    e = ranges.stop_position()

    split_ = float(desired_bundle_size) // float(sample_size_bytes)
    split_count = int(math.ceil(sample_size_bytes // desired_bundle_size))

    for i in range(split_count):
      estimate_position = ((i + 1) * split_)
      position = self.fraction_to_position(estimate_position,
                                           ranges.start_position(),
                                           ranges.stop_position())
      e = position
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
      s = position
    if not s == last_key:
      yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key)

  def read(self, range_tracker):
    if range_tracker.start_position() is not None:
      if not range_tracker.try_claim(range_tracker.start_position()):
        # there needs to be a way to cancel the request.
        return
    start_position = range_tracker.start_position()
    stop_position = range_tracker.stop_position()
    filter_ = self.beam_options['filter_']
    read_rows = self._getTable().read_rows(start_key=start_position,
                                           end_key=stop_position,
                                           filter_=filter_)

    for row in read_rows:
      self.read_row.inc()
      yield row

  def display_data(self):
    ret = {'projectId': DisplayDataItem(self.beam_options['project_id'],
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
  def __init__(self, project_id, instance_id, table_id):
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pbegin):
    project_id = self.beam_options['project_id']
    instance_id = self.beam_options['instance_id']
    table_id = self.beam_options['table_id']
    return (pbegin
            | 'ReadFromBigtable' >> beam.io.Read(_BigTableReadFn(project_id,
                                                                 instance_id,
                                                                 table_id)))
