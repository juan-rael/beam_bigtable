from beam_bigtable import BigTableSource
from apache_beam.io import iobase
import math

project_id = 'grass-clump-479'
instance_id = 'python-write-2'
table_id = 'testmillion1c1d2c39'


class BBigTableSource(BigTableSource):
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    super(BigTableSource, self).__init__(project_id,
                                       instance_id,
                                       table_id,
                                       row_set=row_set,
                                       filter_=filter_)
  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    addition_size = 0
    last_offset = 0
    current_size = 0

    start_key = b''
    end_key = b''

    for sample_row_key in self.get_sample_row_keys():
        current_size = sample_row_key.offset_bytes - last_offset
        addition_size += current_size
        if addition_size >= desired_bundle_size:
            end_key = sample_row_key.row_key
            for fraction in self.range_split_fraction(addition_size,
                                                      desired_bundle_size,
                                                      start_key,
                                                      end_key):
              yield fraction
            start_key = sample_row_key.row_key
            addition_size = 0
        last_offset = sample_row_key.offset_bytes
  def split_range_subranges(self, sample_size_bytes,
                            desired_bundle_size, ranges):
    start_position = ranges.start_position()
    end_position = ranges.stop_position()
    start_key = start_position
    end_key = end_position

    split_ = float(desired_bundle_size)/float(sample_size_bytes)
    split_ = math.floor(split_*100)/100
    size_portion = int(sample_size_bytes*split_)
    if split_ == 1 or (start_position == b'' or end_position == b''):
      yield iobase.SourceBundle(sample_size_bytes,
                                self,
                                start_position,
                                end_position)
    else:
      size_portion = int(sample_size_bytes*split_)

      sum_portion = size_portion
      while sum_portion < sample_size_bytes:
        fraction_portion = float(sum_portion)/float(sample_size_bytes)
        position = self.fraction_to_position(fraction_portion,
                                             start_position,
                                             end_position)
        end_key = position
        yield iobase.SourceBundle(size_portion, self, start_key, end_key)
        start_key = position
        sum_portion += size_portion
      last_portion = (sum_portion-size_portion)
      last_size = sample_size_bytes-last_portion
      yield iobase.SourceBundle(last_size, self, end_key, end_position)

source = BBigTableSource(project_id, instance_id,
                        table_id)
full = 805306368

splits = list(source.split(805306368))

for i in splits:
  print(i.start_position, i.stop_position)
  print('+++++')