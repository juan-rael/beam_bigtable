from beam_bigtable import BigTableSource
import math
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.io import iobase

class BBigTableSource(BigTableSource):
    def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
        """ Constructor of the Read connector of Bigtable
        Args:
          project_id(str): GCP Project of to write the Rows
          instance_id(str): GCP Instance to write the Rows
          table_id(str): GCP Table to write the `DirectRows`
          row_set(RowSet): This variable represents the RowRanges
          you want to use, It used on the split, to set the split
          only in that ranges.
          filter_(RowFilter): Get some expected rows, bases on
          certainly information in the row.
        """
        super(BigTableSource, self).__init__(project_id,instance_id,table_id,row_set=row_set,filter_=filter_)
    def get_range_tracker(self, start_position, stop_position):
        if stop_position == b'':
            return LexicographicKeyRangeTracker(start_position)
        else:
            return LexicographicKeyRangeTracker(start_position, stop_position)
    
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
                for fraction in self.range_split_fraction(addition_size, desired_bundle_size, start_key, end_key):
                    yield fraction
                start_key = sample_row_key.row_key
                addition_size = 0
            last_offset = sample_row_key.offset_bytes
    
    def range_split_fraction(self, current_size, desired_bundle_size,
                           start_key, end_key):
        range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
        return self.split_range_subranges(current_size, desired_bundle_size, range_tracker)
    def split_range_subranges(self, sample_size_bytes, desired_bundle_size, ranges):
        start_position = ranges.start_position()
        end_position = ranges.stop_position()
        start_key = start_position
        end_key = end_position

        if start_position == b'' or end_position == b'':
            yield iobase.SourceBundle(long(sample_size_bytes), self, start_position, end_position)
        else:
            split_ = float(desired_bundle_size)/float(sample_size_bytes)
            split_ = math.floor(split_*100)/100
            size_portion = int(sample_size_bytes*split_)

            sum_portion = size_portion
            while sum_portion<sample_size_bytes:
                fraction_portion = float(sum_portion)/float(sample_size_bytes)
                position = self.fraction_to_position(fraction_portion,
                                                     start_position,
                                                     end_position)
                end_key = position
                yield iobase.SourceBundle(long(size_portion), self, start_key, end_key)
                start_key = position
                sum_portion+= size_portion
            last_portion = (sum_portion-size_portion)
            last_size = sample_size_bytes-last_portion
            yield iobase.SourceBundle(long(last_size), self, end_key, end_position)

        
    def read(self, range_tracker):
        filter_ = self.beam_options['filter_']
        table = self._getTable()
        read_rows = table.read_rows(start_key=range_tracker.start_position(),
                                    end_key=range_tracker.stop_position(),
                                    filter_=filter_)

        for row in read_rows:
            try_claim = range_tracker.try_claim(row.row_key)
            if try_claim:
                self.read_row.inc()
                yield row


bigtable = BBigTableSource('grass-clump-479',
                           'python-write-2',
                           'testmillion1c1d2c39')
def split_source(total_size):
        if total_size:
          # 1MB = 1 shard, 1GB = 32 shards, 1TB = 1000 shards, 1PB = 32k shards
          chunk_size = max(1 << 20, 1000 * int(math.sqrt(total_size)))
        else:
          chunk_size = 64 << 20  # 64mb
        return chunk_size

split_ = split_source(bigtable.estimate_size())
for i in bigtable.split(split_):
    range_tracker = bigtable.get_range_tracker(i.start_position, i.stop_position)
    print(range_tracker.start_position(), range_tracker.stop_position())