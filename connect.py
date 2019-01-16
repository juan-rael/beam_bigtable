import json 
import argparse

from bigtable import BigtableReadConfiguration,ReadBigtableOptions
from bigtable_read import *
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange

class PrintKeys(beam.DoFn):
    def __init__(self):
        self.printing = Metrics.counter(self.__class__, 'printing')
    def process(self, row):
        self.printing.inc()
        print(row.row_key)
        return [row.row_key]

class RReadFromBigtable(ReadFromBigtable):
    def check_range_adjacency(self, ranges=None, other=None):
        if ranges is None or other is None:
            raise ValueError('Both ranges cannot be null.')
        merged_ranges = []
        if ranges is not None:
            merged_ranges = merged_ranges + ranges
        if other is not None:
            merged_ranges = merged_ranges + other

        index = 0
        if len(merged_ranges) < 2:
            return True

        last_end_key = merged_ranges[0].stop_position
        for ranged in range(1, len(merged_ranges)):
            if not last_end_key == merged_ranges[ranged].start_position:
                return False
            last_end_key = merged_ranges[ranged].stop_position
        return True
project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

row_set = RowSet()
row_set.add_row_range(RowRange(start_key=b'user354',
                               end_key=b'user384'))
row_set.add_row_range(RowRange(start_key=b'user945',
                               end_key=b'user984'))

config = BigtableReadConfiguration(project_id, instance_id, table_id,
                                   row_set=row_set)
read_from_bigtable = RReadFromBigtable(config)

current_size = 805306368
desired_bundle_size = 201326592
# desired_bundle_size = 402653184

def check_a(read_from_bigtable, current_size, desired_bundle_size):
    for i in read_from_bigtable.split(desired_bundle_size):
        print('Split', i.start_position, i.stop_position)
        get_table = read_from_bigtable._getTable()
        for row in get_table.read_rows(start_key=i.start_position,
                                       end_key=i.stop_position):
            print("\t" + row.row_key)
        print("++++")
    print("----")
def check_b(read_from_bigtable):
    #
    array = [
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'', b'user0038'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user0038', b'user0163'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user0163', b'user04'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user04', b'user054'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user054', b'user083'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user083', b'user105'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user105', b'user127'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user127', b'user162'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user162', b'user183'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user183', b'user2'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user2', b'user213'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user213', b'user231'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user231', b'user262'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user262', b'user285'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user285', b'user311'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user311', b'user32'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user32', b'user338'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user338', b'user354'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user354', b'user384'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user384', b'user4026034'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user4026034', b'user412'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user412', b'user4365'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user4365', b'user4511'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user4511', b'user482'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user482', b'user503'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user503', b'user5112'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user5112', b'user536'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user536', b'user553'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user553', b'user58'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user58', b'user603'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user603', b'user627'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user627', b'user646'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user646', b'user665'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user665', b'user682'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user682', b'user701'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user701', b'user714'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user714', b'user732'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user732', b'user76'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user76', b'user783'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user783', b'user805'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user805', b'user817'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user817', b'user833'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user833', b'user857'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user857', b'user887'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user887', b'user903'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user903', b'user924'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user924', b'user945'),
        iobase.SourceBundle(805306368L, ReadFromBigtable, b'user945', b'user96'),
    ]
    c = iobase.SourceBundle(805306368L, ReadFromBigtable, b'user96', b'user984')

    print(read_from_bigtable.check_range_adjacency(array, [c]))
check_b(read_from_bigtable)
#check_a(read_from_bigtable,805306368, 201326592)
#check_a(read_from_bigtable,805306368, 402653184)

