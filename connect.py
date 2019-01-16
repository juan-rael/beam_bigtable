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
read_from_bigtable = ReadFromBigtable(config)

current_size = 805306368
desired_bundle_size = 201326592
# desired_bundle_size = 402653184


for i in read_from_bigtable.split(desired_bundle_size):
    print('Split', i.start_position, i.stop_position)
    get_table = read_from_bigtable._getTable()
    for row in get_table.read_rows(start_key=i.start_position,
                                   end_key=i.stop_position):
        print("\t" + row.row_key)
    print("++++")
print("----")
