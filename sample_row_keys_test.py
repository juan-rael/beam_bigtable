from apache_beam.io import iobase
from google.cloud.bigtable.client import Client
import pprint
from beam_bigtable.bigtable import BigtableReadConfiguration,ReadFromBigtable
from google.cloud.bigtable.row_set import RowSet

project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

client = Client(
	project = project_id
)
instance = client.instance( instance_id )
table = instance.table( table_id )

start_key = b''

size = [k.offset_bytes for k in table.sample_row_keys()][-1]
print( "Size:" + str( size ) )


config = BigtableReadConfiguration(project_id, instance_id, table_id)
read = ReadFromBigtable(config)
size = read.estimate_size()
for read_split in read.split(size):
	range_tracker = read.get_range_tracker(read_split.start_position, read_split.stop_position)
	for row in read.read(range_tracker):
		print( row )

