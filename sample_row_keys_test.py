from apache_beam.io import iobase
from google.cloud.bigtable.client import Client
import pprint

def sample(table,desired_bundle_size):
	sample_row_keys = table.sample_row_keys()
	start_key = b''
	suma = long(0)
	for sample_row_key in sample_row_keys:
		tmp = suma + desired_bundle_size
		if tmp <= sample_row_key.offset_bytes:
			yield iobase.SourceBundle(1, iobase.SourceBundle, start_key, sample_row_key.row_key)
			start_key = sample_row_key.row_key
			suma += desired_bundle_size
	if start_key != b'':
		yield iobase.SourceBundle(1, iobase.SourceBundle, start_key, b'')



project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

desired_bundle_size = long()
client = Client(
	project = project_id
)
instance = client.instance( instance_id )
table = instance.table( table_id )

start_key = b''

size = [k.offset_bytes for k in table.sample_row_keys()][-1]
size = size / 50
print( "Size:" + str( size ) )

for i in sample(table,size):
	print( i )