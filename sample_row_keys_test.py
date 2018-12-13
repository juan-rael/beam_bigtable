from apache_beam.io import iobase
from google.cloud.bigtable.client import Client
import pprint

def sample(desired_bundle_size):
	sample_row_keys = table.sample_row_keys()
	start_key = b''
	suma = long(desired_bundle_size)
	last = b''
	for sample_row_key in sample_row_keys:
		if suma < sample_row_key.offset_bytes:
			yield iobase.SourceBundle(1, iobase.SourceBundle, start_key, last)
			suma += desired_bundle_size
			start_key = last
		last = sample_row_key.row_key
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

size = long(1710612736)

print( "Size:" + str( size ) )



pp = pprint.PrettyPrinter(indent=4)
for i in sample(size):
	print( i )