from apache_beam.io import iobase
from google.cloud.bigtable.client import Client

project_id = 'grass-clump-479'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

desired_bundle_size = ''

client = Client(
	project = project_id
)
instance = client.instance( instance_id )
table = instance.table( table_id )

start_key = b''
for i in [k for k in table.sample_row_keys()]:
	print( i.row_key )
	print( str( i.offset_bytes ) )
	print( type( i.offset_bytes ) )
	print( '+++++++' )
	start_key = i.row_key