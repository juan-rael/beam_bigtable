from beam_bigtable.bigtable import BigtableConfiguration, BigtableReadConfiguration,ReadFromBigtable

project_id = 'grass-clump-479'
#instance_id = 'quickstart-instance-php'
#table_id = 'bigtable-php-table'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = ReadFromBigtable(config)
size = read_from_bigtable.estimate_size()
split_data = read_from_bigtable.split(size)

for i in split_data:
	source = i.source
	range_tracker = source.get_range_tracker(i.start_position, i.stop_position)
	source_read = source.read(range_tracker)
	for x in source_read:
		#print( dir(x) )
		#print( x.to_dict )
		#print( x.cells )

		#if 'field8' in x.cells['cf1']:
		print( x.row_key )
		#	print( x.cells['cf1']['field8'][0].value.decode('utf-8') )
		#elif 'greeting' in x.cells['cf1']:
		#	print( x.row_key )
		#	print( x.cells['cf1']['greeting'][0].value.decode('utf-8') )
		#else:
		#	pass
		print( "-------" )
	print( "+++++" )