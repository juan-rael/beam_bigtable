from bigtable import BigtableReadConfiguration, ReadFromBigtable

project_id='grass-clump-479'
instance_id='cpp-integration-tests'
table_id='table-c3ucqi86'

def round_up(index):
    return int(index)
def run(project_id, instance_id, table_id):
	config = BigtableReadConfiguration(project_id, instance_id, table_id)
	read_from_bigtable = ReadFromBigtable(config)
	est = read_from_bigtable.estimate_size()
	print(est)
	print('++++++++')
	for i in read_from_bigtable.sample_row_keys():
		print( i.offset_bytes )
		if est > i.offset_bytes:
			print( 'mayor' )
		else:
			print( 'menor' )
run(project_id, instance_id, table_id)