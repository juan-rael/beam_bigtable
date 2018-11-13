from beam_bigtable.bigtable import BigtableConfiguration, BigtableReadConfiguration,ReadFromBigtable

project_id = 'grass-clump-479'
#instance_id = 'quickstart-instance-php'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'
config = BigtableReadConfiguration(project_id, instance_id, table_id)
read_from_bigtable = ReadFromBigtable(config)
size = read_from_bigtable.estimate_size()
split_data = read_from_bigtable.split(size)

for i in split_data:
	print(i.source)