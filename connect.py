from google.cloud.bigtable import Client

project_id = 'grass-clump-479'
instance_id = 'python-write-2'
table_id = 'testmillionb38b8c9f'


client = Client(project=project_id)
instance = client.instance(instance_id)
table = instance.table(table_id)
sample_row_keys = table.sample_row_keys()

s = [k.offset_bytes for k in sample_row_keys][-1]

print(s)

desired_bundle_size = 8053063680
piece = 80
chunk = desired_bundle_size / piece
denew = chunk*piece


print(desired_bundle_size)
print(piece)
print(chunk)

print(desired_bundle_size-denew)