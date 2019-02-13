from google.cloud.bigtable import Client

project_id = 'grass-clump-479'
instance_id = 'python-write-2'
table_id = 'testmillionb38b8c9f'


client = Client(project=project_id,admin=True)
instance = client.instance(instance_id)
table = instance.table(table_id)
