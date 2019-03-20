import datetime
import uuid
from google.cloud.bigtable import row, column_family, Client
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import Table

DEFAULT_TABLE_PREFIX = "testtimeout"

beam_options = {
    'project_id': 'grass-clump-479',
    'instance_id': 'python-write-2',
    'table_id': DEFAULT_TABLE_PREFIX + str(uuid.uuid4())[:8]
}


class CustomTable(Table):
    def __init__(self, table_id, instance, mutation_timeout=None, app_profile_id=None):
        self.table_id = table_id
        self._instance = instance
        self._app_profile_id = app_profile_id
        self.mutation_timeout = mutation_timeout


class CustomInstance(Instance):
    def table(self, table_id, mutation_timeout=None, app_profile_id=None):
        return CustomTable(table_id, self,
                           app_profile_id=app_profile_id,
                           mutation_timeout=mutation_timeout)


class CustomClient(Client):
    def instance(self, instance_id, display_name=None, instance_type=None, labels=None):
        return CustomInstance(
            instance_id,
            self,
            display_name=display_name,
            instance_type=instance_type,
            labels=labels,
        )


client = Client(project=beam_options['project_id'], admin=True)
instance = client.instance(beam_options['instance_id'])
table = instance.table(beam_options['table_id'], mutation_timeout=600000)
if not table.exists():
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}
    table.create(column_families=column_families)
mutation_batcher = table.mutations_batcher()

for i in range(0, 10):
    key = "beam_key%s" % i
    row_element = row.DirectRow(row_key=key)

    row_element.set_cell(
        'cf1',
        ('field%s' % i).encode('utf-8'),
        'abc',
        datetime.datetime.now())

    mutation_batcher.mutate(row_element)
mutation_batcher.flush()
