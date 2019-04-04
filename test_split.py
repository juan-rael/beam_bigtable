from apache_beam.io import iobase
from google.cloud.bigtable import Client
from beam_bigtable.bigtable import BigTableSource

from google.cloud.bigtable.row_set import RowRange
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from google.cloud.bigtable.row_set import RowSet
from apache_beam.metrics import Metrics

import copy
import math

project_id = 'grass-clump-479'
instance_id = 'python-write-2'
table_id = 'testmillionb38c02c4'
#table_id = 'testmillioned113e20'
client = Client(project=project_id, admin=True)
instance = client.instance(instance_id)
table = instance.table(table_id)

bigtable = BigTableSource(project_id, instance_id,
                          table_id)
for i in bigtable.get_sample_row_keys():
  print(i.row_key)
  print(i.offset_bytes)