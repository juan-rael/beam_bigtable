from __future__ import absolute_import
from __future__ import division

from beam_bigtable import BigTableSource
from apache_beam.io import source_test_utils

def check(project_id, instance_id, table_id):
  print('ProjectId:', project_id)
  print('InstanceId:', instance_id)
  print('TableId:', table_id)
  print('')
  source = BigTableSource(project_id, instance_id, table_id)

  start_size = 805306368
  count = 805306368/100663296
  splits = list(source.split(desired_bundle_size=100663296))

  splits2 = list(source.split(start_position=splits[0].start_position, stop_position=splits[0].stop_position))
  print(splits)
  print(splits2)
  

check('grass-clump-479','python-write-2', 'testmillion1c1d2c39')
