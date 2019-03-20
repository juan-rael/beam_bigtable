from apache_beam.io import iobase
from google.cloud.bigtable import Client
from beam_bigtable.bigtable import BigTableSource


project_id = 'grass-clump-479'
instance_id = 'python-write-2'
table_id = 'testmillion7abb2dc3'
client = Client(project=project_id, admin=True)
instance = client.instance(instance_id)
table = instance.table(table_id)

class NewBiTableSource(BigTableSource):
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    super(NewBiTableSource, self).__init__(project_id, instance_id, table_id, row_set, filter_)

  def split(self,
            desired_bundle_size,
            start_position=None,
            stop_position=None):
    suma = 0
    last_offset = 0
    current_size = 0

    start_key = b''
    last_key = b''
    end_key = b''

    last_byte_size = self.estimate_size()

    sample_row_keys = self.get_sample_row_keys()
    for sample_row_key in sample_row_keys:
      current_size = sample_row_key.offset_bytes-last_offset
      suma += current_size
      print('Suma', suma)
      print('DesiredBundleSize', desired_bundle_size)
      if suma >= desired_bundle_size:
        end_key = sample_row_key.row_key
        print(iobase.SourceBundle(current_size,
                                  self,
                                  start_key,
                                  end_key))
        start_key = sample_row_key.row_key

        suma = 0
      
      last_offset = sample_row_key.offset_bytes
      print('+++++++++')
    print(iobase.SourceBundle(last_byte_size-last_offset, self, end_key, last_key))
    print('+++++++++')

bigtable = NewBiTableSource(project_id, instance_id,
                                  table_id)

bigtable.split(805306368)
