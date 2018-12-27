from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_set import RowRange
from google.cloud._helpers import _to_bytes




class MakeRowSet():
	def test__update_message_request(self):
		row_set = RowSet()
		table_name = "table_name"
		row_set.add_row_range( RowRange(b"user354", b"user384") )
		row_set.add_row_range( RowRange(b"user646", b"user665") )

		request = _ReadRowsRequestPB(table_name=table_name)
		row_set._update_message_request(request)

		for i in row_set.row_ranges:
			print( i.start_key )
			print( i.end_key )
			print( "++++" )

def _ReadRowsRequestPB(*args, **kw):
	from google.cloud.bigtable_v2.proto import bigtable_pb2 as messages_v2_pb2
	return messages_v2_pb2.ReadRowsRequest(*args, **kw)

a = MakeRowSet()
a.test__update_message_request()