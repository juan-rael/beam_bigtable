from bigtable import BigtableConfiguration

from google.cloud.bigtable.row_set import RowSet, RowRange

from google.cloud.bigtable.row_filters import CellsColumnLimitFilter
from google.cloud.bigtable.row_filters import ColumnQualifierRegexFilter
from google.cloud.bigtable.row_filters import FamilyNameRegexFilter
from google.cloud.bigtable.row_filters import RowFilterChain
from google.cloud.bigtable.row_filters import RowFilterUnion
from google.cloud.bigtable.row_filters import TimestampRange
from google.cloud.bigtable.row_filters import TimestampRangeFilter
from google.cloud.bigtable.row_filters import ApplyLabelFilter

from apache_beam.transforms.display import DisplayData, DisplayDataItem


import calendar
import inspect
import json
from builtins import object
from datetime import datetime
from datetime import timedelta

class BigtableReadConfiguration(BigtableConfiguration):
    """ Bigtable read configuration variables.

    :type configuration: :class:`~BigtableConfiguration`
    :param configuration: class:`~BigtableConfiguration`

    :type row_set: :class:`row_set.RowSet`
    :param row_set: (Optional) The row set containing multiple row keys and
                    row_ranges.

    :type filter_: :class:`.RowFilter`
    :param filter_: (Optional) The filter to apply to the contents of the
                    specified row(s). If unset, reads every column in
                    each row.
    """

    def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
        super(BigtableReadConfiguration, self).__init__(project_id, instance_id, table_id)
        self.row_set = row_set
        self.filter_ = filter_
    def __str__(self):
    	import json
    	row_set = self.row_set.row_keys
    	for r in self.row_set.row_ranges:
			row_set.append(r.get_range_kwargs())
    	return json.dumps({
			'project_id': self.project_id,
			'instance_id': self.instance_id,
			'table_id': self.table_id,
			'row_set': row_set,
			'filter_': str(self.filter_.to_pb())
    	})

col1_filter = ColumnQualifierRegexFilter(b'columnbia')
label1 = u'label-read'
label1_filter = ApplyLabelFilter(label1)
chain1 = RowFilterChain(filters=[col1_filter, label1_filter])
col2_filter = ColumnQualifierRegexFilter(b'columnseeya')
label2 = u'label-blue'
label2_filter = ApplyLabelFilter(label2)
chain2 = RowFilterChain(filters=[col2_filter, label2_filter])
row_filter = RowFilterUnion(filters=[chain1, chain2])

row_set = RowSet()
row_range = RowRange(start_key=b'row-key-1', end_key=b'row-key-2',start_inclusive=True,end_inclusive=True)
row_set.add_row_range(row_range)
row_set.add_row_key(b'row-key-1')

#config = BigtableReadConfiguration('grass-clump-479', 'cpp-integration-tests', 'table-8lzvsh0u', row_set, row_filter)

#print(config)
print( dir( row_set ) )
print( row_set.row_keys )
print( row_set.row_ranges )
#data = DisplayDataItem(config, label='Bigtable Options',key='bigtableOptions',namespace=namespace)