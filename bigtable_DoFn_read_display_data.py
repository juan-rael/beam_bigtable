class PrintKeys(beam.DoFn):
    def process(self, row):
        #print( row.row_key )
        return [row.row_key]
	def display_data(self):
	  getBigtableConfig().populateDisplayData(builder);
	  
	  ret = {
	    'projectId': DisplayDataItem(getProjectId(), label='Bigtable Project Id').drop_if_none(),
	    'instanceId': DisplayDataItem(getInstanceId(), label='Bigtable Instance Id').drop_if_none(),
	    'tableId': DisplayDataItem(getTableId(), label='Bigtable Table Id').drop_if_none(),
	    'withValidation': DisplayDataItem(getValidate(), label='Check is table exists').drop_if_none()
	  }
	  if getBigtableOptions() is None:
	    ret['bigtableOptions'] = DisplayDataItem(getBigtableOptions().toString(), label='Bigtable Options').drop_if_none()
	  return ret