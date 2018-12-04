from display_data import HasDisplayData, DisplayData, DisplayDataItem
from datetime import datetime, date, time

expected_items = {
	'minFilter': DisplayDataItem(42),
	'useTransactions': DisplayDataItem(''),
	'topic': DisplayDataItem(
		"projects/myproject/topics/mytopic",
		label="Pub/Sub Topic"
	),
	'serviceInstance': DisplayDataItem(
		"myservice.com/fizzbang",
		url="http://www.myservice.com/fizzbang"
	)
}

data = DisplayData(HasDisplayData()._namespace(), expected_items)
data.add(DisplayDataItem(45), 'maxFilter')
defaultStartTime = datetime.now()
startTime = defaultStartTime
data.addIfNotDefault(
	DisplayDataItem(startTime,key='startTime'),
	defaultStartTime
)
print(data)
#for i in data.get_dict():
#	print(i)