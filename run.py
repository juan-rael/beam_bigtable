import os
tables = [
	#'perf7sjKJEPN',
	#'perf8QZOFaLr',
	'perfAhpwalkz',
	'perfDBpXPRfl',
	#'rbperf66dc6470'
]

for table in tables:
	job_name = 'test-table-' + table.lower()
	os.system("python test.py --project=grass-clump-479 --instance=endurance --table=" + table + " --job_name=" + job_name)