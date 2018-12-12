from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

JOBNAME = 'read_bigtable_code'
PROJECT = 'grass-clump-479'
BUCKET = 'juantest'
TEMPLATE = 'read_bigtable'

BODY = {
    "jobName": "{jobname}".format(jobname=JOBNAME),
    "parameters": {
     },
     "environment": {
        "tempLocation": "gs://{bucket}/temp".format(bucket=BUCKET),
        "zone": "us-central1-f"
     }
}

request = service.projects().templates().launch(
	projectId=PROJECT,
	gcsPath="gs://{bucket}/templates/{template}".format(bucket=BUCKET, template=TEMPLATE),
	body=BODY
)
response = request.execute()

print(response)