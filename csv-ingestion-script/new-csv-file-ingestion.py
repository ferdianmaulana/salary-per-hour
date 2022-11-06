#In this case, I want to insert the data to Google BigQuery

#Import required library
import pandas as pd
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
from google.oauth2 import service_account
import pandas_gbq
import utils as ut

#Define required parameter for the insert_data function
date
file_name
path
project_id
schema
table_name

project_id = 'xxx'
schema = 'playground'
credential = 'credential.json'

#Call the function with the defined paramters to insert the olist data
ut.insert_data(project_id=project_id, schema=schema, credential=credential)