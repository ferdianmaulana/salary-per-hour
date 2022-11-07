#In this case, the data will to ingested to Google BigQuery
#Import required libraries
import pandas as pd
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
from google.oauth2 import service_account
import pandas_gbq

#Define the insert_data funtion
def csv_to_bq(file_name, path, table_name, project_id, dataset_id, credential, method):
    try:
        df = pd.read_csv(path+'/'+file_name)
    except:
        status = 'Failed to Read the CSV File! '        
        raise
    else:
        try:
            credentials = service_account.Credentials.from_service_account_file(credential)
            df.to_gbq(destination_table = dataset_id+'.'+table_name,
                        project_id = project_id, if_exists = method,credentials=credentials)
            status = 'Ingestion Success!'
        except:
            status = 'Ingestion Failed!'
            raise
    return print(status)