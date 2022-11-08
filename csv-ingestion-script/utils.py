#In this case, the data will to ingested to Google BigQuery
#Import required libraries
import pandas as pd
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
from google.oauth2 import service_account
import pandas_gbq
from datetime import date
today = date.today()
today = today.strftime("%d%m%Y")

#Define the insert_data funtion
def csv_to_bq_dif_file(file_name, path, table_name, project_id, dataset_id, credential, method):
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

def csv_to_bq_same_file(file_name, path, table_name, project_id, dataset_id, credential, method):
    try:
        log = pd.read_csv('log_'+table_name+'.csv')
    except:
        print('First Time Ingestion.')
        skiprows = None
    else:
        log_row = log.last_row.iloc[0]
        skiprows = range(1,log_row+1)
        
    try:
        df = pd.read_csv(path+'/'+file_name, skiprows=skiprows)
    except:
        status = 'Failed to Read the CSV File!'        
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
    if skiprows == None:
        last_row = df.shape[0]
    else:
        last_row = df.shape[0]+log_row
        
    log_write = pd.DataFrame([{'last_ingestion_date':today,'last_row':last_row}])
    log_write = log_write.to_csv('log_'+table_name+'.csv', index=False)
    return print(status)