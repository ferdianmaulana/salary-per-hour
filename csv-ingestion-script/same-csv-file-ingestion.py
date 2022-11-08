#This script will be scheduled daily to ingest the data to Google BigQuery
# #Import required libraries
import utils as ut                              #utils library, that already created

#Define required parameter for the csv_to_bq_same_file function
file_name = 'timesheets.csv'               #Data Source File Name
path = 'data/'                             #Data Source Path Folder
table_name = 'timesheets'                  #Destination Table Name
project_id = 'xxx'                         #GCP Project ID
dataset_id = 'playground'                  #BigQuery Dataset ID
credential = 'credential.json'             #GCP Credential (service account)
method = 'append'                          #Ingest Methodology (replace or append or fail)

#Call the function with the defined paramters to ingest the csv file
ut.csv_to_bq_same_file(file_name=file_name, path=path, table_name=table_name, 
                project_id=project_id, dataset_id=dataset_id, credential=credential, method = method)