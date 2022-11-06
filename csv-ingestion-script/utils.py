#Define the insert_data funtion
def insert_data(datasets, project_id, schema, credential):
    try:
        for dataset in datasets:
            df = pd.read_csv('data/'+dataset)
            credentials = service_account.Credentials.from_service_account_file(credential)
            df.to_gbq(destination_table = schema+'.'+dataset.replace('.csv',''),
                        project_id = project_id, if_exists = 'replace',credentials=credentials)
        status = 'Insert data berhasil!'
    except:
        status = 'Insert data gagal!'
    return print(status)
