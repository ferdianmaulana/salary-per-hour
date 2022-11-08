# Cost Efficiency Monitoring (salary-per-hour) Repository
This repo is created for current payroll cost-effective monitoring SQL and Python Scripts. There are two main folders in this repo, salary-per-hour-sql & csv-ingestion-script.

## salary-per-hour-sql Folder
This folder contains SQL script that will create or replace *salary_per_hour* table from CTE query. There are six (6) sub-cte in this CTE query.
1. *transform_hours* <br /> 
This sub-cte load the raw data by join two (2) tables which are timesheets & employees, and do some transformation or feature engineering and resulting 3 new columns which are:
    - *hours_raw that* contains total working hours for each employees per day.
    - *v_avg_hours that* contains the result of the moving average calculation that averaging five (5) following and five (5) preceding working hours for each employees. This feature created because there are some employee timesheets data that have working hours more than 12 hours. I assume that this company is from Indonesia, and based on Keputusan Menteri Tenaga Kerja dan Transmigrasi Nomor 102 Tahun 2004, the maximum working hours is 12 hours. So I, think this is a data anomaly that needed to be handled.
![lebih dari 12 jam](https://user-images.githubusercontent.com/37076565/200488318-bca8d19b-4dbb-451b-aaa8-aff9b8823847.PNG)
    Moreover, there are other anomaly that spotted. There are employee timesheets data that have Null value on the checkin or checkout columns. So, it's not possible to get the total working hours for some timesheets data.
![riwayat dari akhir sampai awal checkin atau out null](https://user-images.githubusercontent.com/37076565/200488393-4c96c1cb-729a-4670-a017-e5a5a94b9210.PNG)
    So, with mov_avg_hours columns, we want to replace the working hours data that have null values with this column and hopefully it can capture the working hours pattern for each person.
    - *avg_branch_hours* contains the working hours average for each branch. This columns is used to replace the timesheet data that the employees from the first time they work until the current time always have NULL value on the checkin or checkout column. So, the working hours pattern of those employees can't be captured using moving average.
2. *missing_hours_handling* <br />
This sub-cte is created to handling the NULL values in hours_raw (working hours) by replace the NULL values using mov_avg_hours column and avg_branch_hours column.
3. *get_distinct_salary* <br />
This sub-cte is created to get the distinct salary for each branch per month because the sum of salary in a branch can't be different for each month.
4. *get_total_salaries* <br /> 
This sub-cte is creted to get the total salary for each branch per month.
5. *get_total_hours* <br /> 
This sub-cte is created to get the total hours for each branch per month.
6. The Main Query <br />
In this query joined the *get_total_salaries* and *get_total_hours* sub-cte so the *salary_per_hour* can be calculated using *total_salaries* divided by total_hours.
The picture below is the outpot of the SQL script.
![output](https://user-images.githubusercontent.com/37076565/200488615-2292d5da-69b0-4164-a8d7-b7c9b7ea4646.PNG)

## csv-ingestion-script Folder
This folder contains Python script that will ingest the csv data to Google BigQuery using append method. So, it will only read the new data. In this case, I assume that it can be two (2) different cases. The first one is the new data will be updated by using new csv file that differentiated by the prefix (date) file name. The second case is the new data will be updated in the same csv file. So, it will incrementally update in the same csv file. Based on that assumption, I create two (2) functions that can accommodate those two cases. In this folder, there three (3) scripts:
1. *utils* <br />
    This script contains the data ingestion functions. There are two (2) functions in this script, namely:
    - *csv_to_bq_dif_file* <br />
        This function will be used when the new data updated by difference csv file that differentiate using the date prefix of the file name. This function has seven (7) parameters such as:
        + file_name = the csv file name that concatenate with h-1 date. so it can be dynamically call the latest file
        + path = the path folder of the csv files
        + table_name = the destnation table name
        + project_id = the GCP project id
        + dataset_id = the schema of the destination table
        + credential = the GCP credential (Service Account)
        + method = ingestion method (replace or append)

    - *csv_to_bq_same_file* <br />
        This function will be used when the new data updated in the same csv file (incremental update). This function read the new data by saving the last ingestion log. The log will save the date and the last row number of the last time the csv ingested to BigQuery. So, before read the csv files, the function will read the log first and then it only read the new data on the csv files. This function also has seven (7) parameters such as:
        + file_name = the csv file name
        + path = the path folder of the csv files
        + table_name = the destnation table name
        + project_id = the GCP project id
        + dataset_id = the schema of the destination table
        + credential = the GCP credential (Service Account)
        + method = ingestion method (replace or append)
