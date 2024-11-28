#Author: Bismark Dankwa
#importing libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#Defining the DAG default args/ this should be a dictionary
default_args = {
    'owner' : 'Bismark Dankwa',
    'start_date' : days_ago(0),
    'email' : ['dankwabismark52@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#Defining the DAG/ using the DAG class which take some parameters
dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval = timedelta(days=1),
)

#Define the unzip_data task
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag = dag, 
)

#Defining extract_data_from_csv task
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag = dag,
)

#Defining extract_data_from_tsv task
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -d$"\t" -f5-7 /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag = dag,
)

#Defining extract_data_from_fixed_width task
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command= 'cut -c59-70 /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag = dag, 
)

#Defining the consolidate_data task
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command='paste /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag= dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command= 'cut -d"," -f4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr "a-z" "A-Z" > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag = dag,
)

#Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data


