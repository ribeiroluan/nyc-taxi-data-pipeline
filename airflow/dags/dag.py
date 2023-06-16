import sys
sys.path.append('nyc_taxi/airflow/code/')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import extract

data = extract.ExtractTaxiData()

args = {'owner': 'luan', 'start_date': days_ago(1), 'retries': 1}

with DAG(
    dag_id = 'nyc_taxi_pipeline'
    description= 'NYC taxi pipeline',
    default_args= args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['NYC TAXI ETL']
) as dag:
    task_extract = PythonOperator()
    task_transform = PythonOperator()
    task_load_to_bq = PythonOperator()

task_extract > task_transform > task_load_to_bq