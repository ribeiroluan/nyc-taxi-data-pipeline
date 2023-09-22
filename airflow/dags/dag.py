import sys
sys.path.append('/opt/airflow/code/')

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

#import extract
#import transform
#import load_bq

args = {'owner': 'luan', 'start_date': days_ago(1), 'retries': 1}

with DAG(
    dag_id= 'nyc-taxi-pipeline',
    description= 'NYC taxi ETL',
    default_args= args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['NYC ETL'],
) as dag:
    
    run_pipeline = BashOperator(
        task_id = "run_taxi_pipeline",
        bash_command = "python /opt/airflow/code/main.py",
        dag = dag
    )