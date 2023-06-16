import sys
sys.path.append('/airflow/code/')


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import extract
import transform
import load_bq

args = {'owner': 'luan', 'start_date': days_ago(1), 'retries': 1}

with DAG(
    dag_id= 'nba-player-career-etlv2',
    description= 'NBA career ETL',
    default_args= args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['NBA ETL'],
) as dag:

    extract = PythonOperator(
        task_id = "extract",
        python_callable = extract.ExtractTaxiData().write(),
        dag=dag)
    
    transform = PythonOperator(
        task_id = "extract",
        python_callable = transform.TransformTaxiData().create_fact_and_dimensions(),
        dag=dag)
    
    load_to_bq = PythonOperator(
        task_id = "load_to_bq",
        python_callable = load_bq.LoadToBQ().create_fact_and_dimensions(),
        dag=dag)