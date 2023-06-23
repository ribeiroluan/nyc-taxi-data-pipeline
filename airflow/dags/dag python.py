import sys
sys.path.append('/opt/airflow/code/')


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import extract
import transform
import load_bq

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

    extract_task = PythonOperator(
        task_id = "extract-taxi-data",
        python_callable = extract.ExtractTaxiData().writer,
        dag=dag)
    
    transform_task = PythonOperator(
        task_id = "transform-taxi-data",
        python_callable = transform.TransformTaxiData().create_fact_and_dimensions,
        dag=dag)
    
    load_to_bq_task = PythonOperator(
        task_id = "load-taxi-data-to_bq",
        python_callable = load_bq.LoadToBQ().load,
        dag=dag)
    
    extract_task > transform_task > load_to_bq_task