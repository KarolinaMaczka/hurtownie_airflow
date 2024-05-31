from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from python_scripts import transform_complaints

default_args = {
    'owner':'myowner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'complaints_dag',
    dag_id = 'complaints_dag_id',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 0 * * *'
)

transform_complaints = PythonOperator(
    task_id='print_random_quote',
    python_callable=transform_complaints,
    dag=dag
)

transform_complaints 