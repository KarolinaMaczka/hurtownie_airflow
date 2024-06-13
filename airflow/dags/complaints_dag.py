from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from python_scripts.transform_complaints import transform_all_complaints

default_args = {
    'owner': 'myowner',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='complaints_dag_id',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 0 * * *' #daily
)

def run_transform_complaints():
    transform_all_complaints(1000)


transform_complaints = PythonOperator(
    task_id='transform_complaints_task',
    python_callable=run_transform_complaints,
    dag=dag
)

