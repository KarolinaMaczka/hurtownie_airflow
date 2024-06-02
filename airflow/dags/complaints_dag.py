from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from python_scripts.transform_financial_statements import transform_fin_statements

default_args = {
    'owner': 'myowner',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='complaints_dag_id',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 0 * * *'
)

def run_transform_fin_statements():
    return transform_fin_statements("2023q4")

transform_complaints = PythonOperator(
    task_id='transform_complaints_task',
    python_callable=run_transform_fin_statements,
    dag=dag
)

transform_complaints
