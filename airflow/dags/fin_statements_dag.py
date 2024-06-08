from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from python_scripts.transform_financial_statements import transform_fin_statements

default_args = {
    'owner': 'myowner',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='fin_statements_dag_id',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 0 1 */3 *',
)

def run_transform_fin_statements(execution_date=None, **kwargs):
    execution_date_str = kwargs.get('execution_date_str')
    
    if execution_date_str:
        try:
            execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            try:
                execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d %H:%M:%S.%f%z')
            except ValueError:
                try:
                    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d %H:%M:%S.%f%z')
                except ValueError:
                    execution_date = datetime.now()    
    else:
        if not execution_date:
            execution_date = datetime.now()  # Use current date if no execution_date_str and no execution_date

    year = execution_date.year
    month = execution_date.month
    if month in [1, 2, 3]:
        year -= 1
        quarter = 'q4'
    elif month in [4, 5, 6]:
        quarter = 'q1'
    elif month in [7, 8, 9]:
        quarter = 'q2'
    else:
        quarter = 'q3'
    
    period = f"{year}{quarter}"
    return transform_fin_statements(period)

transform_fin = PythonOperator(
    task_id='transform_fin_statements_task',
    python_callable=run_transform_fin_statements,
    provide_context=True,
    op_kwargs={'execution_date_str': '{{ dag_run.conf["execution_date"] if dag_run and dag_run.conf else None }}',
               'execution_date': '{{ execution_date }}'},
    dag=dag,
)




