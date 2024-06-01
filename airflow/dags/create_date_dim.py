from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'populate_date_dim',
    default_args=default_args,
    description='Create and populate date_dim table',
    schedule_interval='@once',
)

# Step 2: Generate Date Dimension Data in Python
def create_date_dim(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)
    date_dim = pd.DataFrame({
        'date_id': date_range.strftime('%Y%m%d').astype(int),
        'date': date_range,
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.strftime('%A').astype("string"),
        'month_name': date_range.strftime('%B').astype("string"),
        'quarter': date_range.quarter,
        'is_weekend': date_range.dayofweek >= 5
    })

    return date_dim

# Step 3: Define the functions to create and populate the table
def create_date_dim_table():
    pg_hook = PostgresHook(postgres_conn_id='postgress-baza')
    create_table_query = """
    CREATE TABLE IF NOT EXISTS date_dim (
        date_id BIGINT PRIMARY KEY,
        date DATE NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        day_of_week INT NOT NULL,
        day_name VARCHAR(10),
        month_name VARCHAR(10),
        quarter INT,
        is_weekend BOOLEAN
    );
    """
    pg_hook.run(create_table_query)

def populate_date_dim_table():
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection_id')
    date_dim_df = create_date_dim('2000-01-01', '2100-12-31')

    # Insert data into the date_dim table
    insert_query = """
    INSERT INTO date_dim (date_id, date, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date_id) DO NOTHING;
    """
    rows = date_dim_df.values.tolist()

    pg_hook.insert_rows(table='date_dim', rows=rows, target_fields=[
        'date_id', 'date', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter', 'is_weekend'
    ], replace=False)

# Create PythonOperator tasks
create_table_task = PythonOperator(
    task_id='create_date_dim_table',
    python_callable=create_date_dim_table,
    dag=dag,
)

populate_table_task = PythonOperator(
    task_id='populate_date_dim_table',
    python_callable=populate_date_dim_table,
    dag=dag,
)

# Define task dependencies
create_table_task >> populate_table_task
