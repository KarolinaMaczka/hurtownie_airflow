from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2023, 1, 1),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_and_populate_date_dim',
    default_args=default_args,
    description='Create and populate date_dim table',
    schedule_interval='@once',
    start_date=datetime(2023, 6, 1),
)

DB_CONN = {
    'dbname': 'hurtownie-airflow-db',
    'user': 'admin',
    'password': 'admin',  
    'host': 'postgres',
    'port': 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONN)


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
        'is_weekend': date_range.dayofweek >= 5,
        'is_fake': False
    })

    # Update is_fake for the min and max dates
    min_date_idx = date_dim['date'].idxmin()
    max_date_idx = date_dim['date'].idxmax()
    date_dim.at[min_date_idx, 'is_fake'] = True
    date_dim.at[max_date_idx, 'is_fake'] = True

    return date_dim

# Step 3: Define the functions to create and populate the table
def create_date_dim_table():
    conn = get_db_connection()
    cursor = conn.cursor()
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
        is_weekend BOOLEAN,
		is_fake BOOLEAN
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
     

def populate_date_dim_table():
    date_dim_df = create_date_dim('1900-01-01', '2100-12-31')
    conn = get_db_connection()
    cursor = conn.cursor()

    # Insert data into the date_dim table
    insert_query = """
    INSERT INTO date_dim (date_id, date, year, month, day, day_of_week, day_name, month_name, quarter, is_weekend, is_fake) VALUES %s
    """
    values = [tuple(row) for row in date_dim_df.to_numpy()]

    try:
        execute_values(cursor, insert_query, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

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
