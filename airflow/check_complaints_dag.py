from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.db import provide_session
from airflow.models import DagRun
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import hashlib

default_args = {
    'owner': 'myowner',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='check_complaints_dag',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 1 * * *',
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

@provide_session
def check_last_dag_run_status(dag1_id, dag2_id, session=None, **kwargs):
    dag1_run = session.query(DagRun).filter(DagRun.dag_id == dag1_id).order_by(DagRun.execution_date.desc()).first()
    dag2_run = session.query(DagRun).filter(DagRun.dag_id == dag2_id).order_by(DagRun.execution_date.desc()).first()
    if dag1_run and dag1_run.state == 'success':
        print("dag1 success")
        if dag2_run and dag2_run.state == 'success':
            print("dag2 success")
            return 'proceed_with_tasks'
    print("failure")
    return 'skip_tasks'


def insert_complaints_data(**kwargs):
    # Retrieve data, hash tagId and get connection
    df_complaints_fact = pd.read_csv('/opt/airflow/clean_data/complaints_fact.csv')
    df_complaints_dim = pd.read_csv('/opt/airflow/clean_data/complaints_dim.csv')
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Alter the column type for industry to NUMERIC
        cursor.execute("ALTER TABLE company_complaints_fact ALTER COLUMN industry TYPE NUMERIC;")
        
        query_get_company_cik = """
            SELECT CIK 
            FROM company_dim
            WHERE companyName = %s
            LIMIT 1
        """
        query_get_active_record = """
            SELECT companyId, companyName, industry
            FROM company_dim
            WHERE CIK = %s AND isActive = TRUE
        """
        query_insert = """
            INSERT INTO company_complaints_fact (complaintId, companyId, companyName, industry, complaintSentDateId, complaintRecivedDateId)
            VALUES %s
            ON CONFLICT DO NOTHING
        """ 
        query_complaints_dim = """
            INSERT INTO complaints_dim (
                complaintId, product, subProduct, issue, subIssue, customerTag, 
                customerConsent, submissionMethod, didCompanyRespondPublicly, 
                timelyResponse, consumerDisputed, consumerState, consumerZipCode
            ) VALUES %s
            ON CONFLICT (complaintId) DO NOTHING
        """

        for i, row in df_complaints_fact.iterrows():
            company_name = row['companyName']

            # Execute the query to get the company_id
            cursor.execute(query_get_company_cik, (company_name,))
            company_cik_result = cursor.fetchone()
        
            if not company_cik_result:
                print(f"No company found with the name: {company_name}")
                continue

            company_cik = company_cik_result[0]

            # Execute the query to get the active record
            cursor.execute(query_get_active_record, (company_cik,))
            active_record_result = cursor.fetchone()

            if active_record_result:
                df_complaints_fact.at[i, 'companyId'] = active_record_result[0]
                df_complaints_fact.at[i, 'companyName'] = active_record_result[1]
                df_complaints_fact.at[i, 'industry'] = active_record_result[2]
            else:
                df_complaints_dim = df_complaints_dim[df_complaints_dim['complaintId'] != row['complaintId']]
                df_complaints_fact = df_complaints_fact[df_complaints_fact['companyName'] != row['companyName']]
                print(f"No active company found with the CIK: {company_cik}")

        if not df_complaints_dim.empty:
            complaint_values = [tuple(row) for row in df_complaints_dim.to_numpy()]
            execute_values(cursor, query_complaints_dim, complaint_values)
        if not df_complaints_fact.empty:
            df_complaints_fact = df_complaints_fact.applymap(str)
            records = df_complaints_fact.to_records(index=False)
            fact_records_list = [tuple(record) for record in records]
            print("Fact Records List (first 5):", fact_records_list[:5])  # Debug print to inspect records
            execute_values(cursor, query_insert, fact_records_list)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

check_task = BranchPythonOperator(
    task_id='check_last_dag_run_status',
    python_callable=check_last_dag_run_status,
    op_kwargs = {'dag1_id':'complaints_dag_id', 'dag2_id':'check_fin_statements_dag'},
    provide_context=True,
    dag=dag,
)

proceed_with_tasks = DummyOperator(
    task_id='proceed_with_tasks',
    dag=dag,
)

skip_tasks = DummyOperator(
    task_id='skip_tasks',
    dag=dag,
)

insert_complaints_data_task = PythonOperator(
    task_id='insert_complaints_data_task',
    python_callable=insert_complaints_data,
    provide_context=True,
    dag=dag,
)



check_task >> [proceed_with_tasks, skip_tasks]
proceed_with_tasks >> insert_complaints_data_task

