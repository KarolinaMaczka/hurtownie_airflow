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
    schedule_interval='0 2 * * *',
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

        company_name_to_cik = {}
        active_companies = {}

        # Retrieve all unique company names
        unique_company_names = df_complaints_fact['companyName'].unique()

        # Fetch company CIKs for all unique company names
        cursor.execute("SELECT companyName, CIK FROM company_dim WHERE companyName IN %s", (tuple(unique_company_names),))
        for company_name, cik in cursor.fetchall():
            company_name_to_cik[company_name] = cik

        # Fetch active records for all company CIKs
        unique_ciks = list(company_name_to_cik.values())
        cursor.execute("SELECT CIK, companyId, companyName, industry FROM company_dim WHERE CIK IN %s AND isActive = TRUE", (tuple(unique_ciks),))
        for cik, company_id, company_name, industry in cursor.fetchall():
            active_companies[cik] = (company_id, company_name, industry)

        valid_fact_rows = []
        valid_dim_rows = []

        for i, row in df_complaints_fact.iterrows():
            company_name = row['companyName']
            cik = company_name_to_cik.get(company_name)
            
            if cik:
                active_record = active_companies.get(cik)
                if active_record:
                    company_id, company_name, industry = active_record
                    row['companyId'] = company_id
                    row['companyName'] = company_name
                    row['industry'] = industry
                    valid_fact_rows.append(tuple(row))
                else:
                    df_complaints_dim = df_complaints_dim[df_complaints_dim['complaintId'] != row['complaintId']]
            else:
                df_complaints_dim = df_complaints_dim[df_complaints_dim['complaintId'] != row['complaintId']]

        if not df_complaints_dim.empty:
            complaint_values = [tuple(row) for row in df_complaints_dim.to_numpy()]
            execute_values(cursor, query_complaints_dim, complaint_values)

        if valid_fact_rows:
            execute_values(cursor, query_insert, valid_fact_rows)

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

