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
    dag_id='check_fin_statements_dag',
    default_args=default_args,
    start_date=datetime(2024, 5, 14),
    schedule_interval='0 0 1 */3 *',
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

def process_statement_id(value):
    """Convert a statementId string to a numeric value by removing '-'."""
    return int(value.replace('-', ''))

@provide_session
def check_last_dag_run_status(session=None, **kwargs):
    dag_id = 'fin_statements_dag_id'
    dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
    print(f'Last dag run date: {dag_run.execution_date}')
    if dag_run and dag_run.state == 'success':
        return 'proceed_with_tasks'
    else:
        return 'skip_tasks'
    
def hash_string_to_int(value):
    """Convert a string to a hashed integer value using MD5."""
    return int(hashlib.md5(value.encode()).hexdigest(), 16)

def insert_tags_data(**kwargs):
    # Load data from tags.csv and upsert into database
    df_tags = pd.read_csv('/opt/airflow/clean_data/tags.csv')
    df_tags['tagId'] = df_tags['tagId'].apply(lambda x: int(hashlib.md5(x.encode()).hexdigest(), 16))
    records_tags = df_tags.to_dict('records')

    query_tags = """
        INSERT INTO tags (tagId, Tag, version, tagDescription, type)
        VALUES %s
        ON CONFLICT (tagId) DO UPDATE SET
        Tag = EXCLUDED.Tag,
        version = EXCLUDED.version,
        tagDescription = EXCLUDED.tagDescription,
        type = EXCLUDED.type;
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        execute_values(cursor, query_tags, [(
            record['tagId'],
            record['Tag'],
            record['version'],
            record['tagDescription'],
            record['type']
        ) for record in records_tags])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def insert_statement_dim_data(**kwargs):
    dtype_mapping = {
        'statementId': str,
        'fiscalYear': 'Int64',
        'fiscalPeriod': str,
        'formType': str,
        'balanceSheetDateId': 'Int64',
        'filledDateId': 'Int64',
        'acceptedDateId': 'Int64',
        'previousReport': 'bool',
        'numberCIK': 'Int64'
    }
    # Load data from fact_item.csv and upsert into database
    df_statements = pd.read_csv('/opt/airflow/clean_data/statement_dim.csv', dtype=dtype_mapping)
    #TODO przekopiuj do transformacji
    df_statements['statementId'] = df_statements['statementId'].apply(process_statement_id)

    records_statements = df_statements.to_dict('records')

    query_statements = """
        INSERT INTO statement_dim (statementId, fiscalYear, fiscalPeriod, formType, balanceSheetDateId, filledDateId, acceptedDateId, previousReport, numberCIK)
        VALUES %s
        ON CONFLICT (statementId) DO UPDATE SET
        fiscalYear = EXCLUDED.fiscalYear,
        fiscalPeriod = EXCLUDED.fiscalPeriod,
        formType = EXCLUDED.formType,
        balanceSheetDateId = EXCLUDED.balanceSheetDateId,
        filledDateId = EXCLUDED.filledDateId,
        acceptedDateId = EXCLUDED.acceptedDateId,
        previousReport = EXCLUDED.previousReport,
        numberCIK = EXCLUDED.numberCIK;
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        execute_values(cursor, query_statements, [(
            record['statementId'],
            record['fiscalYear'],
            record['fiscalPeriod'],
            record['formType'],
            record['balanceSheetDateId'],
            record['filledDateId'],
            record['acceptedDateId'],
            record['previousReport'],
            record['numberCIK']
        ) for record in records_statements])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def insert_company_dim_data(**kwargs):
    # Load data from company_dim.csv
    df_company = pd.read_csv('/opt/airflow/clean_data/company_dim.csv')
    
    # Prepare current date for updates
    today = datetime.now().strftime('%Y%m%d')
    three_months_ago = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df_company.iterrows():
            # Hash companyId
            companyId = hash_string_to_int(row['companyId'])
            row['startDate'] = str(row['startDate'])

            # POPRAWKA: Check if startDate is older than 3 months
            if row['startDate'] < three_months_ago:
                if row['formerName'] not in ['NaN', 'N/A', "null", None, np.nan]:
                    # POPRAWKA: Search for formerName in companyName in the database
                    cursor.execute("""
                        SELECT companyId FROM company_dim
                        WHERE companyName = %s AND endDate = '99991231'
                    """, (row['formerName'],))
                    matching_row = cursor.fetchone()
                    
                    if matching_row:
                        matching_companyId = matching_row[0]
                        # POPRAWKA: Update existing row's endDate and set isActive to False
                        cursor.execute("""
                            UPDATE company_dim
                            SET endDate = %s, isActive = False
                            WHERE companyId = %s AND endDate = '99991231'
                        """, (row['startDate'], matching_companyId))
            
            # POPRAWKA: Check if companyId exists
            cursor.execute("""
                SELECT * FROM company_dim
                WHERE companyId = %s
            """, (companyId,))
            existing_row = cursor.fetchone()
            
            if existing_row:
                # Check if all columns are the same
                row_data = (
                    row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                    row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                    row['CIK'], row['startDate'], row['endDate'], row['isActive'], row['formerName']
                )
                existing_data = existing_row[1:-1]  # Exclude the last column which is the primary key

                if row_data == existing_data:
                    continue  # Skip inserting if data is the same

                # Unhash existing companyId to get the original string
                original_companyId = row['companyId']
                
                # POPRAWKA: Update existing row's endDate
                cursor.execute("""
                    UPDATE company_dim
                    SET endDate = %s, isActive = False
                    WHERE companyId = %s AND endDate = '99991231'
                """, (today, companyId))
                
                # POPRAWKA: Update the row's startDate and create new companyId
                row['startDate'] = today
                row['companyId'] = f"{row['companyName'].replace(' ', '')}{today}"
                new_companyId = hash_string_to_int(row['companyId'])
                row['companyId'] = new_companyId
                
                # POPRAWKA: Check if new companyId exists before insertion
                cursor.execute("""
                    SELECT 1 FROM company_dim WHERE companyId = %s
                """, (new_companyId,))
                if cursor.fetchone():
                    continue  # Skip inserting if new companyId already exists

                # POPRAWKA: Insert new row with updated startDate
                cursor.execute("""
                    INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '99991231', %s, True, %s)
                """, (
                    row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                    row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                    row['CIK'], row['startDate'], new_companyId, row['formerName']
                ))
            else:
                # POPRAWKA: Check if new companyId exists before insertion
                cursor.execute("""
                    SELECT 1 FROM company_dim WHERE companyId = %s
                """, (companyId,))
                if cursor.fetchone():
                    continue  # Skip inserting if new companyId already exists

                # POPRAWKA: Insert new row
                cursor.execute("""
                    INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '99991231', %s, True, %s)
                """, (
                    row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                    row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                    row['CIK'], row['startDate'], companyId, row['formerName']
                ))
        
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

insert_tags_data_task = PythonOperator(
    task_id='insert_tags_data_task',
    python_callable=insert_tags_data,
    provide_context=True,
    dag=dag,
)

insert_statement_dim_data_task = PythonOperator(
    task_id='insert_statement_dim_data_task',
    python_callable=insert_statement_dim_data,
    provide_context=True,
    dag=dag,
)

insert_company_dim_data_task = PythonOperator(
    task_id='insert_company_dim_data_task',
    python_callable=insert_company_dim_data,
    provide_context=True,
    dag=dag,
)

check_task >> [proceed_with_tasks, skip_tasks]
proceed_with_tasks >> [insert_tags_data_task, insert_statement_dim_data_task, insert_company_dim_data_task]


