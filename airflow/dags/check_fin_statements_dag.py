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

@provide_session
def check_last_dag_run_status(session=None, **kwargs):
    dag_id = 'fin_statements_dag_id'
    dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(DagRun.execution_date.desc()).first()
    print(f'Last dag run date: {dag_run.execution_date}')
    if dag_run and dag_run.state == 'success':
        kwargs['ti'].xcom_push(key='execution_date', value=dag_run.execution_date)
        return 'proceed_with_tasks'
    else:
        return 'skip_tasks'
    
def hash_string_to_int(value):
    """Convert a string to a hashed integer value using MD5."""
    return int(hashlib.md5(value.encode()).hexdigest(), 16)

def insert_tags_data(**kwargs):
    df_tags = pd.read_csv('/opt/airflow/clean_data/tags.csv', keep_default_na=False, na_values=['NA'])
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
    df_statements = pd.read_csv('/opt/airflow/clean_data/statement_dim.csv', dtype=dtype_mapping, keep_default_na=False, na_values=['NA'])
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

def normalize_value(value, column_name=None):
    """Normalize value for comparison."""
    if pd.isna(value):
        return 'NA'
    if column_name == 'cik':
        return str(value)
    if isinstance(value, (np.int64, int)):
        return int(value)
    if isinstance(value, (np.float64, float)):
        return float(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.datetime64, datetime)):
        return str(value)
    return str(value)

def insert_company_dim_data(**kwargs):
    df_company = pd.read_csv('/opt/airflow/clean_data/company_dim.csv')
    df_company.fillna("NA", inplace=True)

    # Prepare current date for updates
    # today = datetime.now().strftime('%Y%m%d')
    # three_months_ago = (datetime.now() - timedelta(days=90)).strftime('%Y%m%d')
    
    execution_date = kwargs['ti'].xcom_pull(key='execution_date', task_ids='check_last_dag_run_status')
    today = execution_date.strftime('%Y%m%d')  
    three_months_ago = (execution_date - timedelta(days=93)).strftime('%Y%m%d')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df_company.iterrows():
            filled_date = row['filedDate']
            # Hash companyId
            companyId = hash_string_to_int(row['companyId'])
            row['startDate'] = str(row['startDate'])
            
            """
            Logika zmiany nazwy: w tabelkach przechowujemy dokładną datę zmiany nazwy, jeśli zmieniła się w ciągu ostatnich 3 miesięcy to updatujemy
            """
            if row['startDate'] > three_months_ago:
                if row['formerName'] not in ['NaN', 'N/A', "null", None, np.nan, 'NA']:
                    # Search for formerName in companyName in the database
                    cursor.execute("""
                        SELECT companyId FROM company_dim
                        WHERE companyName = %s AND endDate = '21001231'
                    """, (row['formerName'],))
                    matching_row = cursor.fetchone()
                    
                    if matching_row:
                        print(f'matching row {matching_row}')
                        print(f'row {row}')
                        matching_companyId = matching_row[0]
                        #  Update existing row's endDate and set isActive to False
                        cursor.execute("""
                            UPDATE company_dim
                            SET endDate = %s, isActive = False
                            WHERE companyId = %s AND endDate = '21001231'
                        """, (row['startDate'], matching_companyId))
            
            '''
            Logika zmiany danych: 
            '''
            # Check if companyId exists
            cursor.execute("""
                SELECT * FROM company_dim
                WHERE companyId = %s
            """, (companyId,))
            existing_row = cursor.fetchone()
            col_names = [desc[0] for desc in cursor.description]
            
            if existing_row:
                # Map existing data to column names using cursor description
                existing_data = {col: normalize_value(val, col) for col, val in zip(col_names, existing_row)}
                row_data = {
                    'companyname': normalize_value(row['companyName'], 'companyname'),
                    'industry': normalize_value(row['industry'], 'industry'),
                    'countryname': normalize_value(row['countryName'], 'countryname'),
                    'statename': normalize_value(row['stateName'], 'statename'),
                    'cityname': normalize_value(row['cityName'], 'cityname'),
                    'zipcode': normalize_value(row['zipCode'], 'zipcode'),
                    'street': normalize_value(row['street'], 'street'),
                    'street2': normalize_value(row['street2'], 'street2'),
                    'countryregistered': normalize_value(row['countryRegistered'], 'countryregistered'),
                    'stateregistered': normalize_value(row['stateRegistered'], 'stateregistered'),
                    'companyphonenumber': normalize_value(row['companyPhoneNumber'], 'companyphonenumber'),
                    'cik': normalize_value(row['CIK'], 'cik'),
                }
                existing_data_subset = {k: v for k, v in existing_data.items() if k in row_data}
                
                if row_data == existing_data_subset:
                    continue  # Skip inserting if data is the same
                else:
                    print(f'row {row}')
                    # if the data exist in a database - skip
                    cursor.execute("""
                    SELECT 1 FROM company_dim WHERE companyName = %s AND industry = %s AND countryName = %s AND stateName = %s AND cityName = %s AND zipCode = %s AND street = %s AND street2 = %s AND countryRegistered = %s AND stateRegistered = %s AND companyPhoneNumber = %s AND CIK = %s
                    """, (
                        row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                        row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                        row['CIK']
                    ))
                    if cursor.fetchone():
                        continue
                    else:
                        print("not the same")
                        print(f'existing data {existing_data_subset}')
                        print(f'existing data fetch: {cursor.fetchone()}')


                # Unhash existing companyId to get the original string
                original_companyId = row['companyId']
                
                if row_data == existing_data_subset:
                    continue  # Skip inserting if data is the same

                if filled_date <= int(existing_data['startdate']):
                    # Find the least startDate of a company with the same name but bigger than the filledDate
                    cursor.execute("""
                        SELECT MIN(startdate) FROM company_dim
                        WHERE companyname = %s AND startdate > %s
                    """, (row['companyName'], filled_date))
                    min_start_date_row = cursor.fetchone()
                    
                    if min_start_date_row and min_start_date_row[0]:
                        min_start_date = str(min_start_date_row[0])
                        # Insert historical row with startDate as filledDate and endDate as min_start_date
                        row['startDate'] = filled_date
                        row['endDate'] = min_start_date
                        row['isActive'] = False
                        row['companyId'] = f"{row['companyName'].replace(' ', '')}{filled_date}"
                        new_companyId = hash_string_to_int(row['companyId'])
                        row['companyId'] = new_companyId
                        cursor.execute("""
                            SELECT companyId FROM company_dim WHERE companyId = %s
                        """, (new_companyId,))
                        if cursor.fetchone():
                            pass  
                        else:
                            cursor.execute("""
                                INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                                row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                                row['CIK'], row['startDate'], row['endDate'], new_companyId, row['isActive'], row['formerName']
                            ))
                
                # Check for overlapping periods
                cursor.execute("""
                    SELECT companyId, startDate, endDate, isActive FROM company_dim
                    WHERE companyName = %s AND startDate < %s AND endDate > %s
                """, (row['companyName'], filled_date, filled_date))
                overlapping_row = cursor.fetchone()

                if overlapping_row:
                    overlap_companyId, overlap_start, overlap_end, overlap_isActive = overlapping_row
                    row['companyId'] = f"{row['companyName'].replace(' ', '')}{filled_date}"
                    new_companyId = hash_string_to_int(row['companyId'])
                    row['companyId'] = new_companyId

                    cursor.execute("""
                        SELECT companyId FROM company_dim WHERE companyId = %s
                    """, (new_companyId,))
                    if cursor.fetchone():
                        pass  
                    else:
                        # Insert historical row for the overlapping period
                        cursor.execute("""
                            INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                            row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                            row['CIK'], filled_date, overlap_end, new_companyId, overlap_isActive, row['formerName']
                        ))

                        # Update the overlapping row's endDate
                        cursor.execute("""
                            UPDATE company_dim
                            SET endDate = %s, isActive = False
                            WHERE companyId = %s AND endDate = %s
                        """, (filled_date, overlap_companyId, overlap_end))
                
                # # Update existing row's endDate
                # cursor.execute("""
                #     UPDATE company_dim
                #     SET endDate = %s, isActive = False
                #     WHERE companyId = %s AND endDate = '21001231'
                # """, (filled_date, companyId))
                
                # # Update the row's startDate and create new companyId
                # row['startDate'] = filled_date
                # row['companyId'] = f"{row['companyName'].replace(' ', '')}{filled_date}"
                # new_companyId = hash_string_to_int(row['companyId'])
                # row['companyId'] = new_companyId
                
                # # Check if the new companyId already exists before insertion
                # cursor.execute("""
                #     SELECT companyId FROM company_dim WHERE companyId = %s
                # """, (new_companyId,))
                # if cursor.fetchone():
                #     continue  # Skip inserting if new companyId already exists

                # # Insert new row with updated startDate
                # cursor.execute("""
                #     INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                #     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '21001231', %s, True, %s)
                # """, (
                #     row['companyName'], row['industry'], row['countryName'], row['stateName'], row['cityName'], row['zipCode'], 
                #     row['street'], row['street2'], row['countryRegistered'], row['stateRegistered'], row['companyPhoneNumber'], 
                #     row['CIK'], row['startDate'], new_companyId, row['formerName']
                # ))
            else:
                print(existing_row)
                # Insert new row
                cursor.execute("""
                    INSERT INTO company_dim (companyName, industry, countryName, stateName, cityName, zipCode, street, street2, countryRegistered, stateRegistered, companyPhoneNumber, CIK, startDate, endDate, companyId, isActive, formerName)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '21001231', %s, True, %s)
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

def insert_fact_item_data(**kwargs):
    # Retrieve data, hash tagId and get connection
    df_fact_item = pd.read_csv('/opt/airflow/clean_data/fact_item.csv', keep_default_na=False, na_values=['N/A'])
    df_fact_item['tagId'] = df_fact_item['tagId'].apply(lambda x: hash_string_to_int(x))
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Retrieve existing tags and company ID
        cursor.execute("SELECT tagId FROM tags")
        existing_tags = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT companyId, companyName, startDate, endDate FROM company_dim")
        existing_companies = cursor.fetchall()
        
        company_mapping = {}
        for companyId, companyName, startDate, endDate in existing_companies:
            if companyName not in company_mapping:
                company_mapping[companyName] = []
            company_mapping[companyName].append((startDate, endDate, companyId))

        # Filter rows with existing tags
        df_fact_item = df_fact_item[df_fact_item['tagId'].isin(existing_tags)]

        # Prepare data for batch processing
        fact_item_data = []
        for index, row in df_fact_item.iterrows():
            company_name = row['companyName']
            end_date_id = row['endDateId']
            company_id = None

            if company_name in company_mapping:
                for start_date, end_date, comp_id in company_mapping[company_name]:
                    if str(start_date) <= str(end_date_id) <= str(end_date):
                        company_id = comp_id
                        break

            if company_id is None:
                print(f"No company found for companyName: {row['companyName']} and endDateId: {row['endDateId']}")
                continue

            fact_item_data.append((
                row['statementId'],
                company_id,
                row['tagId'],
                row['companyName'],
                row['coregistrantName'],
                row['value'],
                row['unitOfMeasure'],
                row['form'],
                row['endDateId'],
                row['startDateId']
            ))

        # Check for duplicates
        existing_fact_items = set()
        if fact_item_data:
            cursor.execute("""
                SELECT statementId, companyId, tagId, companyName, coregistrantName, value, unitOfMeasure, form, endDateId, startDateId
                FROM finanacial_statement_fact
            """)
            existing_fact_items = {tuple(row) for row in cursor.fetchall()}

        fact_item_data = [item for item in fact_item_data if tuple(item) not in existing_fact_items]

        # Insert data into finanacial_statement_fact table
        if fact_item_data:
            execute_values(cursor, """
                INSERT INTO finanacial_statement_fact (statementId, companyId, tagId, companyName, coregistrantName, value, unitOfMeasure, form, endDateId, startDateId)
                VALUES %s
            """, fact_item_data)

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

insert_fact_item_data_task = PythonOperator(
    task_id='insert_fact_item_data_task',
    python_callable=insert_fact_item_data,
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
proceed_with_tasks >> [insert_tags_data_task, insert_statement_dim_data_task, insert_company_dim_data_task] >> insert_fact_item_data_task


