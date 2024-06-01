import pandas as pd
import numpy as np
from utils import clean_company_name, determine_type

def transform_fin_statements(folder_name):
    df_num = pd.read_csv(f"./airflow/data/2023q4/num.txt", sep='\t')
    df_sub = pd.read_csv(f"./airflow/data/2023q4/sub.txt", sep='\t')
    df_tag = pd.read_csv(f"./airflow/data/2023q4/tag.txt", sep='\t')
    create_statement_dim(df_sub)
    create_financial_statement_item_fact_dim(df_sub, df_num)
    create_tag_dim(df_tag)
    create_company_dim(df_sub)
    

def create_statement_dim(df_sub):
    """
    Zamiana nazw kolumn.
    Usunięcie nulli z balanceSheetDateId (zamiana na 0)
    Usunięcie czasu z acceptedDateId i skonkatenowanie daty
    """
    print("creating statement_dim")
    statement_dim = df_sub[["adsh", "fy", "fp", "form", "period","filed","accepted", "prevrpt", "nciks"]]
    statement_dim = statement_dim.rename(columns={"adsh": "statementId", "fy":"fiscalYear", "fp":"fiscalPeriod", "form":"formType", "period":"balanceSheetDateId","filed":"filledDateId","accepted":"acceptedDateId", "prevrpt":"previousReport", "nciks":"numberCIK"})
    statement_dim = statement_dim[statement_dim["formType"].isin(['10-Q', '10-K','8-K', '6-K', '20-F', 'DEF 14A', '8-K/A, 10-Q/A, 10-K/A', '20-F/A', '40-F', '6-K/A'])]
    statement_dim["balanceSheetDateId"] = statement_dim["balanceSheetDateId"].fillna("00010101")
    statement_dim["acceptedDateId"] = pd.to_datetime(statement_dim['acceptedDateId']).dt.date
    acc_date_str = statement_dim['acceptedDateId'].values.astype('datetime64[D]').astype(str)
    statement_dim['acceptedDateId'] = pd.Series(acc_date_str).str.replace('-', '')
    statement_dim["balanceSheetDateId"] = statement_dim["balanceSheetDateId"].astype('Int64')
    statement_dim['fiscalYear'] = statement_dim['fiscalYear'].astype('Int64')
    statement_dim.to_csv('./airflow/clean_data/statement_dim.csv', index=False)

def create_financial_statement_item_fact_dim(df_sub, df_num):
    """
    Zamiana nazw kolumn.
    Przefiltrowanie po formularzach.
    Obliczenie startdate
    """
    print("creating financial_statement_item_fact_dim")
    fact_item = df_num.merge(df_sub, on='adsh')
    fact_item["tag"] = fact_item["tag"] + fact_item["version"]
    fact_item = fact_item[["adsh", "name", "coreg", "value", "uom", "ddate", "qtrs", "form", "tag"]]
    fact_item = fact_item[fact_item["form"].isin(['10-Q', '10-K','8-K', '6-K', '20-F', 'DEF 14A', '8-K/A, 10-Q/A, 10-K/A', '20-F/A', '40-F', '6-K/A'])]
    fact_item['endDateId'] = pd.to_datetime(fact_item['ddate'], format='%Y%m%d')
    form_months = {
        '10-Q': 3,
        '6-K': 6,
        '10-K': 12,
        '20-F/A': 3,
        '20-F': 12,
        '6-K/A': 9,
        '40-F': 12,
        '8-K': 0,
        'DEF 14A': 0
    }
    fact_item['months'] = fact_item['form'].map(form_months)
    fact_item['months'] = fact_item['months'].fillna(fact_item['qtrs'] * 3)
    fact_item['startDateId'] = fact_item['endDateId'] - fact_item['months'].astype('timedelta64[M]')
    fact_item = fact_item.drop(columns=['months'])
    fact_item = fact_item.rename(columns={"adsh" : "statementId", "name": "companyName", "coreg":"coregistrantName", "uom": "unitOfMeasure", 'tag':'tagId'})
    fact_item = fact_item.drop(columns=['qtrs', 'ddate'])
    end_date_str = fact_item['endDateId'].values.astype('datetime64[D]').astype(str)
    start_date_str = fact_item['startDateId'].values.astype('datetime64[D]').astype(str)
    fact_item['endDateId'] = pd.Series(end_date_str).str.replace('-', '')
    fact_item['startDateId'] = pd.Series(start_date_str).str.replace('-', '')
    fact_item.to_csv('./airflow/clean_data/fact_item.csv', index=False)

def create_tag_dim(df_tag):
    '''
    tagId to skonkatenowane kolumny tag i version
    obliczenie kolumny type na podstawie abstract, datatype i crdr
    '''
    print("creating tag_dim")
    tag_dim = df_tag[['tag', 'version', 'tlabel', 'datatype', 'crdr', 'abstract']]
    tag_dim = tag_dim.assign(tagId = tag_dim['tag'] + tag_dim['version'])
    tag_dim.rename(columns={'tag': 'tag', 'version': 'version', 'tlabel': 'tagDescription'}, inplace=True)
    
    tag_dim = tag_dim.assign(type=tag_dim.apply(determine_type, axis=1))
    tag_dim = tag_dim[['tagId', 'tag', 'version', 'tagDescription', 'type']]
    tag_dim.to_csv('./airflow/clean_data/tags.csv', index=False)

def create_company_dim(df_sub):
    print("creating company_dim")
    company_dim = df_sub.rename(columns={
        'name': 'companyName',
        'sic': 'industry',
        'countryba': 'countryName',
        'stprba': 'stateName',
        'cityba': 'cityName',
        'zipba': 'zipCode',
        'bas1': 'street',
        'bas2': 'street2',
        'countryinc': 'countryRegistered',
        'stprinc': 'stateRegistered',
        'baph': 'companyPhoneNumber',
        'cik':"CIK",
        'changed': 'startDate'
    })

    company_dim['countryRegistered'] = company_dim['countryRegistered'].fillna('N/A')
    company_dim['stateRegistered'] = company_dim['stateRegistered'].fillna('N/A')
    company_dim['companyPhoneNumber'] = company_dim['companyPhoneNumber'].fillna('N/A')
    company_dim['companyPhoneNumber'] = company_dim['companyPhoneNumber'].str.replace(r'\D', '', regex=True)
    company_dim['companyPhoneNumber'] = company_dim['companyPhoneNumber'].apply(lambda x: f'{x[:3]}-{x[3:6]}-{x[6:]}' if x != 'N/A' else x)
    company_dim['endDate'] = '99991231'
    company_dim = company_dim[['companyName', 'industry', 'countryName', 'stateName', 
                               'cityName','zipCode','street','street2', 'countryRegistered','stateRegistered','companyPhoneNumber', 'CIK', "startDate", 'endDate']]
    company_dim['industry'] = company_dim['industry'].astype('Int64')
    company_dim['startDate'] = company_dim['startDate'].fillna(int("00010101")).astype(str).str.replace(r'\.0$', '', regex=True).astype('Int64')
    company_dim['companyName'] = company_dim['companyName'].apply(clean_company_name)
    company_dim.to_csv('./airflow/clean_data/company_dim.csv', index=False)

if __name__=='__main__':
    transform_fin_statements("2023q4")