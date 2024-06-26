import pandas as pd
from python_scripts.utils import clean_company_name, determine_type, fill_na_with_na, clean_dataframe

def transform_fin_statements(folder_name):
    print(folder_name)
    df_num = pd.read_csv(f"/opt/airflow/data/{folder_name}/num.txt", sep='\t')
    df_sub = pd.read_csv(f"/opt/airflow/data/{folder_name}/sub.txt", sep='\t')
    df_tag = pd.read_csv(f"/opt/airflow/data/{folder_name}/tag.txt", sep='\t')
    create_statement_dim(df_sub)
    create_financial_statement_item_fact_dim(df_sub, df_num, df_tag)
    create_tag_dim(df_tag)
    create_company_dim(df_sub)
    

def create_statement_dim(df_sub):
    """
    Zamiana nazw kolumn.
    Usunięcie wierszy z nullami w niezbędnych kolumnach.
    Wypełnienie kolumn "fy" i "fp" wartościami "NA".
    Usunięcie nulli z kolumny "balanceSheetDateId" (zamiana na 21001231).
    Usunięcie czasu z "acceptedDateId" i przekształcenie daty do formatu YYYYMMDD.
    """
    print("creating statement_dim")
    # Wybranie istotnych kolumn z DataFrame 'df_sub' do nowego DataFrame 'statement_dim'
    statement_dim = df_sub[["adsh", "fy", "fp", "form", "period","filed","accepted", "prevrpt", "nciks"]]

    # Usunięcie wierszy z brakującymi wartościami lub wypełnienie wartości
    statement_dim = clean_dataframe(statement_dim, ["adsh", "form", "period", "filed","accepted", "prevrpt", "nciks"])
    statement_dim = fill_na_with_na(statement_dim, ["fy", "fp"], default_str="NA", default_int=-1)
    statement_dim = statement_dim.rename(columns={"adsh": "statementId", "fy":"fiscalYear", "fp":"fiscalPeriod", "form":"formType", "period":"balanceSheetDateId","filed":"filledDateId","accepted":"acceptedDateId", "prevrpt":"previousReport", "nciks":"numberCIK"})
    statement_dim = statement_dim[statement_dim["formType"].isin(['10-Q', '10-K','8-K', '6-K', '20-F', 'DEF 14A', '8-K/A, 10-Q/A, 10-K/A', '20-F/A', '40-F', '6-K/A'])]
    statement_dim["balanceSheetDateId"] = statement_dim["balanceSheetDateId"].fillna("21001231")
    statement_dim = fill_na_with_na(statement_dim, ["acceptedDateId"], default_int=21001231, default_str='21001231')
    
    # transformacja dat do odpowiednich typów
    statement_dim["acceptedDateId"] = pd.to_datetime(statement_dim['acceptedDateId']).dt.date
    acc_date_str = statement_dim['acceptedDateId'].values.astype('datetime64[D]').astype(str)
    statement_dim['acceptedDateId'] = pd.Series(acc_date_str).str.replace('-', '')
    statement_dim["acceptedDateId"] = statement_dim["acceptedDateId"].replace('', '21001231')
    statement_dim["balanceSheetDateId"] = statement_dim["balanceSheetDateId"].astype('Int64')
    statement_dim["acceptedDateId"] = statement_dim["acceptedDateId"].astype('Int64')
    statement_dim['fiscalYear'] = statement_dim['fiscalYear'].astype('Int64')
    statement_dim = fill_na_with_na(statement_dim, ["acceptedDateId"], default_int=21001231, default_str='21001231')
    
    # usuwanie myślników z statementId
    statement_dim['statementId'] = statement_dim['statementId'].apply(lambda x: int(x.replace('-', '')))

    print(statement_dim["acceptedDateId"].unique())
    statement_dim.to_csv('/opt/airflow/clean_data/statement_dim.csv', index=False)

def create_financial_statement_item_fact_dim(df_sub, df_num, df_tags):
    """
    Usunięcie niezbędnych kolumn zawierających nulle.
    Wypełnienie kolumny coregistrantName wartością "consolidated", jeśli były w niej nulle.
    Zamiana nazw kolumn.
    Przefiltrowanie po formularzach.
    Obliczenie startDateId na podstawie formularza.
    Ujednolicenie companyName.
    """
    print("creating financial_statement_item_fact_dim")
    fact_item = df_num.merge(df_sub, on='adsh')
    fact_item["tag"] = fact_item["tag"] + fact_item["version"]

    # timesaving only - filtrowanie tagów na podstawie df_tags (logika jest też napisana przy wstawianiu (sql) tak aby uwzględnić historyczne dane - tutaj tylko filtrujemy, ze wzgledów czasowych na potrzeby projektu)
    df_tags['combined_tag'] = df_tags['tag'] + df_tags['version']
    fact_item = fact_item[fact_item['tag'].isin(df_tags['combined_tag'])]

    # Wybór odpowiednich kolumn, usuwanie potrzebnych kolumn zawierających nulle
    fact_item = fact_item[["adsh", "name", "coreg", "value", "uom", "ddate", "qtrs", "form", "tag"]]
    fact_item = clean_dataframe(fact_item, ["adsh", "uom", "ddate", "qtrs", "form", "tag", "value"]) 
    fact_item = fill_na_with_na(fact_item, ["coreg"], default_str="consolidated")

    # Filtracja po typie formularza
    fact_item = fact_item[fact_item["form"].isin(['10-Q', '10-K','8-K', '6-K', '20-F', 'DEF 14A', '8-K/A, 10-Q/A, 10-K/A', '20-F/A', '40-F', '6-K/A'])]
    fact_item['endDateId'] = pd.to_datetime(fact_item['ddate'], format='%Y%m%d', errors='coerce')
    fact_item = fact_item.dropna(subset=['endDateId'])

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

    # Obliczenie startDateId na podstawie endDateId i formularza
    fact_item['months'] = fact_item['form'].map(form_months)
    fact_item['months'] = fact_item['months'].fillna(fact_item['qtrs'] * 3)
    fact_item['startDateId'] = fact_item['endDateId'] - fact_item['months'].astype('timedelta64[M]')
    fact_item = fact_item.drop(columns=['months'])

     # Zmiana nazw kolumn
    fact_item = fact_item.rename(columns={"adsh" : "statementId", "name": "companyName", "coreg":"coregistrantName", "uom": "unitOfMeasure", 'tag':'tagId'})
    fact_item = fact_item.drop(columns=['qtrs', 'ddate'])
    
    # Konwersja dat do formatu YYYYMMDD
    end_date_str = fact_item['endDateId'].dt.strftime('%Y%m%d')
    start_date_str = fact_item['startDateId'].dt.strftime('%Y%m%d')
    fact_item['endDateId'] = end_date_str
    fact_item['startDateId'] = start_date_str
    print(fact_item['endDateId'].unique())
    print(f' NAs in enddate  {fact_item['endDateId'].isna().sum()}')

    # Ujednolicenie companyName i przekształcenie statementId
    fact_item['companyName'] = fact_item['companyName'].apply(clean_company_name)
    fact_item['statementId'] = fact_item['statementId'].apply(lambda x: int(x.replace('-', '')))

    # timesaving only
    fact_item = fact_item.sample(frac=1).head(1000000)

    fact_item.to_csv('/opt/airflow/clean_data/fact_item.csv', index=False)

def create_tag_dim(df_tag):
    '''
    tagId to skonkatenowane kolumny tag i version
    Obliczenie kolumny type na podstawie abstract, datatype i crdr
    Wypełnienie kolumny tagDescription z not provided
    Usunięcie niezbednych kolumn zawierających nulle
    '''
    print("creating tag_dim")
    # Wybór istotnych kolumn, usuniecie potrzebnych kolumn z nullami, wypełnienie danych
    tag_dim = df_tag[['tag', 'version', 'tlabel', 'datatype', 'crdr', 'abstract', 'doc']]
    tag_dim = clean_dataframe(tag_dim, ["tag", "version"])
    tag_dim = fill_na_with_na(tag_dim, ['doc'], default_str="not provided")

    # Stworzenie nowej kolumny 'tagId' jako konkatenacja kolumn 'tag' i 'version'
    tag_dim = tag_dim.assign(tagId = tag_dim['tag'] + tag_dim['version'])

    tag_dim.rename(columns={'tlabel': 'Tag', 'version': 'version', 'doc': 'tagDescription'}, inplace=True)
    
    # tworzenie kolumny typ
    tag_dim = tag_dim.assign(type=tag_dim.apply(determine_type, axis=1))
    tag_dim = clean_dataframe(tag_dim, ["type"])

    tag_dim = tag_dim[['tagId', 'Tag', 'version', 'tagDescription', 'type']]
    tag_dim.to_csv('/opt/airflow/clean_data/tags.csv', index=False)

def create_company_dim(df_sub):
    '''
    Usunięcie niezbednych kolumn zawierających nulle
    Ujednolicenie companyName
    Stworzenie companyId jako nazwy firmy i daty początku obowiązywania tej nazwy
    '''
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
        'changed': 'startDate',
        'former': 'formerName',
        'filed': 'filedDate'
    })

    # Usunięcie wierszy z brakującymi wartościami w kluczowych kolumnach lub wypełnienie wartości
    company_dim = clean_dataframe(company_dim, ["companyName", "CIK", "countryName", 'cityName'])
    columns_to_fill = [
        'industry', 'countryName', 'stateName', 'cityName',
        'zipCode', 'street', 'street2', 'countryRegistered', 'stateRegistered', 'companyPhoneNumber',
        'formerName'
    ]
    company_dim = fill_na_with_na(company_dim, columns_to_fill, default_str='NA')
    
    # Daty 
    company_dim['endDate'] = '21001231'
    company_dim = company_dim[['companyName', 'industry', 'countryName', 'stateName', 
                               'cityName','zipCode','street','street2', 'countryRegistered','stateRegistered','companyPhoneNumber', 'CIK', "startDate", 'endDate', 'formerName', "filedDate"]]
    company_dim['industry'] = company_dim['industry'].astype('Int64')
    company_dim['startDate'] = company_dim['startDate'].fillna(int("19000101")).astype(str).str.replace(r'\.0$', '', regex=True).astype('Int64')
    company_dim['companyName'] = company_dim['companyName'].apply(clean_company_name)
    company_dim["companyId"] = company_dim["companyName"].str.replace(' ', '', regex=False) + company_dim["startDate"].astype(str)
    
    company_dim['isActive'] = True
    company_dim.to_csv('/opt/airflow/clean_data/company_dim.csv', index=False)


if __name__=='__main__':
    transform_fin_statements("2023q4")