import pandas as pd
from dateutil import parser
from python_scripts.utils import clean_company_name, clean_dataframe, replace_out_of_range_values, fill_na_with_na


def convert_date_to_int(date_str):
    try:
        # Parse the date string into a datetime object
        date_obj = parser.parse(date_str)
    except parser.ParserError:
        return 0

    # Format the datetime object as an integer in YYYYMMDD format
    return int(date_obj.strftime('%Y%m%d'))

def create_fact_table(df):
    fact_table = pd.DataFrame()
    fact_table["complaintId"] = df["Complaint ID"].astype("int")
    fact_table["companyId"] = None
    fact_table["companyName"] = df["Company"].astype("string")
    fact_table["industry"] = None
    fact_table["complaintSentDateId"] = df["Date sent to company"].fillna("2100-12-31").astype("string").apply(convert_date_to_int)
    fact_table["complaintRecivedDateId"] = df["Date received"].fillna("2100-12-31").astype("string").apply(convert_date_to_int)
    fact_table= replace_out_of_range_values(fact_table,["complaintRecivedDateId", "complaintSentDateId"],19000101,21001231)
    return fact_table

def create_complaints_dim(df):
    """
    Creates complaints_dim from given data frame filling missing data and changing types of columns
    """
    complaints_dim = pd.DataFrame()
    complaints_dim["complaintId"] = df["Complaint ID"].astype("int")
    complaints_dim["product"] = df["Product"].astype("string")
    complaints_dim["subProduct"] = df["Sub-product"].astype("string").fillna('not provided')
    complaints_dim["issue"] = df["Issue"].astype("string")
    complaints_dim["subIssue"] = df["Sub-issue"].astype("string").fillna('not provided')
    complaints_dim["customerTag"] = df["Tags"].astype("string").fillna('standard')
    complaints_dim["customerConsent"] = df["Consumer consent provided?"].astype("string").map({'N/A': 'NA'}).fillna('NA')
    complaints_dim["submissionMethod"] = df["Submitted via"].astype("string").fillna('not provided')
    complaints_dim["didCompanyRespondPublicly"] = df["Company response to consumer"].notna()
    complaints_dim["timelyResponse"] = df["Timely response?"].str.lower().map({'yes': True, 'no': False})
    complaints_dim["consumerDisputed"] = df["Consumer disputed?"].astype("string").map({'N/A': 'NA'}).fillna('NA')
    complaints_dim["consumerState"] = df["State"].astype("string").fillna('not provided')
    complaints_dim["consumerZipCode"] = df["ZIP code"].astype("string").fillna('XXXXX')
    return complaints_dim

def transform_all_complaints(chunk_size):
    mapping_dict = {
    "EXPERIAN INFORMATION SOLUTIONS": "CONSOLIDATED EDISON",
    "TRANSUNION INTERMEDIATE HOLDINGS": "PINNACLE WEST CAPITAL CORP",
    "BANK OF AMERICA": "EDISON INTERNATIONAL",
    "YAMAHA MOTOR FINANCE USA": "HUNTSMAN CORP",
    "DAIMLER TRUCK FINANCIAL SERVICES USA": "CONSTELLATION ENERGY CORP",
    "ACCSCIENT": "BANCO SANTANDER SA",
    "PROCOLLECT": "BANCO BILBAO VIZCAYA ARGENTARIA SA",
    "FIRST NATIONAL BANK OF OMAHA": "AES CORP",
    "LEXISNEXIS": "DOW",
    "CBC COMPANIES": "HUDSON PACIFIC PROPERTIES",
    "RENT RECOVERY SOLUTIONS": "PUBLIC SERVICE ENTERPRISE GROUP",
    "TD BANK US HOLDING": "KIMCO REALTY CORP",
    "HW HOLDING": "ESSEX PROPERTY TRUST",
    "CAPITAL ONE FINANCIAL": "IDACORP",
    "CITIBANK": "NEXTERA ENERGY",
    "HANCOCK WHITNEY BANK": "HCI GROUP",
    "TRANSWORLD SYSTEMS": "SIMON PROPERTY GROUP DE",
    "PNC BANK": "PGE CORP",
    "THE CBE GROUP": "CMS ENERGY CORP",
    "CAINE WEINER": "FMC CORP",
    "JPMORGAN CHASE": "WINTRUST FINANCIAL CORP"
}

    """
    Reads a CSV file containing complaints data in chunks, cleans the data, performs additional processing, and saves the final DataFrame.
    Parameters:
    - chunk_size (int): The size of each data chunk to read from the CSV file.
    - max_chunks (int): The maximum number of chunks to read from the CSV file.
    """
    input_filepath = "/opt/airflow/data/complaints/complaints_concatenated.csv"
    output_filepath = "/opt/airflow/clean_data/"

    # Process chunks
    complaints_dfs = []
    fact_dfs = []
    for _, chunk in enumerate(pd.read_csv(input_filepath, chunksize=chunk_size), start=1):
        # Data cleaning
        chunk = clean_dataframe(chunk, ['Product', 'Issue', 'Company', 'Complaint ID', 'Timely response?'])
        chunk['Company'] = chunk['Company'].apply(clean_company_name)
        chunk["Company"] = chunk["Company"].replace(mapping_dict)

        # Additional processing
        complaints_chunk = create_complaints_dim(chunk)
        complaints_dfs.append(complaints_chunk)

        fact_chunk = create_fact_table(chunk)
        fact_dfs.append(fact_chunk)

    # Concatenate DataFrames
    complaints_final_df = pd.concat(complaints_dfs, ignore_index=True)
    fact_final_df = pd.concat(fact_dfs, ignore_index=True)

    fact_final_df = fill_na_with_na(fact_final_df, fact_final_df.columns, default_str='NA', default_int=-1)
    complaints_final_df = fill_na_with_na(complaints_final_df, complaints_final_df.columns, default_str='NA', default_int=-1)

    # Save final DataFrame
    complaints_final_df.to_csv(output_filepath+'complaints_dim.csv', index=False)
    fact_final_df.to_csv(output_filepath+'complaints_fact.csv', index=False)

