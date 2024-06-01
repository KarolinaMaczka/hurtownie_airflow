import pandas as pd
import re
import string
from dateutil import parser

def clean_dataframe(df, columns):
    """
    Cleans the DataFrame by dropping rows where any value in the specified columns from given list is missing.
    """
    cleaned_df = df.dropna(subset=columns)
    return cleaned_df

def clean_company_name(name):
    """
    Cleans a company name by converting it to uppercase, removing punctuation, typical company suffixes, and extra whitespace.
    """
    # Convert to uppercase
    name = name.upper()

    # Remove punctuation
    name = name.translate(str.maketrans('', '', string.punctuation))
    
    # Remove typical company suffixes
    company_suffixes = [' CO', ' INC', ' CORPORATION', ' LTD', ' LLC', ' LLP', ' LIMITED', ' COMPANY', 'NA', 'LP', 'NATIONAL ASSOCIATION']
    for suffix in company_suffixes:
        name = re.sub(r'\b' + suffix + r'\b', '', name)
    
    # Remove extra whitespace
    name = ' '.join(name.split())
    
    return name

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
    fact_table["ComplaintId"] = df["Complaint ID"].astype("int")
    fact_table["CompanyId"] = None
    fact_table["CompanyName"] = df["Company"].astype("string")
    fact_table["Industry"] = None
    fact_table["ComplaintSentDateId"] = df["Date sent to company"].fillna("").astype("string").apply(convert_date_to_int)
    fact_table["ComplaintRecivedDateId"] = df["Date received"].fillna("").astype("string").apply(convert_date_to_int)
    return fact_table

def create_complaints_dim(df):
    """
    Creates complaints_dim from given data frame filling missing data and changing types of columns
    """
    complaints_dim = pd.DataFrame()
    complaints_dim["Id"] = df["Complaint ID"].astype("int")
    complaints_dim["Product"] = df["Product"].astype("string")
    complaints_dim["SubProduct"] = df["Sub-product"].astype("string").fillna('not provided')
    complaints_dim["Issue"] = df["Issue"].astype("string")
    complaints_dim["SubIssue"] = df["Sub-issue"].astype("string").fillna('not provided')
    complaints_dim["CustomerTag"] = df["Tags"].astype("string").fillna('standard')
    complaints_dim["CustomerConsent"] = df["Consumer consent provided?"].astype("string").fillna('N/A')
    complaints_dim["SubmissionMethod"] = df["Submitted via"].astype("string").fillna('not provided')
    complaints_dim["CompanyResponse"] = df["Company response to consumer"].astype("string").fillna('not provided')
    complaints_dim["TimelyResponse"] = df["Timely response?"].astype("string").fillna('N/A')
    complaints_dim["ConsumerDisputed"] = df["Consumer disputed?"].astype("string").fillna('N/A')
    complaints_dim["State"] = df["State"].astype("string").fillna('not provided')
    complaints_dim["ZipCode"] = df["ZIP code"].astype("string").fillna('XXXXX')
    return complaints_dim

def transform_complaints(chunk_size,max_chunks):
    """
    Reads a CSV file containing complaints data in chunks, cleans the data, performs additional processing, and saves the final DataFrame.
    Parameters:
    - chunk_size (int): The size of each data chunk to read from the CSV file.
    - max_chunks (int): The maximum number of chunks to read from the CSV file.
    """
    input_filepath = "airflow\\data\\complaints\\complaints.csv"
    output_filepath = "airflow\\clean_data\\complaints\\"

    # Process chunks
    complaints_dfs = []
    fact_dfs = []
    for chunk_count, chunk in enumerate(pd.read_csv(input_filepath, chunksize=chunk_size), start=1):
        if chunk_count > max_chunks:
            break

        # Data cleaning
        chunk = clean_dataframe(chunk, ['Product', 'Issue', 'Company', 'Complaint ID'])
        chunk['Company'] = chunk['Company'].apply(clean_company_name)

        # Additional processing
        complaints_chunk = create_complaints_dim(chunk)
        complaints_dfs.append(complaints_chunk)

        fact_chunk = create_fact_table(chunk)
        fact_dfs.append(fact_chunk)

    # Concatenate DataFrames
    complaints_final_df = pd.concat(complaints_dfs, ignore_index=True)
    fact_final_df = pd.concat(fact_dfs, ignore_index=True)

    # Save final DataFrame
    complaints_final_df.to_csv(output_filepath+'complaints_dim.csv', index=False)
    fact_final_df.to_csv(output_filepath+'complaints_fact.csv', index=False)


transform_complaints(10000,3)