import string
import re
import pandas as pd

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

def determine_type(row):
    if row['abstract'] == 1:
        return 'abstract'
    elif row['datatype'] == 'monetary':
        return row['crdr']
    else:
        return row['datatype']
    
def fill_na_with_na(df, columns, default_str='N/A', default_int=-1):
    for column in columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].fillna(default_int)
        else:
            df[column] = df[column].fillna(default_str)
    return df

def clean_dataframe(df, columns):
    """
    Cleans the DataFrame by dropping rows where any value in the specified columns from given list is missing.
    """
    cleaned_df = df.dropna(subset=columns)
    return cleaned_df