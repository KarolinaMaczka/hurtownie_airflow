import string
import re

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