import pandas as pd

def transform_complaints():
    df = pd.read_csv("./airflow/data/complaints/complaints_short.csv")
    create_company_complaints_fact(df)


def create_company_complaints_fact(df):
   transformed_df = df[["Date received", "Date sent to company", "Company"]]
   print(df.head())

if __name__=='__main__':
    transform_complaints()