import json
import pandas as pd
import requests
from urllib import parse
from sqlalchemy import create_engine
import requests

def extract_json_data(file_path: str, encoding: str):
    """Extract data from a json file
    Args:
        file_path => Path to file containing the json to be extracted
        encoding => File encoding
    Returns:
        data => Json object of file
    """
    data = json.load(open(file_path, encoding=encoding))
    return data


def load_db(table_name: str, df: pd.DataFrame):
    """Load data to postgres database for example
       Method can be used with other database flavours
    Args:
        table_name => Name to assign to table
        df => dataframe to load to db
    Returns:
        df => dataframe to load to db
    NOTE:
        Connection string can be obtained from an authenticated user in
        your database environment or via googling how to find your 
        connection string
    """
    connecting_string = 'Driver={ODBC Driver 13 for SQL Server};Server=tcp:happy-db.database.windows.net,1433;Database=happy_db;Uid=adminadmin;Pwd=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)    
    df.astype(str).to_sql(table_name, engine, if_exists = 'replace')
    return df
    
def generate_csv(df: pd.DataFrame, file_name: str):
    """Generate CSV file from a JSON
    Args:
        file_name => Name to assign to csv file
        df => dataframe to convert to csv
    Returns:
        N/A
    """
    df.to_csv(file_name)
    

if __name__ == '__main__':
    data = extract_json_data('balances_model.json', "utf8")
    df = pd.DataFrame([data])
    print(df)
    load_db('json_data', df)
    generate_csv(df3, 'balances_model.csv')



