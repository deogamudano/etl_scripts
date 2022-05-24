import json
import pandas as pd
import requests
from urllib import parse
from sqlalchemy import create_engine

def extract_api_data():
    """Extract data from json API
    Args:
        None
    Returns:
        api_response => json response from API
        
    NOTE: You have to replace UserAuthToken and the API URL
          to reuse with other endpoints or when token expires
    """
    
    UserAuthToken = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik5RSHdQTi1RNUN6Zm4yV0k5amVBVSJ9.eyJodHRwczovL2FwaS5wcm9kLnJlZC52aXJnaW4uY29tL2NsYWltcyI6ImV4ZWN1dGVfYXBpOnVzZXIiLCJodHRwczovL2FwaS5wcm9kLnJlZC52aXJnaW4uY29tL2NsYWltcy9kYXRhIjoie1wiY29kZVwiOlwiVlJFRFwifSIsImlzcyI6Imh0dHBzOi8vbG9naW4ucmVkLnZpcmdpbi5jb20vIiwic3ViIjoiZW1haWx8NjI0ZDRjMTE0YzA5MDAxMTI1NmU3ZWUxIiwiYXVkIjpbImh0dHBzOi8vd2ViLnByb2QucmVkLnZpcmdpbi5jb20vIiwiaHR0cHM6Ly92aXJnaW4tcmVkLmV1LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NTMzOTA0ODksImV4cCI6MTY1MzM5MjI4OSwiYXpwIjoiRjBxS05rWnlPTktoWWdEc2dBdzN3MlRtRFJodDRVazEiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIn0.TJ9uteg-psMCwrozyYWsukeIi_1S32qe6HTrenOrFcvpfevi90p5JTZPrz9SKaZk_mgsYd8PjVBI4Dloy2VXrRNkdzRODPoMg15NF1ofTdmoDRCP1FubuY7smADS3EtUbI-QITV_BRi-CE7mk7SmkXbyardSaooDhck9DBmj5SGkJ9vKlCCSRejy8JBBejg4Us5t0O4YzKbA8x22-S7JVDDIbywJGM0x2K_Y4EaMD83Vg7I3Z84iUGvheU-TqNmrDBIzk6JHl7bnJHW8WgWMAidHCbIiZV-ifKz1unOO2gpDe1U5fI0aILhMsEVlGoL14WAgxKCdVGsHxrBgemz6Fg'
    header = { 'accept': 'application/json',
                'Authorization' : 'Bearer ' + UserAuthToken}
    print(header['Authorization'])

    ## Usage of parameters defined in your API
    params = (
       ('offset', '0'),
       ('limit', '200'),
    )

    # Making sample API call with authentication and API parameters data

    response = requests.get('https://red-api-r1.prod.loyalty.virgin.com/discoveries/rewards/earn/v1', headers=header, params=params)
    print(response)
    api_response = response.json()
    print(api_response)
    return api_response
    
    


def load_db(table_name: str, df: pd.DataFrame):
    """Load dataframe to postgres database
    Args:
        csv_location => File containing worldbank GDP data
        table_name => Name of table in database
    Returns:
        df => Dataframe that was input into the db
    """

    connecting_string = 'Driver={ODBC Driver 13 for SQL Server};Server=tcp:happy-db.database.windows.net,1433;Database=happy_db;Uid=adminadmin;Pwd=Password1!;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    params = parse.quote_plus(connecting_string)

    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)    
    df.astype(str).to_sql(table_name, engine, if_exists = 'replace')
    return df
   
if __name__ == '__main__':
    json_data = extract_api_data()
    print(json_data)
    df3 = pd.DataFrame(json_data['rewards'])
    print(df3)
    df3 = pd.concat([df3.drop(['content'], axis = 1), df3.content.apply(pd.Series).add_prefix('content_')], axis = 1)
    print(df3.content_categories[0])
    df4 = pd.concat([df3.explode('content_categories').drop(['content_categories'], axis=1),
               df3.explode('content_categories')['content_categories'].apply(pd.Series).add_prefix('content_categories_')],
              axis=1).reset_index()
    print(df4.content_categories_categoryName.unique())
    load_db('earn_test', df4)


    df4.to_csv('earn_test.csv')






