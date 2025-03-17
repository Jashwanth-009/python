import os
import logging
import configparser
import io
import sqlalchemy
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

try:
    
    logging.basicConfig(level=logging.INFO, filemode='w', filename='logs/sharepoint.log', encoding='utf-8')
    
    
    config = configparser.ConfigParser()
    config.read("logs/configfile.properties")

    
    def sql_conc():
        server = config.get('sql', 'server')
        dbname = config.get('sql', 'database')
        driver = "ODBC Driver 17 for SQL Server"
        conn_str = f"mssql+pyodbc://@{server}/{dbname}?driver={driver}&trusted_connection=yes"
        
        engine = sqlalchemy.create_engine(conn_str, isolation_level='AUTOCOMMIT', fast_executemany=True)
        return engine

    engine = sql_conc()
    logging.info('SQL connection successful')

    # SharePoint Connection
    def sharepoint_conc():
        sitelink = config.get('sharepoint', 'sitelink')
        username = config.get('sharepoint', 'username')
        password = config.get('sharepoint', 'password')
        return ClientContext(sitelink).with_credentials(UserCredential(username, password))

    conc = sharepoint_conc()
    file_url = config.get('sharepoint', 'file_url')
    
    file = conc.web.get_file_by_server_relative_url(file_url)
    file_properties = file.get().execute_query()

    file_name = file_properties.properties.get("Name", None)
    logging.info(f"Processing file: {file_name}")

    with io.BytesIO() as file_stream:
        file.download(file_stream).execute_query()
        file_stream.seek(0)
        
        
        df = pd.read_excel(file_stream)
        df = df.fillna('Null')  

        
        df.columns = [col.strip() for col in df.columns]  

        
        df = df.astype(str)

        logging.info(df.head())
        logging.info(f"Columns: {df.columns.tolist()}")
        logging.info(f"Data types: {df.dtypes}")

        table_name = config.get('sql', 'table_name') 

        
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, chunksize=100)

        logging.info("Data inserted successfully using SQLAlchemy to_sql().")

except Exception as e:
    logging.error("An error occurred:", exc_info=True)
