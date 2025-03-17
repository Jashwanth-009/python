import logging
import configparser
import pyodbc
import pandas as pd
import io
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

try:
    # Logging Configuration
    logging.basicConfig(level=logging.INFO, filemode='w', filename='logs/sharepoint.log', encoding='utf-8')

    
    config = configparser.ConfigParser()
    config.read("logs/configfile.properties")

    
    sitelink = config.get('sharepoint', 'sitelink')
    username = config.get('sharepoint', 'username')
    password = config.get('sharepoint', 'password')
    file_url = config.get('sharepoint', 'file_url')

    
    server = config.get('sql', 'server')
    database = config.get('sql', 'database')
    
    # Connect to SharePoint
    ctx = ClientContext(sitelink).with_credentials(UserCredential(username, password))
    file = ctx.web.get_file_by_server_relative_url(file_url)

    # Read File Content into Memory
    file_content = file.read().execute_query()
    file_data = file_content.content

    # Convert Data to Pandas DataFrame
    file_stream = io.BytesIO(file_data)
    df = pd.read_csv(file_stream)  # Modify for different file formats (e.g., Excel: pd.read_excel(file_stream))

    # Connect to SQL Server
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Insert Data into SQL Table
    for index, row in df.iterrows():
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])
        values = tuple(row)
        table_name = config.get('sql', 'table_name')
        sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql_query, values)

    # Commit and Close Connection
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data inserted successfully into SQL Server.")

except Exception as e:
    logging.error("An error occurred:", exc_info=True)
