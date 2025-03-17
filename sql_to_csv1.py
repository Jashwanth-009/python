import pypyodbc as odbc
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO,filemode='w',filename='logs/sqltocsv1',encoding='utf-8')

def connection(driver_name,server_name,database_name):
    connection_string=f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        trust=yes
    """
    conc=odbc.connect(connection_string)
    return conc

def load_to_csv(csv_path,sql,conc):
    df=pd.read_sql(sql,conc)
    df.to_csv(csv_path,index=False)

try:
    csv_path=r"c:\Users\Jashwasnth B\Documents\sql_to_csv1.csv"
    sql="SELECT *  FROM business_employment_data "

    driver_name='sql server'
    server_name=r'DESKTOP-NP34182\SQLEXPRESS'
    database_name='jashwanth'

    conc=connection(driver_name,server_name,database_name)
    
    load=load_to_csv(csv_path,sql,conc)
    
    
except Exception as e:
    logging.error("an error occured:",exc_info=True)
