import logging
import pandas as pd
import configparser
import json
import sqlalchemy
import pyodbc
import requests

config=configparser.ConfigParser()
config.read('logs\configfile.properties')

logging.basicConfig(level=logging.INFO,filemode='w',filename='logs/2apis.log',encoding='utf-8')
logging.info("logging successful")
logging.info("config success")
try:
    def sql_con():
        server=config.get('sql','server')
        dbname=config.get('sql','database')
        eng_con=sqlalchemy.URL.create(
            'mssql+pyodbc',
            host=server,
            database=dbname,
            query=dict(driver='ODBC driver 17 for sql server', Trusted_Connection='yes')

        )
        engine=sqlalchemy.create_engine(eng_con,isolation_level='AUTOCOMMIT',fast_executemany=True)
        logging.info("sql connection successful")
        return engine
    engine=sql_con()
    logging.info(engine)

    first_api=config.get('api','first_api')
    second_api=config.get('api','second_api')
    #logging.info(first_api)
    first_api_data=requests.get(first_api)
    first_data=first_api_data.json()
    second_api_data=requests.get(second_api)
    second_data=second_api_data.json()
    logging.info(first_data)
    logging.info(second_data)
    

    df=pd.json_normalize(first_data['data'])
    df['status']="Api"
    df1=pd.json_normalize(second_data['data'])
    df1['page'] = second_data.get('page')
    df1['per_page'] = second_data.get('per_page')
    df1['total'] = second_data.get('total')
    df1['total_pages'] = second_data.get('total_pages')
    df1['status']="Api"
    
    df.columns = df.columns.str.strip()
    df1.columns = df1.columns.str.strip()
    
    logging.info(df.columns)
    logging.info(df1.columns)
    
    logging.info(df)
    logging.info(df1)
    if df.empty:
        logging.warning("first_api is empty")
    else:
        df.to_sql(name='Nation_Population_Stats', schema='staging', con=engine, if_exists='replace', index=False, chunksize=100, method="multi")
    if df1.empty:
        logging.warning("second_api is empty")
    else:
        df1.to_sql(name='User_Data', schema='staging', con=engine, if_exists='replace', index=False, chunksize=100, method="multi")

    query="SELECT [ID Nation],[Nation],[ID Year],Year,status FROM [staging].[Nation_Population_Stats]"
    query1="SELECT  id,email,first_name,last_name,avatar,status FROM [staging].[User_Data]"
    main_df=pd.read_sql(query,engine)
    main_df1=pd.read_sql(query1,engine)
    main_df.to_sql(name='Nation_Population_Stats_Main',con=engine, if_exists='replace', index=False, chunksize=100, method="multi")
    main_df1.to_sql(name='User_Data_Main',con=engine, if_exists='replace', index=False, chunksize=100, method="multi")

    logging.info(f"Inserted {len(df)} records into staging.Nation_Population_Stats")
    logging.info(f"Inserted {len(df1)} records into staging.User_Data")

except Exception as e:
    logging.info("an error occured:",exc_info=True)



    
    