import logging
import pandas as pd 
import pyodbc
import json
import sqlalchemy
import configparser
import os


logging.basicConfig(level=logging.INFO,filemode='w',filename='logs/redeem.log',encoding='utf-8')
config=configparser.ConfigParser()
config.read('logs/configfile.properties')
try:
    def sql_con():
        server=config.get('sql','server')
        dbname=config.get('sql','database')
        eng_con=sqlalchemy.engine.URL.create(
            'mssql+pyodbc',
            host=server,
            database=dbname,
            query=dict(driver='ODBC driver 17 for sql server',Trusted_Connection='yes')
        )
        engine=sqlalchemy.create_engine(eng_con,isolation_level='AUTOCOMMIT', fast_executemany=True)
        return engine
    
    engine=sql_con()
    logging.info(engine)
    
    json_path=config.get('json','json_path')
     
    df=pd.read_json(json_path)
    #logging.info(df.columns)
    

    df.to_sql(name='RedeemData',index=False,con=engine,if_exists='replace', chunksize=100)
    logging.info("data loaded successfully")
    
    
    
    

except  Exception as e:
    logging.info('an error occured:',exc_info=True)

        