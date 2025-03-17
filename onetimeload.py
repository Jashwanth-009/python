import  logging, configparser, os, platform,sys, datetime, sqlalchemy, requests, keyring
import pickle

from pandas import json_normalize
import pandas as pd
from datetime import timedelta

sys.path.insert(1,'D:\\scripts\\bobs')
import email_config as email
import subprocess

##DB
def sql_engine(config, server):
    logging.info("conneting to Database")
    dbServer = config.get(server, 'server')
    dbName = config.get(server, 'dbname')
    # dbServer = 'devsqlbi'
    # dbName = 'bdm'
    dbUid = config.get(server, 'uid')
    dbPwd = keyring.get_password(server, dbUid)
    eng_con = sqlalchemy.engine.url.URL.create(
                    'mssql+pyodbc',
                    # username=dbUid,
                    # password=dbPwd,
                    host=dbServer,
                    database=dbName,
                    query=dict(driver='ODBC Driver 17 for SQL Server',Trusted_Connection='yes')
                    )
    engine = sqlalchemy.create_engine(eng_con, fast_executemany=True, isolation_level='AUTOCOMMIT')
    return engine


config = configparser.ConfigParser()
config_path = [
    'D:\\scripts\\dataverse\\properties\\dataverse.properties',
    'D:\\scripts\\bobs\\bobs.properties'
]
config.read(config_path)
program_name = os.path.splitext(os.path.basename(__file__))[0]

##connecting to DB
#server = 'dev-sqlbi'
server = config.get('dataverse', 'db_server')
engine = sql_engine(config, server)

df1 = pd.read_csv('D:\\vamsi\\onetimeload\\ProductFilesForReporting.csv')
#Index(['ProfitCenterNbr', 'PredictedDate', 'Prediction'], dtype='object')
TableName = 'temp_md_product_master_ecomm'
schema = 'mtdt'
df1.to_sql(name=TableName,schema=schema,con=engine, index=False, if_exists='append', chunksize=2000)
