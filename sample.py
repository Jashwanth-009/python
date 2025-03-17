import pyodbc
import logging
import sqlalchemy
import configparser

config=configparser.ConfigParser()

config.read("logs\configfile.properties")

logging.basicConfig(level=logging.INFO,filemode='w',filename='logs\sample.log',encoding='utf-8')

def sql_con():
    dbname=config.get('sql','database')
    server=config.get('sql','server')
    eng_con=sqlalchemy.engine.URL.create(
        'mssql+pyodbc',
         host=server,
         database=dbname,
         query=dict(driver='ODBC Driver 17 for SQL Server',Trusted_Connection='yes')
    )
    engine=sqlalchemy.create_engine(eng_con,isolation_level='AUTOCOMMIT',fast_executemany=True)
    return engine

engine=sql_con()
logging.info(engine)