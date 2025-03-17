import configparser
import pypyodbc as odbc
import logging
import pandas as pd

try:

    logging.basicConfig(level=logging.INFO,filemode='w',filename='logs/property1',encoding='utf-8')

    config=configparser.ConfigParser()

    confi=config.read('logs/configfile.properties')
    logging.info(confi)
    server=config.get('sql','server')
    driver=config.get('sql','drivername')
    database=config.get('sql','database')
    sql=config.get('csv','sql_path')
    csv=config.get('csv','csv_path')

    def connection(server,driver,database):
        connectionstring=f"""
            DRIVER={driver};
            SERVER={server};
            DATABASE={database};
        trust=yes
        """
        conc=odbc.connect(connectionstring)
        return conc
    def load_to_csv(sql,csv,conc):
        df=pd.read_sql(sql,conc)
        df.to_csv(csv,index=False)

    conc=connection(server,driver,database)
    load_to_csv(sql,csv,conc)
    logging.info("data loading successful")

except Exception as e:
    logging.error("an error occured:",exc_info=True)
    


