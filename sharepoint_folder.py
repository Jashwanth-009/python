import pandas as pd
import logging
import configparser
import sqlalchemy
import pyodbc
import io
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

logging.basicConfig(level=logging.INFO,filemode='w',filename='logs/sharepoint_folder',encoding='utf-8')

config=configparser.ConfigParser()
config.read('logs/configfile.properties')

try:
    def sharepoint_con():
        sitelink=config.get('sharepoint','sitelink')
        username=config.get('sharepoint','username')
        password=config.get('sharepoint','password')
        return ClientContext(sitelink).with_credentials(UserCredential(username, password))
    conc=sharepoint_con()
    logging.info(conc)
    def sql_con():
        server=config.get('sql','server')
        dbname=config.get('sql','database')
        eng_con=sqlalchemy.engine.URL.create(
            'mssql+pyodbc',
            host=server,
            database=dbname,
            query=dict(driver="ODBC driver 17 for sql server",Trusted_connection='yes')

            
        )
        engine=sqlalchemy.create_engine(eng_con,isolation_level='AUTOCOMMIT',fast_executemany=True)
        return engine
    engine=sql_con()
    logging.info(engine)
    folder_url=config.get('sharepoint','folder_url')
    logging.info(folder_url)
    folder=conc.web.get_folder_by_server_relative_url(folder_url)
    conc.load(folder,['files'])
    conc.execute_query()
    target_filename="newdata.xlsx"
    
    for file in folder.files:
        file_name=file.properties["Name"]
        logging.info(file)
        if file_name == target_filename:
            logging.info('file found')
            file_url=f"{folder_url}\{file_name}"
            logging.info(file_url)
            file_1=conc.web.get_file_by_server_relative_url(file_url)

            with io.BytesIO() as file_stream:
                file_1.download(file_stream).execute_query()
                file_stream.seek(0)
                
                df=pd.read_excel(file_stream)
                logging.info(df.columns)
                df.columns=[col.strip() for col in df.columns]
                df = df.astype(str)
                df=df.fillna("Null")
                df.to_sql(name='data', con=engine, index=False, chunksize=100, if_exists="replace")
                logging.info("data loaded successfully")
except  Exception as e:
    logging.info("an error occured:",exc_info=True)