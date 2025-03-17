import pypyodbc as odbc
import pandas as pd
import logging

logging.basicConfig(filename="logs/sql_csv",level=logging.INFO,filemode='w',encoding='utf-8')
try:
    def connection():
        driver_name='sql server'
        server_name=r'DESKTOP-NP34182\SQLEXPRESS' 
        database_name='jashwanth'
        connection=f"""
            driver={driver_name};
            server={server_name};
            database={database_name};
            trust=yes
        """
        conc=odbc.connect(connection)
        logging.info(conc)
        logging.info("connection successful")
        return conc

    conc=connection()
    cur=conc.cursor()
    def database():
        sql='SELECT * FROM Products' 
        logging.info(sql)
        df=pd.read_sql(sql,conc)
        return df
        
    df=database()
    logging.info(df)
    df1=df.to_csv(r'c:\Users\Jashwasnth B\Documents\sql_to_csv.csv')
    logging.info(df1)
except Exception as e:
    logging.error("error occured:",exc_info=True)



        




                  



