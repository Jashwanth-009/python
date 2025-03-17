import openpyxl
import pypyodbc as odbc
from sql_to_csv import connection 
from sql_to_csv import database
import pandas as pd
import logging

try:

    logging.basicConfig(level=logging.INFO,filename="logs/sqltoexcel.log",filemode="w",encoding="utf-8")
    conc=connection()
    logging.info(conc)
    logging.info("connection successful")
    logging.info("secure connection" )
    cur=conc.cursor()

    df=database()
    logging.info(df)
    df1=df.to_excel(r'c:\Users\Jashwasnth B\Documents\sql_to_excel.xlsx')
    logging.info("loaded successfully")

except Exception as e:
    logging.error("an error occured:",exc_info=True)

