import pypyodbc as odbc
import pandas as pd
import logging
import smtplib
import getpass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email
def send_email(subject,message,receiver_email):
    sender_email="jashwanthbommakanti6@gmail.com"
    sender_password = getpass.getpass(prompt="Enter your email password: ")
    logging.info(sender_password)
    smtp_server="smtp.gmail.com"
    smtp_port=587

    msg=MIMEMultipart()
    msg["from"]=sender_email
    msg["to"]=receiver_email
    msg["subject"]=subject
    msg.attach(MIMEText(message,"plain"))
    try:
        with smtplib.SMTP(smtp_server,smtp_port) as server:
            server.starttls()
            server.login(sender_email,sender_password)
            server.sendmail(sender_email,receiver_email,msg.as_string())
        logging.info("mail send successfully")
    except Exception  as e:
        logging.error("an error occured:",exc_info=True)

try:
    logging.basicConfig(level=logging.INFO, filename="logs/csvretrive",filemode="w",encoding="utf-8")

    server_name = r'DESKTOP-NP34182\SQLEXPRESS'
    driver_name = 'sql server'
    database_name = 'jashwanth'


    connection=f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        trusted=yes;
    """
    conc=odbc.connect(connection)
    logging.info(conc)
    df=pd.read_csv(r"c:\Users\Jashwasnth B\Documents\business-employment-data-Jun-2024-quarter.csv")
    
    logging.info(df.dtypes)
    logging.info(df)
    cur=conc.cursor()
    sql="""
        insert into business_employment_data
        (Series_reference, Period, Data_value, Suppressed, STATUS,UNITS, Magnitude, Subject, [Group], Series_title_1,
            Series_title_2, Series_title_3, Series_title_4, Series_title_5)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      """
    for  j,i in df.iterrows():
        # logging.info(j)
        # logging.info(i)
        Series_reference = i['Series_reference'] if pd.notna(i['Series_reference']) else None
        Period = i['Period'] if pd.notna(i['Period']) else None
        Data_value = i['Data_value'] if pd.notna(i['Data_value']) else None
        Suppressed = i['Suppressed'] if pd.notna(i['Suppressed']) else None
        STATUS = i['STATUS'] if pd.notna(i['STATUS']) else None
        UNITS = i['UNITS'] if pd.notna(i['UNITS']) else None
        Magnitude = i['Magnitude'] if pd.notna(i['Magnitude']) else None
        Subject = i['Subject'] if pd.notna(i['Subject']) else None
        Group = i['Group'] if pd.notna(i['Group']) else None
        Series_title_1 = i['Series_title_1'] if pd.notna(i['Series_title_1']) else None
        Series_title_2 = i['Series_title_2'] if pd.notna(i['Series_title_2']) else None
        Series_title_3 = i['Series_title_3'] if pd.notna(i['Series_title_3']) else None
        Series_title_4 = i['Series_title_4'] if pd.notna(i['Series_title_4']) else None
        Series_title_5 = i['Series_title_5'] if pd.notna(i['Series_title_5']) else None


        val=(Series_reference, Period, Data_value, Suppressed, STATUS,UNITS, Magnitude, Subject, Group, Series_title_1,
            Series_title_2, Series_title_3, Series_title_4, Series_title_5)
        
        cur.execute(sql,val)
        
    
    cur.commit()

except Exception as e:
    error_message=f"an error occured:{str(e)}"
    logging.error("error occured:",exc_info=True)

    send_email(
        subject="an error occured CSV to SQL process",
        message=error_message,
        receiver_email="jashwanthbommakanti123@gmail.com"
    )

