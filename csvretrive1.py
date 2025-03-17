import pypyodbc as odbc
import pandas as pd
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Function to send email
def send_error_email(subject, message, recipient_email):
    sender_email = "your_email@example.com"  
    sender_password = "your_email_password"  
    
    # SMTP server details
    smtp_server = "smtp.gmail.com"
    smtp_port = 587

    # Create the email message
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = subject

    # Attach the message body
    msg.attach(MIMEText(message, "plain"))

    # Connect to the SMTP server and send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, sender_password)  # Log in to your email account
            server.sendmail(sender_email, recipient_email, msg.as_string())  # Send the email
        logging.info("Error email sent successfully.")
    except Exception as e:
        logging.error("Failed to send email:", exc_info=True)

try:
    logging.basicConfig(level=logging.INFO, filename="logs/csvretrive", filemode="w", encoding="utf-8")

    server_name = r'DESKTOP-NP34182\SQLEXPRESS'
    driver_name = 'sql server'
    database_name = 'jashwanth'

    connection = f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        trusted=yes;
    """
    conc = odbc.connect(connection)
    logging.info(conc)
    
    # Load the CSV
    df = pd.read_csv(r"c:\Users\Jashwasnth B\Documents\business-employment-data-Jun-2024-quarter.csv")
    logging.info(df.dtypes)
    logging.info(df)

    cur = conc.cursor()
    sql = """
        INSERT INTO business_employment_data
        (Series_reference, Period, Data_value, Suppressed, STATUS, UNITS, Magnitude, Subject, [Group], Series_title_1,
         Series_title_2, Series_title_3, Series_title_4, Series_title_5)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for j, i in df.iterrows():
        # Process each row and insert it into the database
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

        val = (Series_reference, Period, Data_value, Suppressed, STATUS, UNITS, Magnitude, Subject, Group,
               Series_title_1, Series_title_2, Series_title_3, Series_title_4, Series_title_5)

        cur.execute(sql, val)

    # Commit the changes
    cur.commit()

except Exception as e:
    error_message = f"An error occurred: {str(e)}"
    logging.error(error_message, exc_info=True)
    
    # Send an email notification with the error message
    send_error_email(
        subject="Error in CSV to SQL Process",
        message=error_message,
        recipient_email="recipient_email@example.com"  
    )
