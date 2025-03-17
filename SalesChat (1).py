import os,sys
import json
import pandas as pd
import shutil
import sqlalchemy
import keyring
import logging
import configparser
import pysftp as sftp
import traceback
import datetime
import email_config as email

def sft_connection():
    knownhosts = None
    cnopts = sftp.CnOpts()
    cnopts.hostkeys = None
    srvcon = sftp.Connection(host=host, username=username,password=privkey_pass,cnopts=cnopts)
    return srvcon




def establish_sql_connection(config):
    try:
        # Database Connection Details
        dbserver = config.get('saleschat', 'dbserver')
        server = config.get(dbserver, 'server')
        dbname = config.get(dbserver, 'dbname')
        uid = config.get(dbserver, 'uid')
        pwd = keyring.get_password(server, uid)

        url_params = {
            'drivername': 'mssql+pyodbc',
            'username': uid,
            'password': pwd,
            'host': server,
            'database': dbname,
            'query': {
                'driver': 'ODBC Driver 17 for SQL Server',
                'Trusted_Connection': 'yes'
            }
        }
        engine = sqlalchemy.create_engine(sqlalchemy.engine.url.URL.create(**url_params), fast_executemany=True, isolation_level="AUTOCOMMIT")

        logging.info('Established SQL connection successfully')
        return engine

    except Exception as e:
        logging.error(f"Error establishing SQL connection: {str(e)}")
        raise


config = configparser.ConfigParser()
# Load multiple configuration files with UTF-8 encoding
config_files = [
    "D:\\scripts\\bobs\\bobs.properties",
    "D:\\scripts\\SalesChat\\properties\\SalesChat.properties"
]
config.read(config_files, encoding='utf-8')

#Log file path settings
logfile_path= config.get('logging', 'logfile')
log_filename= config.get('logging', 'filename')
log_full_path=os.path.join(logfile_path, log_filename+'_'+datetime.datetime.now().strftime("%Y%m%d%H%M")+'.log')


logging.basicConfig(filename = log_full_path, level = logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Log Start Time")

logging.info("Email id settings")
from_email = config.get('email', 'from')
to_email_prop = config.get('email', 'to')
to_email = list(to_email_prop.split(','))
cc_email_prop = config.get('email', 'cc')
cc_email = list(cc_email_prop.split(','))
subject = config.get('email', 'subject')

from_email_error = config.get('emailerror', 'from')
to_email_error_prop = config.get('emailerror', 'to')
to_email_error = list(to_email_error_prop.split(','))
cc_email_error_prop = config.get('emailerror', 'cc')
cc_email_error = list(cc_email_error_prop.split(','))
subject_error=config.get('emailerror', 'subject_error')


try:

    #Logging Variables
    package_start_time = datetime.datetime.now()
    logging_description = 'Export from SFTP' #Source of Feed
    file_details = None #Dictionary of filename and record count
    package_type = 'Inbound'

    knownhosts = config.get('bdfsftp', 'knownhosts')
    host = config.get('bdfsftp', 'host')
    username = config.get('bdfsftp', 'username')
    privkey = config.get('bdfsftp', 'private_key')
    privkey_pass = keyring.get_password("bdf_sftp_privkey",username) 

    logging.info('Read SFTP Inboud, Archive and Local folder paths from properties')
    
    # Local folder path
    localfolder = config.get('saleschat', 'localfolder')
    # Archive local folder path
    localArchiveFolderProcessed = config.get('saleschat', 'localArchiveFolderProcessed')
    localArchiveFolderUnProcessed = config.get('saleschat', 'localArchiveFolderUnProcessed')

    # Remote Inbound folder location
    inboxfolder = config.get('saleschat', 'inboxfolder')

    # Remote Archive folder path
    remoteArchiveFolderProcessed = config.get('saleschat', 'remoteArchiveFolderProcessed')
    remoteArchiveFolderUnProcessed = config.get('saleschat', 'remoteArchiveFolderUnProcessed')
    
    # Make connection to SFTP
    logging.info('Sftp connection')   
    srvcon = sft_connection()

    logging.info('Connect to sFTP and copy files to local folder')    
    remotepath = 'test'
    localpath = 'test'
    archivepath = 'test'  
    remotearchivepath= 'test'

    # Flag to track if any file was found
    file_found = False

    # Iterate through the files in the SFTP inbox folder
    logging.info(f'Get files from SFTP to local folder')
    for f in (srvcon.listdir(inboxfolder)):        
        if f.endswith('.json'):
            file_found = True
            remotepath = os.path.join(inboxfolder, f)
            localpath = os.path.join(localfolder, f)
            # Copy file from SFTP to local folder
            srvcon.get(remotepath, localpath)  
    srvcon.close()
    logging.info("Close sFTP connection") 

    logging.info("sFTP connection") 
    srvcon = sft_connection()

    # Establish SQL connection
    logging.info("Establish SQL connection")
    engine = establish_sql_connection(config)

    logging.info("Truncate staging.Fact_Ecom_Sales_Chat Table")
    engine.execute("TRUNCATE TABLE staging.Fact_Ecom_Sales_Chat")
    logging.info("Truncated staging.Fact_Ecom_Sales_Chat Staging Table")
   
    logging.info("Iterate through each file in the folder") 
    # Iterate through each file in the folder
    for f in os.listdir(localfolder):
        if f.endswith('.json'):
            file_path = os.path.join(localfolder, f)
            remote_path=os.path.join(inboxfolder,f)

            local_archivepath_folder_processed = os.path.join(localArchiveFolderProcessed,f)
            local_archivepath_folder_unprocessed = os.path.join(localArchiveFolderUnProcessed,f)

            remote_archivepath_processed = os.path.join(remoteArchiveFolderProcessed,f)
            remote_archivepath_unprocessed = os.path.join(remoteArchiveFolderUnProcessed,f)
            
            # Read JSON data from the file with explicit encoding
            logging.info(f"Read JSON data from the file '{f}'")
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    json_data = json.load(file)
            except Exception as e:
                # print(f"Error reading JSON file '{f}': {e}")
                logging.error("Error reading JSON file ")
                continue
            logging.info("Extract customer data")
            # Extract customer data
            customer_data = json_data.get('data', {}).get('customer', {})

            logging.info(f"Check if any of the required fields (phoneNumber, email, lastName, firstName) have values...")

            # Check if any of the required fields have values
            required_fields = ['phoneNumber', 'email', 'lastName', 'firstName']
            if any(customer_data.get(field) for field in required_fields):
                # Proceed with loading into staging table
                logging.info("Required fields have values. Proceeding with loading into staging table...")


                ## Process the DataFrame
                logging.info(f"Processing DataFrame from file...")

                start_time = pd.to_datetime(json_data['data']['startTime'], unit='ms')
                end_time = pd.to_datetime(json_data['data']['endTime'], unit='ms')
                custom_fields = customer_data.get('custom', {})
                blueconic_id = custom_fields.get('blueconicId', '') if custom_fields else ''
                agent_info = json_data['data']['agentInfo']
                metrics = json_data['data']['metrics']
                agent_info_flat = {
                    'humanAgentMostMessages': agent_info.get('humanAgentMostMessages', ''),
                    'lastHumanAgent': agent_info.get('lastHumanAgent', ''),
                    'mostMessages': agent_info.get('mostMessages', '')
                }

                # Handle handleTime metric with two properties
                if isinstance(metrics, dict):
                    handle_time = metrics.get('handleTime', None)
                    if handle_time is not None:
                        handle_time_basic = handle_time.get('basic', None)
                        handle_time_concurrency_adjusted = handle_time.get('concurrencyAdjusted', None)
                    else:
                        handle_time_basic = None
                        handle_time_concurrency_adjusted = None
                else:
                    handle_time_basic = None
                    handle_time_concurrency_adjusted = None

                df = pd.DataFrame([{
                    'Start Time': start_time,
                    'End Time': end_time,
                    'Customer Email': customer_data['email'],
                    'Customer First Name': customer_data['firstName'],
                    'Customer Last Name': customer_data['lastName'],
                    'Customer Phone number': customer_data['phoneNumber'],
                    'Blueconic ID': blueconic_id,
                    'Agent Most Messages': agent_info_flat['humanAgentMostMessages'],
                    'Last Human Agent': agent_info_flat['lastHumanAgent'],
                    'Most Messages': agent_info_flat['mostMessages'],
                    'Handle Time Basic': handle_time_basic,
                    'Handle Time Concurrency Adjusted': handle_time_concurrency_adjusted,
                    'Number of Customer Messages': metrics.get('numberOfCustomerMessages', None),
                    'Number of Response Timer Expirations': metrics.get('numberOfResponseTimerExpirations', None),
                    'Number of System Messages': metrics.get('numberOfSystemMessages', None),
                    'Initial Agent Response Duration Since Assignment': metrics.get('initialAgentResponseDurationSinceAssignment', None),
                    'Initial Agent Response Duration': metrics.get('initialAgentResponseDuration', None),
                    'Average Agent Message Duration': metrics.get('averageAgentMessageDuration', None),
                    'Number of Accepted Transfers': metrics.get('numberOfAcceptedTransfers', None),
                    'Average Response Time': metrics.get('averageResponseTime', None),
                    'Number of Agent Messages': metrics.get('numberOfAgentMessages', None),
                    'Work Time': metrics.get('workTime', None),
                    'Time to First Response': metrics.get('timeToFirstResponse', None),
                    'Number of Requeues': metrics.get('numberOfRequeues', None),
                    'Average Customer Message Duration': metrics.get('averageCustomerMessageDuration', None),
                    'Proactive Status':metrics.get('proactiveStatus',None),
                }])


                logging.info('Loading data into stag table: staging.Fact_Ecom_Sales_Chat started...')
                df.to_sql(name='Fact_Ecom_Sales_Chat', schema='STAGING', con=engine, index=False, if_exists='append')
                logging.info('Staging table has been loaded.')

                # Move the file to the appropriate local archive folder
                shutil.move(file_path, local_archivepath_folder_processed)

                # Move the file to the appropriate remote archive folder
                logging.info(f'Moving {remote_path} to {remote_archivepath_processed} in sftp started')
                srvcon.rename(remote_path, remote_archivepath_processed)
                logging.info(f'Moving {remote_path} to {remote_archivepath_processed} in sftp completed')

            else:
                # Skip loading into staging table
                logging.info("None of the required fields have values. Skipping loading into staging table.")
                # Move the file to the appropriate local archive folder
                shutil.move(file_path, local_archivepath_folder_unprocessed)

                # Move the file to the appropriate remote archive folder
                logging.info(f'Moving {remote_path} to {remote_archivepath_unprocessed} in sftp started')
                srvcon.rename(remote_path, remote_archivepath_unprocessed)
                logging.info(f'Moving {remote_path} to {remote_archivepath_unprocessed} in sftp completed')
    
    logging.info('Executing stored procedure to move data into ext.Fact_Ecom_Sales_Chat table..')
    engine.execute("EXEC [ext].[SP_FACT_Ecom_Sales_Chat]")
    logging.info('Loading data into ext.Fact_Ecom_Sales_Chat has been completed.')
    
    body = f"Sales Chat data has been loaded successfully."
    email.
    
    
    
     (log_file_name=log_full_path, subject=subject, body=body, from_mail=from_email,
                                 to_mail=to_email, cc_mail=cc_email)

except Exception as e:
    traceback.print_exc()
    logging.exception(f'Error: {type(e).__name__}')
    body = f'Error: {type(e).__name__}<br>'
    body += traceback.format_exc()
    email.send_email(log_file_name=log_full_path, subject=subject_error, body=body,
                        from_mail=from_email_error, to_mail=to_email_error, cc_mail=cc_email_error)
finally:
    srvcon.close()
    logging.info("Close sFTP connection")
    logging.info("Log End Time")



