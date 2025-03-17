import os
import logging
import configparser
import re
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

try:
    # Logging setup
    logging.basicConfig(level=logging.INFO, filemode='w', filename='logs/sharepoint1.log', encoding='utf-8')

    # Config setup
    config = configparser.ConfigParser()
    config.read("logs/configfile.properties")

    sitelink = config.get('sharepoint', 'sitelink')
    username = config.get('sharepoint', 'username')
    password = config.get('sharepoint', 'password')
    file_url = config.get('sharepoint', 'file_url')

    # Authentication
    conc = ClientContext(sitelink).with_credentials(UserCredential(username, password))
    logging.info(conc)

    if not file_url.startswith("/"):
        raise ValueError("Invalid file_url. It must be a server-relative URL (e.g., /sites/yoursite/Shared Documents/filename.ext)")

    # Get file and validate its properties
    file = conc.web.get_file_by_server_relative_url(file_url)
    file_properties = file.properties
    raw_file_name = file_properties.get("Name", file_url.split('/')[-1])  # Extract filename or use fallback
    file_name = re.sub(r'[<>:"/\\|?*]', '_', raw_file_name)  # Sanitize name

    # Optional: Force correct extension if necessary
    if not file_name.endswith(".pdf"):  # Replace with your expected extension
        file_name += ".pdf"

    # Save to disk
    file_path = os.path.abspath(file_name)
    with open(file_path, 'wb') as localfile:
        file.download(localfile).execute_query()

    logging.info(f"File downloaded successfully to {file_path}")
    print(f"File downloaded successfully to {file_path}")

except Exception as e:
    logging.error("An error occurred:", exc_info=True)
