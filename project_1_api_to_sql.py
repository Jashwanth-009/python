import sqlalchemy
import pyodbc as odbc
import logging
import configparser
import requests
import json

import sqlalchemy.connectors

config = configparser.ConfigParser()
config.read('logs\configfile.properties')

try:
    logging.basicConfig(filemode='w', filename='logs\project_1_api_to_sql.', level=logging.INFO, encoding='utf-8')
    
    def sql_con():
        server = config.get('sql', 'server')
        dbname = config.get('sql', 'database')

        eng_con = sqlalchemy.engine.URL.create(
            'mssql+pyodbc',
            host=server,
            database=dbname,
            query=dict(driver='ODBC Driver 17 for SQL Server', Trusted_Connection='yes')
        )
        engine = sqlalchemy.create_engine(eng_con, fast_executemany=True, isolation_level='AUTOCOMMIT')
        return engine

    engine = sql_con()
    conc = engine.connect()
    logging.info(engine)
    
    def fetch_data_from_api():
        api_link = config.get('api', 'page_api')
        api = requests.get(api_link)
        api_data = api.json()
        return api_data
    
    data = fetch_data_from_api()
    logging.info(data)
    
    def api_to_sql(data):
        insert_query = """
            INSERT INTO users_data(id, email, first_name, last_name, avatar, page, per_page, total, total_pages, support_url, support_text)
            VALUES (:id, :email, :first_name, :last_name, :avatar, :page, :per_page, :total, :total_pages, :support_url, :support_text)
        """

        values = []
        page = data.get('page')
        per_page = data.get('per_page')
        total = data.get('total')
        total_pages = data.get('total_pages')
        support_url = data.get('support').get('url')
        support_text = data.get('support').get('text')
        user_data = data.get('data')

        for user in user_data:
            values.append({
                'id': user.get('id'),
                'email': user.get('email'),
                'first_name': user.get('first_name'),
                'last_name': user.get('last_name'),
                'avatar': user.get('avatar'),
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages,
                'support_url': support_url,
                'support_text': support_text
            })
        
        if values:
            with engine.begin() as conn:
                conn.execute(sqlalchemy.text(insert_query), values)
    
    api_to_sql(data)
    logging.info("data inserted successfully")

except Exception as e:
    logging.error("an error occured:", exc_info=True)
