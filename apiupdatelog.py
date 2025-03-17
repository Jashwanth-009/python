import json
import pandas as pd 
import requests
import pypyodbc as odbc
import logging

logging.basicConfig(level=logging.DEBUG,filename="logs/apiupdatelog",filemode="w",encoding="utf-8")

server_name=r'DESKTOP-NP34182\SQLEXPRESS'
driver_name='sql server'
database_name='jashwanth'
try:
    connection=f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        Trust=yes;
    """
    conc=odbc.connect(connection)
    logging.info(conc)
    logging.info("connection successful")
    cur=conc.cursor()
    sql="SELECT * FROM Products"
    df=pd.read_sql_query(sql,conc)
    # logging.info(df)
    logging.info(type(df))
    for i in range(len(df)):
        api_id=df["id"].iloc[i]
        logging.info(api_id)
        url=f"https://api.restful-api.dev/objects/{api_id}"
        data=requests.get(url)
        api_data=data.json()
        # logging.info(api_data)
        logging.info(type(api_data))
        db_values=df.iloc[i].to_dict()
        # logging.info(db_values)
        logging.info(type(db_values))
        db_columns=list(db_values.keys())
        # logging.info(db_columns)
        db_column_values=list(db_values.values())
        # logging.info(db_column_values)
        api_columns=list(api_data.keys())
        # logging.info(api_columns)
        api_column_values=list(api_data.values())
        logging.info(api_column_values)
        
        api_column_values = {
        "id": api_data.get("id", ""),
        "name": api_data.get("name", ""),
        "color": api_data.get("color", ""),
        "capacity": api_data.get("capacity", ""),
        "capacity_GB": api_data.get("capacityGB", ""),
        "price": api_data.get("price", ""),
        "generation": api_data.get("generation", ""),
        "year": api_data.get("year", ""),
        "cpu_model": api_data.get("cpu_model", ""),
        "hard_disk_size": api_data.get("hard_disk_size", ""),
        "strap_colour": api_data.get("strap_color", ""),
        "case_size": api_data.get("case_size", ""),
        "description": api_data.get("description", ""),
        "screen_size": api_data.get("screen_size", ""),
        "capacity_full": api_data.get("capacity_full", ""),
        }

        # Compare and update only if there are changes
        if any(db_values[key] != api_column_values[key] for key in api_column_values.keys() if key in db_values):
            update_query = """
            UPDATE Products
            SET 
                name = ?,
                color = ?,
                capacity = ?,
                capacity_GB = ?,
                price = ?,
                generation = ?,
                year = ?,
                cpu_model = ?,
                hard_disk_size = ?,
                strap_colour = ?,
                case_size = ?,
                description = ?,
                screen_size = ?,
                capacity_full = ?
            WHERE id = ?
            """
            cur.execute(update_query, (
                api_column_values["name"], api_column_values["color"], api_column_values["capacity"], 
                api_column_values["capacity_GB"], api_column_values["price"], api_column_values["generation"], 
                api_column_values["year"], api_column_values["cpu_model"], api_column_values["hard_disk_size"], 
                api_column_values["strap_colour"], api_column_values["case_size"], api_column_values["description"], 
                api_column_values["screen_size"], api_column_values["capacity_full"], api_column_values["id"]
            ))
            conc.commit()
            logging.info(f"Data updated for ID: {api_id}")


        
   
except Exception as e:
    logging.error("error occured:",exc_info=True)