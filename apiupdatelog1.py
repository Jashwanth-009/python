import json
import pandas as pd 
import requests
import pypyodbc as odbc
import logging

# logging.basicConfig(level=logging.INFO, filename="logs/apiupdatelog", filemode="w", encoding="utf-8")


def connection():

    server_name = r'DESKTOP-NP34182\SQLEXPRESS'
    driver_name = 'sql server'
    database_name = 'jashwanth'


    connection = f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        Trust=yes;
    """
    conc = odbc.connect(connection)
    logging.info("Connection successful")
    
    return conc


try:
    conc=connection()
    cur = conc.cursor()
    
    # Fetch data from the Products table
    sql = "SELECT * FROM Products"
    df = pd.read_sql_query(sql, conc)
    df1=df.iloc[0]
    logging.info(df1)
    
    for i in range(len(df)):
        api_id = df["id"].iloc[i]
        logging.info(f"Processing API ID: {api_id}")
        
        
        # Fetch data from the API for the current product
        url = f"https://api.restful-api.dev/objects/{api_id}"
        data = requests.get(url)
        api_data = data.json()
        
        # Get database values for the current row
        db_values = df.iloc[i].to_dict()
        db_columns = list(db_values.keys())
        
        # Prepare API values, using empty string as default when the key is not present
        api_column_values = {
            "id": api_data.get("id", None),
            "name": api_data.get("name", None),
            "color": api_data.get("color", None),
            "capacity": api_data.get("capacity", None),
            "capacity_GB": api_data.get("capacityGB", None),
            "price": api_data.get("price", None),
            "generation": api_data.get("generation", None),
            "year": api_data.get("year", None),
            "cpu_model": api_data.get("cpu_model", None),
            "hard_disk_size": api_data.get("hard_disk_size", None),
            "strap_colour": api_data.get("strap_color", None),
            "case_size": api_data.get("case_size", None),
            "description": api_data.get("description", None),
            "screen_size": api_data.get("screen_size", None),
            "capacity_full": api_data.get("capacity_full", None),
        }
        
        # Compare and update only if there are changes
        if any(db_values[key] != api_column_values[key] for key in api_column_values if key in db_columns):
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
            
            # Prepare the update values in the same order as the query
            update_values = [
                api_column_values["name"],
                api_column_values["color"],
                api_column_values["capacity"],
                api_column_values["capacity_GB"],
                api_column_values["price"],
                api_column_values["generation"],
                api_column_values["year"],
                api_column_values["cpu_model"],
                api_column_values["hard_disk_size"],
                api_column_values["strap_colour"],
                api_column_values["case_size"],
                api_column_values["description"],
                api_column_values["screen_size"],
                api_column_values["capacity_full"],
                api_column_values["id"]  # ID should be at the end as it is part of the WHERE clause
            ]
            
            # Execute the update query
            cur.execute(update_query, update_values)
            conc.commit()
            logging.info(f"Data updated for ID: {api_id}")

except Exception as e:
    logging.error("An error occurred:", exc_info=True)
