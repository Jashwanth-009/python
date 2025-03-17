import pypyodbc as odbc
import requests
import pandas as pd
import json
import sqlalchemy

driver_name="sql server"
server_name=r"DESKTOP-NP34182\SQLEXPRESS"
database_name="jashwanth"

connection=f"""
    driver={driver_name};
    server={server_name};
    database={database_name};
    Trusted_connection=yes;
"""
conc=odbc.connect(connection)
print(conc)
print("connection successful")
cur=conc.cursor()




try:
    query = "SELECT * FROM Products"
    df = pd.read_sql_query(query, conc)

    for i in range(len(df)):
        api_id = df['id'].iloc[i]
        url = f"https://api.restful-api.dev/objects/{api_id}"
        data = requests.get(url)
        api_data = data.json()

        # db_row = df[df["id"] == api_id]

        ## API Vs DB

        ## df


        # if not db_row.empty:
        #     db_id = db_row.iloc[0]["id"]

        #     if api_id == db_id:
        # Extract database values for comparison
        db_values = df.iloc[i].to_dict()
        
        # Map API values to database fields
        api_values = {
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
        if any(db_values[key] != api_values[key] for key in api_values.keys() if key in db_values):
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
                api_values["name"], api_values["color"], api_values["capacity"], 
                api_values["capacity_GB"], api_values["price"], api_values["generation"], 
                api_values["year"], api_values["cpu_model"], api_values["hard_disk_size"], 
                api_values["strap_colour"], api_values["case_size"], api_values["description"], 
                api_values["screen_size"], api_values["capacity_full"], api_values["id"]
            ))
            conc.commit()
            print(f"Data updated for ID: {api_id}")

except Exception as e:
    print("Error occurred:", e)
finally:
    cur.close()
db_values[key]    conc.close()


                
               
                    
                   

    
    
       
        
            
    
    
        
       

