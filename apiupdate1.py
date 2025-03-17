import pypyodbc as odbc
import requests
import json
try:
#connecting to sql
    driver_name="sql server"
    server_name=r'DESKTOP-NP34182\SQLEXPRESS'  
    database_name="jashwanth"
    connection_string=f"""
        driver={driver_name};
        server={server_name};
        database={database_name};
        Trusted_Connection=yes;
    """
    conc=odbc.connect(connection_string)
    print(conc)
    print("connection successful")
#getting data from sql
    cur=conc.cursor()  #used to execute sql queries
    sql="SELECT * FROM Products"
    cur.execute(sql)
    result=cur.fetchall()
# print(result)
#getting api data
    url="https://api.restful-api.dev/objects"
    api=requests.get(url)
    api_data=api.json()
    # print(api_data)
    update=json.dumps(api_data)
    print(update)
    # print(api_data[0:])
    for row in result:
        db_id=row[0]
        db_name=row[1]
        db_color=row[2]
        db_capacity=row[3]
        db_capacitygb=row[4]
        db_price=row[5]
        db_generation=row[6]
        db_year=row[7]
        db_cpu_model=row[8]
        db_hard_disk_size=row[9]
        db_strap_color=row[10]
        db_case_size=row[11]
        db_description=row[12]
        for row1 in api_data:
            api_id = row1["id"]
            api_name = row1["name"]
            
            # Get the "data" field, ensure it exists and is a dictionary
            api_data_field = row1.get("data", None)
            
            if not isinstance(api_data_field, dict):      # Check if api_data_field is not None and is a dict
                api_data_field={}     
            api_color = api_data_field.get("color", None)
            api_capacity = api_data_field.get("capacity", None)
            api_capacitygb = api_data_field.get("capacity GB", None)
            api_price = api_data_field.get("price", None)
            api_generation = api_data_field.get("generation", None)
            api_year = api_data_field.get("year", None)
            api_cpu_model = api_data_field.get("CPU model", None)
            api_hard_disk_size = api_data_field.get("Hard disk size", None)
            api_strap_color = api_data_field.get("Strap Colour", None)
            api_case_size = api_data_field.get("Case Size", None)
            api_description = api_data_field.get("description", None)

            if db_id == api_id:
                if (db_name != api_name or db_color != api_color or db_capacity != api_capacity or 
                    db_capacitygb != api_capacitygb or db_price != api_price or db_generation != api_generation or 
                    db_year != api_year or db_cpu_model != api_cpu_model or db_strap_color != api_strap_color or 
                    db_case_size != api_case_size or db_description != api_description):
                        
                    print(f"Updating record id:", db_id)
                        
                    update_query = """
                        UPDATE Products
                        SET name=?, color=?, capacity=?, capacity_GB=?, price=?, generation=?, year=?, cpu_model=?,
                        hard_disk_size=?, strap_colour=?, case_size=?, description=?
                        WHERE id=?
                    """
                        
                        # Execute the update query with the retrieved API values
                    cur.execute(update_query, (
                        api_name, api_color, api_capacity, api_capacitygb, api_price, 
                        api_generation, api_year, api_cpu_model, api_hard_disk_size, 
                        api_strap_color, api_case_size, api_description, db_id
                    ))
            else:
                # Print if "data" is missing or not a dictionary
                print(f"Missing or invalid 'data' for API ID: {api_id}")
                
        conc.commit()
        print("Database update completed successfully.")



            

except Exception as e:
    print("error occured:",e)
