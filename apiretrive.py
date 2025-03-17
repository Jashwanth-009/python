import pypyodbc as odbc
import requests
import json



DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = r'DESKTOP-NP34182\SQLEXPRESS'
DATABASE_NAME = 'jashwanth'

connection_string = f"""
    DRIVER={DRIVER_NAME};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trusted_Connection=yes;
"""
conc = odbc.connect(connection_string)
print(conc)
print("Connection successful with:", conc)
try:
    url = "https://api.restful-api.dev/objects"
    resp = requests.get(url)
    jresp = resp.json()
    # jresp1=dict(jresp)
    print(jresp)
    
    print(type(jresp)) 
    cur = conc.cursor()
    sql = """
        INSERT INTO Products
        (id, name, color, capacity, capacity_GB, price, generation, cpu_model, hard_disk_size, strap_colour, case_size, screen_size, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
    """
    for item in jresp:
        
        id = item["id"]
        name = item["name"]
        data = item.get("data")  
        
        if not isinstance(data, dict):
            data = {}
        color = data.get("color", None)
        capacity = data.get("capacity", None)
        capacity_GB = data.get("capacity GB", None)  
        price = data.get("price", None)
        generation = data.get("generation", None)
        cpu_model = data.get("CPU model", None)
        hard_disk_size = data.get("Hard disk size", None)
        strap_colour = data.get("Strap Colour", None)
        case_size = data.get("Case Size", None)
        screen_size = data.get("Screen size", None)
        description = data.get("Description", None)
        
        val = (id, name, color, capacity, capacity_GB, price, generation, cpu_model, hard_disk_size, strap_colour, case_size, screen_size, description)
        cur.execute(sql, val)
    conc.commit()
    print(cur.rowcount, "rows got inserted.")

except Exception as e:
    print("Error occurred:", e)

finally:
    
    conc.close()
    
 
        


