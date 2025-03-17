import pypyodbc as odbc
import requests
import json


DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = r'DESKTOP-NP34182\SQLEXPRESS'
DATABASE_NAME = 'jashwanth' 
try:
    connection_string = f"""
        DRIVER={DRIVER_NAME};
        SERVER={SERVER_NAME};
        DATABASE={DATABASE_NAME};
        Trusted_Connection=yes;
    """
    conc = odbc.connect(connection_string)
    print(conc)
    print("Connection successful with:", conc)
    
    url= "https://api.restful-api.dev/objects"
    resp=requests.get(url)
    jresp=resp.json()
    # print(jresp) 
    
#getting data from sql database
    cur=conc.cursor()
    sql="SELECT * FROM Products;"
    cur.execute(sql)
    sql_data=cur.fetchall()
    # print(sql_data)
    # cur1=cur.description
    # print(cur1)

#getting column names from database
    columns=[desc[0]for desc in cur.description]
    print(columns)

#process database data
    for item in jresp:
        id=item["id"]
        name=item["name"]
        data=item.get("data")
        # print(id,name,data)
# handling default data structure 
        if not isinstance(data,dict):
            data={}
            
    for row in sql_data:
        if row==id:
            dict1=dict(zip(columns, row))
            
            mismatches=[]
           



except Exception as e:
    print("error occured:",e)
