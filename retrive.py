import pypyodbc as odbc
import json




DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = r'DESKTOP-NP34182\SQLEXPRESS'
DATABASE_NAME = 'jashwanth'

connection_string=f"""
    DRIVER={DRIVER_NAME};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_connection=yes;
"""
conc=odbc.connect(connection_string)
print(conc)
print("database connected successfully")
with open ('data.json','r') as file:
    data= json.load(file)
    print(type(data))
    

    

cur=conc.cursor()
try:
    customer = data['data']['customer']
    
    val= [
        customer['firstName'],
        customer['lastName'],
        customer['phoneNumber'],
        customer['email']
    ]
    sql = """INSERT INTO Customer(firstname, lastname, phoneNumber, email) VALUES (?, ?, ?, ?)"""
    cur.execute(sql,val)
    conc.commit()
    print(cur.rowcount,"rows got insetted")
except Exception as e:
    print("error occured:",e)
finally:
    cur.close()
    conc.close()