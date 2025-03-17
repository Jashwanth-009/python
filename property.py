import logging
import configparser
import pypyodbc as odbc

try:

    logging.basicConfig(filename="logs/property",filemode="w",level=logging.INFO,encoding="utf-8")

    config=configparser.ConfigParser()

    config.add_section('sql')
    config.set('sql','drivername','sql server')
    config.set('sql','server','DESKTOP-NP34182\SQLEXPRESS')
    config.set('sql','database','jashwanth')
    config.set('sql','table_name','cases_data')

    config.add_section('csv')
    config.set('csv','csv_path',r'c:\Users\Jashwasnth B\Documents\property.csv')
    config.set('csv','sql_path','SELECT *  FROM business_employment_data')

    config.add_section('sharepoint')
    config.set('sharepoint','sitelink','https://techtriadteam.sharepoint.com/sites/jashwanth')
    config.set('sharepoint','username','Jashwanth@techtriad.com')
    config.set('sharepoint','password','Jash@123')
    config.set('sharepoint','file_url','/Shared Documents/Bobs Discount Furniture Power BI Data Input.xlsx')
    config.set('sharepoint','folder_url','/sites/jashwanth/Shared Documents/folder2/myfolder3/newfolder2/data')

    config.add_section('api')
    config.set('api','page_api','https://reqres.in/api/users?page')
    config.set('api','first_api','https://datausa.io/api/data?drilldowns=Nation&measures=Population')
    config.set('api','second_api','https://reqres.in/api/users?page')

    config.add_section('json')
    config.set('json','json_path',r'C:\Users\Jashwasnth B\Documents\python\redeem.json')

    with open(r"C:\\Users\\Jashwasnth B\\Documents\\python\\logs\\configfile.properties",'w') as configfile:
        config.write(configfile)
    logging.info("config successful")

except Exception as e:
    logging.error("an error occured:",exc_info=True)
