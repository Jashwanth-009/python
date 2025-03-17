import pypyodbc as odbc
import requests

# Database connection details
DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = r'DESKTOP-NP34182\SQLEXPRESS'
DATABASE_NAME = 'jashwanth'

try:
    # Establish the database connection
    connection_string = f"""
        DRIVER={DRIVER_NAME};
        SERVER={SERVER_NAME};
        DATABASE={DATABASE_NAME};
        Trusted_Connection=yes;
    """
    conn = odbc.connect(connection_string)
    print("Connection successful with:", conn)

    # Fetch data from API
    url = "https://api.restful-api.dev/objects"
    resp = requests.get(url)
    api_data = resp.json()
    print("API Data Fetched.")

    # Fetch data from database
    cur = conn.cursor()
    sql = "SELECT * FROM Products;"
    cur.execute(sql)
    db_data = cur.fetchall()

    # Get column names from database
    column_names = [desc[0] for desc in cur.description]
    print("Column Names in Database:", column_names)

    # Process API and database data
    for api_item in api_data:
        api_id = api_item.get("id")
        api_name = api_item.get("name")
        api_details = api_item.get("data", {})

        # Default data structure handling
        if not isinstance(api_details, dict):
            api_details = {}

        # Check for matching ID in database
        for db_row in db_data:
            db_id = db_row[0]  # Assuming 'id' is the first column in the database

            if api_id == db_id:  # Match found
                # Create a dictionary for the database row
                db_row_dict = dict(zip(column_names, db_row))

                # Check for mismatches and update
                mismatches = []
                for key, api_value in {**{"id": api_id, "name": api_name}, **api_details}.items():
                    db_value = db_row_dict.get(key)
                    if db_value != api_value:  # Mismatch detected
                        mismatches.append((key, api_value))

                # Update mismatched fields in the database
                if mismatches:
                    print(f"Mismatches for ID {api_id}: {mismatches}")
                    update_query = f"UPDATE Products SET " + ", ".join(
                        [f"{key} = ?" for key, _ in mismatches]
                    ) + " WHERE id = ?"
                    cur.execute(update_query, [value for _, value in mismatches] + [api_id])
                    conn.commit()
                    print(f"Updated ID {api_id} with API data.")

    print("Comparison and updates completed.")

except Exception as e:
    print("Error occurred:", e)

finally:
    if 'conn' in locals() and conn:
        conn.close()
        print("Database connection closed.")



