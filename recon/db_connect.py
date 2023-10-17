import pyodbc
import pandas as pd
from dotenv import load_dotenv
from os import getenv
import os

def execute_query(server_name, database_name, username, password, query, query_type="SELECT"):
    conn = None  # Initialize conn to None
    try:
        # Define the connection string
        connection_string = f"Driver={{SQL Server}};Server={server_name};Database={database_name};UID={username};PWD={password};TrustServerCertificate=yes;"

        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Execute the query based on the query type
        if query_type == "SELECT":
            cursor.execute(query)
            # Fetch data directly into a DataFrame with column names
            df = pd.DataFrame.from_records(cursor.fetchall(), columns=[column[0] for column in cursor.description])
            # print(df)
            return df
        elif query_type == "UPDATE":
            cursor.execute(query)      
            conn.commit()  # Commit the changes to the database            
            return None
        elif query_type == "INSERT":
            cursor.execute(query)
            conn.commit()  # Commit the changes to the database
            return None
        else:
            raise ValueError("Invalid query type. Supported types are 'SELECT', 'UPDATE', and 'INSERT'.")

    except Exception as e:
        print(f"Error: {str(e)}")
        return None  # Return None in case of an error
    finally:
        if conn:
            conn.close()
