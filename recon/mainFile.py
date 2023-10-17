import pandas as pd
import math
import pyodbc
from .db_connect import execute_query
import os
from dotenv import load_dotenv
# from fastapi import FastAPI, Query, UploadFile, Form,File,HTTPException
from .db_recon_stats import insert_recon_stats
from .db_recon_data import update_reconciliation
from .models import Transactions

# Log errors and relevant information using the Python logging module
import logging
from .setle_sabs import pre_processing

reconciled_data = None
succunreconciled_data = None

# Load the .env file
load_dotenv()

# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

# Example usage for SELECT query:   
# connection_string = execute_query(server, database, username, password)
queryTst = "SELECT 1"
connection_string = execute_query(server, database, username, password,queryTst)

def use_cols(df):
    """
    Renames the 'Original_ABC Reference' column to 'Reference' and selects specific columns.

    :param df: DataFrame to be processed.
    :return: New DataFrame with selected and renamed columns.
    """
    df = df.rename(columns={'TXN_TYPE_y': 'TXN_TYPE', 'Original_TRN_REF': 'TRN_REF2'})

    # Convert 'DATE_TIME' to datetime
    df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'].astype(str), format='%Y%m%d')

    # Select only the desired columns
    selected_columns = ['DATE_TIME', 'AMOUNT', 'TRN_REF2', 'BATCH', 'TXN_TYPE', 
                        'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE', '_merge', 'Recon Status']
    df_selected = df[selected_columns]
    
    return df_selected

def backup_refs(df, reference_column):
    # Backup the original reference column
    df['Original_' + reference_column] = df[reference_column]
    
    return df

def date_range(dataframe, date_column):
    min_date = dataframe[date_column].min().strftime('%Y-%m-%d')
    max_date = dataframe[date_column].max().strftime('%Y-%m-%d')
    return min_date, max_date

def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Rename columns of DF1 to match DF2 for easier merging
    DF1 = DF1.rename(columns={'Date': 'DATE_TIME','ABC Reference': 'TRN_REF','Amount': 'AMOUNT','Transaction type': 'TXN_TYPE'})
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)
    
    # Create a new column 'Recon Status'
    merged_df['Recon Status'] = 'Unreconciled'
    merged_df.loc[(merged_df['Recon Status'] == 'Unreconciled') & (merged_df['RESPONSE_CODE'] == '0') | (merged_df['Response_code'] == '0'), 'Recon Status'] = 'succunreconciled'
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'

    # Separate the data into three different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    succunreconciled_data = merged_df[merged_df['Recon Status'] == 'succunreconciled']
    unreconciled_data = merged_df[merged_df['Recon Status'] == 'Unreconciled']
    exceptions = merged_df[(merged_df['Recon Status'] == 'Reconciled') & (merged_df['RESPONSE_CODE'] != '0')]

    return merged_df, reconciled_data, succunreconciled_data, exceptions

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
    return df
    

def reconcileMain(path, bank_code, user):

    try:
        global reconciled_data, succunreconciled_data  # Indicate these are global variables
       
        # Read the uploaded dataset from Excel
        uploaded_df = pd.read_excel(path , usecols=[0, 1, 2, 3], skiprows=0)

        # Check if the uploaded file is empty
        if uploaded_df.empty:
            return None, None, None, None, "Your uploaded file is empty", None, None, None

        # Now, you can use strftime to format the 'Date' column
        min_date, max_date = date_range(uploaded_df, 'Date')

        date_range_str = f"{min_date},{max_date}"

        uploaded_df = backup_refs(uploaded_df, 'ABC Reference')
        uploaded_df['Response_code'] = '0'
        UploadedRows = len(uploaded_df)
        
        # Clean and format columns in the uploaded dataset
        uploaded_df_processed = pre_processing(uploaded_df)        
        
        query = f"""
         SELECT DISTINCT DATE_TIME, BATCH,TRN_REF, TXN_TYPE, ISSUER_CODE, ACQUIRER_CODE,
                AMOUNT, RESPONSE_CODE
         FROM Transactions
         WHERE (ISSUER_CODE = '{bank_code}' OR ACQUIRER_CODE = '{bank_code}')
             AND CONVERT(DATE, DATE_TIME) BETWEEN '{min_date}' AND '{max_date}'
            AND AMOUNT <> 0
            AND TXN_TYPE NOT IN ('ACI','AGENTFLOATINQ','BI','MINI')
     """
        # Execute the SQL query
        datadump = execute_query(server, database, username, password, query, query_type="SELECT")
      
        if datadump is not None:
            datadump = backup_refs(datadump, 'TRN_REF')
            requestedRows = len(datadump[datadump['RESPONSE_CODE'] == '0'])

            # Clean and format columns in the datadump        
            db_preprocessed = pre_processing(datadump)
                                
            merged_df, reconciled_data, succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed, db_preprocessed)  
           
            if not reconciled_data.empty:
                succunreconciled_data = use_cols(succunreconciled_data) 
                reconciled_data = use_cols(reconciled_data)
                exceptions = use_cols(exceptions)                                      

                feedback = update_reconciliation(reconciled_data, server, database, username, password, bank_code)               
                                               
                # insert_recon_stats(
                #                     bank_code, len(reconciled_data), len(succunreconciled_data), len(exceptions), feedback,
                #                     (requestedRows), (UploadedRows), date_range_str
                #                 )

                try:
                                        
                    insert_recon_stats(bank_code, len(reconciled_data), len(succunreconciled_data),len(exceptions), 
                                   feedback, (requestedRows), (UploadedRows),date_range_str, server, database, username, 
                                   password)
                    
                    # Log or handle success
                except Exception as e:
                    # Handle the exception (e.g., log the error or take appropriate action)
                    print(f"Error inserting data: {str(e)}")                           
                
            
                return merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str
            
            else:
                feedback = "Sorry, we couldn't reconcile any records."
        
        else:
            feedback = "Oops! ABC doesn't seem to have your records."

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None, None, None, None, None, None, None, None
