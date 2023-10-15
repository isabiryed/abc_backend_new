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

def update_exception_flag(df, server, database, username, password, swift_code):

    # Check if DataFrame is empty
    if df.empty:
        logging.warning("No Exceptions Records to Update.")
        return

    # Check if 'TRN_REF' column exists in the DataFrame
    if 'TRN_REF' not in df.columns:
        logging.error("'TRN_REF' column is missing from the DataFrame.")
        logging.error(f"Columns in DataFrame: {df.columns}")
        return

    update_count = 0

    for index, row in df.iterrows():
        # Safely retrieve 'TRN_REF' from the row
        trn_ref = row.get('TRN_REF', None)

        # Check if trn_ref is None or an empty string
        if not trn_ref:
            logging.warning(f"Empty or missing Exceptions Trn Reference for index {index}.")
            logging.error(f"Row Data: {row}")
            continue

        # Update Query
        update_query = f"""
            UPDATE recon
        SET
            EXCEP_FLAG = CASE WHEN (EXCEP_FLAG IS NULL OR EXCEP_FLAG = 0 OR EXCEP_FLAG != 1)  
            AND (ISSUER_CODE = '{swift_code}' OR ACQUIRER_CODE = '{swift_code}')  
            THEN 'Y' ELSE 'N' END            
            WHERE TRN_REF = '{trn_ref}'
        """

        try:
            execute_query(server, database, username, password, update_query, query_type="UPDATE")
            update_count += 1
        except pyodbc.Error as err:
            logging.error(f"Error updating PK '{trn_ref}': {err}")

    if update_count == 0:
        logging.info("No Exceptions were updated.")

    exceptions_feedback = f"Updated: {update_count}"
    logging.info(exceptions_feedback)

    return exceptions_feedback



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
    

def reconcileMain(path, bank_code,user):
    try:
        global reconciled_data, succunreconciled_data  # Indicate these are global variables
       
        # Read the uploaded dataset from Excel
        uploaded_df = pd.read_excel(path , usecols=[0, 1, 2, 3], skiprows=0)

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
            succunreconciled_data = use_cols(succunreconciled_data) 
            reconciled_data = use_cols(reconciled_data) 

            feedback = update_reconciliation(reconciled_data, server, database, username, password, bank_code)      
            # Initialize exceptions_feedback with a default value
            exceptions_feedback = None 
            # Check if exceptions DataFrame is not empty, if not empty then update exception flag
            # if not exceptions.empty:
            exceptions_feedback = update_exception_flag(exceptions, server, database, username, password,bank_code)
            # else:
            #     exceptions_feedback = "No exceptions to update."
                
            insert_recon_stats(
                bank_code, user, len(reconciled_data), len(succunreconciled_data), 
                len(exceptions), feedback, (requestedRows), (UploadedRows), 
                date_range_str, server, database, username, password
            )
            
            """ print('Thank you, your reconciliation is complete. ' + feedback) """

            return merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None, None, None, None, None, None, None, None
        
