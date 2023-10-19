import pandas as pd
from django.db.models import Q
import os

# Log errors and relevant information using the Python logging module
import logging
from dotenv import load_dotenv

from .models import Transactions
from .utils import  backup_refs, date_range, pre_processing, process_reconciliation,insert_recon_stats, update_reconciliation, use_cols

# Load the .env file
load_dotenv()

# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
   

def reconcileMain(path, bank_code, user):    

    try:
        # global  succunreconciled_data #reconciled_data,  # Indicate these are global variables
       
        # Read the uploaded dataset from Excel
        uploaded_df = pd.read_excel(path , usecols=[0, 1, 2, 3], skiprows=0)

        # Check if the uploaded file is empty
        if uploaded_df.empty:
            return None, None, None, None, "Your uploaded file is empty", None, None, None
        
        # Apply the date_range method to 'uploaded_df' and update it       
        min_date, max_date = date_range(uploaded_df, 'Date')       
        date_range_str = f"{min_date},{max_date}"

        # Continue working with the modified DataFrame
        uploaded_df = backup_refs(uploaded_df,'ABC Reference')           
           
        #Add new column Response_code with sucess_code
        uploaded_df['Response_code'] = '0'
        UploadedRows = len(uploaded_df)        
        
        # Clean and format columns in the uploaded dataset
        uploaded_df_processed = pre_processing(uploaded_df)        
        
        extract = Transactions.objects.filter(
                        Q(issuer_code=bank_code) | Q(acquirer_code=bank_code),
                        date_time__date__range=(min_date, max_date),
                        amount__gt=0,
                    ).exclude(txn_type__in=['ACI', 'AGENTFLOATINQ', 'BI', 'MINI']).values(
                        'date_time','batch','trn_ref','txn_type',
                        'issuer_code','acquirer_code','amount','response_code',
                    ).distinct()
        
        dbextract = pd.DataFrame.from_records(extract)
        new_column_names = {
            'date_time': 'DATE_TIME', 'batch': 'BATCH','trn_ref': 'TRN_REF','txn_type': 'TXN_TYPE','issuer_code': 'ISSUER_CODE',
            'acquirer_code': 'ACQUIRER_CODE','amount': 'AMOUNT','response_code': 'RESPONSE_CODE'           
        }
        
        dbextract = dbextract.rename(columns=new_column_names)           
        if dbextract is not None:
            
            datadump = backup_refs(dbextract,'TRN_REF')
            requestedRows = len(datadump[datadump['RESPONSE_CODE'] == '0'])

            # Clean and format columns in the datadump        
            db_preprocessed = pre_processing(datadump)
            
            merged_df, reconciled_data, succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed, db_preprocessed)
            
            if not reconciled_data.empty:

                
                reconciled_data = use_cols(reconciled_data)
                exceptions = use_cols(exceptions)                                                  

                feedback = update_reconciliation(reconciled_data, bank_code)                 

                try:                                        
                    insert_recon_stats(
                                    bank_code, len(reconciled_data), len(succunreconciled_data), len(exceptions), feedback,
                                    (requestedRows), (UploadedRows), date_range_str
                                )                                  
                                        
                    # Log or handle success
                except Exception as e:
                    # Handle the exception (e.g., log the error or take appropriate action)
                    print(f"Error inserting reconstats: {str(e)}")                 
            
                return merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str
            
            else:
                feedback = "Sorry, Reconciliation failed."
        
        else:
            feedback = "Oops! ABC doesn't seem to have your records."

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None, None, None, None, None, None, None, None
