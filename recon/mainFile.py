import pandas as pd
from django.db.models import Q
import os
import logging

from .models import Transactions
from .utils import  backup_refs, date_range, pre_processing, process_reconciliation,insert_recon_stats, update_reconciliation, use_cols, use_cols_succunr
 

def reconcileMain(path, bank_code, user):
    try:
        # Read the uploaded dataset from Excel
        uploaded_df = pd.read_excel(path, usecols=[0, 1, 2, 3], skiprows=0)
        
        # Check if the uploaded file is empty
        if uploaded_df.empty:
            return None, None, None, None, "Your uploaded file is empty", None, None, None 
        
        # Apply the date_range method to 'uploaded_df' and update it
        min_date, max_date = date_range(uploaded_df.iloc[:, 0])
        date_range_str = f"{min_date},{max_date}"

        # Create a copy of the 4th column (index 3) and store it as a new column
        uploaded_df = backup_refs(uploaded_df, uploaded_df.columns[3])

        # Add a new column 'Response_code' with success_code
        uploaded_df['Response_code'] = '0'
        UploadedRows = len(uploaded_df)

        # Clean and format columns in the uploaded dataset
        uploaded_df_processed = pre_processing(uploaded_df)

        try:
            # Query the database for transactions
            extract = Transactions.objects.filter(
                Q(issuer_code=bank_code) | Q(acquirer_code=bank_code),
                date_time__date__range=(min_date, max_date),
                request_type='1200',
            ).exclude(Q(txn_type__in=['BI', 'MINI']) & ~Q(processing_code__in=['320000', '340000', '510000', '370000', '180000', '360000'])).values(
                'date_time', 'batch', 'trn_ref', 'txn_type', 'issuer_code', 'acquirer_code', 'amount', 'response_code',
            ).distinct()

            dbextract = pd.DataFrame.from_records(extract)
            new_column_names = {
                'date_time': 'DATE_TIME', 'batch': 'BATCH', 'trn_ref': 'TRN_REF', 'txn_type': 'TXN_TYPE', 'issuer_code': 'ISSUER_CODE',
                'acquirer_code': 'ACQUIRER_CODE', 'amount': 'AMOUNT', 'response_code': 'RESPONSE_CODE'
            }

            dbextract = dbextract.rename(columns=new_column_names)
            
            if not dbextract.empty:
                datadump = backup_refs(dbextract, 'TRN_REF')
                requestedRows = len(datadump[datadump['RESPONSE_CODE'] == '0'])
                
                # Clean and format columns in the datadump
                db_preprocessed = pre_processing(datadump)

                merged_df, reconciled_data, succunreconciled_data, exceptions = process_reconciliation(uploaded_df_processed, db_preprocessed)
                
                if not reconciled_data.empty: 
                    # List of dataframes to process
                    datafiles = [reconciled_data, exceptions]
                    # Apply the use_cols function to each dataframe in the list
                    for i in range(len(datafiles)):
                        datafiles[i] = use_cols(datafiles[i])
                    succunreconciled_data= use_cols_succunr(succunreconciled_data)
                    # Extract dataframes after applying use_cols
                    reconciled_data, exceptions = datafiles                                 
                    
                    feedback = update_reconciliation(reconciled_data, bank_code)
                    try:
                        insert_recon_stats(
                            bank_code,user, len(reconciled_data), len(succunreconciled_data), len(exceptions), feedback,
                            requestedRows, UploadedRows, date_range_str
                        )                        
                        # Log or handle success
                    except Exception as e:
                        # Handle the exception (e.g., log the error or take appropriate action)
                        logging.error(f"Error inserting reconstats: {str(e)}")
                        feedback = "Sorry, an error occurred during reconciliation."

                    return merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str
                else:
                    feedback = "Sorry, Reconciliation failed."
            else:
                feedback = "Oops! No Matched Records were found."
        except Exception as e:
            # Handle the exception (e.g., log the error or take appropriate action)
            logging.error(f"Error: {str(e)}")
            feedback = "No records. Check your date range."
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return None, None, None, None, None, None, None, None

# Example usage:
# merged_df, reconciled_data, succunreconciled_data, exceptions, feedback, requestedRows, UploadedRows, date_range_str = reconcileMain(path, bank_code, user)
