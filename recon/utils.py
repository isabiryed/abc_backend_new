import logging
import math
import re
import pandas as pd
import datetime as dt
from .models import ReconLog ,Recon, Transactions
from django.db import transaction

current_date = dt.date.today().strftime('%Y-%m-%d')

def pre_processing(df):
    def clean_amount(value):
        try:
            return str(int(float(value)))
        except:
            return '0'

    def remo_spec_x(value):
        cleaned_value = re.sub(r'[^0-9a-zA-Z]', '', str(value))
        if cleaned_value == '':
            return '0'
        return cleaned_value

    def pad_strings_with_zeros(input_str):
        if len(input_str) < 12:
            num_zeros = 12 - len(input_str)
            padded_str = '0' * num_zeros + input_str
            return padded_str
        else:
            return input_str[:12]

    def clean_date(value):
        try:
            date_value = pd.to_datetime(value).date()
            return str(date_value).replace("-", "")
        except:
            return value

    for column in df.columns:
        if column in ['Date', 'DATE_TIME']:
            df[column] = df[column].apply(clean_date)
        elif column in ['Amount', 'AMOUNT']:
            df[column] = df[column].apply(clean_amount)
        else:
            df[column] = df[column].apply(remo_spec_x)

        if column in ['ABC Reference', 'TRN_REF']:
            df[column] = df[column].apply(pad_strings_with_zeros)

    return df

def use_cols(df):
    # Rename columns
    df = df.rename(columns={'TXN_TYPE_x': 'TXN_TYPE', 'Original_TRN_REF': 'ABC REFERENCE','_merge':'MERGE','Recon Status':'STATUS'})

    # Convert 'DATE_TIME' to datetime
    df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'].astype(str), format='%Y%m%d')

    # Select and retain only the desired columns
    selected_columns = ['DATE_TIME', 'ABC REFERENCE', 'BATCH', 'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE', 'MERGE', 'STATUS']
    df_selected = df[selected_columns]

    return df_selected

def backup_refs(df, reference_column):
    df['Original_' + reference_column] = df[reference_column]
    return df

def date_range(column):
    min_date = column.min().strftime('%Y-%m-%d')
    max_date = column.max().strftime('%Y-%m-%d')
    return min_date, max_date
    
def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Rename columns of DF1 to match DF2 for easier merging
    DF1 = DF1.rename(columns={'Date': 'DATE_TIME', 'ABC Reference': 'TRN_REF', 'Amount': 'AMOUNT', 'Transaction type': 'TXN_TYPE'})
    
    # Merge the dataframes on the relevant columns
    merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)
    
    # Create a new column 'Recon Status' with initial value 'Unreconciled'
    merged_df['Recon Status'] = 'Unreconciled'
    
    # Update 'Recon Status' based on conditions
    reconciled_condition = (merged_df['Recon Status'] == 'Unreconciled') & ((merged_df['RESPONSE_CODE'] == '0') | (merged_df['Response_code'] == '0'))
    merged_df.loc[reconciled_condition, 'Recon Status'] = 'succunreconciled'
    merged_df.loc[merged_df['_merge'] == 'both', 'Recon Status'] = 'Reconciled'

    # Separate the data into different dataframes based on the reconciliation status
    reconciled_data = merged_df[merged_df['Recon Status'] == 'Reconciled']
    succunreconciled_data = merged_df[merged_df['Recon Status'] == 'succunreconciled']
    unreconciled_data = merged_df[merged_df['Recon Status'] == 'Unreconciled']
    exceptions = merged_df[(merged_df['Recon Status'] == 'Reconciled') & (merged_df['RESPONSE_CODE'] != '0')]

    return merged_df, reconciled_data, succunreconciled_data, exceptions

def update_reconciliation(df, bank_code):
    if df.empty:
        logging.warning("No Records to Update.")
        return "No records to update"

    update_count = 0
    insert_count = 0

    with transaction.atomic():
        for index, row in df.iterrows():
            date_time = row['DATE_TIME']
            batch = row['BATCH']
            trn_ref = row['ABC REFERENCE']
            issuer_code = row['ISSUER_CODE']
            acquirer_code = row['ACQUIRER_CODE']
            response_code = row['RESPONSE_CODE']  # Add this line to get RESPONSE_CODE

            if pd.isnull(trn_ref):
                logging.warning(f"No References to run Update {index}.")
                continue

            # Try to retrieve an existing record based on TRN_REF
            try:
                recon_obj = Recon.objects.get(trn_ref=trn_ref)

                # Update recon_obj fields conditionally
                if recon_obj.iss_flg is None or recon_obj.iss_flg == 0 or recon_obj.iss_flg != 1:
                    if recon_obj.issuer_code == bank_code:
                        recon_obj.iss_flg = 1
                        recon_obj.iss_flg_date = current_date

                if recon_obj.acq_flg is None or recon_obj.acq_flg == 0 or recon_obj.acq_flg != 1:
                    if recon_obj.acquirer_code == bank_code:
                        recon_obj.acq_flg = 1
                        recon_obj.acq_flg_date = current_date

                if recon_obj.excep_flag is None or recon_obj.excep_flag == 'N' or recon_obj.excep_flag != 'Y':
                    if response_code != 0:
                        recon_obj.excep_flag = 'Y'

                recon_obj.save()
                update_count += 1

            # If the record doesn't exist, create a new one
            except Recon.DoesNotExist:
                recon_obj = Recon(
                    date_time=current_date,
                    tran_date=date_time,
                    batch=batch,
                    trn_ref=trn_ref,
                    issuer_code=issuer_code,
                    acquirer_code=acquirer_code                    
                )

                # Set / Update fields based on your logic
                
                if recon_obj.issuer_code == bank_code:
                    recon_obj.iss_flg = 1
                    recon_obj.iss_flg_date = current_date

                if recon_obj.acquirer_code == bank_code:
                    recon_obj.acq_flg = 1
                    recon_obj.acq_flg_date = current_date

                if response_code != 0:
                    recon_obj.excep_flag = 'Y'

                recon_obj.save()
                insert_count += 1

    feedback = f"Updated: {update_count}, Inserted: {insert_count}"
    logging.info(feedback)

    return feedback

def insert_recon_stats(bank_id, reconciled_rows, unreconciled_rows, exceptions_rows, feedback, 
                        requested_rows, uploaded_rows, date_range_str):
    
    # Create a new ReconLog instance and save it to the database
    recon_log = ReconLog(
        date_time=current_date,
        bank_id=bank_id,
        rq_date_range=date_range_str,
        upld_rws=uploaded_rows,
        rq_rws=requested_rows,
        recon_rws=reconciled_rows,
        unrecon_rws=unreconciled_rows,
        excep_rws=exceptions_rows,
        feedback=feedback
    )
    recon_log.save()

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
    return df

####***************************************************####
#### ***************Settlemt file**********************####
####***************************************************####                                    

class SettlementProcessor:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def combine_transactions(df: pd.DataFrame, acquirer_col: str = 'Payer', issuer_col: str = 'Beneficiary', 
                         amount_col: str = 'Tran Amount', type_col: str = 'Tran Type') -> pd.DataFrame:
        """
        Combine transactions based on certain conditions.

        :param df: Input DataFrame.
        :param acquirer_col: Column name for Acquirer.
        :param issuer_col: Column name for Issuer.
        :param amount_col: Column name for Transaction Amount.
        :param type_col: Column name for Transaction Type.
        :return: New DataFrame with combined transaction amounts.
        """
        combined_dict = {}

        for index, row in df.iterrows():
            acquirer = row[acquirer_col]
            issuer = row[issuer_col]
            tran_amount = row[amount_col]
            tran_type = row[type_col]
            key = (acquirer, issuer)
        
            if acquirer != issuer and tran_type not in ["CLF", "CWD"]:
                combined_dict[key] = combined_dict.get(key, 0) + tran_amount

            if acquirer != issuer and tran_type in ["CLF", "CWD"]:
                combined_dict[key] = combined_dict.get(key, 0) + tran_amount

            # where issuer & acquirer = TROP BANK AND service = NWSC , UMEME settle them with BOA
            if acquirer == "TROAUGKA" and issuer == "TROAUGKA" and tran_type in ["NWSC", "UMEME"]:
                tro_key = ("TROAUGKA", "AFRIUGKA")
                combined_dict[tro_key] = combined_dict.get(tro_key, 0) + tran_amount

        # Convert combined_dict to DataFrame
        combined_result = pd.DataFrame(combined_dict.items(), columns=["Key", amount_col])
        # Split the "Key" column into Acquirer and Issuer columns
        combined_result[[acquirer_col, issuer_col]] = pd.DataFrame(combined_result["Key"].tolist(), index=combined_result.index)
        
        # Drop the "Key" column
        combined_result = combined_result.drop(columns=["Key"])
        
        return combined_result

    def add_payer_beneficiary(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds 'Payer' and 'Beneficiary' columns to the DataFrame.

        :param df: Input DataFrame.
        :return: DataFrame with 'Payer' and 'Beneficiary' columns added.
        """
        df['Payer'] = df['ACQUIRER']
        df['Beneficiary'] = df['ISSUER']
        return df

    def pre_processing_amt(df):
        # Helper function
        def clean_amount(value):
            try:
                # Convert the value to a float, round to nearest integer
                return round(float(value))  # round the value and return as integer
            except:
                return value  # Return the original value if conversion fails
        
        # Cleaning logic
        for column in ['AMOUNT', 'FEE', 'ABC_COMMISSION']:  # only these columns
            df[column] = df[column].apply(clean_amount)
        
        return df

    def convert_batch_to_int(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converts the 'BATCH' column to numeric, rounds it to the nearest integer, and fills NaN with 0.

        :param df: DataFrame containing the 'BATCH' column to convert.
        :return: DataFrame with the 'BATCH' column converted.
        """
        # Check data type and convert 'BATCH' column to numeric
        df['BATCH'] = pd.to_numeric(df['BATCH'], errors='coerce')
        # Apply the round method
        df['BATCH'] = df['BATCH'].round(0).fillna(0).astype(int)
        
        return df

def select_setle_file(batch):
    try:
        # Query the Transactions table using Django's database API
        datafile = Transactions.objects.filter(
            RESPONSE_CODE='0',
            BATCH=batch,
            ISSUER_CODE__exact='730147',  # Assuming this is the issuer code to exclude
            TXN_TYPE__in=['ACI', 'AGENTFLOATINQ']
        ).exclude(REQUEST_TYPE__in=['1420', '1421'])

        # Convert the QuerySet to a DataFrame
        datafile = pd.DataFrame(datafile.values())

        return datafile
    except Exception as e:
        logging.error(f"Error fetching data from the database: {str(e)}")
        return None

####***************************************************####
#### ***************Recon Setle file**********************####
####***************************************************####    

class ExcelFileProcessor:
    def __init__(self, file_path, sheet_name):
        self.file_path = file_path
        self.sheet_name = sheet_name

    def read_excel_file(self):
        try:
            with pd.ExcelFile(self.file_path) as xlsx:
                df = pd.read_excel(xlsx, sheet_name=self.sheet_name, usecols=[0, 1, 2, 7, 8, 9, 11], skiprows=0)
            # Rename the columns
            df.columns = ['TRN_REF', 'DATE_TIME', 'BATCH', 'TXN_TYPE', 'AMOUNT', 'FEE', 'ABC_COMMISSION']
            return df
        except Exception as e:
            logging.error(f"An error occurred while opening the Excel file: {e}")
            return None   

def merge(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    
    # Merge the dataframes on the relevant columns
    merged_setle = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF'], how='outer', suffixes=('_DF1', '_DF2'), indicator=True)
    
        # Now perform the subtraction
    merged_setle.loc[merged_setle['_merge'] == 'both', 'AMOUNT_DIFF'] = (
        pd.to_numeric(merged_setle['AMOUNT_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['AMOUNT_DF2'], errors='coerce')
    )

    merged_setle.loc[merged_setle['_merge'] == 'both', 'ABC_COMMISSION_DIFF'] = (
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF1'], errors='coerce') - 
        pd.to_numeric(merged_setle['ABC_COMMISSION_DF2'], errors='coerce')
    )
    
    # Create a new column 'Recon Status'
    merged_setle['Recon Status'] = 'Unreconciled'    
    merged_setle.loc[merged_setle['_merge'] == 'both', 'Recon Status'] = 'Reconciled'
    
    # Separate the data into different dataframes based on the reconciliation status
    matched_setle = merged_setle[merged_setle['Recon Status'] == 'Reconciled']
    unmatched_setle = merged_setle[merged_setle['Recon Status'] == 'Unreconciled']
    unmatched_setlesabs = merged_setle[(merged_setle['AMOUNT_DIFF'] != 0) | (merged_setle['ABC_COMMISSION_DIFF'] != 0)]
    
    # Define the columns to keep for merged_setle
    use_columns = ['TRN_REF', 'DATE_TIME', 'BATCH_DF1', 'TXN_TYPE_DF1', 'AMOUNT_DF1', 
                            'FEE_DF1', 'ABC_COMMISSION_DF1', 'AMOUNT_DIFF', 'ABC_COMMISSION_DIFF', 
                            '_merge', 'Recon Status']

    # Select only the specified columns for merged_setle
    merged_setle = merged_setle.loc[:, use_columns]    
    matched_setle = matched_setle.loc[:, use_columns]
    unmatched_setle = unmatched_setle.loc[:, use_columns]
    unmatched_setlesabs = unmatched_setlesabs.loc[:, use_columns]

    return merged_setle, matched_setle, unmatched_setle,unmatched_setlesabs









    


    

