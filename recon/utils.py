import logging
import math
import re
import pandas as pd
import datetime as dt
from .models import ReconLog ,Recon, Transactions
from django.db import transaction,IntegrityError
from django.core.exceptions import ObjectDoesNotExist


logging.basicConfig(level=logging.DEBUG)

current_date = dt.date.today().strftime('%Y-%m-%d')

##Custom Errors
class CustomTypeError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class CustomDatabaseError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class CustomValueError(ValueError):
    pass

class CustomTypeError(TypeError):
    pass

def pre_processing(df):
    try:
        def clean_amount(value):
            try:
                return str(int(float(value)))
            except (ValueError, TypeError):
                raise CustomValueError(f"Error cleaning 'Amount' column for value: {value}")

        def remo_spec_x(value):
            try:
                cleaned_value = re.sub(r'[^0-9a-zA-Z]', '', str(value))
                if cleaned_value == '':
                    return '0'
                return cleaned_value
            except (TypeError, AttributeError):
                raise CustomTypeError(f"Error removing special characters for value: {value}")

        def pad_strings_with_zeros(input_str):
            try:
                input_str = str(input_str)
                if len(input_str) < 12:
                    num_zeros = 12 - len(input_str)
                    padded_str = '0' * num_zeros + input_str
                    return padded_str
                else:
                    return input_str[:12]
            except (TypeError, ValueError):
                raise CustomTypeError(f"Error padding string with zeros for value: {input_str}")

        def clean_date(value):
            try:
                date_value = pd.to_datetime(value, errors='coerce').date()
                if pd.notna(date_value):
                    return str(date_value).replace("-", "")
                else:
                    return '0'
            except (ValueError, TypeError):
                raise CustomValueError(f"Error cleaning 'Date' column for value: {value}")

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
    except (CustomValueError, CustomTypeError) as e:
        # Handle the custom exceptions here
        print(f"Custom Error: {e}")
        return None

def use_cols(df):
    try:
        # Rename columns
        df = df.rename(columns={'TXN_TYPE_x': 'TXN_TYPE', 'Original_TRN_REF': 'ABC REFERENCE', '_merge': 'MERGE',
                                'Recon Status': 'STATUS'})

        # Convert 'DATE_TIME' to datetime
        df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'].astype(str), format='%Y%m%d')

        # Select and retain only the desired columns
        selected_columns = ['DATE_TIME', 'ABC REFERENCE', 'BATCH','AMOUNT', 'ISSUER_CODE', 'ACQUIRER_CODE', 'RESPONSE_CODE',
                            'MERGE', 'STATUS']
        df_selected = df[selected_columns]

        return df_selected
    except KeyError:
        raise CustomTypeError("Missing columns in the DataFrame")
    except (ValueError, TypeError):
        raise CustomTypeError("Invalid data format")

def use_cols_succunr(df):
    try:
        # List of columns to select
        columns_to_select = ['DATE_TIME', 'Transaction type', 'AMOUNT', 'TRN_REF', '_merge', 'Recon Status']

        # Check if all required columns exist
        if not all(col in df.columns for col in columns_to_select):
            raise CustomValueError("Missing columns in the DataFrame")

        # Create a new DataFrame with selected columns
        new_df = df[columns_to_select]
        # Replace NaN values with "UNKWN" in the entire DataFrame
        new_df = new_df.apply(lambda col: col.astype(str).fillna("NULL"))

        # Rename the columns
        new_df = new_df.rename(columns={
            'DATE_TIME': 'DATE',
            'Transaction type': 'TXN TYPE',
            'AMOUNT': 'AMOUNT',
            'TRN_REF': 'TRN_REF',
            '_merge': 'MERGE',
            'Recon Status': 'STATUS'
        })

        # # Replace NaN values with "UNKWN" in the entire DataFrame
        # new_df = new_df.apply(lambda col: col.astype(str).fillna("NULL"))

        return new_df
    except CustomValueError as e:
        raise e

def backup_refs(df, reference_column):
    try:
        df['Original_' + reference_column] = df[reference_column]
        return df
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in backup_refs: {str(e)}") from e

def date_range(column):
    try:
        min_date = column.min().strftime('%Y-%m-%d')
        max_date = column.max().strftime('%Y-%m-%d')
        return min_date, max_date
    except ValueError as e:
        # Handle the case where the column is empty or contains non-date values
        # Raise a custom exception with additional context
        raise CustomValueError("Invalid date values in the column") from e
    except Exception as e:
        # Handle other exceptions as needed
        raise CustomValueError(f"Error in date_range: {str(e)}") from e
    
def process_reconciliation(DF1: pd.DataFrame, DF2: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    try:
        # Rename columns of DF1 to match DF2 for easier merging
        DF1 = DF1.rename(columns={'Date': 'DATE_TIME', 'ABC Reference': 'TRN_REF', 'Amount': 'AMOUNT'})

        # Remove duplicates based on 'TRN_REF'
        DF1 = DF1.drop_duplicates(subset='TRN_REF', keep='first')
        DF2 = DF2.drop_duplicates(subset='TRN_REF', keep='first')

        # Merge the dataframes on the relevant columns
        merged_df = DF1.merge(DF2, on=['DATE_TIME', 'TRN_REF', 'AMOUNT'], how='outer', indicator=True)

        # Replace '_merge' values
        merged_df['_merge'] = merged_df['_merge'].replace('left_only', 'Bank_only').replace('right_only', 'ABC_only')

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
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in process_reconciliation: {str(e)}") from e

def update_reconciliation(df, bank_code):
    try:
        if df.empty:
            logging.warning("No Records to Update.")
            return "No records to update"

        update_count = 0
        insert_count = 0

        # Extract all unique ABC REFERENCE values from your DataFrame
        unique_refs = df['ABC REFERENCE'].unique()

        # Fetch existing trn_ref values from the database based on ABC REFERENCE
        existing_refs = Recon.objects.filter(trn_ref__in=unique_refs).values_list('trn_ref', flat=True)

        # Create a set for faster membership testing
        existing_refs_set = set(existing_refs)

        with transaction.atomic():
            for index, row in df.iterrows():
                date_time = row['DATE_TIME']
                batch = row['BATCH']
                amount = row['AMOUNT']
                abc_ref = row['ABC REFERENCE']
                issuer_code = row['ISSUER_CODE']
                acquirer_code = row['ACQUIRER_CODE']
                response_code = row['RESPONSE_CODE']

                if pd.isnull(abc_ref):
                    logging.warning(f"No References to run Update {index}.")
                    continue

                # Check if the ABC REFERENCE exists in the database
                if abc_ref in existing_refs_set:
                    try:
                        # Try to retrieve the record with the same abc_ref from the database
                        existing_record = Recon.objects.get(trn_ref=abc_ref)

                        # Update recon_obj fields conditionally
                        if response_code != '0':
                            if existing_record.excep_flag == 'N':
                                existing_record.excep_flag = 'Y'

                        if existing_record.iss_flg is None or existing_record.iss_flg == 0 or existing_record.iss_flg != 1:
                            if existing_record.issuer_code == bank_code:
                                existing_record.iss_flg = 1
                                existing_record.iss_flg_date = current_date

                        if existing_record.acq_flg is None or existing_record.acq_flg == 0 or existing_record.acq_flg != 1:
                            if existing_record.acquirer_code == bank_code:
                                existing_record.acq_flg = 1
                                existing_record.acq_flg_date = current_date

                        existing_record.save()
                        update_count += 1

                    except Recon.DoesNotExist:
                        # Handle the case where the record no longer exists
                        pass
                else:
                    # If the ABC REFERENCE doesn't exist, insert a new record
                    try:
                        Recon.objects.create(
                            date_time=current_date,
                            tran_date=date_time,
                            batch=batch,
                            amount=amount,
                            trn_ref=abc_ref,
                            issuer_code=issuer_code,
                            acquirer_code=acquirer_code,
                            iss_flg=1 if issuer_code == bank_code else 0,
                            iss_flg_date=current_date if issuer_code == bank_code else None,
                            acq_flg=1 if acquirer_code == bank_code else 0,
                            acq_flg_date=current_date if acquirer_code == bank_code else None,
                            excep_flag='Y' if response_code != '0' else 'N'
                        )
                        insert_count += 1
                    except IntegrityError:
                        # Another thread/process inserted a record with the same abc_ref simultaneously
                        logging.warning(f"IntegrityError encountered for ABC REFERENCE: {abc_ref}. Skipping insertion.")
                        pass

        feedback = f"Updated: {update_count}, Inserted: {insert_count}"
        logging.info(feedback)

        return feedback
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in update_reconciliation: {str(e)}") from e

def insert_recon_stats(bank_id,User, reconciled_rows, unreconciled_rows, exceptions_rows, feedback, 
                        requested_rows, uploaded_rows, date_range_str):
    try:
        # Create a new ReconLog instance and save it to the database
        recon_log = ReconLog(
            date_time=current_date,
            bank_id=bank_id,
            user_id=User,
            rq_date_range=date_range_str,
            upld_rws=uploaded_rows,
            rq_rws=requested_rows,
            recon_rws=reconciled_rows,
            unrecon_rws=unreconciled_rows,
            excep_rws=exceptions_rows,
            feedback=feedback
        )
        recon_log.save()
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in insert_recon_stats: {str(e)}") from e

def unserializable_floats(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.replace({math.nan: "NaN", math.inf: "Infinity", -math.inf: "-Infinity"})
        return df
    except Exception as e:
        # Handle exceptions and raise CustomTypeError with additional context
        raise CustomTypeError(f"Error in unserializable_floats: {str(e)}") from e

####***************************************************####
#### ***************Settlemt file**********************####
####***************************************************####                                    

def combine_transactions(df: pd.DataFrame, acquirer_col: str = 'Payer', issuer_col: str = 'Beneficiary', 
                         amount_col: str = 'Tran Amount', type_col: str = 'Tran Type') -> pd.DataFrame:
    try:
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
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in combine_transactions: {str(e)}") from e

def add_payer_beneficiary(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df['Payer'] = df['ACQUIRER']
        df['Beneficiary'] = df['ISSUER']
        return df
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in add_payer_beneficiary: {str(e)}") from e

def pre_processing_amt(df):
    try:
        # Helper function
        def clean_amount(value):
            try:
                # Convert the value to a float, round to nearest integer
                return round(float(value))  # round the value and return as an integer
            except:
                return value  # Return the original value if conversion fails
        
        # Cleaning logic
        for column in ['AMOUNT', 'FEE', 'ABC_COMMISSION']:  # only these columns
            df[column] = df[column].apply(clean_amount)
        
        return df
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in pre_processing_amt: {str(e)}") from e

def convert_batch_to_int(df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Check data type and convert 'BATCH' column to numeric
        df['BATCH'] = pd.to_numeric(df['BATCH'], errors='coerce')
        # Apply the round method
        df['BATCH'] = df['BATCH'].round(0).fillna(0).astype(int)
        
        return df
    except Exception as e:
        # Handle exceptions and raise CustomValueError with additional context
        raise CustomValueError(f"Error in convert_batch_to_int: {str(e)}") from e

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
        # Handle exceptions and raise CustomDatabaseError with additional context
        raise CustomDatabaseError(f"Error fetching data from the database: {str(e)}") from e


####***************************************************####
#### ***************Recon Setle file**********************####
####***************************************************####    

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
    try:
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

        return merged_setle, matched_setle, unmatched_setle, unmatched_setlesabs

    except Exception as e:
        # Handle exceptions and raise CustomDatabaseError with additional context
        logging.error(f"An error occurred while merging data: {str(e)}")
        raise CustomDatabaseError(f"Error merging data: {str(e)}")

    


    

