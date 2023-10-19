import os
from dotenv import load_dotenv
import logging
from .utils import  pre_processing, select_setle_file, SettlementProcessor, ExcelFileProcessor, merge, select_setle_file
import glob

# Load the .env file
load_dotenv()
# Get the environment variables
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

import logging

def settle(batch):
    try:
        logging.basicConfig(filename='settlement.log', level=logging.ERROR)

        # Execute the SQL query
        datadump = select_setle_file(batch)
        
        # Check if datadump is not None
        if datadump is not None and not datadump.empty:         
            # Create a settlementProcessor instance
            transaction_setle = SettlementProcessor(datadump)
            
            # Apply the processing methods
            datadump = transaction_setle.convert_batch_to_int()
            datadump = transaction_setle.pre_processing_amt()
            datadump = transaction_setle.add_payer_beneficiary()
                  
        else:
            logging.warning("No records for processing found.")
            return None  # Return None to indicate that no records were found

        # Now you can use the combine_transactions method
        setlement_result = SettlementProcessor.combine_transactions(acquirer_col='Payer', issuer_col='Beneficiary', amount_col='AMOUNT', type_col='TXN_TYPE')

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return None  # Return None to indicate that an error occurred

    return setlement_result


def setleSabs(path, batch):

    try:
        # Create instances of ExcelFileProcessor and SettlementProcessor
        excel_processor = ExcelFileProcessor()
        settlement_processor = SettlementProcessor()
        

        datadump = select_setle_file(batch)

        # Check if datadump is not None and not empty
        if datadump is not None and not datadump.empty:
            datadump = settlement_processor.pre_processing_amt(datadump)
            datadump = pre_processing(datadump)

            # Processing SABSfile_ regardless of datadump's status
            excel_files = glob.glob(path)
            if not excel_files:
                logging.error(f"No matching Excel file found for '{path}'.")
            else:
                matching_file = excel_files[0]
                SABSfile_ = excel_processor.read_excel_file(matching_file, 'Transaction Report')
                SABSfile_ = settlement_processor.pre_processing_amt(SABSfile_)
                SABSfile_ = pre_processing(SABSfile_)

            merged_setle, matched_setle, unmatched_setle, unmatched_setlesabs = merge(SABSfile_, datadump)

            logging.basicConfig(filename='settlement_recon.log', level=logging.ERROR)

            print('Settlement Report has been generated')                
        
        else:
            print("No records for processing found.")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return merged_setle, matched_setle, unmatched_setle, unmatched_setlesabs



