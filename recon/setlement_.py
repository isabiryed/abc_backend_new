import os
import logging
from .utils import convert_batch_to_int,  add_payer_beneficiary, combine_transactions, pre_processing, pre_processing_amt, read_excel_file, select_setle_file, merge, select_setle_file
import glob

def settle(batch):
    try:
        logging.basicConfig(filename='settlement.log', level=logging.ERROR)

        # Execute the SQL query
        datadump = select_setle_file(batch)
        
        # Check if datadump is not None
        if datadump is not None and not datadump.empty:         
                        
            # Apply the processing methods
            datadump = convert_batch_to_int(datadump)
            datadump = pre_processing_amt(datadump)
            datadump = add_payer_beneficiary(datadump)
                  
        else:
            logging.warning("No records for processing found.")
            return None  # Return None to indicate that no records were found

        # Now you can use the combine_transactions method
        setlement_result = combine_transactions(acquirer_col='Payer', issuer_col='Beneficiary', amount_col='AMOUNT', type_col='TXN_TYPE')

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return None  # Return None to indicate that an error occurred

    return setlement_result


def setleSabs(path, batch):

    try:     
        datadump = select_setle_file(batch)

        # Check if datadump is not None and not empty
        if datadump is not None and not datadump.empty:
            datadump = pre_processing_amt(datadump)
            datadump = pre_processing(datadump)

            # Processing SABSfile_ regardless of datadump's status
            excel_files = glob.glob(path)
            if not excel_files:
                logging.error(f"No matching Excel file found for '{path}'.")
            else:
                matching_file = excel_files[0]
                SABSfile_ = read_excel_file(matching_file, 'Transaction Report')
                SABSfile_ = pre_processing_amt(SABSfile_)
                SABSfile_ = pre_processing(SABSfile_)

            merged_setle, matched_setle, unmatched_setle, unmatched_setlesabs = merge(SABSfile_, datadump)

            logging.basicConfig(filename='settlement_recon.log', level=logging.ERROR)

            print('Settlement Report has been generated')                
        
        else:
            print("No records for processing found.")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

    return merged_setle, matched_setle, unmatched_setle, unmatched_setlesabs



