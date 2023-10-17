import pandas as pd
import logging
from datetime import datetime
from .db_connect import execute_query
import pyodbc

# Configuring logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def update_reconciliation(df, server, database, username, password, swift_code):
#     if df.empty:
#         logging.warning("No Records to Update.")
#         return "No records to update"

#     update_count = 0
#     insert_count = 0

#     insert_queries = []
#     update_queries = []

#     for index, row in df.iterrows():
#         date_time = row['DATE_TIME']
#         batch = row['BATCH']
#         trn_ref = row['TRN_REF2']
#         issuer_code = row['ISSUER_CODE']
#         acquirer_code = row['ACQUIRER_CODE']

#         if pd.isnull(trn_ref):
#             logging.warning(f"No TRN_REFS to run Update {index}.")
#             continue

#         select_query = f"SELECT * FROM Recon WHERE TRN_REF = '{trn_ref}'"
#         existing_data = execute_query(server, database, username, password, select_query, query_type="SELECT")

#         # Update Query
#         update_query = f"""
#             UPDATE Recon
#             SET
#                 ISS_FLG = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN 1 ELSE ISS_FLG END,
#                 ACQ_FLG = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN 1 ELSE ACQ_FLG END,
#                 ISS_FLG_DATE = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN GETDATE() ELSE ISS_FLG_DATE END,
#                 ACQ_FLG_DATE = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN GETDATE() ELSE ACQ_FLG_DATE END
#             WHERE TRN_REF = '{trn_ref}'                
#         """

#         if existing_data.empty:
#             # If not existing, add insert query to batch and update query to batch
#             insert_query = f"""
#                 INSERT INTO Recon 
#                     (DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE)
#                 VALUES 
#                     (GETDATE(),
#                      '{date_time}',
#                      '{trn_ref}', 
#                      '{batch}', 
#                      '{acquirer_code}',
#                      '{issuer_code}')
#             """
#             insert_queries.append(insert_query)
#             update_queries.append(update_query)
#             insert_count += 1
#         else:
#             # If already existing, add update query to batch
#             update_queries.append(update_query)
#             update_count += 1

#     # Execute insert and update queries in batches
#     if insert_queries:
#         batch_insert_query = "; ".join(insert_queries)
#         execute_query(server, database, username, password, batch_insert_query, query_type="INSERT")

#     if update_queries:
#         batch_update_query = "; ".join(update_queries)
#         execute_query(server, database, username, password, batch_update_query, query_type="UPDATE")

#     feedback = f"Updated: {update_count}, Inserted: {insert_count}"
#     logging.info(feedback)

#     return feedback


def update_reconciliation(df, server, database, username, password, swift_code):
    if df.empty:
        logging.warning("No Records to Update.")
        return "No records to update"

    update_count = 0
    insert_count = 0

    insert_queries = []
    update_queries = []

    for index, row in df.iterrows():
        date_time = row['DATE_TIME']
        batch = row['BATCH']
        trn_ref = row['TRN_REF2']
        issuer_code = row['ISSUER_CODE']
        acquirer_code = row['ACQUIRER_CODE']
        response_code = row['RESPONSE_CODE']  # Add this line to get RESPONSE_CODE

        if pd.isnull(trn_ref):
            logging.warning(f"No TRN_REFS to run Update {index}.")
            continue

        select_query = f"SELECT * FROM Recon WHERE TRN_REF = '{trn_ref}'"
        existing_data = execute_query(server, database, username, password, select_query, query_type="SELECT")

        # Update Query
        update_query = f"""
            UPDATE Recon
            SET
                ISS_FLG = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN 1 ELSE ISS_FLG END,
                ACQ_FLG = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN 1 ELSE ACQ_FLG END,
                ISS_FLG_DATE = CASE WHEN (ISS_FLG IS NULL OR ISS_FLG = 0 OR ISS_FLG != 1) AND ISSUER_CODE = '{swift_code}' THEN GETDATE() ELSE ISS_FLG_DATE END,
                ACQ_FLG_DATE = CASE WHEN (ACQ_FLG IS NULL OR ACQ_FLG = 0 OR ACQ_FLG != 1) AND ACQUIRER_CODE = '{swift_code}' THEN GETDATE() ELSE ACQ_FLG_DATE END,
                EXCEP_FLAG = CASE WHEN EXCEP_FLAG IS NULL OR EXCEP_FLAG = 'N' OR EXCEP_FLAG != 'Y' THEN 
                    CASE WHEN {response_code} != 0 THEN 'Y' ELSE 'N' END ELSE EXCEP_FLAG END
            WHERE TRN_REF = '{trn_ref}'                
        """

        if existing_data.empty:
            # If not existing, add insert query to batch and update query to batch
            insert_query = f"""
                INSERT INTO Recon 
                    (DATE_TIME, TRAN_DATE, TRN_REF, BATCH, ACQUIRER_CODE, ISSUER_CODE)
                VALUES 
                    (GETDATE(),
                     '{date_time}',
                     '{trn_ref}', 
                     '{batch}', 
                     '{acquirer_code}',
                     '{issuer_code}')
            """
            insert_queries.append(insert_query)
            update_queries.append(update_query)
            insert_count += 1
        else:
            # If already existing, add update query to batch
            update_queries.append(update_query)
            update_count += 1

    # Execute insert and update queries in batches
    if insert_queries:
        batch_insert_query = "; ".join(insert_queries)
        execute_query(server, database, username, password, batch_insert_query, query_type="INSERT")

    if update_queries:
        batch_update_query = "; ".join(update_queries)
        execute_query(server, database, username, password, batch_update_query, query_type="UPDATE")

    feedback = f"Updated: {update_count}, Inserted: {insert_count}"
    logging.info(feedback)

    return feedback

def reconcileddata_req(server, database, username, password, bank_code):
    # Get the current date in the format 'YYYY-MM-DD'
    current_date = datetime.date.today().strftime('%Y-%m-%d')
    
    # Define the SQL query to select records where DATE_TIME is equal to the current date
    select_query = f"""
        SELECT *
        FROM ReconLog
        WHERE BANK_ID = '{bank_code}' AND CONVERT(DATE, DATE_TIME) = '{current_date}'
    """    
    # Execute the SQL query and retrieve the results
    reconciled_results = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return reconciled_results

