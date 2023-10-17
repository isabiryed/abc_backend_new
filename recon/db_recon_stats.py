
import os
# from .models import ReconLog
from .db_connect import execute_query
import datetime as dt


current_date = dt.date.today().strftime('%Y-%m-%d')

def insert_recon_stats(bankid, reconciledRows, unreconciledRows, exceptionsRows, feedback, 
                        requestedRows, UploadedRows, date_range_str, server, database, username, password):
    
    # Define the SQL query for insertion
    insert_query = f"""
        INSERT INTO ReconLog
        ([DATE_TIME], [BANK_ID], [RQ_DATE_RANGE], [UPLD_RWS], [RQ_RWS], [RECON_RWS], [UNRECON_RWS], [EXCEP_RWS], [FEEDBACK])
        VALUES
        ('{current_date}', {bankid}, '{date_range_str}', {UploadedRows}, {requestedRows}, {reconciledRows}, {unreconciledRows}, {exceptionsRows}, '{feedback}')
    """

    # Execute the SQL query
    execute_query(server, database, username, password, insert_query, query_type="INSERT")

def recon_stats_req(server, database, username, password, bank_id):
    # Define the SQL query for selection using an f-string to insert swift_code
    select_query = f"""
        SELECT RQ_RWS, RQ_DATE_RANGE, UPLD_RWS, EXCEP_RWS, RECON_RWS, UNRECON_RWS, FEEDBACK 
        FROM ReconLog WHERE BANK_ID = '{bank_id}'
    """    
    # Execute the SQL query and retrieve the results
    recon_results = execute_query(server, database, username, password, select_query, query_type="SELECT")
    
    return recon_results

# import datetime as dt
# from .models import ReconLog

# current_date = dt.date.today()

# def insert_recon_stats(bank_id, recon_rws, unrecon_rws, excep_rws, feedback, rq_rws, upld_rws, rq_date_range):
#     recon_log = ReconLog(using='your_database_alias')
#     recon_log.bank_id = bank_id
#     recon_log.rq_date_range = rq_date_range
#     recon_log.upld_rws = upld_rws
#     recon_log.rq_rws = rq_rws
#     recon_log.recon_rws = recon_rws
#     recon_log.unrecon_rws = unrecon_rws
#     recon_log.excep_rws = excep_rws
#     recon_log.feedback = feedback
#     recon_log.save()

  
# def recon_stats_req(bank_id):
#     recon_results = ReconLog.objects.using('your_database_alias').filter(bank_id=bank_id).values(
#         'rq_rws', 'rq_date_range', 'upld_rws', 'excep_rws', 'recon_rws', 'unrecon_rws', 'feedback'
#     )
#     return recon_results
