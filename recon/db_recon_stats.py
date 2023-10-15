from .db_connect import execute_query
from datetime import datetime
from .models import ReconLog

current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def insert_recon_stats(bankid,userid,reconciledRows, unreconciledRows, exceptionsRows, feedback, 
                       requestedRows, UploadedRows, date_range_str, server, database, username, password):
    # Define the SQL query for insertion
    insert_query = f"""
        INSERT INTO ReconLog
        (DATE_TIME,BANK_ID, USER_ID,RECON_RWS, UNRECON_RWS, EXCEP_RWS, FEEDBACK, RQ_RWS, UPLD_RWS, RQ_DATE_RANGE)
        VALUES
        ('{current_datetime}',{bankid},{userid},{reconciledRows}, {unreconciledRows}, {exceptionsRows}, '{feedback}', {requestedRows}, {UploadedRows}, '{date_range_str}')
    """

    # define the log objects
    log = ReconLog(
        date_time=current_datetime,
        bank_id = bankid,
        user_id = userid,
        recon_rws = reconciledRows,
        unrecon_rws = unreconciledRows,
        excep_rws = exceptionsRows,
        feedback=feedback,
        rq_rws = requestedRows,
        upld_rws = UploadedRows,
        rq_date_range = date_range_str

        )
    log.save()
    
    # Execute the SQL query
    execute_query(server, database, username, password, insert_query, query_type = "INSERT")

def recon_stats_req( bank_id):
    # Define the SQL query for selection using an f-string to insert swift_code
    select_query = f"""
        SELECT RQ_RWS, RQ_DATE_RANGE, UPLD_RWS, EXCEP_RWS, RECON_RWS, UNRECON_RWS, FEEDBACK 
        FROM ReconLog WHERE BANK_ID = '{bank_id}'
    """
    
    # Execute the SQL query and retrieve the results
    recon_results = ReconLog.objects.filter(bank_id=bank_id)
    
    return recon_results
