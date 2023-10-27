# Create your models here.
from django.utils import timezone

from django.db import models
from django.contrib.auth.models import User
from django.core.exceptions import ValidationError

# Create your models here.
class Bank(models.Model):
    name = models.CharField(max_length=50,unique=True)
    swift_code = models.CharField(max_length=10,unique=True)
    bank_code = models.CharField(max_length=10,null=True,unique=True)
    def __str__(self) -> str:
        return f"{self.name}"

class UserBankMapping(models.Model):
    user = models.OneToOneField(User,on_delete=models.CASCADE,primary_key=True)
    bank = models.ForeignKey(Bank,on_delete=models.CASCADE)
    def __str__(self) -> str:
        return f"{self.user.username}:{self.bank.name}"


class ReconLog(models.Model):
    date_time = models.DateTimeField(db_column='DATE_TIME', blank=True, null=True,default=timezone.now)  # Field name made lowercase.
    recon_id = models.CharField(db_column='RECON_ID', max_length=35, blank=True, null=True)  # Field name made lowercase.
    bank_id = models.CharField(db_column='BANK_ID', max_length=15, blank=True, null=True)  # Field name made lowercase.
    user_id = models.ForeignKey(User,db_column='USER_ID',on_delete=models.CASCADE) # Field name made lowercase.
    rq_date_range = models.CharField(db_column='RQ_DATE_RANGE', max_length=255, blank=True, null=True)  # Field name made lowercase.
    upld_rws = models.CharField(db_column='UPLD_RWS', max_length=15, blank=True, null=True)  # Field name made lowercase.
    rq_rws = models.CharField(db_column='RQ_RWS', max_length=15, blank=True, null=True)  # Field name made lowercase.
    recon_rws = models.CharField(db_column='RECON_RWS', max_length=15, blank=True, null=True)  # Field name made lowercase.
    unrecon_rws = models.CharField(db_column='UNRECON_RWS', max_length=15, blank=True, null=True)  # Field name made lowercase.
    excep_rws = models.CharField(db_column='EXCEP_RWS', max_length=15, blank=True, null=True)  # Field name made lowercase.
    feedback = models.TextField(db_column='FEEDBACK',blank=True, null=True)

    class Meta:
        db_table = 'ReconLog'
                

class Recon(models.Model):
    date_time = models.DateTimeField(db_column='DATE_TIME',blank=True, null=True,default=timezone.now)  # Field name made lowercase.
    tran_date = models.DateTimeField(db_column='TRAN_DATE',blank=True, null=True)  # Field name made lowercase.
    trn_ref = models.CharField(db_column='TRN_REF',max_length=255, blank=True, null=True,unique=True)  # Field name made lowercase.
    batch = models.CharField(db_column='BATCH',max_length=255, blank=True, null=True)  # Field name made lowercase.
    amount = models.DecimalField(db_column='AMOUNT', max_digits=18, decimal_places=2, blank=True, null=True)
    acquirer_code = models.CharField(db_column='ACQUIRER_CODE',max_length=255, blank=True, null=True)  # Field name made lowercase.
    issuer_code = models.CharField(db_column='ISSUER_CODE',max_length=255, blank=True, null=True)  # Field name made lowercase.
    excep_flag = models.CharField(db_column='EXCEP_FLAG',max_length=6, blank=True, null=True)  # Field name made lowercase.
    acq_flg = models.CharField(db_column='ACQ_FLG', max_length=6, blank=True, null=True)  # Field name made lowercase.
    iss_flg = models.CharField(db_column='ISS_FLG', max_length=6, blank=True, null=True)  # Field name made lowercase.
    acq_flg_date = models.DateTimeField(db_column='ACQ_FLG_DATE', blank=True, null=True)  # Field name made lowercase.
    iss_flg_date = models.DateTimeField(db_column='ISS_FLG_DATE', blank=True, null=True)  # Field name made lowercase.
    last_modified_by_user = models.ForeignKey(User,db_column='USER_ID',on_delete=models.CASCADE,blank=True,null=True)

    class Meta:
        db_table = 'Recon'
        

    def __str__(self) -> str:
        return self.trn_ref
    
class Transactions(models.Model):
    date_time = models.DateTimeField(db_column='DATE_TIME', blank=True, null=True)  # Field name made lowercase.
    trn_ref = models.CharField(db_column='TRN_REF', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    batch = models.CharField(db_column='BATCH', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    txn_type = models.CharField(db_column='TXN_TYPE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    txn_id = models.CharField(db_column='TXN_ID', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', primary_key=True)  # Field name made lowercase.
    issuer = models.CharField(db_column='ISSUER', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    acquirer = models.CharField(db_column='ACQUIRER', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    issuer_code = models.CharField(db_column='ISSUER_CODE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    acquirer_code = models.CharField(db_column='ACQUIRER_CODE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    branch_name = models.CharField(db_column='BRANCH_NAME', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    agentnames = models.CharField(db_column='AGENTNAMES', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    channel = models.CharField(db_column='CHANNEL', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    agent_code = models.CharField(db_column='AGENT_CODE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    agent_code_alias = models.CharField(db_column='AGENT_CODE_ALIAS', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    amount = models.DecimalField(db_column='AMOUNT', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    acc_no = models.CharField(db_column='ACC_NO', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    stan = models.CharField(db_column='STAN', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    fee = models.DecimalField(db_column='FEE', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    request_type = models.CharField(db_column='REQUEST_TYPE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.    
    trn_ref_0 = models.CharField(db_column='TRAN_REF_0', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    trn_ref_1 = models.CharField(db_column='TRAN_REF_1', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.    
    trn_status_0 = models.CharField(db_column='TRAN_STATUS_0', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    trn_status_1 = models.CharField(db_column='TRAN_STATUS_1', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    beneficiary_entity = models.CharField(db_column='BENEFICIARY_ENTITY', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    issuer_commission = models.DecimalField(db_column='ISSUER_COMMISSION', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    acquirer_commission = models.DecimalField(db_column='ACQUIRER_COMMISSION', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    agent_commission = models.DecimalField(db_column='AGENT_COMMISSION', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    abc_commission = models.DecimalField(db_column='ABC_COMMISSION', max_digits=18, decimal_places=2, blank=True, null=True)  # Field name made lowercase.
    response_code = models.CharField(db_column='RESPONSE_CODE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    transaction_status = models.CharField(db_column='TRANSACTION_STATUS', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    processing_code = models.CharField(db_column='PROCESSING_CODE', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    Tel_num = models.CharField(db_column='TEL_NUMBER', max_length=255, db_collation='SQL_Latin1_General_CP1_CI_AS', blank=True, null=True)  # Field name made lowercase.
    
    class Meta:
        managed = False
        db_table = 'Transactions'        
        
def validate_file_extension(value):
    if(value.file.content_type not in ['application/vnd.ms-excel','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']):
        raise ValidationError(u'Wrong File Type')
class UploadedFile(models.Model):
    file = models.FileField(upload_to="uploaded_files")
    time = models.DateTimeField(default=timezone.now)
    user = models.ForeignKey(User,on_delete=models.CASCADE,blank=True,null=True)
    def __str__(self) -> str:
        return self.file.name
    



