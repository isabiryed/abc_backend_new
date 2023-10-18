from rest_framework import serializers
from .models import Bank,Recon,ReconLog,UploadedFile,Transactions

class TransactionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Transactions
        fields = "__all__"

class ReconciliationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Recon
        fields = '__all__' 
        
class BankSerializer(serializers.ModelSerializer):
    class Meta:
        model = Bank

class LogSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReconLog
        fields = "__all__"

class UploadedFileSerializer(serializers.ModelSerializer):
    class Meta:
        model = UploadedFile
        fields = ["id","file"]
        
class ReconcileSerializer(serializers.Serializer):
    file = serializers.FileField()
    #swift_code = serializers.CharField(max_length=200)


class SabsSerializer(serializers.Serializer):
    file = serializers.FileField()
    batch_number = serializers.CharField(max_length=100)

class SettlementSerializer(serializers.Serializer):
    batch_number = serializers.CharField(max_length=100)