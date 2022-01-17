from simple_salesforce import Salesforce, SalesforceLogin
from pyspark.sql.functions import when
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import instr,upper,initcap
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import timedelta
import pandas as pd
import xlsxwriter
import numpy as np
import time
pd.options.mode.chained_assignment = None

Sel_Col_List = ['Id','Publisher_Name','CurrencyIsoCode','Account_Type__c','Account__c','Billing_Approval_Status__c','Credit_Limit__c','Discount_Percent__c','Payment_Type_c','Publisher_Account_Id__c','Publisher_Name__c','Publisher__c','Terms_of_Payment__c','VAT_Status__c','Category__c','External_UID_c','Market_Account_Name_Local__c','Market_Account_Status__c','PO_Number_Required_c','Sub_Category__c','Billing_Currencies1__c','Finance_ID__c','Created_Date','LastModifiedDate']

def SF_connectivity_Supdev():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.supdev',
                                           password='sfDc1234', security_token='kvtGqGhKGlm0PNa8rkFkyaPX',
                                           domain='test')
    sf = Salesforce(instance=instance, session_id=session_id)
    return sf
    
def SF_connectivity_MIG2():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.migration2',
                                           password='sfDc1234', security_token='I44Wl7gLwQGiIIEBoJmXhTnAc',
                                           domain='test')
    sf = Salesforce(instance=instance, session_id=session_id)
    return sf
    
def getSFData(queryStr, sf):
    df = pd.DataFrame(sf.query_all(queryStr)['records'])
    if df.empty:
        return df
    else:
        return df.drop(columns='attributes')
        
def get_input_data():
    sf=SF_connectivity_Supdev()
    df=getSFData(""" SELECT Id,RecordTypeId,Name,BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingCountryCode, BillingLatitude, BillingLongitude, Fax, Website, CurrencyIsoCode, CNI_Payment_Period__c,ParentId,CNI_Credit_Check__c,CNI_Account_Discount__c,CNI_PO_Number_required__c,Credit_Limit__c,External_ID__c
                     FROM Account where
                     RecordTypeId in(select Id from RecordType where Name in ('CHN Client', 'CHN Agency','CHN Brand')) """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Account_UKSUPDEV_df):
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.astype(str)
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.fillna('Null')
    Account_UKSUPDEV_Spark_df = spark.createDataFrame(Account_UKSUPDEV_df)
    return Account_UKSUPDEV_Spark_df
    
def LKP_TGT_PUBLISHER_ACCOUNT():
    Lkp_Tgt_Account_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Publisher_Account\Target\Tgt_Pub_Acc.csv")\
                                  .select('Id','Created_Date','LastModifiedDate','External_UID_c','Publisher_Name')
                                                       
    return Lkp_Tgt_Account_df
    
def LKP_SF_Target_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name,External_UID__c
                     FROM Account """,sf)
                    
    return df
    
def LKP_SF_Target_Publisher_c_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, Name from Publisher__c """,sf)
                    
    return df
    
def LKP_SF_RecordType_Supdev():
    sf=SF_connectivity_Supdev()
    df=getSFData(""" select Id, Name from RecordType """,sf)
                    
    return df
    
def LKP_FF_English_Translation():
    Lkp_English_Translation_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\ChineseAccountTranslations.csv")\
                                  .select('Salesforce ID','English Translation','Account Name')\
                                                       .withColumnRenamed('Salesforce ID','Salesforce_ID')\
                                                       .withColumnRenamed('English Translation','English_Translation')\
                                                       .withColumnRenamed('Account Name','Account_Name')
    return Lkp_English_Translation_df
    
def LKP_FF_Account_Dedupe():
    Lkp_Account_Dedupe_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                 .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\ChinaMarketToAtlasAccountDedup.csv")\
                                 .select('Market Account English Name','Atlas Account Name')\
                                             .withColumnRenamed('Market Account English Name','Market_Account_English_Name')\
                                             .withColumnRenamed('Atlas Account Name','Atlas_Account_Name')
                                             
    return Lkp_Account_Dedupe_df
    
def LKP_FF_Category():
    Lkp_Category_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                 .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\China_Accounts_CategorizationAccountsLookup.csv")\
                                 .select('Salesforce ID','Category__c','Sub_Category__c')\
                                             .withColumnRenamed('Salesforce ID','Salesforce_ID')
                                             
    return Lkp_Category_df
    
def Name_ETL(Account_UKSUPDEV_Spark_df):

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.withColumn('concat',F.lit('CN China: '))
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.select(Account_UKSUPDEV_Spark_df.Id,concat(Account_UKSUPDEV_Spark_df.concat,Account_UKSUPDEV_Spark_df.Name)\
                                 .alias('Name')).drop('concat')
    
    return Account_UKSUPDEV_Spark_df
    
def Account_Type_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_RecordType_Supdev_Spark_df = LKP_SF_RecordType_Supdev_Spark_df.alias('LKP_SF_RecordType_Supdev_Spark_df')

    RecordTypeNameLkp_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_RecordType_Supdev_Spark_df,col('Account_UKSUPDEV_Spark_df.RecordTypeId') == col('LKP_SF_RecordType_Supdev_Spark_df.Id'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id','Account_UKSUPDEV_Spark_df.RecordTypeId','LKP_SF_RecordType_Supdev_Spark_df.Name')
    
    Account_Type_df = RecordTypeNameLkp_df.withColumn('Account_Type__c',when(RecordTypeNameLkp_df.Name == 'CHN Client','Client')
                                                                   .when(RecordTypeNameLkp_df.Name == 'CHN Brand','Client')
                                                                   .when(RecordTypeNameLkp_df.Name == 'CHN Agency','Agency')
                                                                   .otherwise('null')).select('Id','Account_Type__c')
    
    return Account_Type_df
    
def Account__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Account_data_Spark_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')
    
    Account_IdById_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Account_data_Spark_df,col('Account_UKSUPDEV_Spark_df.Id') == col('LKP_SF_Target_Account_data_Spark_df.External_UID__c'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_Account_data_Spark_df.Id').alias('Account_IdById'))
    
    v_EnglishTranslatedAccName = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,Account_UKSUPDEV_Spark_df.Id == Lkp_English_Translation_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_English_Translation_df.English_Translation)
    
    v_AccName_DedupeLkp = v_EnglishTranslatedAccName.join(Lkp_Account_Dedupe_df,v_EnglishTranslatedAccName.English_Translation == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                         .select(v_EnglishTranslatedAccName.Id,v_EnglishTranslatedAccName.English_Translation,Lkp_Account_Dedupe_df.Atlas_Account_Name)
    
    Name_df = v_AccName_DedupeLkp.withColumn('Tgt_Name',when(v_AccName_DedupeLkp.Atlas_Account_Name.isNull(),v_AccName_DedupeLkp.English_Translation)
                                                .otherwise(v_AccName_DedupeLkp.Atlas_Account_Name)).select('Id','Tgt_Name')
    
    Name_df = Name_df.alias('Name_df')
    
    Account_IdByName_df = Name_df.join(LKP_SF_Target_Account_data_Spark_df,col('Name_df.Tgt_Name') == col('LKP_SF_Target_Account_data_Spark_df.Name'),"left")\
                       .select('Name_df.Id','Name_df.Tgt_Name',col('LKP_SF_Target_Account_data_Spark_df.Id').alias('Account_IdByName'))
    
    Account_IdById_df = Account_IdById_df.alias('Account_IdById_df')
    Account_IdByName_df = Account_IdByName_df.alias('Account_IdByName_df')
    
    Account_c_join_df = Account_IdById_df.join(Account_IdByName_df,col('Account_IdById_df.Id') == col('Account_IdByName_df.Id'),"inner")\
                       .select('Account_IdById_df.Id','Account_IdById_df.Account_IdById','Account_IdByName_df.Account_IdByName')
    
    Account_c_df = Account_c_join_df.withColumn('Account__c',when(Account_c_join_df.Account_IdById.isNull(),Account_c_join_df.Account_IdByName)
                                                .otherwise(Account_c_join_df.Account_IdById)).select('Id','Account__c')
    
    return Account_c_df
    
def Billing_Approval_Status_ETL(Account_UKSUPDEV_Spark_df):
    Billing_Approval_Status_df = Account_UKSUPDEV_Spark_df.withColumn('Billing_Approval_Status__c',when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == 'Pending','Pending')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == 'Approved','Good Credit')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == 'Stop','Block')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == '信用通过','Good Credit')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == 'Rejected','Block')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Credit_Check__c == 'Hold','On Hold')
                                                                   .otherwise('null')).select('Id','Billing_Approval_Status__c')
    
    return Billing_Approval_Status_df
    
def Publisher_Account_Id__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_data_Spark_df,Account_c_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.withColumn('Publisher_Name',F.lit('CN China'))

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_data_Spark_df = LKP_SF_Target_Publisher_data_Spark_df.alias('LKP_SF_Target_Publisher_data_Spark_df')
    
    Publisher_Account__c_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_data_Spark_df,col('Account_UKSUPDEV_Spark_df.Publisher_Name') == col('LKP_SF_Target_Publisher_data_Spark_df.Name'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_Publisher_data_Spark_df.Id').alias('Publisher_Account__c'))
    
    Publisher_Account__c_df = Publisher_Account__c_df.alias('Publisher_Account__c_df')
    Account_c_df = Account_c_df.alias('Account_c_df')

    Publisher_Account_Id_join_df = Publisher_Account__c_df.join(Account_c_df,col('Publisher_Account__c_df.Id') == col('Account_c_df.Id'),"inner")\
                       .select('Publisher_Account__c_df.Id','Publisher_Account__c_df.Publisher_Account__c','Account_c_df.Account__c')
    
    Publisher_Account_Id__c_df = Publisher_Account_Id_join_df.withColumn('Publisher_Account_Id__c',concat(Publisher_Account_Id_join_df.Account__c,Publisher_Account_Id_join_df.Publisher_Account__c))\
                                                         .select('Id','Publisher_Account_Id__c')
    
    return Publisher_Account_Id__c_df
    
def Publisher__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_data_Spark_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.withColumn('Publisher_Name',F.lit('CN China'))

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_data_Spark_df = LKP_SF_Target_Publisher_data_Spark_df.alias('LKP_SF_Target_Publisher_data_Spark_df')
    
    Publisher_c_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_data_Spark_df,col('Account_UKSUPDEV_Spark_df.Publisher_Name') == col('LKP_SF_Target_Publisher_data_Spark_df.Name'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_Publisher_data_Spark_df.Id').alias('Publisher__c'))
    
    return Publisher_c_df
    
def Terms_of_Payment__c_ETL(Account_UKSUPDEV_Spark_df):
    
    Terms_of_Payment__c_df = Account_UKSUPDEV_Spark_df.withColumn('Terms_of_Payment__c',when(Account_UKSUPDEV_Spark_df.CNI_Payment_Period__c == '30','Net_30')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Payment_Period__c == '60','Net_60')
                                                                   .when(Account_UKSUPDEV_Spark_df.CNI_Payment_Period__c == '90','Net_90')
                                                                   .otherwise('null')).select('Id','Terms_of_Payment__c')
    
    return Terms_of_Payment__c_df
    
def Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Category__c)
                         
    return Category_c_df
    
def Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Sub_Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Sub_Category__c)
                         
    return Sub_Category_c_df
    
def Market_Account_Name_Local__c_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Name_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    AcctName_EngTranslationId_df = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,col('Account_UKSUPDEV_Spark_df.Id') == col('Lkp_English_Translation_df.Salesforce_ID'),"left")\
                         .select('Account_UKSUPDEV_Spark_df.Id',col('Lkp_English_Translation_df.English_Translation').alias('AcctName_EngTranslationId'))
    
    English_translation_Name_df = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,col('Account_UKSUPDEV_Spark_df.Name') == col('Lkp_English_Translation_df.Account_Name'),"left")\
                         .select('Account_UKSUPDEV_Spark_df.Id',col('Lkp_English_Translation_df.English_Translation').alias('English_translation_Name'))
    
    AcctName_EngTranslationId_df = AcctName_EngTranslationId_df.alias('AcctName_EngTranslationId_df')
    English_translation_Name_df = English_translation_Name_df.alias('English_translation_Name_df')

    Acct_name_English_join_df = AcctName_EngTranslationId_df.join(English_translation_Name_df,col('AcctName_EngTranslationId_df.Id') == col('English_translation_Name_df.Id'),"inner")\
                         .select('AcctName_EngTranslationId_df.Id','AcctName_EngTranslationId_df.AcctName_EngTranslationId','English_translation_Name_df.English_translation_Name')
    
    Acct_name_English_df = Acct_name_English_join_df.withColumn('Acct_name_English',when(Acct_name_English_join_df.AcctName_EngTranslationId.isNotNull(),Acct_name_English_join_df.AcctName_EngTranslationId)
                                                .otherwise(Acct_name_English_join_df.English_translation_Name)).select('Id','Acct_name_English')
    
    Acct_name_English_df = Acct_name_English_df.withColumn('concat',F.lit('CN China: '))

    Acct_name_English_df = Acct_name_English_df.select(Acct_name_English_df.Id,concat(Acct_name_English_df.concat,Acct_name_English_df.Acct_name_English)\
                                 .alias('Acct_name_English')).drop('concat')
    
    Acct_name_English_df = Acct_name_English_df.alias('Acct_name_English_df')
    Name_df = Name_df.alias('Name_df')

    Market_Account_Name_Local__c_join_df = Acct_name_English_df.join(Name_df,col('Acct_name_English_df.Id') == col('Name_df.Id'),"inner")\
                         .select('Acct_name_English_df.Id','Acct_name_English_df.Acct_name_English','Name_df.Name')


    Market_Account_Name_Local__c_df = Market_Account_Name_Local__c_join_df.withColumn('Market_Account_Name_Local__c',when(Market_Account_Name_Local__c_join_df.Acct_name_English.isNull(),Market_Account_Name_Local__c_join_df.Name)
                                                .otherwise(Market_Account_Name_Local__c_join_df.Acct_name_English)).select('Id','Market_Account_Name_Local__c')
    
    return Market_Account_Name_Local__c_df
    
def creating_final_df(Account_UKSUPDEV_Spark_df,Name_df,Account_Type_df,Account_c_df,Billing_Approval_Status_df,Publisher_Account_Id__c_df,Publisher__c_df,Terms_of_Payment__c_df,Category_c_df,Sub_Category_c_df,Market_Account_Name_Local__c_df,Ins_Upd_Flg_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Name_df = Name_df.alias('Name_df')
    Account_Type_df = Account_Type_df.alias('Account_Type_df')
    Account_c_df = Account_c_df.alias('Account_c_df')
    Billing_Approval_Status_df = Billing_Approval_Status_df.alias('Billing_Approval_Status_df')
    Publisher_Account_Id__c_df = Publisher_Account_Id__c_df.alias('Publisher_Account_Id__c_df')
    Publisher__c_df = Publisher__c_df.alias('Publisher__c_df')
    Terms_of_Payment__c_df = Terms_of_Payment__c_df.alias('Terms_of_Payment__c_df')
    Category_c_df = Category_c_df.alias('Category_c_df')
    Sub_Category_c_df = Sub_Category_c_df.alias('Sub_Category_c_df')
    Market_Account_Name_Local__c_df = Market_Account_Name_Local__c_df.alias('Market_Account_Name_Local__c_df')
    Ins_Upd_Flg_df = Ins_Upd_Flg_df.alias('Ins_Upd_Flg_df')
    
    Final_Df = Account_UKSUPDEV_Spark_df.join(Name_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Name_df.Id'), 'left') \
                                        .join(Account_Type_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_Type_df.Id'), 'left') \
                                        .join(Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_c_df.Id'), 'left')\
                                        .join(Billing_Approval_Status_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Billing_Approval_Status_df.Id'), 'left')\
                                        .join(Publisher_Account_Id__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Publisher_Account_Id__c_df.Id'), 'left')\
                                        .join(Publisher__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Publisher__c_df.Id'), 'left')\
                                        .join(Terms_of_Payment__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Terms_of_Payment__c_df.Id'), 'left')\
                                        .join(Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Category_c_df.Id'), 'left')\
                                        .join(Sub_Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Sub_Category_c_df.Id'), 'left')\
                                        .join(Market_Account_Name_Local__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Market_Account_Name_Local__c_df.Id'), 'left')\
                                        .join(Ins_Upd_Flg_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Ins_Upd_Flg_df.Id'), 'left')\
                                        .select('Account_UKSUPDEV_Spark_df.*', col('Name_df.Name').alias('Publisher_Name'), 'Account_Type_df.Account_Type__c','Account_c_df.Account__c','Billing_Approval_Status_df.Billing_Approval_Status__c','Publisher_Account_Id__c_df.Publisher_Account_Id__c','Publisher__c_df.Publisher__c','Terms_of_Payment__c_df.Terms_of_Payment__c','Category_c_df.Category__c','Sub_Category_c_df.Sub_Category__c','Market_Account_Name_Local__c_df.Market_Account_Name_Local__c','Ins_Upd_Flg_df.Ins_Upd_Flg')

    return Final_Df
    
def alter_final_df(Final_df):
    
    alter_final_df = Final_df.withColumn('Payment_Type_c',lit('Account'))\
                             .withColumn('Publisher_Name__c',lit('CN China'))\
                             .withColumn('VAT_Status__c',lit('Normal'))\
                             .withColumn('Market_Account_Status__c',lit('Approved'))\
                             .withColumn('Billing_Currencies1__c',lit('USD;CNY'))\
                             .withColumnRenamed('CNI_Account_Discount__c','Discount_Percent__c')\
                             .withColumnRenamed('CNI_Payment_Period__c','Terms_of_Payment_c')\
                             .withColumnRenamed('Id','External_UID_c')\
                             .withColumnRenamed('CNI_PO_Number_required__c','PO_Number_Required_c')\
                             .withColumnRenamed('External_ID__c','Finance_ID__c')\
                             .drop('RecordTypeId','Name','BillingStreet','BillingCity','BillingState','BillingPostalCode','BillingCountry','BillingCountryCode','BillingLatitude','BillingLongitude','Fax','Website','ParentId')
                             
    return alter_final_df
    
def Ins_Upd_flg(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Publisher_Account_df,Account_c_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_Tgt_Publisher_Account_df = Lkp_Tgt_Publisher_Account_df.alias('Lkp_Tgt_Publisher_Account_df')

    Publisher_Account_ID_df = Account_UKSUPDEV_Spark_df.join(Lkp_Tgt_Publisher_Account_df,col('Account_UKSUPDEV_Spark_df.Id') == col('Lkp_Tgt_Publisher_Account_df.External_UID_c'),"left")\
                         .select('Account_UKSUPDEV_Spark_df.Id',col('Lkp_Tgt_Publisher_Account_df.Id').alias('Pub_Acconunt_ID'))

    Account_c_df = Account_c_df.alias('Account_c_df')
    Publisher_Account_ID_df = Publisher_Account_ID_df.alias('Publisher_Account_ID_df')


    Pubsliher_Account_Join_df = Account_c_df.join(Publisher_Account_ID_df,col('Account_c_df.Id') == col('Publisher_Account_ID_df.Id'),"inner")\
                         .select('Account_c_df.Id','Account_c_df.Account__c','Publisher_Account_ID_df.Pub_Acconunt_ID')

    Ins_Upd_Flg_df = Pubsliher_Account_Join_df.withColumn('Ins_Upd_Flg',when((Pubsliher_Account_Join_df.Account__c.isNotNull()) & (Pubsliher_Account_Join_df.Pub_Acconunt_ID.isNull()), "I")
                                                     .when(Pubsliher_Account_Join_df.Account__c.isNull(),'D')
                                                     .otherwise("U"))

    return Ins_Upd_Flg_df
    
def Handling_Insert_records(Ins_records_df,Lkp_Tgt_Publisher_Account_df):
    if Lkp_Tgt_Publisher_Account_df.count() == 0:
        Ins_records_df_index = Ins_records_df.withColumn("idx",F.monotonically_increasing_id())
        windowSpec = W.orderBy("idx")
        Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
        Ins_records_df_index = Ins_records_df_index.drop("idx",'Ins_Upd_Flg')
        Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
        Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    else:
        Max_Id = Lkp_Tgt_Publisher_Account_df.groupBy().max('Id').collect()
        results={}
        for i in Max_Id:
            results.update(i.asDict())
        Max_Id_var = results['max(Id)']
        Ins_records_df_index = Ins_records_df.withColumn("idx",F.monotonically_increasing_id())
        windowSpec = W.orderBy("idx")
        Ins_records_df_index = Ins_records_df_index.withColumn("Id",  Max_Id_var +  F.row_number().over(windowSpec))
        Ins_records_df_index = Ins_records_df_index.drop("idx",'Ins_Upd_Flg')
        Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
        Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    
    return Ins_records_df_index
    
def Handling_Update_records(Upd_records_df,Lkp_Tgt_Publisher_Account_df):
    Lkp_Tgt_Publisher_Account_df = Lkp_Tgt_Publisher_Account_df.alias('Lkp_Tgt_Publisher_Account_df')
    Upd_records_df = Upd_records_df.alias('Upd_records_df')
    Upd_records_df_index = Upd_records_df.join(Lkp_Tgt_Publisher_Account_df,col('Upd_records_df.External_UID_c') == col('Lkp_Tgt_Publisher_Account_df.External_UID_c'),"left")\
                       .select('Lkp_Tgt_Publisher_Account_df.Id','Lkp_Tgt_Publisher_Account_df.Created_Date','Upd_records_df.*').drop('Ins_Upd_Flg')\
                       .withColumn('LastModifiedDate',current_timestamp())
                       
    return Upd_records_df_index
    
def drop_duplicates_target(drop_dup_df):
    drop_dup_df = drop_dup_df.dropDuplicates(['Publisher_Name'])
    return drop_dup_df
    
def Ins_Upd_Split(alter_final_df,Lkp_Tgt_Publisher_Account_df):

    Ins_records_df = alter_final_df.filter(col('Ins_Upd_Flg') == 'I')
    Upd_records_df = alter_final_df.filter(col('Ins_Upd_Flg') == 'U')
    Del_records_df = alter_final_df.filter(col('Ins_Upd_Flg') == 'D')
    
    global Ins_count
    global upd_count
    global delete_count
    
    Ins_count = Ins_records_df.count()
    upd_count = Upd_records_df.count()
    delete_count = Del_records_df.count()
    
    if Ins_count > 0:
        Final_Ins_records_df = Handling_Insert_records(Ins_records_df,Lkp_Tgt_Publisher_Account_df)
        Final_Ins_records_df = Final_Ins_records_df.select(*Sel_Col_List)
        Final_Ins_records_Distinct_df = drop_duplicates_target(Final_Ins_records_df)
        Ins_count = Final_Ins_records_Distinct_df.count()
    else:
        pass
    
    if upd_count > 0:
        Final_Upd_records_df = Handling_Update_records(Upd_records_df,Lkp_Tgt_Publisher_Account_df)
        Final_Upd_records_df = Final_Upd_records_df.select(*Sel_Col_List)
        Final_Upd_records_Distinct_df = drop_duplicates_target(Final_Upd_records_df)
        upd_count = Final_Upd_records_Distinct_df.count()
    else:
        pass
        
    if ((Ins_count == 0) & (upd_count != 0)):
        df = Final_Upd_records_Distinct_df
    elif ((upd_count == 0) & (Ins_count != 0)):
        df = Final_Ins_records_Distinct_df
    elif ((Ins_count != 0) & (upd_count != 0)): 
        dfs = [Final_Ins_records_Distinct_df,Final_Upd_records_Distinct_df]
        df = reduce(DataFrame.union, dfs)
        df = df.sort(col("Id").asc())
    else:
        df = spark.createDataFrame([], StructType([]))
    
    return df
    
def Writing_to_Target(Ins_Upd_Split_df):
    df = Ins_Upd_Split_df.toPandas()
    df.to_csv(r'C:\Users\skumar6\Documents\Informatica_Pyspark\Publisher_Account\Target\Tgt_Pub_Acc.csv',index=False)
    
    
if __name__ == "__main__":
    start = time.time()
    Account_UKSUPDEV_df = get_input_data()
    Account_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Account_UKSUPDEV_df)  
    LKP_SF_RecordType_Supdev_df = LKP_SF_RecordType_Supdev()
    LKP_SF_RecordType_Supdev_Spark_df = pandas_to_pyspark_df(LKP_SF_RecordType_Supdev_df)
    LKP_SF_Target_Account_data_df = LKP_SF_Target_Account_data()
    LKP_SF_Target_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Account_data_df)
    LKP_SF_Target_Publisher_data_df = LKP_SF_Target_Publisher_c_data()
    LKP_SF_Target_Publisher_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_data_df)
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    Lkp_Category_df = LKP_FF_Category()
    Lkp_Tgt_Publisher_Account_df = LKP_TGT_PUBLISHER_ACCOUNT()
    Name_df = Name_ETL(Account_UKSUPDEV_Spark_df)
    Account_Type_df = Account_Type_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df)
    Account_c_df = Account__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Account_data_Spark_df)
    Billing_Approval_Status_df = Billing_Approval_Status_ETL(Account_UKSUPDEV_Spark_df)
    Publisher_Account_Id__c_df = Publisher_Account_Id__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_data_Spark_df,Account_c_df)
    Publisher__c_df = Publisher__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_data_Spark_df)
    Terms_of_Payment__c_df = Terms_of_Payment__c_ETL(Account_UKSUPDEV_Spark_df)
    Category_c_df = Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Sub_Category_c_df = Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Market_Account_Name_Local__c_df = Market_Account_Name_Local__c_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Name_df)
    Ins_Upd_Flg_df = Ins_Upd_flg(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Publisher_Account_df,Account_c_df)
    Final_df = creating_final_df(Account_UKSUPDEV_Spark_df,Name_df,Account_Type_df,Account_c_df,Billing_Approval_Status_df,Publisher_Account_Id__c_df,Publisher__c_df,Terms_of_Payment__c_df,Category_c_df,Sub_Category_c_df,Market_Account_Name_Local__c_df,Ins_Upd_Flg_df)
    alter_final_df = alter_final_df(Final_df)
    Ins_Upd_Split_df = Ins_Upd_Split(alter_final_df,Lkp_Tgt_Publisher_Account_df)
    Final_Distinct_df = Ins_Upd_Split_df.sort(col("Id").asc())
    Ins_Upd_Split_count = Final_Distinct_df.count()
    if Ins_Upd_Split_count == 0:
        pass
    else:
        Final_Target_Account_Df = Writing_to_Target(Final_Distinct_df)
        
    src_count = Account_UKSUPDEV_Spark_df.count()
        
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Ins_count)
    print("Total Records Updated: ",upd_count)
    print("Total Records not loaded due to Null Name: ",delete_count)
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))