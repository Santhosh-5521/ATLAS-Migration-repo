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
import numpy as np
import time
pd.options.mode.chained_assignment = None

Billing_Sel_list = ['Id','CurrencyIsoCode','RecordTypeID_Billing','Address_Line_1__c','Tgt_Billing_country','Address_Line_6__c','Address_Line_9__c','Tgt_Billing_city','Tgt_BillingPostalCode','Tgt_BillingState']
Shipping_Sel_list = ['Id','CurrencyIsoCode','RecordTypeID_Shipping','Address_Line_1__c','Tgt_Shipping_country','Address_Line_6__c','Address_Line_9__c','Tgt_Shipping_city','Tgt_ShippingPostalCode','Tgt_ShippingState']
Sorted_Sel_list = ['Src_Id','Name','CurrencyIsoCode','RecordTypeId','Address_Line_1__c','Address_Line_5__c','Address_Line_6__c','Address_Line_8__c','Address_Line_9__c','City__c','Postcode__c','Region__c','Account_Src_Id']
Sel_Col_List = ['Id','Name','CurrencyIsoCode','RecordTypeId','Address_Line_1__c','Address_Line_5__c','Address_Line_6__c','Address_Line_8__c','Address_Line_9__c','City__c','Postcode__c','Region__c']

def SF_connectivity_Supdev():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.supdev',
                                           password='sfDc1234', security_token='kvtGqGhKGlm0PNa8rkFkyaPX',
                                           domain='test')
    sf = Salesforce(instance=instance, session_id=session_id)
    return sf
    
def SF_connectivity_MIG2():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.migration2',
                                           password='sfDc1234', security_token='UwitV4zYwGn43HtDfvcpogHh4',
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
    df=getSFData(""" SELECT Id,Name,BillingStreet,ShippingStreet,External_ID__c,CurrencyIsoCode,BillingCountry,BillingCountryCode,BillingCity,BillingPostalCode,BillingState,
                            ShippingCountry,ShippingCity,ShippingPostalCode,ShippingState
                     FROM Account where
                     RecordTypeId in (select Id from RecordType where SobjectType='Account') and RecordTypeId in (select Id from RecordType where Name like 'CHN%')
                     and Id in ('001b000000lGMcDAAW','001b000000T6DzdAAF') """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Account_UKSUPDEV_df):
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.astype(str)
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.fillna('Null')
    Account_UKSUPDEV_Spark_df = spark.createDataFrame(Account_UKSUPDEV_df)
    return Account_UKSUPDEV_Spark_df
    
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
    
def LKP_SF_Target_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name,External_UID__c
                     FROM Account """,sf)
                    
    return df
    
def LKP_SF_Target_Publisher_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Account__c,External_UID__c,Publisher_Name__c
                     FROM Publisher_Account__c """,sf)
                    
    return df
    
def LKP_SF_Target_RecordType_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name,DeveloperName,SobjectType
                     FROM RecordType """,sf)
                    
    return df
    
######################### ETL FOR BILLING #################################################
    
def Billing_Acc_PubAcc_Name_ETL(Account_UKSUPDEV_Spark_df):
    Billing_Acc_PubAcc_Name_df = Account_UKSUPDEV_Spark_df.withColumn('Name_Acc_Billing',\
                                                                    concat(ltrim(rtrim(initcap('Name'))),lit("-Billing-"),lit("Account")))\
                                                         .withColumn('Name_PubAcc_Billing',\
                                                                    concat(ltrim(rtrim(initcap('Name'))),lit("-Billing-"),lit("CN China-Market Account")))\
                                                         .withColumn('Name_Array',array('Name_Acc_Billing','Name_PubAcc_Billing'))\
                                                         .select('Id','Name','Name_Acc_Billing','Name_PubAcc_Billing','Name_Array')
    return Billing_Acc_PubAcc_Name_df
    
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
    
def Publisher_Account_c_ETL(Account_UKSUPDEV_Spark_df,Account_c_df,LKP_SF_Target_Publisher_Account_data_Spark_df,):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.withColumn('Publisher_Name',F.lit('CN China'))
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Account_c_df = Account_c_df.alias('Account_c_df')
    
    Publisher_Account_Id_df = Account_UKSUPDEV_Spark_df.join(Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_c_df.Id'), 'left')\
                                                       .select('Account_UKSUPDEV_Spark_df.Id','Account_UKSUPDEV_Spark_df.Publisher_Name','Account_c_df.Account__c')
    
    Publisher_Account_Id_df = Publisher_Account_Id_df.alias('Publisher_Account_Id_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')
    
    cond = [Publisher_Account_Id_df.Publisher_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Publisher_Name__c, Publisher_Account_Id_df.Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c]
    
    Publisher_Account_c_df = Publisher_Account_Id_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df, cond, 'left')\
                                                       .select('Publisher_Account_Id_df.Id',col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Publisher_Account__c'))
    
    return Publisher_Account_c_df
    
def RecordTypeID_Billing_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Billingcn_ed',when(col('BillingCountry') == "None", "")\
                                                    .otherwise(col('BillingCountry')))\
                                                    .select('Id','BillingCountry','Billingcn_ed')

    Account_UKSUPDEV_Spark_regmatch_df = regmatch_df.withColumn('Billing_Country_Regex',col("Billingcn_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Billing_country',when(col('Billing_Country_Regex') == "false", "China")\
                                                                                                .otherwise(initcap(col('BillingCountry'))))\
                         .select('Id','BillingCountry','Billingcn_ed','Billing_Country_Regex','Tgt_Billing_country')
    
    Account_UKSUPDEV_Spark_regmatch_df = Account_UKSUPDEV_Spark_regmatch_df.withColumn('BillingCountry_v',lower(col('Tgt_Billing_country')))

    BillingCountry_Decode_v_df = Account_UKSUPDEV_Spark_regmatch_df.withColumn('BillingCountry_Decode_v',when(col('BillingCountry_v') == "united states", "United States of America")\
                                                                       .when(col('BillingCountry_v') == "viet nam", "Vietnam")\
                                                                       .when(col('BillingCountry_v') == "czech republic", "Czechia")\
                                                                       .when(col('BillingCountry_v') == "tanzania, united republic of", "Tanzania")\
                                                                       .when(col('BillingCountry_v') == "venezuela, bolivarian republic of", "Venezuela")\
                                                                       .when(col('BillingCountry_v') == "virgin islands, british", "British Virgin Islands")\
                                                                       .when(col('BillingCountry_v') == "swaziland", "Eswatini")\
                                                                       .otherwise(col('Tgt_Billing_country')))\
                                  .select('Id','BillingCountry_Decode_v')
    
    BillingCountry_Decode_v_df = BillingCountry_Decode_v_df.withColumn('v_SobjectType',F.lit('AMS_Address__c'))
    
    BillingCountry_Decode_v_df = BillingCountry_Decode_v_df.alias('BillingCountry_Decode_v_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    cond = [BillingCountry_Decode_v_df.v_SobjectType == LKP_SF_Target_RecordType_Spark_df.SobjectType, BillingCountry_Decode_v_df.BillingCountry_Decode_v == LKP_SF_Target_RecordType_Spark_df.Name]
    
    RecordTypeId_df = BillingCountry_Decode_v_df.join(LKP_SF_Target_RecordType_Spark_df, cond, 'left')\
                                                       .select('BillingCountry_Decode_v_df.Id',col('LKP_SF_Target_RecordType_Spark_df.Id').alias('RecordTypeID_Billing'))
    
    return RecordTypeId_df
    
def Biling_Address_Line_1__c_ETL(Account_UKSUPDEV_Spark_df):
    Address_Line_1__c_Biling_df = Account_UKSUPDEV_Spark_df.withColumn('Address_Line_1__c',regexp_replace(Account_UKSUPDEV_Spark_df.BillingStreet, " ", ""))\
                                                       .select('Id','Address_Line_1__c')
    
    return Address_Line_1__c_Biling_df
    
def Biling_Address_Line_5__c_ETL(Account_UKSUPDEV_Spark_df):

    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Billingcn_ed',when(col('BillingCountry') == "None", "")\
                                           .otherwise(col('BillingCountry')))\
                                           .select('Id','BillingCountry','Billingcn_ed')

    Address_Line_5__c_Biling_df = regmatch_df.withColumn('Billing_Country_Regex',col("Billingcn_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Billing_country',when(col('Billing_Country_Regex') == "false", "China")\
                                                                                                .otherwise(initcap(col('BillingCountry'))))\
                                                    .select('Id','BillingCountry','Billingcn_ed','Billing_Country_Regex','Tgt_Billing_country')
    
    return Address_Line_5__c_Biling_df
    
def Address_Line_6__c_ETL(Account_UKSUPDEV_Spark_df):
    Address_Line_6__c_df = Account_UKSUPDEV_Spark_df.withColumn('Address_Line_6__c',F.lit('CN China'))\
                                                       .select('Id','Address_Line_6__c')
    
    return Address_Line_6__c_df
    
def Address_Line_8__c_ETL(Account_c_df,Publisher_Account_c_df):
    Account_c_df = Account_c_df.alias('Account_c_df')
    Publisher_Account_c_df = Publisher_Account_c_df.alias('Publisher_Account_c_df')
    
    Address_Line_8__c_df = Account_c_df.join(Publisher_Account_c_df,col('Account_c_df.Id') == col('Publisher_Account_c_df.Id'),"inner")\
                       .select('Account_c_df.Id',col('Account_c_df.Account__c'),col('Publisher_Account_c_df.Publisher_Account__c'))

    Address_Line_8__c_Array_df = Address_Line_8__c_df.withColumn('Address_Line_8__c_Array',array('Account__c','Publisher_Account__c'))
    
    return Address_Line_8__c_Array_df
    
def Biling_Address_Line_9__c_ETL(Account_UKSUPDEV_Spark_df):
    Address_Line_9__c_Biling_df = Account_UKSUPDEV_Spark_df.withColumn('Address_Line_9__c',F.lit('Billing'))\
                                                       .select('Id','Address_Line_9__c')
    
    return Address_Line_9__c_Biling_df
    
def Billing_City__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Billingcity_ed',when(col('BillingCity') == "None", "")\
                                           .otherwise(col('BillingCity')))\
                                           .select('Id','BillingCity','Billingcity_ed')
    
    Billing_City_out_df = regmatch_df.withColumn('Billing_City_Regex',col("Billingcity_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Billing_city',when(col('Billing_City_Regex') == "false", "NULL")\
                                                                                          .otherwise(initcap(col('BillingCity'))))\
                                                    .select('Id','BillingCity','Billingcity_ed','Billing_City_Regex','Tgt_Billing_city')
    
    return Billing_City_out_df
    
def Billing_Postcode__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('BillingPostalCode_ed',when(col('BillingPostalCode') == "None", "")\
                                           .otherwise(col('BillingPostalCode')))\
                                           .select('Id','BillingPostalCode','BillingPostalCode_ed')
    
    BillingPostalCode_out_df = regmatch_df.withColumn('BillingPostalCode_Regex',col("BillingPostalCode_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_BillingPostalCode',when(col('BillingPostalCode_Regex') == "false", "NULL")\
                                                                                          .otherwise(col('BillingPostalCode')))\
                                                    .select('Id','BillingPostalCode','BillingPostalCode_ed','BillingPostalCode_Regex','Tgt_BillingPostalCode')
    
    return BillingPostalCode_out_df
    
def Billing_Region__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('BillingState_ed',when(col('BillingState') == "None", "")\
                                           .otherwise(col('BillingState')))\
                                           .select('Id','BillingState','BillingState_ed')
    
    BillingState_out_df = regmatch_df.withColumn('BillingState_Regex',col("BillingState_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_BillingState',when(col('BillingState_Regex') == "false", "NULL")\
                                                                                          .otherwise(col('BillingState')))\
                                                    .select('Id','BillingState','BillingState_ed','BillingState_Regex','Tgt_BillingState')
    
    return BillingState_out_df
    
def Account_Src_Id_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')
    
    Account_IdById_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Account_data_Spark_df,col('Account_UKSUPDEV_Spark_df.Id') == col('LKP_SF_Target_Account_data_Spark_df.External_UID__c'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_Account_data_Spark_df.Id').alias('Account_IdById'))

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    Publisher_Account_IdById_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,col('Account_UKSUPDEV_Spark_df.Id') == col('LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c'),"left")\
                       .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('PubAccount_IdById'))
    
    Account_IdById_df = Account_IdById_df.alias('Account_IdById_df')
    Publisher_Account_IdById_df = Publisher_Account_IdById_df.alias('Publisher_Account_IdById_df')

    Account_Src_Id_df = Account_IdById_df.join(Publisher_Account_IdById_df,col('Account_IdById_df.Id') == col('Publisher_Account_IdById_df.Id'),"inner")\
                       .select('Account_IdById_df.Id','Account_IdById_df.Account_IdById','Publisher_Account_IdById_df.PubAccount_IdById')
    
    Account_Src_Id_Array_df = Account_Src_Id_df.withColumn('Account_Src_Id_Array',array('Account_IdById','PubAccount_IdById'))
    
    return Account_Src_Id_Array_df
    
def creating_final_Billing_df(Account_UKSUPDEV_Spark_df,Billing_Acc_PubAcc_Name_df,Account_c_df,Publisher_Account_c_df,Account_Src_Id_Array_df,RecordTypeId_Billing_df,Address_Line_1__c_Biling_df,Address_Line_5__c_Biling_df,Address_Line_6__c_df,Address_Line_8__c_Array_df,Address_Line_9__c_Biling_df,Billing_City_out_df,BillingPostalCode_out_df,BillingState_out_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Billing_Acc_PubAcc_Name_df = Billing_Acc_PubAcc_Name_df.alias('Billing_Acc_PubAcc_Name_df')
    Account_c_df = Account_c_df.alias('Account_c_df')
    Publisher_Account_c_df = Publisher_Account_c_df.alias('Publisher_Account_c_df')
    Account_Src_Id_Array_df = Account_Src_Id_Array_df.alias('Account_Src_Id_Array_df')
    RecordTypeId_Billing_df = RecordTypeId_Billing_df.alias('RecordTypeId_Billing_df')
    Address_Line_1__c_Biling_df = Address_Line_1__c_Biling_df.alias('Address_Line_1__c_Biling_df')
    Address_Line_5__c_Biling_df = Address_Line_5__c_Biling_df.alias('Address_Line_5__c_Biling_df')
    Address_Line_6__c_df = Address_Line_6__c_df.alias('Address_Line_6__c_df')
    Address_Line_8__c_Array_df = Address_Line_8__c_Array_df.alias('Address_Line_8__c_Array_df')
    Address_Line_9__c_Biling_df = Address_Line_9__c_Biling_df.alias('Address_Line_9__c_Biling_df')
    Billing_City_out_df = Billing_City_out_df.alias('Billing_City_out_df')
    BillingPostalCode_out_df = BillingPostalCode_out_df.alias('BillingPostalCode_out_df')
    BillingState_out_df = BillingState_out_df.alias('BillingState_out_df')
    
    Billing_Final_Df = Account_UKSUPDEV_Spark_df.join(Billing_Acc_PubAcc_Name_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Billing_Acc_PubAcc_Name_df.Id'), 'left') \
                                                .join(Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_c_df.Id'), 'left')\
                                                .join(Publisher_Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Publisher_Account_c_df.Id'), 'left')\
                                                .join(Account_Src_Id_Array_df,col('Account_UKSUPDEV_Spark_df.Id') == col('Account_Src_Id_Array_df.Id'),'left')\
                                                .join(RecordTypeId_Billing_df, col('Account_UKSUPDEV_Spark_df.Id') == col('RecordTypeId_Billing_df.Id'), 'left')\
                                                .join(Address_Line_1__c_Biling_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_1__c_Biling_df.Id'), 'left')\
                                                .join(Address_Line_5__c_Biling_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_5__c_Biling_df.Id'), 'left')\
                                                .join(Address_Line_6__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_6__c_df.Id'), 'left')\
                                                .join(Address_Line_8__c_Array_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_8__c_Array_df.Id'), 'left')\
                                                .join(Address_Line_9__c_Biling_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_9__c_Biling_df.Id'), 'left')\
                                                .join(Billing_City_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Billing_City_out_df.Id'), 'left')\
                                                .join(BillingPostalCode_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('BillingPostalCode_out_df.Id'), 'left')\
                                                .join(BillingState_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('BillingState_out_df.Id'), 'left')\
                                                .select('Account_UKSUPDEV_Spark_df.*', col('Billing_Acc_PubAcc_Name_df.Name_Array'),'Account_Src_Id_Array_df.Account_Src_Id_Array','RecordTypeId_Billing_df.RecordTypeID_Billing','Address_Line_1__c_Biling_df.Address_Line_1__c','Address_Line_5__c_Biling_df.Tgt_Billing_country','Address_Line_6__c_df.Address_Line_6__c','Address_Line_8__c_Array_df.Address_Line_8__c_Array','Address_Line_9__c_Biling_df.Address_Line_9__c','Billing_City_out_df.Tgt_Billing_city','BillingPostalCode_out_df.Tgt_BillingPostalCode','BillingState_out_df.Tgt_BillingState')

    return Billing_Final_Df
    
def alter_final_Billing_ETL_df(Final_Billing_df):
    alter_final_Billing_df = Final_Billing_df.withColumn("new", F.arrays_zip("Name_Array", "Address_Line_8__c_Array", "Account_Src_Id_Array"))\
                                             .withColumn("new", F.explode("new"))\
                                             .select(*Billing_Sel_list,F.col("new.Name_Array").alias("Name"),F.col("new.Address_Line_8__c_Array").alias("Address_Line_8__c"),F.col("new.Account_Src_Id_Array").alias("Account_Src_Id"))
    
    alter_final_Billing_df = alter_final_Billing_df.withColumnRenamed('RecordTypeID_Billing','RecordTypeId')\
                                                   .withColumnRenamed('Id','Src_Id')\
                                                   .withColumnRenamed('Tgt_Billing_country','Address_Line_5__c')\
                                                   .withColumnRenamed('Tgt_Billing_city','City__c')\
                                                   .withColumnRenamed('Tgt_BillingPostalCode','Postcode__c')\
                                                   .withColumnRenamed('Tgt_BillingState','Region__c')\
                                                   .select(*Sorted_Sel_list)

    alter_final_Billing_df = alter_final_Billing_df.sort(col("Src_Id").asc())
    
                             
    return alter_final_Billing_df
    
######################### ETL FOR SHIPPING #################################################
    
def Shipping_Acc_PubAcc_Name_ETL(Account_UKSUPDEV_Spark_df):
    Shipping_Acc_PubAcc_Name_df = Account_UKSUPDEV_Spark_df.withColumn('Name_Acc_Shipping',\
                                                                    concat(ltrim(rtrim(initcap('Name'))),lit("-Shipping-"),lit("Account")))\
                                                         .withColumn('Name_PubAcc_Shipping',\
                                                                    concat(ltrim(rtrim(initcap('Name'))),lit("-Shipping-"),lit("CN China-Market Account")))\
                                                         .withColumn('Name_Array',array('Name_Acc_Shipping','Name_PubAcc_Shipping'))\
                                                         .select('Id','Name','Name_Acc_Shipping','Name_PubAcc_Shipping','Name_Array')
    return Shipping_Acc_PubAcc_Name_df
    
def RecordTypeID_Shipping_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Shippingcn_ed',when(col('ShippingCountry') == "None", "")\
                                                    .otherwise(col('ShippingCountry')))\
                                                    .select('Id','ShippingCountry','Shippingcn_ed')

    Account_UKSUPDEV_Spark_regmatch_df = regmatch_df.withColumn('Shipping_Country_Regex',col("Shippingcn_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Shipping_country',when(col('Shipping_Country_Regex') == "false", "China")\
                                                                                                .otherwise(initcap(col('ShippingCountry'))))\
                         .select('Id','ShippingCountry','Shippingcn_ed','Shipping_Country_Regex','Tgt_Shipping_country')
    
    Account_UKSUPDEV_Spark_regmatch_df = Account_UKSUPDEV_Spark_regmatch_df.withColumn('ShippingCountry_v',lower(col('Tgt_Shipping_country')))

    ShippingCountry_Decode_v_df = Account_UKSUPDEV_Spark_regmatch_df.withColumn('ShippingCountry_Decode_v',when(col('ShippingCountry_v') == "united states", "United States of America")\
                                                                       .when(col('ShippingCountry_v') == "viet nam", "Vietnam")\
                                                                       .when(col('ShippingCountry_v') == "czech republic", "Czechia")\
                                                                       .when(col('ShippingCountry_v') == "tanzania, united republic of", "Tanzania")\
                                                                       .when(col('ShippingCountry_v') == "venezuela, bolivarian republic of", "Venezuela")\
                                                                       .when(col('ShippingCountry_v') == "virgin islands, british", "British Virgin Islands")\
                                                                       .when(col('ShippingCountry_v') == "swaziland", "Eswatini")\
                                                                       .otherwise(col('Tgt_Shipping_country')))\
                                  .select('Id','ShippingCountry_Decode_v')
    
    ShippingCountry_Decode_v_df = ShippingCountry_Decode_v_df.withColumn('v_SobjectType',F.lit('AMS_Address__c'))
    
    ShippingCountry_Decode_v_df = ShippingCountry_Decode_v_df.alias('ShippingCountry_Decode_v_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    cond = [ShippingCountry_Decode_v_df.v_SobjectType == LKP_SF_Target_RecordType_Spark_df.SobjectType, ShippingCountry_Decode_v_df.ShippingCountry_Decode_v == LKP_SF_Target_RecordType_Spark_df.Name]
    
    RecordTypeId_df = ShippingCountry_Decode_v_df.join(LKP_SF_Target_RecordType_Spark_df, cond, 'left')\
                                                       .select('ShippingCountry_Decode_v_df.Id',col('LKP_SF_Target_RecordType_Spark_df.Id').alias('RecordTypeID_Shipping'))
    
    return RecordTypeId_df
    
def Shipping_Address_Line_1__c_ETL(Account_UKSUPDEV_Spark_df):
    
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Shippingstreet_ed',when(col('ShippingStreet') == "None", "")\
                                                    .otherwise(col('ShippingStreet')))\
                                                    .select('Id','ShippingStreet','Shippingstreet_ed')
    
    Address_Line_1__c_Shipping_df = regmatch_df.withColumn('ShippingStreet_Regex',col("Shippingstreet_ed").rlike('.*[A-Za-z0-9].*'))\
                                                             .withColumn('Address_Line_1__c',when(col('ShippingStreet_Regex') == "false", "NULL")\
                                                                                            .otherwise(col('ShippingStreet')))\
                                                             .select('Id','Address_Line_1__c')
    
    return Address_Line_1__c_Shipping_df
    
def Shipping_Address_Line_5__c_ETL(Account_UKSUPDEV_Spark_df):

    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Shippingcn_ed',when(col('ShippingCountry') == "None", "")\
                                           .otherwise(col('ShippingCountry')))\
                                           .select('Id','ShippingCountry','Shippingcn_ed')

    Address_Line_5__c_Shipping_df = regmatch_df.withColumn('Shipping_Country_Regex',col("Shippingcn_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Shipping_country',when(col('Shipping_Country_Regex') == "false", "China")\
                                                                                                .otherwise(initcap(col('ShippingCountry'))))\
                                                    .select('Id','ShippingCountry','Shippingcn_ed','Shipping_Country_Regex','Tgt_Shipping_country')
    
    return Address_Line_5__c_Shipping_df
    
def Shipping_Address_Line_9__c_ETL(Account_UKSUPDEV_Spark_df):
    Address_Line_9__c_Shipping_df = Account_UKSUPDEV_Spark_df.withColumn('Address_Line_9__c',F.lit('Shipping'))\
                                                       .select('Id','Address_Line_9__c')
    
    return Address_Line_9__c_Shipping_df
    
def Shipping_City__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('Shippingcity_ed',when(col('ShippingCity') == "None", "")\
                                           .otherwise(col('ShippingCity')))\
                                           .select('Id','ShippingCity','Shippingcity_ed')
    
    Shipping_City_out_df = regmatch_df.withColumn('Shipping_City_Regex',col("Shippingcity_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_Shipping_city',when(col('Shipping_City_Regex') == "false", "NULL")\
                                                                                          .otherwise(col('ShippingCity')))\
                                                    .select('Id','ShippingCity','Shippingcity_ed','Shipping_City_Regex','Tgt_Shipping_city')
    
    return Shipping_City_out_df
    
def Shipping_Postcode__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('ShippingPostalCode_ed',when(col('ShippingPostalCode') == "None", "")\
                                           .otherwise(col('ShippingPostalCode')))\
                                           .select('Id','ShippingPostalCode','ShippingPostalCode_ed')
    
    ShippingPostalCode_out_df = regmatch_df.withColumn('ShippingPostalCode_Regex',col("ShippingPostalCode_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_ShippingPostalCode',when(col('ShippingPostalCode_Regex') == "false", "NULL")\
                                                                                          .otherwise(col('ShippingPostalCode')))\
                                                    .select('Id','ShippingPostalCode','ShippingPostalCode_ed','ShippingPostalCode_Regex','Tgt_ShippingPostalCode')
    
    return ShippingPostalCode_out_df
    
def Shipping_Region__c_ETL(Account_UKSUPDEV_Spark_df):
    regmatch_df = Account_UKSUPDEV_Spark_df.withColumn('ShippingState_ed',when(col('ShippingState') == "None", "")\
                                           .otherwise(col('ShippingState')))\
                                           .select('Id','ShippingState','ShippingState_ed')
    
    ShippingState_out_df = regmatch_df.withColumn('ShippingState_Regex',col("ShippingState_ed").rlike('.*[A-Za-z0-9].*'))\
                                                    .withColumn('Tgt_ShippingState',when(col('ShippingState_Regex') == "false", "NULL")\
                                                                                          .otherwise(col('ShippingState')))\
                                                    .select('Id','ShippingState','ShippingState_ed','ShippingState_Regex','Tgt_ShippingState')
    
    return ShippingState_out_df
    
def creating_final_Shipping_df(Account_UKSUPDEV_Spark_df,Shipping_Acc_PubAcc_Name_df,Account_c_df,Publisher_Account_c_df,Account_Src_Id_Array_df,RecordTypeId_Shipping_df,Address_Line_1__c_Shipping_df,Address_Line_5__c_Shipping_df,Address_Line_6__c_df,Address_Line_8__c_Array_df,Address_Line_9__c_Shipping_df,Shipping_City_out_df,ShippingPostalCode_out_df,ShippingState_out_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Shipping_Acc_PubAcc_Name_df = Shipping_Acc_PubAcc_Name_df.alias('Shipping_Acc_PubAcc_Name_df')
    Account_c_df = Account_c_df.alias('Account_c_df')
    Publisher_Account_c_df = Publisher_Account_c_df.alias('Publisher_Account_c_df')
    Account_Src_Id_Array_df = Account_Src_Id_Array_df.alias('Account_Src_Id_Array_df')
    RecordTypeId_Shipping_df = RecordTypeId_Shipping_df.alias('RecordTypeId_Shipping_df')
    Address_Line_1__c_Shipping_df = Address_Line_1__c_Shipping_df.alias('Address_Line_1__c_Shipping_df')
    Address_Line_5__c_Shipping_df = Address_Line_5__c_Shipping_df.alias('Address_Line_5__c_Shipping_df')
    Address_Line_6__c_df = Address_Line_6__c_df.alias('Address_Line_6__c_df')
    Address_Line_8__c_Array_df = Address_Line_8__c_Array_df.alias('Address_Line_8__c_Array_df')
    Address_Line_9__c_Shipping_df = Address_Line_9__c_Shipping_df.alias('Address_Line_9__c_Shipping_df')
    Shipping_City_out_df = Shipping_City_out_df.alias('Shipping_City_out_df')
    ShippingPostalCode_out_df = ShippingPostalCode_out_df.alias('ShippingPostalCode_out_df')
    ShippingState_out_df = ShippingState_out_df.alias('ShippingState_out_df')
    
    Shipping_Final_Df = Account_UKSUPDEV_Spark_df.join(Shipping_Acc_PubAcc_Name_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Shipping_Acc_PubAcc_Name_df.Id'), 'left') \
                                                .join(Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_c_df.Id'), 'left')\
                                                .join(Publisher_Account_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Publisher_Account_c_df.Id'), 'left')\
                                                .join(Account_Src_Id_Array_df,col('Account_UKSUPDEV_Spark_df.Id') == col('Account_Src_Id_Array_df.Id'),'left')\
                                                .join(RecordTypeId_Shipping_df, col('Account_UKSUPDEV_Spark_df.Id') == col('RecordTypeId_Shipping_df.Id'), 'left')\
                                                .join(Address_Line_1__c_Shipping_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_1__c_Shipping_df.Id'), 'left')\
                                                .join(Address_Line_5__c_Shipping_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_5__c_Shipping_df.Id'), 'left')\
                                                .join(Address_Line_6__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_6__c_df.Id'), 'left')\
                                                .join(Address_Line_8__c_Array_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_8__c_Array_df.Id'), 'left')\
                                                .join(Address_Line_9__c_Shipping_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Address_Line_9__c_Shipping_df.Id'), 'left')\
                                                .join(Shipping_City_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Shipping_City_out_df.Id'), 'left')\
                                                .join(ShippingPostalCode_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('ShippingPostalCode_out_df.Id'), 'left')\
                                                .join(ShippingState_out_df, col('Account_UKSUPDEV_Spark_df.Id') == col('ShippingState_out_df.Id'), 'left')\
                                                .select('Account_UKSUPDEV_Spark_df.*', col('Shipping_Acc_PubAcc_Name_df.Name_Array'),'Account_Src_Id_Array_df.Account_Src_Id_Array','RecordTypeId_Shipping_df.RecordTypeID_Shipping','Address_Line_1__c_Shipping_df.Address_Line_1__c','Address_Line_5__c_Shipping_df.Tgt_Shipping_country','Address_Line_6__c_df.Address_Line_6__c','Address_Line_8__c_Array_df.Address_Line_8__c_Array','Address_Line_9__c_Shipping_df.Address_Line_9__c','Shipping_City_out_df.Tgt_Shipping_city','ShippingPostalCode_out_df.Tgt_ShippingPostalCode','ShippingState_out_df.Tgt_ShippingState')

    return Shipping_Final_Df
    
def alter_final_Shipping_ETL_df(Final_Shipping_df):
    alter_final_Shipping_df = Final_Shipping_df.withColumn("new", F.arrays_zip("Name_Array", "Address_Line_8__c_Array", "Account_Src_Id_Array"))\
                                               .withColumn("new", F.explode("new"))\
                                               .select(*Shipping_Sel_list,F.col("new.Name_Array").alias("Name"),F.col("new.Address_Line_8__c_Array").alias("Address_Line_8__c"),F.col("new.Account_Src_Id_Array").alias("Account_Src_Id"))
    
    alter_final_Shipping_df = alter_final_Shipping_df.withColumnRenamed('RecordTypeID_Shipping','RecordTypeId')\
                                                    .withColumnRenamed('Id','Src_Id')\
                                                    .withColumnRenamed('Tgt_Shipping_country','Address_Line_5__c')\
                                                    .withColumnRenamed('Tgt_Shipping_city','City__c')\
                                                    .withColumnRenamed('Tgt_ShippingPostalCode','Postcode__c')\
                                                    .withColumnRenamed('Tgt_ShippingState','Region__c')\
                                                    .select(*Sorted_Sel_list)

    alter_final_Shipping_df = alter_final_Shipping_df.sort(col("Src_Id").asc())                                         
                             
    return alter_final_Shipping_df
    
def Billing_Shipping_union_ETL(alter_final_Billing_df,alter_final_Shipping_df):
    Billing_Shipping_Union_df = alter_final_Billing_df.union(alter_final_Shipping_df).distinct()
    
    return Billing_Shipping_Union_df
    
def Billing_Shipping_Filter_ETL(Billing_Shipping_Union_df):
    Billing_Shipping_Filter_df = Billing_Shipping_Union_df.filter((col("Address_Line_8__c") != 'NULL') & (col("Address_Line_1__c") != 'NULL') & (col("Account_Src_Id").isNotNull()))
    
    Billing_Shipping_Filter_df = Billing_Shipping_Filter_df.sort(col("Src_Id").asc())
    
    return Billing_Shipping_Filter_df
    
def Handling_Insert_records(Billing_Shipping_Filter_df):
    Ins_records_df_index = Billing_Shipping_Filter_df.withColumn("idx",F.monotonically_increasing_id())
    windowSpec = W.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    
    return Ins_records_df_index
    
if __name__ == "__main__":
    start = time.time()
    Account_UKSUPDEV_df = get_input_data()
    Account_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Account_UKSUPDEV_df)
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    LKP_SF_Target_Account_data_df = LKP_SF_Target_Account_data()
    LKP_SF_Target_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Account_data_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    LKP_SF_Target_RecordType_df = LKP_SF_Target_RecordType_data()
    LKP_SF_Target_RecordType_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_RecordType_df)
    Billing_Acc_PubAcc_Name_df = Billing_Acc_PubAcc_Name_ETL(Account_UKSUPDEV_Spark_df)
    Account_c_df = Account__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Account_data_Spark_df)
    Publisher_Account_c_df = Publisher_Account_c_ETL(Account_UKSUPDEV_Spark_df,Account_c_df,LKP_SF_Target_Publisher_Account_data_Spark_df)
    Account_Src_Id_Array_df = Account_Src_Id_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df)
    RecordTypeId_Billing_df = RecordTypeID_Billing_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df)
    Address_Line_1__c_Biling_df = Biling_Address_Line_1__c_ETL(Account_UKSUPDEV_Spark_df)
    Address_Line_5__c_Biling_df = Biling_Address_Line_5__c_ETL(Account_UKSUPDEV_Spark_df)
    Address_Line_6__c_df = Address_Line_6__c_ETL(Account_UKSUPDEV_Spark_df)
    Address_Line_8__c_Array_df = Address_Line_8__c_ETL(Account_c_df,Publisher_Account_c_df)
    Address_Line_9__c_Biling_df = Biling_Address_Line_9__c_ETL(Account_UKSUPDEV_Spark_df)
    Billing_City_out_df = Billing_City__c_ETL(Account_UKSUPDEV_Spark_df)
    BillingPostalCode_out_df = Billing_Postcode__c_ETL(Account_UKSUPDEV_Spark_df)
    BillingState_out_df = Billing_Region__c_ETL(Account_UKSUPDEV_Spark_df)
    Final_Billing_df = creating_final_Billing_df(Account_UKSUPDEV_Spark_df,Billing_Acc_PubAcc_Name_df,Account_c_df,Publisher_Account_c_df,Account_Src_Id_Array_df,RecordTypeId_Billing_df,Address_Line_1__c_Biling_df,Address_Line_5__c_Biling_df,Address_Line_6__c_df,Address_Line_8__c_Array_df,Address_Line_9__c_Biling_df,Billing_City_out_df,BillingPostalCode_out_df,BillingState_out_df)
    alter_final_Billing_df = alter_final_Billing_ETL_df(Final_Billing_df)
    Shipping_Acc_PubAcc_Name_df = Shipping_Acc_PubAcc_Name_ETL(Account_UKSUPDEV_Spark_df)
    RecordTypeId_Shipping_df = RecordTypeID_Shipping_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df)
    Address_Line_1__c_Shipping_df = Shipping_Address_Line_1__c_ETL(Account_UKSUPDEV_Spark_df)
    Address_Line_5__c_Shipping_df = Shipping_Address_Line_5__c_ETL(Account_UKSUPDEV_Spark_df)
    Address_Line_9__c_Shipping_df = Shipping_Address_Line_9__c_ETL(Account_UKSUPDEV_Spark_df)
    Shipping_City_out_df = Shipping_City__c_ETL(Account_UKSUPDEV_Spark_df)
    ShippingPostalCode_out_df = Shipping_Postcode__c_ETL(Account_UKSUPDEV_Spark_df)
    ShippingState_out_df = Shipping_Region__c_ETL(Account_UKSUPDEV_Spark_df)
    Final_Shipping_df = creating_final_Shipping_df(Account_UKSUPDEV_Spark_df,Shipping_Acc_PubAcc_Name_df,Account_c_df,Publisher_Account_c_df,Account_Src_Id_Array_df,RecordTypeId_Shipping_df,Address_Line_1__c_Shipping_df,Address_Line_5__c_Shipping_df,Address_Line_6__c_df,Address_Line_8__c_Array_df,Address_Line_9__c_Shipping_df,Shipping_City_out_df,ShippingPostalCode_out_df,ShippingState_out_df)
    alter_final_Shipping_df = alter_final_Shipping_ETL_df(Final_Shipping_df)
    Billing_Shipping_Union_df = Billing_Shipping_union_ETL(alter_final_Billing_df,alter_final_Shipping_df)
    Billing_Shipping_Filter_df = Billing_Shipping_Filter_ETL(Billing_Shipping_Union_df)
    Final_Address_df = Handling_Insert_records(Billing_Shipping_Filter_df)
    Final_Address_select_df = Final_Address_df.select(*Sel_Col_List)
    
    src_count = Account_UKSUPDEV_Spark_df.count()
    Tgt_count = Final_Address_select_df.count()
    
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Tgt_count)
    
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))