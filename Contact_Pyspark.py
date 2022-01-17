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

Sel_Col_List = ['Id','AccountId','LastName','FirstName','Salutation','RecordTypeId','Phone','Fax','MobilePhone','HomePhone','OtherPhone','AssistantPhone','Email','Title','Department','AssistantName','LeadSource','Birthdate','Description','CurrencyIsoCode','Publisher_Name__c','TPS_ID__c']


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
    df=getSFData(""" SELECT Id,AccountId,LastName,FirstName,Salutation,HomePhone,OtherPhone,AssistantPhone,Email,Title,Department,
                     AssistantName,LeadSource,Birthdate,Description,CurrencyIsoCode
                     FROM Contact where
                     RecordTypeId in ('012b00000009cG1AAI','012b0000000QBYhAAO') and AccountId <>null
                     and Id = '003b000000QbKH5AAN' """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Contact_UKSUPDEV_df):
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.astype(str)
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.fillna('Null')
    Contact_UKSUPDEV_Spark_df = spark.createDataFrame(Contact_UKSUPDEV_df)
    return Contact_UKSUPDEV_Spark_df
    
def LKP_SF_Target_Publisher_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Account__c,External_UID__c
                     FROM Publisher_Account__c """,sf)
                    
    return df

def LKP_SF_Target_RecordType_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,DeveloperName
                     FROM RecordType """,sf)
                    
    return df
    
def LKP_FF_Contacts():
    Lkp_Contacts_FF_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Contact\ContactsChina.csv")\
                                  .select('Id','Phone','Fax','MobilePhone')
                                                       
    return Lkp_Contacts_FF_df
    
def LKP_FF_ContactsToDelete():
    Lkp_ContactsToDelete_FF_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Contact\Chinese_Contacts_to_Delete.csv")\
                                  .select('Source UKSF ID','Name')\
                                  .withColumnRenamed('Source UKSF ID','Source_UKSF_ID')
                                                       
    return Lkp_ContactsToDelete_FF_df
    
def AccountId_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    AccountId_df = Contact_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,col('Contact_UKSUPDEV_Spark_df.AccountId') == col('LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c'),"left")\
                       .select('Contact_UKSUPDEV_Spark_df.Id','Contact_UKSUPDEV_Spark_df.AccountId','LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c')
    
    return AccountId_df
    
def LastName_ETL(Contact_UKSUPDEV_Spark_df):
    LastName_df = Contact_UKSUPDEV_Spark_df.withColumn('Tgt_LastName', 
                                            regexp_replace(Contact_UKSUPDEV_Spark_df.LastName, "[\.' ']", ""))
    
    return LastName_df
    
def FirstName_ETL(Contact_UKSUPDEV_Spark_df,LastName_df):
    FirstName_df = Contact_UKSUPDEV_Spark_df.withColumn('Der_FirstName', 
                                            regexp_replace(Contact_UKSUPDEV_Spark_df.FirstName, "[\.' ']", ""))
    
    LastName_df = LastName_df.alias('LastName_df')
    FirstName_df = FirstName_df.alias('FirstName_df')

    First_Last_Name_df = LastName_df.join(FirstName_df,col('LastName_df.Id') == col('FirstName_df.Id'),"inner")\
                       .select('LastName_df.Id','LastName_df.Tgt_LastName',col('FirstName_df.FirstName').alias('Src_FirstName'),'FirstName_df.Der_FirstName')
    
    FirstName_df = First_Last_Name_df.withColumn('Tgt_FirstName',when(First_Last_Name_df.Src_FirstName.isNull(),First_Last_Name_df.Tgt_LastName)
                                                .otherwise(First_Last_Name_df.Der_FirstName)).select('Id','Tgt_FirstName')
    
    return FirstName_df
    
def RecordTypeId_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.withColumn('In_Developer_Name',F.lit('CN_Contact'))
    
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    RecordTypeId_df = Contact_UKSUPDEV_Spark_df.join(LKP_SF_Target_RecordType_Spark_df,col('Contact_UKSUPDEV_Spark_df.In_Developer_Name') == col('LKP_SF_Target_RecordType_Spark_df.DeveloperName'),"left")\
                       .select('Contact_UKSUPDEV_Spark_df.Id','LKP_SF_Target_RecordType_Spark_df.DeveloperName',col('LKP_SF_Target_RecordType_Spark_df.Id').alias('RecordTypeId'))
    
    return RecordTypeId_df
    
def Phone_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.withColumnRenamed('Id','Id_FromUKSUPDEV')

    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    Lkp_Contacts_FF_df = Lkp_Contacts_FF_df.alias('Lkp_Contacts_FF_df')

    Phone_fromLKP_df = Contact_UKSUPDEV_Spark_df.join(Lkp_Contacts_FF_df,col('Contact_UKSUPDEV_Spark_df.Id_FromUKSUPDEV') == col('Lkp_Contacts_FF_df.Id'),"left")\
                       .select('Lkp_Contacts_FF_df.Id','Lkp_Contacts_FF_df.Phone')
    
    Phone_fromLKP_df = Phone_fromLKP_df.withColumn('SubStr_1',substring('Phone',4,1))\
                                   .withColumn('SubStr_2',substring('Phone',8,1))
    
    Phone_fromLKP_df = Phone_fromLKP_df.alias('Phone_fromLKP_df')
    
    Phone_fromLKP_Der_df = Phone_fromLKP_df.withColumn('Der_SubStr_1',when(col("Phone_fromLKP_df.SubStr_1") == " ", F.lit('null'))
                                                             .otherwise(Phone_fromLKP_df.SubStr_1))\
                                    .withColumn('Der_SubStr_2',when(col("Phone_fromLKP_df.SubStr_2") == " ", F.lit('null'))
                                                              .otherwise(Phone_fromLKP_df.SubStr_2))\
                                    .select('Id','Phone','Der_SubStr_1','Der_SubStr_2')
    
    Phone_fromLKP_Der_df = Phone_fromLKP_Der_df.alias('Phone_fromLKP_Der_df')
    
    Phone_df = Phone_fromLKP_Der_df.withColumn('Tgt_Phone',when((col("Phone_fromLKP_Der_df.Der_SubStr_1") == "null") | (col("Phone_fromLKP_Der_df.Der_SubStr_2") == "null"),\
                                                     concat(substring(regexp_replace('Phone',' ', ''),1,3),lit(" "),\
                                                            substring(regexp_replace('Phone',' ', ''),4,3),lit(" "),\
                                                            substring(regexp_replace('Phone',' ', ''),7,10)))
                                       .otherwise(Phone_fromLKP_Der_df.Phone))\
                .select('Id','Phone','Tgt_Phone')
    
    return Phone_df
    
def Fax_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.withColumnRenamed('Id','Id_FromUKSUPDEV')

    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    Lkp_Contacts_FF_df = Lkp_Contacts_FF_df.alias('Lkp_Contacts_FF_df')

    Fax_fromLKP_df = Contact_UKSUPDEV_Spark_df.join(Lkp_Contacts_FF_df,col('Contact_UKSUPDEV_Spark_df.Id_FromUKSUPDEV') == col('Lkp_Contacts_FF_df.Id'),"left")\
                       .select('Lkp_Contacts_FF_df.Id',col('Lkp_Contacts_FF_df.Fax').alias('Tgt_Fax'))
    
    return Fax_fromLKP_df
    
def MobilePhone_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.withColumnRenamed('Id','Id_FromUKSUPDEV')

    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    Lkp_Contacts_FF_df = Lkp_Contacts_FF_df.alias('Lkp_Contacts_FF_df')

    MobilePhone_fromLKP_df = Contact_UKSUPDEV_Spark_df.join(Lkp_Contacts_FF_df,col('Contact_UKSUPDEV_Spark_df.Id_FromUKSUPDEV') == col('Lkp_Contacts_FF_df.Id'),"left")\
                       .select('Lkp_Contacts_FF_df.Id','Lkp_Contacts_FF_df.MobilePhone')
    
    MobilePhone_fromLKP_df = MobilePhone_fromLKP_df.withColumn('SubStr_1',substring('MobilePhone',4,1))\
                                   .withColumn('SubStr_2',substring('MobilePhone',8,1))
    
    MobilePhone_fromLKP_df = MobilePhone_fromLKP_df.alias('MobilePhone_fromLKP_df')
    
    MobilePhone_fromLKP_Der_df = MobilePhone_fromLKP_df.withColumn('Der_SubStr_1',when(col("MobilePhone_fromLKP_df.SubStr_1") == " ", F.lit('null'))
                                                             .otherwise(MobilePhone_fromLKP_df.SubStr_1))\
                                    .withColumn('Der_SubStr_2',when(col("MobilePhone_fromLKP_df.SubStr_2") == " ", F.lit('null'))
                                                              .otherwise(MobilePhone_fromLKP_df.SubStr_2))\
                                    .select('Id','MobilePhone','Der_SubStr_1','Der_SubStr_2')
    
    MobilePhone_fromLKP_Der_df = MobilePhone_fromLKP_Der_df.alias('MobilePhone_fromLKP_Der_df')
    
    MobilePhone_df = MobilePhone_fromLKP_Der_df.withColumn('Tgt_MobilePhone',when((col("MobilePhone_fromLKP_Der_df.Der_SubStr_1") == "null") | (col("MobilePhone_fromLKP_Der_df.Der_SubStr_2") == "null"),\
                                                     concat(substring(regexp_replace('MobilePhone',' ', ''),1,3),lit(" "),\
                                                            substring(regexp_replace('MobilePhone',' ', ''),4,3),lit(" "),\
                                                            substring(regexp_replace('MobilePhone',' ', ''),7,10)))
                                       .otherwise(MobilePhone_fromLKP_Der_df.MobilePhone))\
                .select('Id','MobilePhone','Tgt_MobilePhone')
    
    return MobilePhone_df
    
def Email_ETL(Contact_UKSUPDEV_Spark_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')

    Email_df = Contact_UKSUPDEV_Spark_df.withColumn('Tgt_Email',when(instr(col('Contact_UKSUPDEV_Spark_df.Email'),'@') > 0,\
                                                                          regexp_replace(Contact_UKSUPDEV_Spark_df.Email, "[\,';']", ""))
                                                              .otherwise("null"))\
                                                              .select('Id','Tgt_Email')
    return Email_df
    
def Source_UKSF_ID_ETL(Contact_UKSUPDEV_Spark_df,Lkp_ContactsToDelete_FF_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    Lkp_ContactsToDelete_FF_df = Lkp_ContactsToDelete_FF_df.alias('Lkp_ContactsToDelete_FF_df')

    Source_UKSF_ID_df = Contact_UKSUPDEV_Spark_df.join(Lkp_ContactsToDelete_FF_df,col('Contact_UKSUPDEV_Spark_df.Id') == col('Lkp_ContactsToDelete_FF_df.Source_UKSF_ID'),"left")\
                                          .select('Contact_UKSUPDEV_Spark_df.Id','Lkp_ContactsToDelete_FF_df.Source_UKSF_ID')
    
    return Source_UKSF_ID_df
    
def creating_final_df(Contact_UKSUPDEV_Spark_df,AccountId_df,LastName_df,FirstName_df,RecordTypeId_df,Phone_df,Fax_df,MobilePhone_df,Email_df,Source_UKSF_ID_df):
    
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    AccountId_df = AccountId_df.alias('AccountId_df')
    LastName_df = LastName_df.alias('LastName_df')
    FirstName_df = FirstName_df.alias('FirstName_df')
    RecordTypeId_df = RecordTypeId_df.alias('RecordTypeId_df')
    Phone_df = Phone_df.alias('Phone_df')
    Fax_df = Fax_df.alias('Fax_df')
    MobilePhone_df = MobilePhone_df.alias('MobilePhone_df')
    Email_df = Email_df.alias('Email_df')
    Source_UKSF_ID_df = Source_UKSF_ID_df.alias('Source_UKSF_ID_df')
    
    Final_Df = Contact_UKSUPDEV_Spark_df.join(AccountId_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('AccountId_df.Id'), 'left') \
                                        .join(LastName_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('LastName_df.Id'), 'left') \
                                        .join(FirstName_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('FirstName_df.Id'), 'left')\
                                        .join(RecordTypeId_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('RecordTypeId_df.Id'), 'left')\
                                        .join(Phone_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Phone_df.Id'), 'left')\
                                        .join(Fax_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Fax_df.Id'), 'left')\
                                        .join(MobilePhone_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('MobilePhone_df.Id'), 'left')\
                                        .join(Email_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Email_df.Id'), 'left')\
                                        .join(Source_UKSF_ID_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Source_UKSF_ID_df.Id'), 'left')\
                                        .select('Contact_UKSUPDEV_Spark_df.*', col('AccountId_df.Account__c'), 'LastName_df.Tgt_LastName','FirstName_df.Tgt_FirstName','RecordTypeId_df.RecordTypeId','Phone_df.Tgt_Phone','Fax_df.Tgt_Fax','MobilePhone_df.Tgt_MobilePhone','Email_df.Tgt_Email','Source_UKSF_ID_df.Source_UKSF_ID')

    return Final_Df
    
def alter_final_df(Final_df):
    
    Final_df = Final_df.drop('AccountId','LastName','FirstName','Email')
    
    alter_final_df = Final_df.withColumn('Publisher_Name__c',lit('CN China'))\
                             .withColumnRenamed('Account__c','AccountId')\
                             .withColumnRenamed('Tgt_LastName','LastName')\
                             .withColumnRenamed('Tgt_FirstName','FirstName')\
                             .withColumnRenamed('Tgt_Phone','Phone')\
                             .withColumnRenamed('Tgt_Fax','Fax')\
                             .withColumnRenamed('Tgt_MobilePhone','MobilePhone')\
                             .withColumnRenamed('Tgt_Email','Email')\
                             .withColumnRenamed('Id','TPS_ID__c')
                             
    return alter_final_df
    
def Handling_Insert_records(alter_final_df):
    Ins_records_df_index = alter_final_df.withColumn("idx",F.monotonically_increasing_id())
    windowSpec = W.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    
    return Ins_records_df_index
    
def Final_contact_filter_ETL(Final_contact_df):

    Final_contact_filter_df = Final_contact_df.filter((col('Source_UKSF_ID').isNull()) & (col('AccountId').isNotNull()))
    return Final_contact_filter_df

    
if __name__ == "__main__":
    start = time.time()
    Contact_UKSUPDEV_df = get_input_data()
    Contact_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Contact_UKSUPDEV_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    LKP_SF_Target_RecordType_df = LKP_SF_Target_RecordType_data()
    LKP_SF_Target_RecordType_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_RecordType_df)
    Lkp_Contacts_FF_df = LKP_FF_Contacts()
    Lkp_ContactsToDelete_FF_df = LKP_FF_ContactsToDelete()
    AccountId_df = AccountId_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df)
    LastName_df = LastName_ETL(Contact_UKSUPDEV_Spark_df)
    FirstName_df = FirstName_ETL(Contact_UKSUPDEV_Spark_df,LastName_df)
    RecordTypeId_df = RecordTypeId_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df)
    Phone_df = Phone_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df)
    Fax_df = Fax_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df)
    MobilePhone_df = MobilePhone_ETL(Contact_UKSUPDEV_Spark_df,Lkp_Contacts_FF_df)
    Email_df = Email_ETL(Contact_UKSUPDEV_Spark_df)
    Source_UKSF_ID_df = Source_UKSF_ID_ETL(Contact_UKSUPDEV_Spark_df,Lkp_ContactsToDelete_FF_df)
    Final_df = creating_final_df(Contact_UKSUPDEV_Spark_df,AccountId_df,LastName_df,FirstName_df,RecordTypeId_df,Phone_df,Fax_df,MobilePhone_df,Email_df,Source_UKSF_ID_df)
    alter_final_df = alter_final_df(Final_df)
    Final_contact_df = Handling_Insert_records(alter_final_df)
    Final_contact_filter_df = Final_contact_filter_ETL(Final_contact_df)
    Final_contact_select_df = Final_contact_filter_df.select(*Sel_Col_List)
    
    src_count = Contact_UKSUPDEV_Spark_df.count()
    Tgt_count = Final_contact_select_df.count()
    
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Tgt_count)
    
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))