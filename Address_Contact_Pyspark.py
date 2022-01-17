from simple_salesforce import Salesforce, SalesforceLogin
from pyspark.sql.functions import when
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import instr,upper,initcap
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import timedelta
import pandas as pd
import numpy as np
import time
pd.options.mode.chained_assignment = None

Sel_Col_List = ['Name','CurrencyIsoCode','RecordTypeId','Address_Line_1__c','Address_Line_6__c','Address_Line_7__c','Address_Line_8__c','Address_Line_9__c','City__c','Postcode__c']

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
    df=getSFData(""" SELECT Id,AccountId,CurrencyIsoCode,MailingCountryCode,MailingStreet,FirstName, LastName, Email,
                            MailingCity,MailingPostalCode
                     FROM Contact where
                     RecordTypeId in ('012b00000009cG1AAI','012b0000000QBYhAAO') and Email<>''
                     and AccountId = '001b000000T3Y0vAAF' """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Contact_UKSUPDEV_df):
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.astype(str)
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.fillna('Null')
    Contact_UKSUPDEV_Spark_df = spark.createDataFrame(Contact_UKSUPDEV_df)
    return Contact_UKSUPDEV_Spark_df
    
def LKP_SF_Target_Publisher_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Account_Name__c,Account__c,External_UID__c,Publisher_Name__c
                     FROM Publisher_Account__c """,sf)
                    
    return df
    
def LKP_SF_Target_RecordType_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,SobjectType,DeveloperName
                     FROM RecordType """,sf)
                    
    return df
    
def LKP_SF_Target_Contact_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id, Email , Name, Publisher_Name__c, AccountId
                     FROM Contact
                     where Publisher_Name__c='CN China' """,sf)
                    
    return df
    
def LKP_SF_Target_AMS_ADDRESS_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id, Address_Line_1__c , Address_Line_5__c, Address_Line_8__c
                     FROM AMS_Address__c """,sf)
                    
    return df
    
def Address_Contact_Name_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')
    
    Address_Contact_Name_df = Contact_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,col('Contact_UKSUPDEV_Spark_df.AccountId') == col('LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c'),"left")\
                       .select('Contact_UKSUPDEV_Spark_df.Id','Contact_UKSUPDEV_Spark_df.AccountId',col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account_Name__c'))
    
    Address_Contact_Name_df = Address_Contact_Name_df.withColumn('Account_Name__c',\
                                    concat(initcap('Account_Name__c'),lit("-Mailing-"),lit("Contact")))
    
    return Address_Contact_Name_df
    
def Recordtype_ID_out_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df):
    Country_Region_df = Contact_UKSUPDEV_Spark_df.withColumn('Country_Region',when(Contact_UKSUPDEV_Spark_df.MailingCountryCode == 'Null','CN')
                                                .otherwise(Contact_UKSUPDEV_Spark_df.MailingCountryCode)).select('Id','Country_Region')\
                                             .withColumn('SObjectType1',F.lit('AMS_Address__c'))
    
    
    Country_Region_df = Country_Region_df.alias('Country_Region_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    cond = [Country_Region_df.SObjectType1 == LKP_SF_Target_RecordType_Spark_df.SobjectType, Country_Region_df.Country_Region == LKP_SF_Target_RecordType_Spark_df.DeveloperName]
    
    Recordtype_ID_df = Country_Region_df.join(LKP_SF_Target_RecordType_Spark_df,cond,"left")\
                       .select('Country_Region_df.Id',col('LKP_SF_Target_RecordType_Spark_df.Id').alias('Recordtype_ID'))
    
    Recordtype_ID_Generic_df = Contact_UKSUPDEV_Spark_df.withColumn('SobjectType_in',F.lit('AMS_Address__c'))\
                         .withColumn('Name_in',F.lit('Generic'))\
                         .select('Id','SobjectType_in','Name_in')
    
    Recordtype_ID_Generic_df = Recordtype_ID_Generic_df.alias('Recordtype_ID_Generic_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    cond = [Recordtype_ID_Generic_df.SobjectType_in == LKP_SF_Target_RecordType_Spark_df.SobjectType, Recordtype_ID_Generic_df.Name_in == LKP_SF_Target_RecordType_Spark_df.DeveloperName]
    
    Recordtype_ID_Generic_LKP_df = Recordtype_ID_Generic_df.join(LKP_SF_Target_RecordType_Spark_df,cond,"left")\
                                                           .select('Recordtype_ID_Generic_df.Id',col('LKP_SF_Target_RecordType_Spark_df.Id').alias('Recordtype_ID_Generic_Out'))
    
    Recordtype_ID_df = Recordtype_ID_df.alias('Recordtype_ID_df')
    Recordtype_ID_Generic_LKP_df = Recordtype_ID_Generic_LKP_df.alias('Recordtype_ID_Generic_LKP_df')

    Recordtype_ID_out_join_df = Recordtype_ID_df.join(Recordtype_ID_Generic_LKP_df,col('Recordtype_ID_df.Id') == col('Recordtype_ID_Generic_LKP_df.Id'),"inner")\
                                                .select('Recordtype_ID_df.Id','Recordtype_ID_df.Recordtype_ID','Recordtype_ID_Generic_LKP_df.Recordtype_ID_Generic_Out')

    Recordtype_ID_out_df = Recordtype_ID_out_join_df.withColumn('Recordtype_ID_Out',when(Recordtype_ID_out_join_df.Recordtype_ID.isNull(),Recordtype_ID_out_join_df.Recordtype_ID_Generic_Out)
                                                                                   .otherwise(Recordtype_ID_out_join_df.Recordtype_ID))\
                                                                                   .select('Id','Recordtype_ID_Out')
    
    return Recordtype_ID_out_df
    
def Address_Line_6__c_ETL(Contact_UKSUPDEV_Spark_df):
    Address_Line_6__c_df = Contact_UKSUPDEV_Spark_df.withColumn('Address_Line_6__c',F.lit('CN China'))\
                                                       .select('Id','Address_Line_6__c')
    
    return Address_Line_6__c_df
    
def Address_Line_7__c_ETL(Contact_UKSUPDEV_Spark_df):
    Address_Line_7__c_df = Contact_UKSUPDEV_Spark_df.withColumn('Country_Region',when(Contact_UKSUPDEV_Spark_df.MailingCountryCode == 'Null','CN')
                                                .otherwise(Contact_UKSUPDEV_Spark_df.MailingCountryCode)).select('Id','Country_Region')
    
    return Address_Line_7__c_df
    
def Address_Line_8__c_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,LKP_SF_Target_Contact_Spark_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')
    
    Account_ID_df = Contact_UKSUPDEV_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,col('Contact_UKSUPDEV_Spark_df.AccountId') == col('LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c'),"left")\
                                             .select('Contact_UKSUPDEV_Spark_df.Id','Contact_UKSUPDEV_Spark_df.AccountId',col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('AccountID_Out'))
    
    Contact_details_df = Contact_UKSUPDEV_Spark_df.withColumn('Contact_name',initcap(concat(ltrim(rtrim('FirstName')),lit(' '),ltrim(rtrim('LastName')))))\
                                                  .withColumn('Publisher_Name',F.lit('CN China'))\
                                                  .select('Id',col('Email').alias('Email_in'),'Publisher_Name','Contact_name')
    
    Contact_details_df = Contact_details_df.alias('Contact_details_df')
    Account_ID_df = Account_ID_df.alias('Account_ID_df')

    Contact_details_join_df = Contact_details_df.join(Account_ID_df,col('Contact_details_df.Id') == col('Account_ID_df.Id'),"inner")\
                                                .select('Contact_details_df.Id','Account_ID_df.AccountID_Out','Contact_details_df.Email_in','Contact_details_df.Publisher_Name','Contact_details_df.Contact_name')
    
    Contact_details_join_df = Contact_details_join_df.alias('Contact_details_join_df')
    LKP_SF_Target_Contact_Spark_df = LKP_SF_Target_Contact_Spark_df.alias('LKP_SF_Target_Contact_Spark_df')

    cond = [Contact_details_join_df.AccountID_Out == LKP_SF_Target_Contact_Spark_df.AccountId, Contact_details_join_df.Email_in == LKP_SF_Target_Contact_Spark_df.Email,
            Contact_details_join_df.Publisher_Name == LKP_SF_Target_Contact_Spark_df.Publisher_Name__c, Contact_details_join_df.Contact_name == LKP_SF_Target_Contact_Spark_df.Name]

    Address_Line_8__c_df = Contact_details_join_df.join(LKP_SF_Target_Contact_Spark_df,cond,"left")\
                       .select('Contact_details_join_df.Id',col('LKP_SF_Target_Contact_Spark_df.Id').alias('Address_Line_8__c'))
    
    return Address_Line_8__c_df
    
def Address_Line_9__c_ETL(Contact_UKSUPDEV_Spark_df):
    Address_Line_9__c_df = Contact_UKSUPDEV_Spark_df.withColumn('Address_Line_9__c',F.lit('Mailing'))\
                                                       .select('Id','Address_Line_9__c')
    
    return Address_Line_9__c_df
    
def creating_final_df(Contact_UKSUPDEV_Spark_df,Address_Contact_Name_df,Recordtype_ID_out_df,Address_Line_6__c_df,Address_Line_7__c_df,Address_Line_8__c_df,Address_Line_9__c_df):
    Contact_UKSUPDEV_Spark_df = Contact_UKSUPDEV_Spark_df.alias('Contact_UKSUPDEV_Spark_df')
    Address_Contact_Name_df = Address_Contact_Name_df.alias('Address_Contact_Name_df')
    Recordtype_ID_out_df = Recordtype_ID_out_df.alias('Recordtype_ID_out_df')
    Address_Line_6__c_df = Address_Line_6__c_df.alias('Address_Line_6__c_df')
    Address_Line_7__c_df = Address_Line_7__c_df.alias('Address_Line_7__c_df')
    Address_Line_8__c_df = Address_Line_8__c_df.alias('Address_Line_8__c_df')
    Address_Line_9__c_df = Address_Line_9__c_df.alias('Address_Line_9__c_df')
    
    Final_Df = Contact_UKSUPDEV_Spark_df.join(Address_Contact_Name_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Address_Contact_Name_df.Id'), 'left') \
                                        .join(Recordtype_ID_out_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Recordtype_ID_out_df.Id'), 'left') \
                                        .join(Address_Line_6__c_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Address_Line_6__c_df.Id'), 'left')\
                                        .join(Address_Line_7__c_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Address_Line_7__c_df.Id'), 'left')\
                                        .join(Address_Line_8__c_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Address_Line_8__c_df.Id'), 'left')\
                                        .join(Address_Line_9__c_df, col('Contact_UKSUPDEV_Spark_df.Id') == col('Address_Line_9__c_df.Id'), 'left')\
                                        .select('Contact_UKSUPDEV_Spark_df.*', col('Address_Contact_Name_df.Account_Name__c').alias('Name'), col('Recordtype_ID_out_df.Recordtype_ID_Out').alias('RecordTypeId'),'Address_Line_6__c_df.Address_Line_6__c',col('Address_Line_7__c_df.Country_Region').alias('Address_Line_7__c'),'Address_Line_8__c_df.Address_Line_8__c','Address_Line_9__c_df.Address_Line_9__c')
    
    return Final_Df
    
def alter_final_df(Final_df):
    
    Final_df = Final_df.drop('Id','AccountId','FirstName','LastName','Email','MailingCountryCode')
    
    alter_final_df = Final_df.withColumnRenamed('MailingStreet','Address_Line_1__c')\
                             .withColumnRenamed('MailingCity','City__c')\
                             .withColumnRenamed('MailingPostalCode','Postcode__c')\
                             .select(*Sel_Col_List)
                             
    return alter_final_df
    
def aggregate_final_df(alter_final_df):
    cols = ["Address_Line_8__c", "Address_Line_1__c"]
    w = Window.partitionBy(cols).orderBy(col('tiebreak').desc())
    aggregate_final_df = alter_final_df.withColumn('tiebreak', monotonically_increasing_id())\
                                       .withColumn('rank', rank().over(w))\
                                       .filter(col('rank') == 1).drop('rank','tiebreak')
    
    return aggregate_final_df
    
def Address_ID_ETL(aggregate_final_df,LKP_SF_Target_AMS_ADDRESS_Spark_df):
    aggregate_final_df = aggregate_final_df.alias('aggregate_final_df')
    LKP_SF_Target_AMS_ADDRESS_Spark_df = LKP_SF_Target_AMS_ADDRESS_Spark_df.alias('LKP_SF_Target_AMS_ADDRESS_Spark_df')

    aggregate_final_df = aggregate_final_df.withColumn('Address_Line_5__c_in',F.lit('China'))\
                                       .withColumnRenamed('Address_Line_1__c','Address_Line_1__c_in')\
                                       .withColumnRenamed('Address_Line_8__c','Address_Line_8__c_in')

    cond = [aggregate_final_df.Address_Line_1__c_in == LKP_SF_Target_AMS_ADDRESS_Spark_df.Address_Line_1__c, aggregate_final_df.Address_Line_8__c_in == LKP_SF_Target_AMS_ADDRESS_Spark_df.Address_Line_8__c,
            aggregate_final_df.Address_Line_5__c_in == LKP_SF_Target_AMS_ADDRESS_Spark_df.Address_Line_5__c ]
    
    Address_ID_df = aggregate_final_df.join(LKP_SF_Target_AMS_ADDRESS_Spark_df,cond,"left")\
                                             .select('aggregate_final_df.*',col('LKP_SF_Target_AMS_ADDRESS_Spark_df.Id').alias('Address_ID'))
    
    return Address_ID_df
    
def Filter_Final_df(Address_ID_df):
    Filter_Final_df = Address_ID_df.filter((col("Address_Line_8__c") != 'NULL') & (col("Address_Line_1__c") != 'NULL'))
    
    return Filter_Final_df
    

    
if __name__ == "__main__":
    Contact_UKSUPDEV_df = get_input_data()
    Contact_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Contact_UKSUPDEV_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    LKP_SF_Target_RecordType_df = LKP_SF_Target_RecordType_data()
    LKP_SF_Target_RecordType_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_RecordType_df)
    LKP_SF_Target_Contact_df = LKP_SF_Target_Contact_data()
    LKP_SF_Target_Contact_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Contact_df)
    LKP_SF_Target_AMS_ADDRESS_df = LKP_SF_Target_AMS_ADDRESS_data()
    LKP_SF_Target_AMS_ADDRESS_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_AMS_ADDRESS_df)
    Address_Contact_Name_df = Address_Contact_Name_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df)
    Recordtype_ID_out_df = Recordtype_ID_out_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_RecordType_Spark_df)
    Address_Line_6__c_df = Address_Line_6__c_ETL(Contact_UKSUPDEV_Spark_df)
    Address_Line_7__c_df = Address_Line_7__c_ETL(Contact_UKSUPDEV_Spark_df)
    Address_Line_8__c_df = Address_Line_8__c_ETL(Contact_UKSUPDEV_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,LKP_SF_Target_Contact_Spark_df)
    Address_Line_9__c_df = Address_Line_9__c_ETL(Contact_UKSUPDEV_Spark_df)
    Final_df = creating_final_df(Contact_UKSUPDEV_Spark_df,Address_Contact_Name_df,Recordtype_ID_out_df,Address_Line_6__c_df,Address_Line_7__c_df,Address_Line_8__c_df,Address_Line_9__c_df)
    alter_final_df = alter_final_df(Final_df)
    aggregate_final_df = aggregate_final_df(alter_final_df)
    Address_ID_df = Address_ID_ETL(aggregate_final_df,LKP_SF_Target_AMS_ADDRESS_Spark_df)
    Filter_Final_df = Filter_Final_df(Address_ID_df)

