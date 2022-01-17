from simple_salesforce import Salesforce, SalesforceLogin
from pyspark.sql.functions import when
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import instr, upper, initcap
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

Sel_Col_List = ['Name', 'RecordTypeId', 'Address__c', 'Account__c', 'Address_Type__c', 'Contact__c', 'End_Date__c',
                'Market_Account__c', 'Market__c', 'Start_Date__c']
Sel_Col_Final_List = ['Id', 'Name', 'RecordTypeId', 'Address__c', 'Account__c', 'Address_Type__c', 'Contact__c',
                      'End_Date__c', 'Market_Account__c', 'Market__c', 'Start_Date__c']


##### Function to establish connectivity to Supdev Salesforce ##############
def SF_connectivity_Supdev():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.supdev',
                                           password='sfDc1234', security_token='kvtGqGhKGlm0PNa8rkFkyaPX',
                                           domain='test')
    sf = Salesforce(instance=instance, session_id=session_id)
    return sf


##### Function to establish connectivity to MIG2 Salesforce ##############
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
    sf = SF_connectivity_MIG2()
    df = getSFData(""" SELECT Id,Name,Address_Line_8__c,Address_Line_9__c,CreatedDate
                     FROM AMS_Address__c where
                     Address_Line_6__c='CN China'
                     and Id in ('a1O3M0000005wMHUAY','a1O3M0000005wMIUAY') """, sf)

    return df


def pandas_to_pyspark_df(AMS_Address_df):
    AMS_Address_df = AMS_Address_df.astype(str)
    AMS_Address_df = AMS_Address_df.fillna('Null')
    AMS_Address_Spark_df = spark.createDataFrame(AMS_Address_df)
    return AMS_Address_Spark_df


def LKP_SF_Target_RecordType_data():
    sf = SF_connectivity_MIG2()
    df = getSFData(""" SELECT Id,SobjectType,Name,DeveloperName
                     FROM RecordType """, sf)

    return df


def Recordtype_ID_ETL(AMS_Address_Spark_df, LKP_SF_Target_RecordType_Spark_df):
    AMS_Address_Spark_df = AMS_Address_Spark_df.alias('AMS_Address_Spark_df')

    v_Type_df = AMS_Address_Spark_df.withColumn('v_Type',
                                                when(instr(col('AMS_Address_Spark_df.Name'), '-Contact') > 0, 'Contact')
                                                .when(instr(col('AMS_Address_Spark_df.Name'), '-Account') > 0,
                                                      'Account')
                                                .otherwise("Market Account")) \
        .withColumn('sobject_type_in', F.lit('AMS_Used_Address__c'))

    v_Type_df = v_Type_df.alias('v_Type_df')
    LKP_SF_Target_RecordType_Spark_df = LKP_SF_Target_RecordType_Spark_df.alias('LKP_SF_Target_RecordType_Spark_df')

    cond = [v_Type_df.sobject_type_in == LKP_SF_Target_RecordType_Spark_df.SobjectType,
            v_Type_df.v_Type == LKP_SF_Target_RecordType_Spark_df.Name]

    Recordtype_ID_df = v_Type_df.join(LKP_SF_Target_RecordType_Spark_df, cond, "left") \
        .select('v_Type_df.Id', col('LKP_SF_Target_RecordType_Spark_df.Id').alias('RecordTypeId'))

    return Recordtype_ID_df


def Account__c_ETL(AMS_Address_Spark_df):
    AMS_Address_Spark_df = AMS_Address_Spark_df.alias('AMS_Address_Spark_df')

    v_Type_df = AMS_Address_Spark_df.withColumn('v_Type',
                                                when(instr(col('AMS_Address_Spark_df.Name'), '-Contact') > 0, 'Contact')
                                                .when(instr(col('AMS_Address_Spark_df.Name'), '-Account') > 0,
                                                      'Account')
                                                .otherwise("Market Account"))

    Account__c_df = v_Type_df.withColumn('Account__c', when(v_Type_df.v_Type == 'Account', v_Type_df.Address_Line_8__c)
                                         .otherwise('Null')) \
        .select('Id', 'Account__c')

    return Account__c_df


def Contact__c_ETL(AMS_Address_Spark_df):
    AMS_Address_Spark_df = AMS_Address_Spark_df.alias('AMS_Address_Spark_df')

    v_Type_df = AMS_Address_Spark_df.withColumn('v_Type',
                                                when(instr(col('AMS_Address_Spark_df.Name'), '-Contact') > 0, 'Contact')
                                                .when(instr(col('AMS_Address_Spark_df.Name'), '-Account') > 0,
                                                      'Account')
                                                .otherwise("Market Account"))

    Contact__c_df = v_Type_df.withColumn('Contact__c', when(v_Type_df.v_Type == 'Contact', v_Type_df.Address_Line_8__c)
                                         .otherwise('Null')) \
        .select('Id', 'Contact__c')

    return Contact__c_df


def Market_Account__c_ETL(AMS_Address_Spark_df):
    AMS_Address_Spark_df = AMS_Address_Spark_df.alias('AMS_Address_Spark_df')

    v_Type_df = AMS_Address_Spark_df.withColumn('v_Type',
                                                when(instr(col('AMS_Address_Spark_df.Name'), '-Contact') > 0, 'Contact')
                                                .when(instr(col('AMS_Address_Spark_df.Name'), '-Account') > 0,
                                                      'Account')
                                                .otherwise("Market Account"))

    Market_Account__c_df = v_Type_df.withColumn('Market_Account__c',
                                                when(v_Type_df.v_Type == 'Market Account', v_Type_df.Address_Line_8__c)
                                                .otherwise('Null')) \
        .select('Id', 'Market_Account__c')

    return Market_Account__c_df


def End_Date__c_ETL(AMS_Address_Spark_df):
    End_Date__c_df = AMS_Address_Spark_df.withColumn('O_End_Date__c', F.lit('2050-01-01')) \
        .withColumn('End_Date__c', to_date(col('O_End_Date__c'), "yyyy-MM-DD")) \
        .select('Id', 'End_Date__c')

    return End_Date__c_df


def Start_Date__c_ETL(AMS_Address_Spark_df):
    Start_date_Df = AMS_Address_Spark_df.withColumn('year', substring('CreatedDate', 1, 4)) \
        .withColumn('month', substring('CreatedDate', 6, 2)) \
        .withColumn('day', substring('CreatedDate', 9, 2)) \
        .withColumn('Start_Date__c',
                    ltrim(rtrim(concat(col('year'), F.lit('-'), col('month'), F.lit('-'), col('day'))))) \
        .select('Id', 'Start_Date__c')

    return Start_date_Df


def creating_final_df(AMS_Address_Spark_df, Recordtype_ID_df, Account__c_df, Contact__c_df, Market_Account__c_df,
                      End_Date__c_df, Start_date_Df):
    AMS_Address_Spark_df = AMS_Address_Spark_df.alias('AMS_Address_Spark_df')
    Recordtype_ID_df = Recordtype_ID_df.alias('Recordtype_ID_df')
    Account__c_df = Account__c_df.alias('Account__c_df')
    Contact__c_df = Contact__c_df.alias('Contact__c_df')
    Market_Account__c_df = Market_Account__c_df.alias('Market_Account__c_df')
    End_Date__c_df = End_Date__c_df.alias('End_Date__c_df')
    Start_date_Df = Start_date_Df.alias('Start_date_Df')

    Final_Df = AMS_Address_Spark_df.join(Recordtype_ID_df, col('AMS_Address_Spark_df.Id') == col('Recordtype_ID_df.Id'),
                                         'left') \
        .join(Account__c_df, col('AMS_Address_Spark_df.Id') == col('Account__c_df.Id'), 'left') \
        .join(Contact__c_df, col('AMS_Address_Spark_df.Id') == col('Contact__c_df.Id'), 'left') \
        .join(Market_Account__c_df, col('AMS_Address_Spark_df.Id') == col('Market_Account__c_df.Id'), 'left') \
        .join(End_Date__c_df, col('AMS_Address_Spark_df.Id') == col('End_Date__c_df.Id'), 'left') \
        .join(Start_date_Df, col('AMS_Address_Spark_df.Id') == col('Start_date_Df.Id'), 'left') \
        .select('AMS_Address_Spark_df.*', 'Recordtype_ID_df.RecordTypeId', 'Account__c_df.Account__c',
                'Contact__c_df.Contact__c', 'Market_Account__c_df.Market_Account__c', 'End_Date__c_df.End_Date__c',
                'Start_date_Df.Start_Date__c')

    return Final_Df


def alter_final_df(Final_df):
    Final_df = Final_df.drop('Address_Line_8__c', 'FirstName', 'LastName', 'Email', 'MailingCountryCode')

    alter_final_df = Final_df.withColumnRenamed('Id', 'Address__c') \
        .withColumnRenamed('Address_Line_9__c', 'Address_Type__c') \
        .withColumn('Market__c', F.lit('CN China')) \
        .select(*Sel_Col_List)

    alter_final_df = alter_final_df.sort(col("Address__c").asc())

    return alter_final_df


def Handling_Insert_records(alter_final_df):
    Ins_records_df_index = alter_final_df.withColumn("idx", F.monotonically_increasing_id())
    windowSpec = Window.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date', current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate', current_timestamp())

    return Ins_records_df_index


if __name__ == "__main__":
    start = time.time()
    AMS_Address_df = get_input_data()
    AMS_Address_Spark_df = pandas_to_pyspark_df(AMS_Address_df)
    LKP_SF_Target_RecordType_df = LKP_SF_Target_RecordType_data()
    LKP_SF_Target_RecordType_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_RecordType_df)
    Recordtype_ID_df = Recordtype_ID_ETL(AMS_Address_Spark_df, LKP_SF_Target_RecordType_Spark_df)
    Account__c_df = Account__c_ETL(AMS_Address_Spark_df)
    Contact__c_df = Contact__c_ETL(AMS_Address_Spark_df)
    Market_Account__c_df = Market_Account__c_ETL(AMS_Address_Spark_df)
    End_Date__c_df = End_Date__c_ETL(AMS_Address_Spark_df)
    Start_date_Df = Start_Date__c_ETL(AMS_Address_Spark_df)
    Final_df = creating_final_df(AMS_Address_Spark_df, Recordtype_ID_df, Account__c_df, Contact__c_df,
                                 Market_Account__c_df, End_Date__c_df, Start_date_Df)
    alter_final_df = alter_final_df(Final_df)
    Final_Used_Address_df = Handling_Insert_records(alter_final_df)
    Final_Used_Address_select_df = Final_Used_Address_df.select(*Sel_Col_Final_List)

    src_count = AMS_Address_Spark_df.count()
    Tgt_count = Final_Used_Address_select_df.count()

    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ", Tgt_count)

    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time / 60
    print("\n Total time taken in Minutes: " + str(total_time_in_mins))
