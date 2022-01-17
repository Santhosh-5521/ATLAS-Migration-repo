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

Sel_Col_List = ['CurrencyIsoCode','CreatedDate','CreatedById','LastModifiedDate','LastModifiedById','Client_Brand__c','Publisher__c','Supdev_Client_ID','Src_Client_ID']
Sel_Col_Final_List = ['Id','CurrencyIsoCode','CreatedDate','CreatedById','LastModifiedDate','LastModifiedById','Client_Brand__c','Publisher__c','Created_Date','LastModified_Date']

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
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name,CurrencyIsoCode,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById
                     FROM Client_Brand__c where
                     Id in ('a1n3M0000005UqNQAU') """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Client_Brand_df):
    Client_Brand_df = Client_Brand_df.astype(str)
    Client_Brand_df = Client_Brand_df.fillna('Null')
    Client_Brand_df_Spark_df = spark.createDataFrame(Client_Brand_df)
    return Client_Brand_df_Spark_df
    
def LKP_SF_Target_Publisher_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, Publisher_Name__c
                     from Publisher__c """,sf)
                    
    return df
    
def LKP_SF_SUPDEV_ACCOUNT_data():
    sf=SF_connectivity_Supdev()
    df=getSFData(""" select Id, Name
                     from Account
                     where RecordTypeId ='012b0000000QHwRAAW' """,sf)
                    
    return df
    
def LKP_TGT_PUBLISHER_CLIENT_BRAND():
    Lkp_Tgt_Publisher_Client_Brand_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Publisher_Client_Brand\Target\Tgt_Publisher_Client_Brand.csv")\
                                  .select('Id','Created_Date','LastModifiedDate','Client_Brand__c','Publisher__c')
                                                       
    return Lkp_Tgt_Publisher_Client_Brand_df
    
def Publisher__c_ETL(Client_Brand_Spark_df,LKP_SF_Target_Publisher_data_Spark_df):

    Client_Brand_Spark_df = Client_Brand_Spark_df.withColumn('Publishe_Name_in', F.lit('CN China'))
    
    Client_Brand_Spark_df = Client_Brand_Spark_df.alias('Client_Brand_Spark_df')
    LKP_SF_Target_Publisher_data_Spark_df = LKP_SF_Target_Publisher_data_Spark_df.alias('LKP_SF_Target_Publisher_data_Spark_df')
    
    Publisher__c_df = Client_Brand_Spark_df.join(LKP_SF_Target_Publisher_data_Spark_df,Client_Brand_Spark_df.Publishe_Name_in == LKP_SF_Target_Publisher_data_Spark_df.Publisher_Name__c,"left")\
                         .select(Client_Brand_Spark_df.Id,col('LKP_SF_Target_Publisher_data_Spark_df.Id').alias('Publisher__c'))
                         
    return Publisher__c_df
    
def Supdev_Client_ID_ETL(Client_Brand_Spark_df,LKP_SF_SUPDEV_ACCOUNT_data_Spark_df):

    Client_Brand_Spark_df = Client_Brand_Spark_df.withColumnRenamed('Name','Name_in')

    Client_Brand_Spark_df = Client_Brand_Spark_df.alias('Client_Brand_Spark_df')
    LKP_SF_SUPDEV_ACCOUNT_data_Spark_df = LKP_SF_SUPDEV_ACCOUNT_data_Spark_df.alias('LKP_SF_SUPDEV_ACCOUNT_data_Spark_df')
    
    Supdev_Client_ID_df = Client_Brand_Spark_df.join(LKP_SF_SUPDEV_ACCOUNT_data_Spark_df,Client_Brand_Spark_df.Name_in == LKP_SF_SUPDEV_ACCOUNT_data_Spark_df.Name,"left")\
                                               .select(Client_Brand_Spark_df.Id,col('LKP_SF_SUPDEV_ACCOUNT_data_Spark_df.Id').alias('Supdev_Client_ID'))

    return Supdev_Client_ID_df
    
def Src_Client_ID_ETL(Client_Brand_Spark_df,LKP_SF_Target_Publisher_data_Spark_df,Lkp_Tgt_Publisher_Client_Brand_df):
    Client_Brand_Spark_df = Client_Brand_Spark_df.withColumn('Publishe_Name_in', F.lit('CN China'))
    
    Client_Brand_Spark_df = Client_Brand_Spark_df.alias('Client_Brand_Spark_df')
    LKP_SF_Target_Publisher_data_Spark_df = LKP_SF_Target_Publisher_data_Spark_df.alias('LKP_SF_Target_Publisher_data_Spark_df')
    
    Publisher__c_df = Client_Brand_Spark_df.join(LKP_SF_Target_Publisher_data_Spark_df,Client_Brand_Spark_df.Publishe_Name_in == LKP_SF_Target_Publisher_data_Spark_df.Publisher_Name__c,"left")\
                                           .select(Client_Brand_Spark_df.Id,col('LKP_SF_Target_Publisher_data_Spark_df.Id').alias('Publisher__c'))

    Publisher__c_df = Publisher__c_df.alias('Publisher__c_df')

    Client_ID_join_df = Client_Brand_Spark_df.join(Publisher__c_df,Client_Brand_Spark_df.Id == Publisher__c_df.Id,"inner")\
                                             .select(col('Client_Brand_Spark_df.Id').alias('Client_ID'),col('Publisher__c_df.Id').alias('Publisher_ID'))

    Client_ID_join_df = Client_ID_join_df.alias('Client_ID_join_df')
    Lkp_Tgt_Publisher_Client_Brand_df = Lkp_Tgt_Publisher_Client_Brand_df.alias('Lkp_Tgt_Publisher_Client_Brand_df')

    cond = [Client_ID_join_df.Client_ID == Lkp_Tgt_Publisher_Client_Brand_df.Client_Brand__c,Client_ID_join_df.Publisher_ID == Lkp_Tgt_Publisher_Client_Brand_df.Publisher__c]

    Src_Client_ID_df = Client_ID_join_df.join(Lkp_Tgt_Publisher_Client_Brand_df,cond,"left")\
                                        .select(col('Client_ID_join_df.Client_ID').alias('Id'),col('Lkp_Tgt_Publisher_Client_Brand_df.Id').alias('Src_Client_ID'))
    
    return Src_Client_ID_df
    
def creating_final_df(Client_Brand_Spark_df,Publisher__c_df,Supdev_Client_ID_df,Src_Client_ID_df):
    Client_Brand_Spark_df = Client_Brand_Spark_df.alias('Client_Brand_Spark_df')
    Publisher__c_df = Publisher__c_df.alias('Publisher__c_df')
    Supdev_Client_ID_df = Supdev_Client_ID_df.alias('Supdev_Client_ID_df')
    Src_Client_ID_df = Src_Client_ID_df.alias('Src_Client_ID_df')
    
    Final_Df = Client_Brand_Spark_df.join(Publisher__c_df, col('Client_Brand_Spark_df.Id') == col('Publisher__c_df.Id'), 'left') \
                                    .join(Supdev_Client_ID_df, col('Client_Brand_Spark_df.Id') == col('Supdev_Client_ID_df.Id'), 'left') \
                                    .join(Src_Client_ID_df, col('Client_Brand_Spark_df.Id') == col('Src_Client_ID_df.Id'), 'left') \
                                    .select('Client_Brand_Spark_df.*','Publisher__c_df.Publisher__c','Supdev_Client_ID_df.Supdev_Client_ID','Src_Client_ID_df.Src_Client_ID')
    
    return Final_Df

def alter_final_df(Final_df):
    
    alter_final_df = Final_df.withColumnRenamed('Id','Client_Brand__c')\
                             .select(*Sel_Col_List)
                             
    alter_final_df = alter_final_df.sort(col("Client_Brand__c").asc())
                             
    return alter_final_df
    
def Filter_df(alter_final_df):
    Filter_Final_df = alter_final_df.filter((col("Supdev_Client_ID").isNotNull()) & (col("Src_Client_ID").isNull()))
    return Filter_Final_df
    
def Handling_Insert_records(Final_Publisher_Client_Brand_filter_df):
    Ins_records_df_index = Final_Publisher_Client_Brand_filter_df.withColumn("idx",F.monotonically_increasing_id())
    windowSpec = Window.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModified_Date',current_timestamp())
    
    return Ins_records_df_index
    
if __name__ == "__main__":
    start = time.time()
    Client_Brand_df = get_input_data()
    Client_Brand_Spark_df = pandas_to_pyspark_df(Client_Brand_df)
    LKP_SF_Target_Publisher_data_df = LKP_SF_Target_Publisher_data()
    LKP_SF_Target_Publisher_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_data_df)
    Lkp_Tgt_Publisher_Client_Brand_df = LKP_TGT_PUBLISHER_CLIENT_BRAND()
    LKP_SF_SUPDEV_ACCOUNT_data_df = LKP_SF_SUPDEV_ACCOUNT_data()
    LKP_SF_SUPDEV_ACCOUNT_data_Spark_df = pandas_to_pyspark_df(LKP_SF_SUPDEV_ACCOUNT_data_df)
    Publisher__c_df = Publisher__c_ETL(Client_Brand_Spark_df,LKP_SF_Target_Publisher_data_Spark_df)
    Supdev_Client_ID_df = Supdev_Client_ID_ETL(Client_Brand_Spark_df,LKP_SF_SUPDEV_ACCOUNT_data_Spark_df)
    Src_Client_ID_df = Src_Client_ID_ETL(Client_Brand_Spark_df,LKP_SF_Target_Publisher_data_Spark_df,Lkp_Tgt_Publisher_Client_Brand_df)
    Final_df = creating_final_df(Client_Brand_Spark_df,Publisher__c_df,Supdev_Client_ID_df,Src_Client_ID_df)
    alter_final_df = alter_final_df(Final_df)
    Final_Publisher_Client_Brand_filter_df = Filter_df(alter_final_df)
    Final_Publisher_Client_Brand_df = Handling_Insert_records(Final_Publisher_Client_Brand_filter_df)
    Final_Publisher_Client_Brand_select_df = Final_Publisher_Client_Brand_df.select(*Sel_Col_Final_List)
    Final_Publisher_Client_Brand_distinct_df = Final_Publisher_Client_Brand_select_df.dropDuplicates(["Client_Brand__c","Publisher__c"])
    
    src_count = Client_Brand_Spark_df.count()
    Tgt_count = Final_Publisher_Client_Brand_distinct_df.count()
    
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Tgt_count)
    
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))