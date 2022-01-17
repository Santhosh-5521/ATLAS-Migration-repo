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

Sel_Col_List = ['Src_Id','CurrencyIsoCode','CreatedDate','CreatedById','Client_Brand__c','Publisher_Account__c','PUB_ACC_CLI_ID']
Sel_Col_Final_List = ['Id','CurrencyIsoCode','CreatedDate','CreatedById','Client_Brand__c','Publisher_Account__c','Created_Date','LastModified_Date']

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
    df=getSFData(""" SELECT Id,Name
                     FROM Account where
                     RecordTypeId ='012b0000000QHwRAAW'
                     and Id in ('001b000000WHJEYAA5') """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Account_UKSUPDEV_df):
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.astype(str)
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.fillna('Null')
    Account_UKSUPDEV_Spark_df = spark.createDataFrame(Account_UKSUPDEV_df)
    return Account_UKSUPDEV_Spark_df
    
def LKP_SF_Target_Publisher_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,CurrencyIsoCode,CreatedDate,CreatedById,Account_Name__c
                     FROM Publisher_Account__c
                     Where Publisher_Name__c ='CN China' """,sf)
                    
    return df
    
def LKP_SF_Target_Client_Brand_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name 
                     FROM Client_Brand__c """,sf)
                    
    return df
    
def LKP_TGT_PUBLISHER_ACCOUNT_CLIENT_BRAND():
    Lkp_Tgt_Publisher_Client_Brand_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Publisher_Account_Client_Brand\Target\Tgt_Publisher_Account_Client_Brand.csv")\
                                  .select('Id','Created_Date','LastModified_Date','Client_Brand__c','Publisher_Account__c')
                                                       
    return Lkp_Tgt_Publisher_Client_Brand_df
    
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
    
def Publisher_Account_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Publisher_Account_data_Spark_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    Acc_Name_ENG_BY_ID_df = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,Account_UKSUPDEV_Spark_df.Id == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                     .select(Account_UKSUPDEV_Spark_df.Id,col('Lkp_English_Translation_df.English_Translation').alias('Acc_Name_ENG_BY_ID'))
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    Acc_Name_ENG_BY_Name_df = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,Account_UKSUPDEV_Spark_df.Name == Lkp_English_Translation_df.Account_Name,"left")\
                                                       .select(Account_UKSUPDEV_Spark_df.Id,col('Lkp_English_Translation_df.English_Translation').alias('Acc_Name_ENG_BY_Name'))
    
    Acc_Name_ENG_BY_ID_df = Acc_Name_ENG_BY_ID_df.alias('Acc_Name_ENG_BY_ID_df')
    Acc_Name_ENG_BY_Name_df = Acc_Name_ENG_BY_Name_df.alias('Acc_Name_ENG_BY_Name_df')

    Acc_Name_ENG_join_df = Acc_Name_ENG_BY_ID_df.join(Acc_Name_ENG_BY_Name_df,Acc_Name_ENG_BY_ID_df.Id == Acc_Name_ENG_BY_Name_df.Id,"left")\
                                                .select(Acc_Name_ENG_BY_ID_df.Id,Acc_Name_ENG_BY_ID_df.Acc_Name_ENG_BY_ID,Acc_Name_ENG_BY_Name_df.Acc_Name_ENG_BY_Name)
    
    Acc_Name_ENG_df = Acc_Name_ENG_join_df.withColumn('Tgt_Acc_Name',when(Acc_Name_ENG_join_df.Acc_Name_ENG_BY_ID.isNull(),Acc_Name_ENG_join_df.Acc_Name_ENG_BY_Name)
                                                                    .otherwise(Acc_Name_ENG_join_df.Acc_Name_ENG_BY_ID))
    
    Acc_Name_ENG_df = Acc_Name_ENG_df.alias('Acc_Name_ENG_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    Acc_Name_Uni_df = Acc_Name_ENG_df.join(Lkp_Account_Dedupe_df,Acc_Name_ENG_df.Tgt_Acc_Name == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                     .select(Acc_Name_ENG_df.Id,col('Lkp_Account_Dedupe_df.Atlas_Account_Name').alias('Acc_Name_Uni'))
    
    Acc_Name_ENG_df = Acc_Name_ENG_df.alias('Acc_Name_ENG_df')
    Acc_Name_Uni_df = Acc_Name_Uni_df.alias('Acc_Name_Uni_df')

    Acc_Name_join_df = Acc_Name_ENG_df.join(Acc_Name_Uni_df,Acc_Name_ENG_df.Id == Acc_Name_Uni_df.Id,"left")\
                                      .select(Acc_Name_ENG_df.Id,col('Acc_Name_ENG_df.Tgt_Acc_Name').alias('Acc_Name_ENG'),Acc_Name_Uni_df.Acc_Name_Uni)

    LKP_Acc_Name_df = Acc_Name_join_df.withColumn('LKP_Acc_Name',when(Acc_Name_join_df.Acc_Name_Uni.isNull(),Acc_Name_join_df.Acc_Name_ENG)
                                                                .otherwise(Acc_Name_join_df.Acc_Name_Uni))
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_Acc_Name_df = LKP_Acc_Name_df.alias('LKP_Acc_Name_df')

    O_Acc_Name_join_df = Account_UKSUPDEV_Spark_df.join(LKP_Acc_Name_df,Account_UKSUPDEV_Spark_df.Id == LKP_Acc_Name_df.Id,"left")\
                                                  .select(Account_UKSUPDEV_Spark_df.Id,Account_UKSUPDEV_Spark_df.Name,LKP_Acc_Name_df.LKP_Acc_Name)
    
    O_Acc_Name_df = O_Acc_Name_join_df.withColumn('O_Acc_Name',when(O_Acc_Name_join_df.LKP_Acc_Name.isNull(),O_Acc_Name_join_df.Name)
                                                               .otherwise(O_Acc_Name_join_df.LKP_Acc_Name))
    
    O_Acc_Name_df = O_Acc_Name_df.alias('O_Acc_Name_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    Tgt_Pub_Account_join_df = O_Acc_Name_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,O_Acc_Name_df.O_Acc_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Account_Name__c,"left")\
                                           .select(O_Acc_Name_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Publisher_Acc_ID'),LKP_SF_Target_Publisher_Account_data_Spark_df.CurrencyIsoCode,LKP_SF_Target_Publisher_Account_data_Spark_df.CreatedDate,LKP_SF_Target_Publisher_Account_data_Spark_df.CreatedById)

    return Tgt_Pub_Account_join_df
    
def Client_Brand__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Client_Brand_data_Spark_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_Client_Brand_data_Spark_df = LKP_SF_Target_Client_Brand_data_Spark_df.alias('LKP_SF_Target_Client_Brand_data_Spark_df')
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.withColumnRenamed('Name','Name_in')

    Client_Brand__c_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_Client_Brand_data_Spark_df,Account_UKSUPDEV_Spark_df.Name_in == LKP_SF_Target_Client_Brand_data_Spark_df.Name,"left")\
                                                     .select(Account_UKSUPDEV_Spark_df.Id,col('LKP_SF_Target_Client_Brand_data_Spark_df.Id').alias('Client_Brand__c'))
    
    return Client_Brand__c_df
    
def Publisher_Account__c_ETL(Tgt_Pub_Account_join_df):
    Publisher_Account__c_df = Tgt_Pub_Account_join_df.select('Id',col('Publisher_Acc_ID').alias('Publisher_Account__c'))
    
    return Publisher_Account__c_df
    
def PUB_ACC_CLI_ID_ETL(Tgt_Pub_Account_join_df,Lkp_Tgt_Publisher_Account_Client_Brand_df,Client_Brand__c_df):
    Tgt_Pub_Account_join_df = Tgt_Pub_Account_join_df.alias('Tgt_Pub_Account_join_df')
    Client_Brand__c_df = Client_Brand__c_df.alias('Client_Brand__c_df')
    
    Tgt_Pub_Account_Cli_Brand_join_df = Tgt_Pub_Account_join_df.join(Client_Brand__c_df,Tgt_Pub_Account_join_df.Id == Client_Brand__c_df.Id,"left")\
                                                               .select('Tgt_Pub_Account_join_df.*',col('Client_Brand__c_df.Client_Brand__c').alias('Clinet_Brand_Id'))
                                                               
    Tgt_Pub_Account_Cli_Brand_join_df = Tgt_Pub_Account_Cli_Brand_join_df.alias('Tgt_Pub_Account_Cli_Brand_join_df')
    Lkp_Tgt_Publisher_Account_Client_Brand_df = Lkp_Tgt_Publisher_Account_Client_Brand_df.alias('Lkp_Tgt_Publisher_Account_Client_Brand_df')
    
    cond = [Tgt_Pub_Account_Cli_Brand_join_df.Clinet_Brand_Id == Lkp_Tgt_Publisher_Account_Client_Brand_df.Client_Brand__c,Tgt_Pub_Account_Cli_Brand_join_df.Publisher_Acc_ID == Lkp_Tgt_Publisher_Account_Client_Brand_df.Publisher_Account__c]

    PUB_ACC_CLI_ID_df = Tgt_Pub_Account_Cli_Brand_join_df.join(Lkp_Tgt_Publisher_Account_Client_Brand_df,cond,"left")\
                                                         .select(Tgt_Pub_Account_Cli_Brand_join_df.Id,col('Lkp_Tgt_Publisher_Account_Client_Brand_df.Id').alias('PUB_ACC_CLI_ID'))
    
    return PUB_ACC_CLI_ID_df
    
def creating_final_df(Account_UKSUPDEV_Spark_df,Tgt_Pub_Account_join_df,Client_Brand__c_df,Publisher_Account__c_df,PUB_ACC_CLI_ID_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Tgt_Pub_Account_join_df = Tgt_Pub_Account_join_df.alias('Tgt_Pub_Account_join_df')
    Client_Brand__c_df = Client_Brand__c_df.alias('Client_Brand__c_df')
    Publisher_Account__c_df = Publisher_Account__c_df.alias('Publisher_Account__c_df')
    PUB_ACC_CLI_ID_df = PUB_ACC_CLI_ID_df.alias('PUB_ACC_CLI_ID_df')
    
    Final_Df = Account_UKSUPDEV_Spark_df.join(Tgt_Pub_Account_join_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Tgt_Pub_Account_join_df.Id'), 'left') \
                                        .join(Client_Brand__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Client_Brand__c_df.Id'), 'left') \
                                        .join(Publisher_Account__c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Publisher_Account__c_df.Id'), 'left') \
                                        .join(PUB_ACC_CLI_ID_df, col('Account_UKSUPDEV_Spark_df.Id') == col('PUB_ACC_CLI_ID_df.Id'), 'left') \
                                        .select('Account_UKSUPDEV_Spark_df.*','Tgt_Pub_Account_join_df.CurrencyIsoCode','Tgt_Pub_Account_join_df.CreatedDate','Tgt_Pub_Account_join_df.CreatedById','Client_Brand__c_df.Client_Brand__c','Publisher_Account__c_df.Publisher_Account__c','PUB_ACC_CLI_ID_df.PUB_ACC_CLI_ID')
    
    return Final_Df
    
def alter_final_df(Final_df):
    
    alter_final_df = Final_df.withColumnRenamed('Id','Src_Id')\
                             .select(*Sel_Col_List)
                             
    alter_final_df = alter_final_df.sort(col("Src_Id").asc())
                             
    return alter_final_df
    
def Filter_df(alter_final_df):
    Filter_Final_df = alter_final_df.filter((col("Client_Brand__c").isNotNull()) & (col("PUB_ACC_CLI_ID").isNull()))
    return Filter_Final_df
    
def Handling_Insert_records(Final_PublisherAccount_Client_Brand_filter_df):
    Ins_records_df_index = Final_PublisherAccount_Client_Brand_filter_df.withColumn("idx",F.monotonically_increasing_id())
    windowSpec = Window.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx","Src_Id")
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModified_Date',current_timestamp())
    
    return Ins_records_df_index
    
     
if __name__ == "__main__":
    start = time.time()
    Account_UKSUPDEV_df = get_input_data()
    Account_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Account_UKSUPDEV_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    LKP_SF_Target_Client_Brand_data_df = LKP_SF_Target_Client_Brand_data()
    LKP_SF_Target_Client_Brand_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Client_Brand_data_df)
    Lkp_Tgt_Publisher_Account_Client_Brand_df = LKP_TGT_PUBLISHER_ACCOUNT_CLIENT_BRAND()
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    Tgt_Pub_Account_join_df = Publisher_Account_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Publisher_Account_data_Spark_df)
    Client_Brand__c_df = Client_Brand__c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_Client_Brand_data_Spark_df)
    Publisher_Account__c_df = Publisher_Account__c_ETL(Tgt_Pub_Account_join_df)
    PUB_ACC_CLI_ID_df = PUB_ACC_CLI_ID_ETL(Tgt_Pub_Account_join_df,Lkp_Tgt_Publisher_Account_Client_Brand_df,Client_Brand__c_df)
    Final_df = creating_final_df(Account_UKSUPDEV_Spark_df,Tgt_Pub_Account_join_df,Client_Brand__c_df,Publisher_Account__c_df,PUB_ACC_CLI_ID_df)
    alter_final_df = alter_final_df(Final_df)
    Final_PublisherAccount_Client_Brand_filter_df = Filter_df(alter_final_df)
    Final_PublisherAccount_Client_Brand_df = Handling_Insert_records(Final_PublisherAccount_Client_Brand_filter_df)
    Final_PublisherAccount_Client_Brand_select_df = Final_PublisherAccount_Client_Brand_df.select(*Sel_Col_Final_List)
    Final_PublisherAccount_Client_Brand_distinct_df = Final_PublisherAccount_Client_Brand_select_df.dropDuplicates(["Client_Brand__c","Publisher_Account__c"])
    
    src_count = Account_UKSUPDEV_Spark_df.count()
    Tgt_count = Final_PublisherAccount_Client_Brand_distinct_df.count()
    
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Tgt_count)
    
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))
    