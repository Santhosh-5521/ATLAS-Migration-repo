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

Sel_Col_List = ['Agency__c','Agency_Account__c','Client_Account__c','Client__c','Relationship_Period__c','Start_Date__c']
Sel_Col_Final_List = ['Id','Agency__c','Agency_Account__c','Client_Account__c','Client__c','Relationship_Period__c','Start_Date__c']

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
    df=getSFData(""" SELECT Id,CNI_Agency_Account__c,CHN_Brand_Account__c,CNI_Relationship_Start_Date__c
                     FROM CNI_Agency_Brand_Relationship__c where
                     CHN_Brand_Account__c <> '' and CNI_Agency_Account__c <> '' """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Contact_UKSUPDEV_df):
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.astype(str)
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.fillna('Null')
    Contact_UKSUPDEV_Spark_df = spark.createDataFrame(Contact_UKSUPDEV_df)
    return Contact_UKSUPDEV_Spark_df
    
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
    
def Agency__c_ETL(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df):
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.alias('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    cond = [CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.CNI_Agency_Account__c == LKP_SF_Target_Account_data_Spark_df.External_UID__c]
    
    lkp_Account_Agency_By_ID_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.join(LKP_SF_Target_Account_data_Spark_df,cond,"left")\
                                                                                 .select('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id',col('LKP_SF_Target_Account_data_Spark_df.Id').alias('lkp_Account_Agency_By_ID'))
    
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.alias('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    lkp_Account_Translation_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.join(Lkp_English_Translation_df,CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.CNI_Agency_Account__c == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                                                .select(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id,Lkp_English_Translation_df.English_Translation)
    
    lkp_Account_Translation_df = lkp_Account_Translation_df.alias('lkp_Account_Translation_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    lkp_AccName_Dedupe_df = lkp_Account_Translation_df.join(Lkp_Account_Dedupe_df,lkp_Account_Translation_df.English_Translation == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                      .select(lkp_Account_Translation_df.Id,lkp_Account_Translation_df.English_Translation,Lkp_Account_Dedupe_df.Atlas_Account_Name)
    
    Target_Name_df = lkp_AccName_Dedupe_df.withColumn('Tgt_Name',when(lkp_AccName_Dedupe_df.Atlas_Account_Name.isNull(),lkp_AccName_Dedupe_df.English_Translation)
                                                               .otherwise(lkp_AccName_Dedupe_df.Atlas_Account_Name)).select('Id','Tgt_Name')
    
    Target_Name_df = Target_Name_df.alias('Target_Name_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    Account_By_Name_df = Target_Name_df.join(LKP_SF_Target_Account_data_Spark_df,Target_Name_df.Tgt_Name == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                       .select(Target_Name_df.Id,col('LKP_SF_Target_Account_data_Spark_df.Id').alias('Account_By_Name_ID'))
    
    lkp_Account_Agency_By_ID_df = lkp_Account_Agency_By_ID_df.alias('lkp_Account_Agency_By_ID_df')
    Account_By_Name_df = Account_By_Name_df.alias('Account_By_Name_df')

    Agency_Account_join_df = lkp_Account_Agency_By_ID_df.join(Account_By_Name_df,lkp_Account_Agency_By_ID_df.Id == Account_By_Name_df.Id,"inner")\
                                                        .select(lkp_Account_Agency_By_ID_df.Id,lkp_Account_Agency_By_ID_df.lkp_Account_Agency_By_ID,Account_By_Name_df.Account_By_Name_ID)
    
    Agency_Account_df = Agency_Account_join_df.withColumn('Agency_Account__c',when(Agency_Account_join_df.lkp_Account_Agency_By_ID.isNull(),Agency_Account_join_df.Account_By_Name_ID)
                                                                             .otherwise(Agency_Account_join_df.lkp_Account_Agency_By_ID)).select('Id','Agency_Account__c')
    
    
    Agency_Account_df = Agency_Account_df.alias('Agency_Account_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    Agency__c_df = Agency_Account_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,Agency_Account_df.Agency_Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c,"left")\
                                              .select(Agency_Account_df.Id,Agency_Account_df.Agency_Account__c,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Agency__c'))
    
    return Agency__c_df
    
def Client__c_ETL(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df):
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.alias('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    cond = [CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.CHN_Brand_Account__c == LKP_SF_Target_Account_data_Spark_df.External_UID__c]
    
    lkp_Account_Brand_ID_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.join(LKP_SF_Target_Account_data_Spark_df,cond,"left")\
                                                                                 .select('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id',col('LKP_SF_Target_Account_data_Spark_df.Id').alias('lkp_Account_Brand_ID'))
    
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.alias('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    lkp_Account_Translation_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.join(Lkp_English_Translation_df,CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.CHN_Brand_Account__c == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                                                .select(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id,Lkp_English_Translation_df.English_Translation)
    
    lkp_Account_Translation_df = lkp_Account_Translation_df.alias('lkp_Account_Translation_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    lkp_AccName_Dedupe_df = lkp_Account_Translation_df.join(Lkp_Account_Dedupe_df,lkp_Account_Translation_df.English_Translation == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                      .select(lkp_Account_Translation_df.Id,lkp_Account_Translation_df.English_Translation,Lkp_Account_Dedupe_df.Atlas_Account_Name)
    
    Target_Name_df = lkp_AccName_Dedupe_df.withColumn('Tgt_Name',when(lkp_AccName_Dedupe_df.Atlas_Account_Name.isNull(),lkp_AccName_Dedupe_df.English_Translation)
                                                               .otherwise(lkp_AccName_Dedupe_df.Atlas_Account_Name)).select('Id','Tgt_Name')
    
    Target_Name_df = Target_Name_df.alias('Target_Name_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    Account_By_Name_df = Target_Name_df.join(LKP_SF_Target_Account_data_Spark_df,Target_Name_df.Tgt_Name == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                       .select(Target_Name_df.Id,col('LKP_SF_Target_Account_data_Spark_df.Id').alias('Account_By_Name_ID'))
    
    lkp_Account_Brand_ID_df = lkp_Account_Brand_ID_df.alias('lkp_Account_Brand_ID_df')
    Account_By_Name_df = Account_By_Name_df.alias('Account_By_Name_df')

    Client_Account_join_df = lkp_Account_Brand_ID_df.join(Account_By_Name_df,lkp_Account_Brand_ID_df.Id == Account_By_Name_df.Id,"inner")\
                                                        .select(lkp_Account_Brand_ID_df.Id,lkp_Account_Brand_ID_df.lkp_Account_Brand_ID,Account_By_Name_df.Account_By_Name_ID)
    
    Client_Account_df = Client_Account_join_df.withColumn('Client_Account__c',when(Client_Account_join_df.lkp_Account_Brand_ID.isNull(),Client_Account_join_df.Account_By_Name_ID)
                                                                             .otherwise(Client_Account_join_df.lkp_Account_Brand_ID)).select('Id','Client_Account__c')
    
    
    Client_Account_df = Client_Account_df.alias('Client_Account_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    Client__c_df = Client_Account_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,Client_Account_df.Client_Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c,"left")\
                                              .select(Client_Account_df.Id,Client_Account_df.Client_Account__c,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Client__c'))
    
    return Client__c_df
    
def creating_final_df(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,Agency__c_df,Client__c_df):
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.alias('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df')
    Agency__c_df = Agency__c_df.alias('Agency__c_df')
    Client__c_df = Client__c_df.alias('Client__c_df')
    
    Final_Df = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.join(Agency__c_df, col('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id') == col('Agency__c_df.Id'), 'left') \
                                                              .join(Client__c_df, col('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.Id') == col('Client__c_df.Id'), 'left') \
                                                              .select('CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.*','Agency__c_df.Agency_Account__c','Agency__c_df.Agency__c','Client__c_df.Client_Account__c','Client__c_df.Client__c')
    
    return Final_Df
    
def alter_final_df(Final_df):
    
    Final_df = Final_df.drop('CNI_Agency_Account__c','CHN_Brand_Account__c')
    
    alter_final_df = Final_df.withColumnRenamed('Id','Src_Id')\
                             .withColumnRenamed('CNI_Relationship_Start_Date__c','Start_Date__c')\
                             .withColumn('Relationship_Period__c',F.lit('Forever'))\
                             .select(*Sel_Col_List)
                             
    alter_final_df = alter_final_df.sort(col("Src_Id").asc())
                             
    return alter_final_df
    
def Handling_Insert_records(alter_final_df):
    Ins_records_df_index = alter_final_df.withColumn("idx",F.monotonically_increasing_id())
    windowSpec = Window.orderBy("idx")
    Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
    Ins_records_df_index = Ins_records_df_index.drop("idx",'Src_Id')
    Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
    Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    
    return Ins_records_df_index
  
def Filter_df(Final_Client_Agency_select_df):
  Filter_Final_df = Final_Client_Agency_select_df.filter((col("Agency__c").isNotNull()) & (col("Agency_Account__c").isNotNull()) & (col("Client_Account__c").isNotNull()) &                 (col("Client__c").isNotNull()))
  return Filter_Final_df
    
if __name__ == "__main__":
    start = time.time()
    CNI_Agency_Brand_Relationship_UKSUPDEV_df = get_input_data()
    CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df = pandas_to_pyspark_df(CNI_Agency_Brand_Relationship_UKSUPDEV_df)
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    LKP_SF_Target_Account_data_df = LKP_SF_Target_Account_data()
    LKP_SF_Target_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Account_data_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    Agency__c_df = Agency__c_ETL(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df)
    Client__c_df = Client__c_ETL(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,LKP_SF_Target_Account_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df)
    Final_df = creating_final_df(CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df,Agency__c_df,Client__c_df)
    alter_final_df = alter_final_df(Final_df)
    Final_Client_Agency_df = Handling_Insert_records(alter_final_df)
    Final_Client_Agency_select_df = Final_Client_Agency_df.select(*Sel_Col_Final_List)
    Final_Client_Agency_filter_df = Filter_df(Final_Client_Agency_select_df)
    Final_Client_Agency_distinct_df = Final_Client_Agency_filter_df.dropDuplicates(["Agency__c","Agency_Account__c","Client_Account__c","Client__c"])
    
    src_count = CNI_Agency_Brand_Relationship_UKSUPDEV_spark_df.count()
    Tgt_count = Final_Client_Agency_distinct_df.count()
    
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Tgt_count)
    
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))