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

Sel_Col_List = ['OwnerId','Name','CurrencyIsoCode','CreatedDate','CreatedById','LastModifiedById','Brand_Description__c','Category__c','Sub_Category__c','Ins_Upd_Flg','Client_Brand_ID']
Sel_Col_Final_List = ['Id','OwnerId','Name','CurrencyIsoCode','CreatedDate','CreatedById','LastModifiedById','Brand_Description__c','Category__c','Sub_Category__c','Created_Date','LastModifiedDate']

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
    df=getSFData(""" SELECT Id,OwnerId,Name,CurrencyIsoCode,CreatedDate,CreatedById
                     FROM Account where
                     RecordTypeId ='012b0000000QHwRAAW'
                     and Id in ('001b000000WHJEYAA5','001b000000axEklAAE') """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Contact_UKSUPDEV_df):
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.astype(str)
    Contact_UKSUPDEV_df = Contact_UKSUPDEV_df.fillna('Null')
    Contact_UKSUPDEV_Spark_df = spark.createDataFrame(Contact_UKSUPDEV_df)
    return Contact_UKSUPDEV_Spark_df
    
def LKP_SF_Target_User_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, Email, TPS_ID__c
                     from user """,sf)
                    
    return df
    
def LKP_FF_Category():
    Lkp_Category_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                 .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\China_Accounts_CategorizationAccountsLookup.csv")\
                                 .select('Salesforce ID','Category__c','Sub_Category__c')\
                                             .withColumnRenamed('Salesforce ID','Salesforce_ID')
                                             
    return Lkp_Category_df
    
def LKP_TGT_CLIENT_BRAND():
    Lkp_Tgt_Client_Brand_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Client_Brand\Target\Tgt_Client_Brand.csv")\
                                  .select('Id','Created_Date','LastModifiedDate','Name')
                                                       
    return Lkp_Tgt_Client_Brand_df
    
def OwnerId_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    cond = [Account_UKSUPDEV_Spark_df.OwnerId == LKP_SF_Target_User_data_Spark_df.TPS_ID__c]
    
    Var_Owner_ID_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_User_data_Spark_df,cond,"left")\
                                                      .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_User_data_Spark_df.Id').alias('Var_Owner_ID'))
    
    Account_UKSUPDEV_Spark_Owner_Id_df = Account_UKSUPDEV_Spark_df.withColumn('Owner_ID_in',F.lit('005b0000001ohQTAAY'))

    Account_UKSUPDEV_Spark_Owner_Id_df = Account_UKSUPDEV_Spark_Owner_Id_df.alias('Account_UKSUPDEV_Spark_Owner_Id_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    cond = [Account_UKSUPDEV_Spark_Owner_Id_df.Owner_ID_in == LKP_SF_Target_User_data_Spark_df.TPS_ID__c]
    
    Var_Owner_ID_Manual_df = Account_UKSUPDEV_Spark_Owner_Id_df.join(LKP_SF_Target_User_data_Spark_df,cond,"left")\
                                                      .select('Account_UKSUPDEV_Spark_Owner_Id_df.Id',col('LKP_SF_Target_User_data_Spark_df.Id').alias('Var_Owner_ID_Manual'))
    
    Var_Owner_ID_df = Var_Owner_ID_df.alias('Var_Owner_ID_df')
    Var_Owner_ID_Manual_df = Var_Owner_ID_Manual_df.alias('Var_Owner_ID_Manual_df')

    cond = [Var_Owner_ID_df.Id == Var_Owner_ID_Manual_df.Id]
    
    Var_Owner_ID_join_df = Var_Owner_ID_df.join(Var_Owner_ID_Manual_df,cond,"inner")\
                                                      .select('Var_Owner_ID_df.Id','Var_Owner_ID','Var_Owner_ID_Manual')
    
    OwnerId_df = Var_Owner_ID_join_df.withColumn('Tgt_OwnerId',when(Var_Owner_ID_join_df.Var_Owner_ID.isNull(),Var_Owner_ID_join_df.Var_Owner_ID_Manual)
                                                               .otherwise(Var_Owner_ID_join_df.Var_Owner_ID)).select('Id','Tgt_OwnerId')
    
    return OwnerId_df
    
def Name_ETL(Account_UKSUPDEV_Spark_df):
    Name_Df = Account_UKSUPDEV_Spark_df.withColumn('Tgt_Name',F.upper('Name'))\
                                       .select('Id','Tgt_Name')
    return Name_Df
    
def CreatedById_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    cond = [Account_UKSUPDEV_Spark_df.CreatedById == LKP_SF_Target_User_data_Spark_df.TPS_ID__c]
    
    Var_CreatedById_df = Account_UKSUPDEV_Spark_df.join(LKP_SF_Target_User_data_Spark_df,cond,"left")\
                                                      .select('Account_UKSUPDEV_Spark_df.Id',col('LKP_SF_Target_User_data_Spark_df.Id').alias('Var_CreatedById'))
    
    Account_UKSUPDEV_Spark_CreatedById_df = Account_UKSUPDEV_Spark_df.withColumn('CreatedById_in',F.lit('005b0000001ohQTAAY'))

    Account_UKSUPDEV_Spark_CreatedById_df = Account_UKSUPDEV_Spark_CreatedById_df.alias('Account_UKSUPDEV_Spark_CreatedById_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    cond = [Account_UKSUPDEV_Spark_CreatedById_df.CreatedById_in == LKP_SF_Target_User_data_Spark_df.TPS_ID__c]
    
    Var_CreatedById_Manual_df = Account_UKSUPDEV_Spark_CreatedById_df.join(LKP_SF_Target_User_data_Spark_df,cond,"left")\
                                                      .select('Account_UKSUPDEV_Spark_CreatedById_df.Id',col('LKP_SF_Target_User_data_Spark_df.Id').alias('Var_CreatedById_Manual'))
    
    Var_CreatedById_df = Var_CreatedById_df.alias('Var_CreatedById_df')
    Var_CreatedById_Manual_df = Var_CreatedById_Manual_df.alias('Var_CreatedById_Manual_df')

    cond = [Var_CreatedById_df.Id == Var_CreatedById_Manual_df.Id]
    
    Var_CreatedById_join_df = Var_CreatedById_df.join(Var_CreatedById_Manual_df,cond,"inner")\
                                                      .select('Var_CreatedById_df.Id','Var_CreatedById_df.Var_CreatedById','Var_CreatedById_Manual_df.Var_CreatedById_Manual')
    
    CreatedById_df = Var_CreatedById_join_df.withColumn('Tgt_CreatedById',when(Var_CreatedById_join_df.Var_CreatedById.isNull(),Var_CreatedById_join_df.Var_CreatedById_Manual)
                                                               .otherwise(Var_CreatedById_join_df.Var_CreatedById)).select('Id','Tgt_CreatedById')
    
    return CreatedById_df

def LastModifiedById_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df):
    Account_UKSUPDEV_Spark_LastModifiedById_df = Account_UKSUPDEV_Spark_df.withColumn('LastModifiedById_in',F.lit('0053N000005brERQAY'))

    Account_UKSUPDEV_Spark_LastModifiedById_df = Account_UKSUPDEV_Spark_LastModifiedById_df.alias('Account_UKSUPDEV_Spark_LastModifiedById_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    cond = [Account_UKSUPDEV_Spark_LastModifiedById_df.LastModifiedById_in == LKP_SF_Target_User_data_Spark_df.TPS_ID__c]
    
    LastModifiedById_df = Account_UKSUPDEV_Spark_LastModifiedById_df.join(LKP_SF_Target_User_data_Spark_df,cond,"left")\
                                                      .select('Account_UKSUPDEV_Spark_LastModifiedById_df.Id',col('LKP_SF_Target_User_data_Spark_df.Id').alias('LastModifiedById'))
    
    return LastModifiedById_df
    
def Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Category__c)
                         
    return Category_c_df
    
def Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Sub_Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Sub_Category__c)
                         
    return Sub_Category_c_df
    
def Ins_Upd_flg_ETL(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Client_Brand_df):

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_Tgt_Client_Brand_df = Lkp_Tgt_Client_Brand_df.alias('Lkp_Tgt_Client_Brand_df')
    
    Join_df = Account_UKSUPDEV_Spark_df.join(Lkp_Tgt_Client_Brand_df,Account_UKSUPDEV_Spark_df.Name == Lkp_Tgt_Client_Brand_df.Name,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,col('Lkp_Tgt_Client_Brand_df.Id').alias('Client_Brand_ID'))
    
    Ins_Upd_Flg_df = Join_df.withColumn('Ins_Upd_Flg',when((Join_df.Client_Brand_ID.isNull()), "I")
                                                     .otherwise("U"))
                                                 
    return Ins_Upd_Flg_df
    
def creating_final_df(Account_UKSUPDEV_Spark_df,OwnerId_df,Name_Df,CreatedById_df,LastModifiedById_df,Category_c_df,Sub_Category_c_df,Ins_Upd_Flg_df):
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    OwnerId_df = OwnerId_df.alias('OwnerId_df')
    Name_Df = Name_Df.alias('Name_Df')
    CreatedById_df = CreatedById_df.alias('CreatedById_df')
    LastModifiedById_df = LastModifiedById_df.alias('LastModifiedById_df')
    Category_c_df = Category_c_df.alias('Category_c_df')
    Sub_Category_c_df = Sub_Category_c_df.alias('Sub_Category_c_df')
    Ins_Upd_Flg_df = Ins_Upd_Flg_df.alias('Ins_Upd_Flg_df')
    
    Final_Df = Account_UKSUPDEV_Spark_df.join(OwnerId_df, col('Account_UKSUPDEV_Spark_df.Id') == col('OwnerId_df.Id'), 'left') \
                                        .join(Name_Df, col('Account_UKSUPDEV_Spark_df.Id') == col('Name_Df.Id'), 'left') \
                                        .join(CreatedById_df, col('Account_UKSUPDEV_Spark_df.Id') == col('CreatedById_df.Id'), 'left') \
                                        .join(LastModifiedById_df, col('Account_UKSUPDEV_Spark_df.Id') == col('LastModifiedById_df.Id'), 'left') \
                                        .join(Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Category_c_df.Id'), 'left') \
                                        .join(Sub_Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Sub_Category_c_df.Id'), 'left') \
                                        .join(Ins_Upd_Flg_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Ins_Upd_Flg_df.Id'), 'left') \
                                        .select('Account_UKSUPDEV_Spark_df.*','OwnerId_df.Tgt_OwnerId','Name_Df.Tgt_Name','CreatedById_df.Tgt_CreatedById','LastModifiedById_df.LastModifiedById','Category_c_df.Category__c','Sub_Category_c_df.Sub_Category__c','Ins_Upd_Flg_df.Ins_Upd_Flg','Ins_Upd_Flg_df.Client_Brand_ID')
    
    return Final_Df
    
def alter_final_df(Final_df):
    
    Final_df = Final_df.drop('OwnerId','CreatedById')
    
    alter_final_df = Final_df.withColumnRenamed('Id','Src_Id')\
                             .withColumnRenamed('Name','Brand_Description__c')\
                             .withColumnRenamed('Tgt_Name','Name')\
                             .withColumnRenamed('Tgt_OwnerId','OwnerId')\
                             .withColumnRenamed('Tgt_CreatedById','CreatedById')\
                             .select(*Sel_Col_List)
                             
    alter_final_df = alter_final_df.sort(col("Src_Id").asc())
                             
    return alter_final_df
    
def drop_duplicates_target(drop_dup_df):
    drop_dup_df = drop_dup_df.dropDuplicates(['Name'])
    return drop_dup_df
    
def Handling_Insert_records(Ins_records_df,Lkp_Tgt_Client_Brand_df):
    if Lkp_Tgt_Client_Brand_df.count() == 0:
        Ins_records_df_index = Ins_records_df.withColumn("idx",F.monotonically_increasing_id())
        windowSpec = Window.orderBy("idx")
        Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
        Ins_records_df_index = Ins_records_df_index.drop("idx",'Ins_Upd_Flg')
        Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
        Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    else:
        Max_Id = Lkp_Tgt_Client_Brand_df.groupBy().max('Id').collect()
        results={}
        for i in Max_Id:
            results.update(i.asDict())
        Max_Id_var = results['max(Id)']
        Ins_records_df_index = Ins_records_df.withColumn("idx",F.monotonically_increasing_id())
        windowSpec = Window.orderBy("idx")
        Ins_records_df_index = Ins_records_df_index.withColumn("Id",  Max_Id_var +  F.row_number().over(windowSpec))
        Ins_records_df_index = Ins_records_df_index.drop("idx",'Ins_Upd_Flg')
        Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
        Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    
    return Ins_records_df_index
    
def Handling_Update_records(Upd_records_df,Lkp_Tgt_Client_Brand_df):
    Lkp_Tgt_Client_Brand_df = Lkp_Tgt_Client_Brand_df.alias('Lkp_Tgt_Client_Brand_df')
    Upd_records_df = Upd_records_df.alias('Upd_records_df')
    Upd_records_df_index = Upd_records_df.join(Lkp_Tgt_Client_Brand_df,col('Upd_records_df.Client_Brand_ID') == col('Lkp_Tgt_Client_Brand_df.Id'),"left")\
                       .select('Lkp_Tgt_Client_Brand_df.Id','Lkp_Tgt_Client_Brand_df.Created_Date','Upd_records_df.*').drop('Ins_Upd_Flg')\
                       .withColumn('LastModifiedDate',current_timestamp())
                       
    return Upd_records_df_index
    
def Ins_Upd_Split(alter_final_df,Lkp_Tgt_Client_Brand_df):

    Ins_records_df = alter_final_df.filter(col('Ins_Upd_Flg') == 'I')
    Upd_records_df = alter_final_df.filter(col('Ins_Upd_Flg') == 'U')
    
    global Ins_count
    global upd_count
    
    Ins_count = Ins_records_df.count()
    upd_count = Upd_records_df.count()
    
    if Ins_count > 0:
        Final_Ins_records_df = Handling_Insert_records(Ins_records_df,Lkp_Tgt_Client_Brand_df)
        Final_Ins_records_df = Final_Ins_records_df.select(*Sel_Col_Final_List)
        Final_Ins_records_Distinct_df = drop_duplicates_target(Final_Ins_records_df)
        Ins_count = Final_Ins_records_Distinct_df.count()
    else:
        pass
    
    if upd_count > 0:
        Final_Upd_records_df = Handling_Update_records(Upd_records_df,Lkp_Tgt_Client_Brand_df)
        Final_Upd_records_df = Final_Upd_records_df.select(*Sel_Col_Final_List)
        Final_Upd_records_Distinct_df = drop_duplicates_target(Final_Upd_records_df)
        upd_count = Final_Upd_records_Distinct_df.count()
    else:
        pass
        
    if ((Ins_count == 0) & (upd_count != 0)):
        df = Final_Upd_records_df
    elif ((upd_count == 0) & (Ins_count != 0)):
        df = Final_Ins_records_df
    elif ((Ins_count != 0) & (upd_count != 0)): 
        dfs = [Final_Ins_records_df,Final_Upd_records_df]
        df = reduce(DataFrame.union, dfs)
        df = df.sort(col("Id").asc())
    else:
        df = spark.createDataFrame([], StructType([]))
    
    return df
    
def Writing_to_Target(Final_Distinct_df):
    df = Final_Distinct_df.toPandas()
    df.to_csv(r'C:\Users\skumar6\Documents\Informatica_Pyspark\Client_Brand\Target\Tgt_Client_Brand.csv',index=False)
    
if __name__ == "__main__":
    start = time.time()
    Account_UKSUPDEV_df = get_input_data()
    Account_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Account_UKSUPDEV_df)
    LKP_SF_Target_User_data_df = LKP_SF_Target_User_data()
    LKP_SF_Target_User_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_User_data_df)
    Lkp_Category_df = LKP_FF_Category()
    Lkp_Tgt_Client_Brand_df = LKP_TGT_CLIENT_BRAND()
    OwnerId_df = OwnerId_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df)
    Name_Df = Name_ETL(Account_UKSUPDEV_Spark_df)
    CreatedById_df = CreatedById_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df)
    LastModifiedById_df = LastModifiedById_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_Target_User_data_Spark_df)
    Category_c_df = Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Sub_Category_c_df = Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Ins_Upd_Flg_df = Ins_Upd_flg_ETL(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Client_Brand_df)
    Final_df = creating_final_df(Account_UKSUPDEV_Spark_df,OwnerId_df,Name_Df,CreatedById_df,LastModifiedById_df,Category_c_df,Sub_Category_c_df,Ins_Upd_Flg_df)
    alter_final_df = alter_final_df(Final_df)
    Ins_Upd_Split_df = Ins_Upd_Split(alter_final_df,Lkp_Tgt_Client_Brand_df)
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
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))