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

Sel_Col_List = ['Id','Name','Type','BillingStreet','BillingCity','BillingState','BillingPostalCode','BillingCountry','BillingCountryCode','BillingLatitude','BillingLongitude','Fax','Website','CurrencyIsoCode','Account_Status1_c','Account_class_c','Customer_Type_c','External_UID_c','Account_Name_Local_c','Payment_Type_c','Terms_of_Payment_c','Category__c','Sub_Category__c','Created_Date','LastModifiedDate']

def SF_connectivity_MIG2():
    session_id, instance = SalesforceLogin(username='sfdata_admins@condenast.com.supdev',
                                           password='sfDc1234', security_token='kvtGqGhKGlm0PNa8rkFkyaPX',
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
    df=getSFData(""" SELECT Id,RecordTypeId,Name,BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingCountryCode, BillingLatitude, BillingLongitude, Fax, Website, CurrencyIsoCode, CNI_Payment_Period__c
                     FROM Account where
                     RecordTypeId in(select Id from RecordType where Name in ('CHN Client', 'CHN Agency','CHN Brand')) """,sf)
                    
    return df
    
def pandas_to_pyspark_df(Account_UKSUPDEV_df):
    Account_UKSUPDEV_df = Account_UKSUPDEV_df.fillna('Null')
    Account_UKSUPDEV_Spark_df = spark.createDataFrame(Account_UKSUPDEV_df)
    return Account_UKSUPDEV_Spark_df

def LKP_TGT_ACCOUNT():
    Lkp_Tgt_Account_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\Target\Tgt_Acc.csv")\
                                  .select('Id','Created_Date','LastModifiedDate','External_UID_c','Name')
                                                       
    return Lkp_Tgt_Account_df
    
def LKP_SF_RecordType_Supdev():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, Name from RecordType """,sf)
                    
    return df
    
def LKP_FF_English_Translation():
    Lkp_English_Translation_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                  .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Account\ChineseAccountTranslations.csv")\
                                  .select('Salesforce ID','English Translation')\
                                                       .withColumnRenamed('Salesforce ID','Salesforce_ID')\
                                                       .withColumnRenamed('English Translation','English_Translation')
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
    
def Name_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df):
                                             
    v_EnglishTranslatedAccName = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,Account_UKSUPDEV_Spark_df.Id == Lkp_English_Translation_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_English_Translation_df.English_Translation)
                         
    
    v_AccName_DedupeLkp = v_EnglishTranslatedAccName.join(Lkp_Account_Dedupe_df,v_EnglishTranslatedAccName.English_Translation == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                         .select(v_EnglishTranslatedAccName.Id,v_EnglishTranslatedAccName.English_Translation,Lkp_Account_Dedupe_df.Atlas_Account_Name)
                         
    Name_df = v_AccName_DedupeLkp.withColumn('Tgt_Name',when(v_AccName_DedupeLkp.Atlas_Account_Name.isNull(),v_AccName_DedupeLkp.English_Translation)
                                                .otherwise(v_AccName_DedupeLkp.Atlas_Account_Name)).select('Id','Tgt_Name')
                                                
                                                
    return Name_df
    
def Type_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df):
    supdev_RecordTypeName = Account_UKSUPDEV_Spark_df.join(LKP_SF_RecordType_Supdev_Spark_df,Account_UKSUPDEV_Spark_df.RecordTypeId == LKP_SF_RecordType_Supdev_Spark_df.Id,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Account_UKSUPDEV_Spark_df.RecordTypeId,LKP_SF_RecordType_Supdev_Spark_df.Name)                 
                         
    Type_df = supdev_RecordTypeName.withColumn('Type',when(supdev_RecordTypeName.Name == 'CHN Client','Client')
                                                     .when(supdev_RecordTypeName.Name == 'CHN Brand','Client')
                                                     .when(supdev_RecordTypeName.Name == 'CHN Agency','Agency')
                                                    .otherwise('null')).select('Id','Type')
                         
    return Type_df
    
def Account_class_c_ETL(Account_UKSUPDEV_Spark_df):
    Account_class_c_df = Account_UKSUPDEV_Spark_df.withColumn('Account_class_c',when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'ALPHABET') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'APPLE COMPUTER') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'BMW') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'BURBERRY') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'CHANEL') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'COTY') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'DOLCE & GABBANA') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'ESTEE LAUDER') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'FACEBOOK') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'GIORGIO ARMANI') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'HERMES') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'KERING') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'OREAL') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'LVMH') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'LOUIS VUITTON') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'MERCEDES') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'MICHAEL KORS') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'MICROSOFT') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'NETFLIX') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'P&G') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'PROCTER & GAMBLE') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'PRADA') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'PUIG') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'RALPH LAUREN') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'RICHEMONT') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'ROLEX') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'SAMSUNG') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'SHISEIDO') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'SWATCH') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'TIFFANY & CO') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'TIFFANY') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'TOYOTA') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'LEXUS') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'VALENTINO') != 0,'Top 25')
                               .when(instr(upper(Account_UKSUPDEV_Spark_df.Name),'VOLKSWAGEN') != 0,'Top 25')
                               .otherwise('Other')).select('Id','Name','Account_class_c')
                               
    return Account_class_c_df
    
def Customer_Type_c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df):
    supdev_RecordTypeName = Account_UKSUPDEV_Spark_df.join(LKP_SF_RecordType_Supdev_Spark_df,Account_UKSUPDEV_Spark_df.RecordTypeId == LKP_SF_RecordType_Supdev_Spark_df.Id,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Account_UKSUPDEV_Spark_df.RecordTypeId,LKP_SF_RecordType_Supdev_Spark_df.Name)                 
                         
    Customer_Type_df = supdev_RecordTypeName.withColumn('Type',when(supdev_RecordTypeName.Name == 'CHN Client','Client')
                                                     .when(supdev_RecordTypeName.Name == 'CHN Brand','Client')
                                                     .when(supdev_RecordTypeName.Name == 'CHN Agency','Agency')
                                                    .otherwise('null')).select('Id',col('Type').alias('Customer_Type_c'))
                         
    return Customer_Type_df
    
def Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Category__c)
                         
    return Category_c_df
    
def Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df):
    Sub_Category_c_df = Account_UKSUPDEV_Spark_df.join(Lkp_Category_df,Account_UKSUPDEV_Spark_df.Id == Lkp_Category_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_Category_df.Sub_Category__c)
                         
    return Sub_Category_c_df

def Ins_Upd_flg(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Account_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df):

    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Lkp_Tgt_Account_df = Lkp_Tgt_Account_df.alias('Lkp_Tgt_Account_df')
    
    AtlasAccountId_df = Account_UKSUPDEV_Spark_df.join(Lkp_Tgt_Account_df,col('Account_UKSUPDEV_Spark_df.Id') == col('Lkp_Tgt_Account_df.External_UID_c'),"left")\
                         .select('Account_UKSUPDEV_Spark_df.Id',col('Lkp_Tgt_Account_df.Id').alias('Tgt_Acc_Id'))
                         
    v_EnglishTranslatedAccName = Account_UKSUPDEV_Spark_df.join(Lkp_English_Translation_df,Account_UKSUPDEV_Spark_df.Id == Lkp_English_Translation_df.Salesforce_ID,"left")\
                         .select(Account_UKSUPDEV_Spark_df.Id,Lkp_English_Translation_df.English_Translation)
                         
    v_AccName_DedupeLkp = v_EnglishTranslatedAccName.join(Lkp_Account_Dedupe_df,v_EnglishTranslatedAccName.English_Translation == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                       .select(v_EnglishTranslatedAccName.Id,v_EnglishTranslatedAccName.English_Translation,Lkp_Account_Dedupe_df.Atlas_Account_Name)
    
    v_AccName_DedupeLkp = v_AccName_DedupeLkp.alias('v_AccName_DedupeLkp')
    
    chk_acct_exists_df = v_AccName_DedupeLkp.join(Lkp_Tgt_Account_df,col('v_AccName_DedupeLkp.Atlas_Account_Name') == col('Lkp_Tgt_Account_df.Name'),"left")\
                         .select('v_AccName_DedupeLkp.Id',col('v_AccName_DedupeLkp.Atlas_Account_Name'),col('Lkp_Tgt_Account_df.Id').alias('chk_acct_exists'))
                         
    Name_df = v_AccName_DedupeLkp.withColumn('Tgt_Name',when(v_AccName_DedupeLkp.Atlas_Account_Name.isNull(),v_AccName_DedupeLkp.English_Translation)
                                                .otherwise(v_AccName_DedupeLkp.Atlas_Account_Name)).select('Id','Tgt_Name')
                                                
    AtlasAccountId_df = AtlasAccountId_df.alias('AtlasAccountId_df')
    chk_acct_exists_df = chk_acct_exists_df.alias('chk_acct_exists_df')
    Name_df = Name_df.alias('Name_df')

    Join_Df = AtlasAccountId_df.join(chk_acct_exists_df, col('AtlasAccountId_df.Id') == col('chk_acct_exists_df.Id'), 'left') \
                              .join(Name_df, col('AtlasAccountId_df.Id') == col('Name_df.Id'), 'left')\
                              .select('AtlasAccountId_df.Id','AtlasAccountId_df.Tgt_Acc_Id','chk_acct_exists_df.chk_acct_exists','Name_df.Tgt_Name')
                              
    Join_Df = Join_Df.alias('Join_Df')
    
    Ins_Upd_Flg_df = Join_Df.withColumn('Ins_Upd_Flg',when((Join_Df.Tgt_Acc_Id.isNull()) & (Join_Df.chk_acct_exists.isNull()) & (Join_Df.Tgt_Name.isNotNull()), "I")
                                                     .when(Join_Df.Tgt_Name.isNull(),'D')
                                                     .otherwise("U"))
                                                 
    return Ins_Upd_Flg_df
    
     
def creating_final_df(Account_UKSUPDEV_Spark_df,Name_df,Type_df,Account_class_c_df,Customer_Type_c_df,Category_c_df,Sub_Category_c_df,Ins_Upd_Flg_df):
    
    Account_UKSUPDEV_Spark_df = Account_UKSUPDEV_Spark_df.alias('Account_UKSUPDEV_Spark_df')
    Name_df = Name_df.alias('Name_df')
    Type_df = Type_df.alias('Type_df')
    Account_class_c_df = Account_class_c_df.alias('Account_class_c_df')
    Customer_Type_c_df = Customer_Type_c_df.alias('Customer_Type_c_df')
    Category_c_df = Category_c_df.alias('Category_c_df')
    Sub_Category_c_df = Sub_Category_c_df.alias('Sub_Category_c_df')
    Ins_Upd_Flg_df = Ins_Upd_Flg_df.alias('Ins_Upd_Flg_df')
    
    Final_Df = Account_UKSUPDEV_Spark_df.join(Name_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Name_df.Id'), 'left') \
                              .join(Type_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Type_df.Id'), 'left') \
                              .join(Account_class_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Account_class_c_df.Id'), 'left')\
                              .join(Customer_Type_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Customer_Type_c_df.Id'), 'left')\
                              .join(Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Category_c_df.Id'), 'left')\
                              .join(Sub_Category_c_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Sub_Category_c_df.Id'), 'left')\
                              .join(Ins_Upd_Flg_df, col('Account_UKSUPDEV_Spark_df.Id') == col('Ins_Upd_Flg_df.Id'), 'left')\
                              .select('Account_UKSUPDEV_Spark_df.*', 'Name_df.Tgt_Name', 'Type_df.Type','Account_class_c_df.Account_class_c','Customer_Type_c_df.Customer_Type_c','Category_c_df.Category__c','Sub_Category_c_df.Sub_Category__c','Ins_Upd_Flg_df.Ins_Upd_Flg')

    return Final_Df
    
def alter_final_df(Final_df):
    
    alter_final_df = Final_df.withColumn('Account_Status1_c',lit('Activated'))\
                             .withColumn('Payment_Type_c',lit('Account'))\
                             .withColumn('Account_Name_Local_c',initcap(Final_df.Name))\
                             .withColumnRenamed('Id','External_UID_c')\
                             .withColumnRenamed('CNI_Payment_Period__c','Terms_of_Payment_c')\
                             .drop('RecordTypeId','Name','CNI_Payment_Period__c')\
                             .withColumnRenamed('Tgt_Name','Name')
                             
    return alter_final_df

def Handling_Insert_records(Ins_records_df,Lkp_Tgt_Account_df):
    if Lkp_Tgt_Account_df.count() == 0:
        Ins_records_df_index = Ins_records_df.withColumn("idx",F.monotonically_increasing_id())
        windowSpec = W.orderBy("idx")
        Ins_records_df_index = Ins_records_df_index.withColumn("Id", F.row_number().over(windowSpec))
        Ins_records_df_index = Ins_records_df_index.drop("idx",'Ins_Upd_Flg')
        Ins_records_df_index = Ins_records_df_index.withColumn('Created_Date',current_timestamp())
        Ins_records_df_index = Ins_records_df_index.withColumn('LastModifiedDate',current_timestamp())
    else:
        Max_Id = Lkp_Tgt_Account_df.groupBy().max('Id').collect()
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

def Handling_Update_records(Upd_records_df,Lkp_Tgt_Account_df):
    Lkp_Tgt_Account_df = Lkp_Tgt_Account_df.alias('Lkp_Tgt_Account_df')
    Upd_records_df = Upd_records_df.alias('Upd_records_df')
    Upd_records_df_index = Upd_records_df.join(Lkp_Tgt_Account_df,col('Upd_records_df.External_UID_c') == col('Lkp_Tgt_Account_df.External_UID_c'),"left")\
                       .select('Lkp_Tgt_Account_df.Id','Lkp_Tgt_Account_df.Created_Date','Upd_records_df.*').drop('Ins_Upd_Flg')\
                       .withColumn('LastModifiedDate',current_timestamp())
                       
    return Upd_records_df_index

def drop_duplicates_target(drop_dup_df):
    drop_dup_df = drop_dup_df.dropDuplicates(['Name'])
    return drop_dup_df
           
def Ins_Upd_Split(alter_final_df,Lkp_Tgt_Account_df):

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
        Final_Ins_records_df = Handling_Insert_records(Ins_records_df,Lkp_Tgt_Account_df)
        Final_Ins_records_df = Final_Ins_records_df.select(*Sel_Col_List)
        Final_Ins_records_Distinct_df = drop_duplicates_target(Final_Ins_records_df)
        Ins_count = Final_Ins_records_Distinct_df.count()
    else:
        pass
    
    if upd_count > 0:
        Final_Upd_records_df = Handling_Update_records(Upd_records_df,Lkp_Tgt_Account_df)
        Final_Upd_records_df = Final_Upd_records_df.select(*Sel_Col_List)
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
     
def Writing_to_Target(Ins_Upd_Split_df):
    df = Ins_Upd_Split_df.toPandas()
    df.to_csv(r'C:\Users\skumar6\Documents\Informatica_Pyspark\Account\Target\Tgt_Acc.csv',index=False)
    
                             
if __name__ == "__main__":
    start = time.time()
    Account_UKSUPDEV_df = get_input_data()
    Account_UKSUPDEV_Spark_df = pandas_to_pyspark_df(Account_UKSUPDEV_df)
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    Lkp_Category_df = LKP_FF_Category()
    LKP_SF_RecordType_Supdev_df = LKP_SF_RecordType_Supdev()
    LKP_SF_RecordType_Supdev_Spark_df = pandas_to_pyspark_df(LKP_SF_RecordType_Supdev_df)
    Lkp_Tgt_Account_df = LKP_TGT_ACCOUNT()
    Name_df = Name_ETL(Account_UKSUPDEV_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df)
    Type_df = Type_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df)
    Account_class_c_df = Account_class_c_ETL(Account_UKSUPDEV_Spark_df)
    Customer_Type_c_df = Customer_Type_c_ETL(Account_UKSUPDEV_Spark_df,LKP_SF_RecordType_Supdev_Spark_df)
    Category_c_df = Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Sub_Category_c_df = Sub_Category_c_ETL(Account_UKSUPDEV_Spark_df,Lkp_Category_df)
    Ins_Upd_Flg_df = Ins_Upd_flg(Account_UKSUPDEV_Spark_df,Lkp_Tgt_Account_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df)
    Final_df = creating_final_df(Account_UKSUPDEV_Spark_df,Name_df,Type_df,Account_class_c_df,Customer_Type_c_df,Category_c_df,Sub_Category_c_df,Ins_Upd_Flg_df)
    alter_final_df = alter_final_df(Final_df)
    Ins_Upd_Split_df = Ins_Upd_Split(alter_final_df,Lkp_Tgt_Account_df)
    Ins_Upd_Split_count = Ins_Upd_Split_df.count()
    if Ins_Upd_Split_count == 0:
        pass
    else:
        Final_Target_Account_Df = Writing_to_Target(Ins_Upd_Split_Distinct_df)
        
    src_count = Account_UKSUPDEV_Spark_df.count()
        
    print("Total No. of records from Source: ", src_count)
    print("Total Records Inserted: ",Ins_count)
    print("Total Records Updated: ",upd_count)
    print("Total Records not loaded due to Null Name: ",delete_count)
    end = time.time()
    total_time = end - start
    total_time_in_mins = total_time/60
    print("\n Total time taken in Minutes: "+ str(total_time_in_mins))