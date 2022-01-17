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
    df=getSFData(""" SELECT Id,CNI_Booked_By__c,CurrencyIsoCode,CreatedDate,CreatedById,Name,CNI_Agency_Account__c,
                            CNI_Gross_Revenue__c,CNI_Account__c,CNI_CampaignID__c,CNI_Billing_Account__c,CNI_Status__c,
                            LastModifiedDate,CNI_PrimaryContact__c,CNI_Platform__c
                     FROM CNI_Booking__c where
                     Id in ('a0Fb0000007in3oEAA') """,sf)
                    
    return df
    
def pandas_to_pyspark_df(CNI_Booking__c_df):
    CNI_Booking__c_df = CNI_Booking__c_df.astype(str)
    CNI_Booking__c_df = CNI_Booking__c_df.fillna('Null')
    CNI_Booking__c_Spark_df = spark.createDataFrame(CNI_Booking__c_df)
    return CNI_Booking__c_Spark_df
    
def LKP_SF_Target_User_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, TPS_ID__c
                     from user """,sf)
                    
    return df

def LKP_SF_Account_uksupdev_data():
    sf=SF_connectivity_Supdev()
    df=getSFData(""" SELECT Id,RecordTypeId,Name
                     FROM Account
                    """,sf)
                    
    return df
    
def LKP_SF_Target_Publisher_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Account__c,External_UID__c,Publisher_Name__c,Account_Name__c,
                            Category__c,Sub_Category__c
                     FROM Publisher_Account__c
                     """,sf)
                    
    return df
    
def LKP_SF_Target_Account_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,Name
                     FROM Account
                     """,sf)
                    
    return df
    
def LKP_SF_Target_Contact_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" SELECT Id,TPS_ID__c
                     FROM Contact
                     """,sf)
                    
    return df
    
def LKP_SF_Target_Client_Brand_c_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Id, Name
                     from Client_Brand__c """,sf)
                    
    return df
    
def LKP_SF_Target_AMS_Exchange_Rate_data():
    sf=SF_connectivity_MIG2()
    df=getSFData(""" select Conversion_Rate__c,From_ISOCurrency__c,To_ISOCurrency__c,Start_Date__c
                     from AMS_Exchange_Rate__c """,sf)
                    
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
    
def LKP_FF_CNI_BOOKINGS():
    Lkp_FF_CNI_BOOKINGS_df = spark.read.options(header='True',inferSchema='True',delimiter=',')\
                                .csv(r"C:\Users\skumar6\Documents\Informatica_Pyspark\Order\china_bookings.csv")\
                                .select('CNI_BOOKING_Id','TGT_BLI_START_DATE')
                                                
    return Lkp_FF_CNI_BOOKINGS_df
    
def OwnerId_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df):
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_USER_BOOKEDBY_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Booked_By__c == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_USER_BOOKEDBY_Id'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Spark_df.withColumn('IN_FAN_USERID',F.lit('005b0000001ohQTAAY'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Fan_user_df.alias('CNI_Booking__c_Fan_user_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_FAN_USER_Id_df = CNI_Booking__c_Fan_user_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Fan_user_df.IN_FAN_USERID == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Fan_user_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_FAN_USER_Id'))
    
    LKP_USER_BOOKEDBY_Id_df = LKP_USER_BOOKEDBY_Id_df.alias('LKP_USER_BOOKEDBY_Id_df')
    LKP_FAN_USER_Id_df = LKP_FAN_USER_Id_df.alias('LKP_FAN_USER_Id_df')

    Owner_Id_join_df = LKP_USER_BOOKEDBY_Id_df.join(LKP_FAN_USER_Id_df,LKP_USER_BOOKEDBY_Id_df.Id == LKP_FAN_USER_Id_df.Id,"inner")\
                                              .select(LKP_USER_BOOKEDBY_Id_df.Id,LKP_USER_BOOKEDBY_Id_df.LKP_USER_BOOKEDBY_Id,LKP_FAN_USER_Id_df.LKP_FAN_USER_Id)
    
    OwnerId_df = Owner_Id_join_df.withColumn('OwnerId',when(Owner_Id_join_df.LKP_USER_BOOKEDBY_Id.isNull(),Owner_Id_join_df.LKP_FAN_USER_Id)
                                                                    .otherwise(Owner_Id_join_df.LKP_USER_BOOKEDBY_Id))\
                                  .select('Id','OwnerId')
    
    return OwnerId_df
    
def CreatedById_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df):
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_CRTBY_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Spark_df.CreatedById == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_CRTBY_Id'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Spark_df.withColumn('IN_FAN_USERID',F.lit('005b0000001ohQTAAY'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Fan_user_df.alias('CNI_Booking__c_Fan_user_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_FAN_USER_Id_df = CNI_Booking__c_Fan_user_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Fan_user_df.IN_FAN_USERID == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Fan_user_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_FAN_USER_Id'))
    
    LKP_CRTBY_Id_df = LKP_CRTBY_Id_df.alias('LKP_CRTBY_Id_df')
    LKP_FAN_USER_Id_df = LKP_FAN_USER_Id_df.alias('LKP_FAN_USER_Id_df')

    CreatedById_join_df = LKP_CRTBY_Id_df.join(LKP_FAN_USER_Id_df,LKP_CRTBY_Id_df.Id == LKP_FAN_USER_Id_df.Id,"inner")\
                                              .select(LKP_CRTBY_Id_df.Id,LKP_CRTBY_Id_df.LKP_CRTBY_Id,LKP_FAN_USER_Id_df.LKP_FAN_USER_Id)
    
    CreatedById_df = CreatedById_join_df.withColumn('CreatedById',when(CreatedById_join_df.LKP_CRTBY_Id.isNull(),CreatedById_join_df.LKP_FAN_USER_Id)
                                                                    .otherwise(CreatedById_join_df.LKP_CRTBY_Id))\
                                  .select('Id','CreatedById')
    
    return CreatedById_df
    
def Agency_Publisher_Account__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df):
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')

    Src_CNIAgency_RecordTypeId_df = CNI_Booking__c_Spark_df.join(LKP_SF_Account_uksupdev_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Agency_Account__c == LKP_SF_Account_uksupdev_data_Spark_df.Id,"left")\
                                                           .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.RecordTypeId').alias('Src_CNIAgency_RecordTypeId'))

    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    LKP_AgencyAccount_byId_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Agency_Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c,"left")\
                                                      .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('LKP_AgencyAccount_byId'))
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    V_CniAgencyAccountTransbyID_df = CNI_Booking__c_Spark_df.join(Lkp_English_Translation_df,CNI_Booking__c_Spark_df.CNI_Agency_Account__c == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                            .select(CNI_Booking__c_Spark_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniAgencyAccountTransbyID'))

    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')
    
    CNI_AgencyAccount_Name_df = CNI_Booking__c_Spark_df.join(LKP_SF_Account_uksupdev_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Agency_Account__c == LKP_SF_Account_uksupdev_data_Spark_df.Id,"left")\
                                                       .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.Name').alias('CNI_AgencyAccount_Name'))

    CNI_AgencyAccount_Name_df = CNI_AgencyAccount_Name_df.alias('CNI_AgencyAccount_Name_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    V_CniAgencyAccountTransbyName_df = CNI_AgencyAccount_Name_df.join(Lkp_English_Translation_df,CNI_AgencyAccount_Name_df.CNI_AgencyAccount_Name == Lkp_English_Translation_df.Account_Name,"left")\
                                                                .select(CNI_AgencyAccount_Name_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniAgencyAccountTransbyName'))

    V_CniAgencyAccountTransbyID_df = V_CniAgencyAccountTransbyID_df.alias('V_CniAgencyAccountTransbyID_df')
    V_CniAgencyAccountTransbyName_df = V_CniAgencyAccountTransbyName_df.alias('V_CniAgencyAccountTransbyName_df')

    LKP_AgencyAccount_byName_join_df = V_CniAgencyAccountTransbyID_df.join(V_CniAgencyAccountTransbyName_df,V_CniAgencyAccountTransbyID_df.Id == V_CniAgencyAccountTransbyName_df.Id,"inner")\
                                                                     .select(V_CniAgencyAccountTransbyID_df.Id,V_CniAgencyAccountTransbyID_df.V_CniAgencyAccountTransbyID,V_CniAgencyAccountTransbyName_df.V_CniAgencyAccountTransbyName)

    LKP_AgencyAccount_byName_df = LKP_AgencyAccount_byName_join_df.withColumn('LKP_AgencyAccount_byName',when(LKP_AgencyAccount_byName_join_df.V_CniAgencyAccountTransbyID.isNull(),LKP_AgencyAccount_byName_join_df.V_CniAgencyAccountTransbyName)
                                                                                       .otherwise(LKP_AgencyAccount_byName_join_df.V_CniAgencyAccountTransbyID))\
                                                                  .select('Id','LKP_AgencyAccount_byName')
    
    V_CniAgencyAccountTransbyID_df = V_CniAgencyAccountTransbyID_df.alias('V_CniAgencyAccountTransbyID_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniAgencyAccountTransDeduped_df = V_CniAgencyAccountTransbyID_df.join(Lkp_Account_Dedupe_df,V_CniAgencyAccountTransbyID_df.V_CniAgencyAccountTransbyID == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                     .select(V_CniAgencyAccountTransbyID_df.Id,col('V_CniAgencyAccountTransbyID_df.V_CniAgencyAccountTransbyID').alias('V_CniAgencyAccountTrans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniAgencyAccountTransDeduped'))

    V_CniAgencyAccountTransDeduped_df = V_CniAgencyAccountTransDeduped_df.alias('V_CniAgencyAccountTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAgencyAccId_dedupe_df = V_CniAgencyAccountTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAgencyAccountTransDeduped_df.V_CniAgencyAccountTransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                         .select(V_CniAgencyAccountTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAgencyAccId_dedupe'))

    V_CniAgencyAccountTransDeduped_df = V_CniAgencyAccountTransDeduped_df.alias('V_CniAgencyAccountTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAgencyAccId_trans_df = V_CniAgencyAccountTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAgencyAccountTransDeduped_df.V_CniAgencyAccountTrans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                         .select(V_CniAgencyAccountTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAgencyAccId_trans'))

    V_Dedupe_CniAgencyAccId_dedupe_df = V_Dedupe_CniAgencyAccId_dedupe_df.alias('V_Dedupe_CniAgencyAccId_dedupe_df')
    V_Dedupe_CniAgencyAccId_trans_df = V_Dedupe_CniAgencyAccId_trans_df.alias('V_Dedupe_CniAgencyAccId_trans_df')

    V_Dedupe_CniAgencyAccId_join_df = V_Dedupe_CniAgencyAccId_dedupe_df.join(V_Dedupe_CniAgencyAccId_trans_df,V_Dedupe_CniAgencyAccId_dedupe_df.Id == V_Dedupe_CniAgencyAccId_trans_df.Id,"inner")\
                                                                       .select(V_Dedupe_CniAgencyAccId_dedupe_df.Id,V_Dedupe_CniAgencyAccId_dedupe_df.V_Dedupe_CniAgencyAccId_dedupe,V_Dedupe_CniAgencyAccId_trans_df.V_Dedupe_CniAgencyAccId_trans)

    V_Dedupe_CniAgencyAccId_df = V_Dedupe_CniAgencyAccId_join_df.withColumn('V_Dedupe_CniAgencyAccId',when(V_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccId_dedupe.isNull(),V_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccId_trans)
                                                                                                  .otherwise(V_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccId_dedupe))\
                                                               .select('Id','V_Dedupe_CniAgencyAccId')

    V_CniAgencyAccountTransbyName_df = V_CniAgencyAccountTransbyName_df.alias('V_CniAgencyAccountTransbyName_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniAgencyAccountNameTransDeduped_df = V_CniAgencyAccountTransbyName_df.join(Lkp_Account_Dedupe_df,V_CniAgencyAccountTransbyName_df.V_CniAgencyAccountTransbyName == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                            .select(V_CniAgencyAccountTransbyName_df.Id,col('V_CniAgencyAccountTransbyName_df.V_CniAgencyAccountTransbyName').alias('V_CniAgencyAccountNameTrans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniAgencyAccountNameTransDeduped'))

    V_CniAgencyAccountNameTransDeduped_df = V_CniAgencyAccountNameTransDeduped_df.alias('V_CniAgencyAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAgencyAccName_dedupe_df = V_CniAgencyAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAgencyAccountNameTransDeduped_df.V_CniAgencyAccountNameTransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                               .select(V_CniAgencyAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAgencyAccName_dedupe'))

    V_CniAgencyAccountNameTransDeduped_df = V_CniAgencyAccountNameTransDeduped_df.alias('V_CniAgencyAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAgencyAccName_trans_df = V_CniAgencyAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAgencyAccountNameTransDeduped_df.V_CniAgencyAccountNameTrans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                             .select(V_CniAgencyAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAgencyAccName_trans'))

    V_Dedupe_CniAgencyAccName_dedupe_df = V_Dedupe_CniAgencyAccName_dedupe_df.alias('V_Dedupe_CniAgencyAccName_dedupe_df')
    V_Dedupe_CniAgencyAccName_trans_df = V_Dedupe_CniAgencyAccName_trans_df.alias('V_Dedupe_CniAgencyAccName_trans_df')

    V_Dedupe_CniAgencyAccName_join_df = V_Dedupe_CniAgencyAccName_dedupe_df.join(V_Dedupe_CniAgencyAccName_trans_df,V_Dedupe_CniAgencyAccName_dedupe_df.Id == V_Dedupe_CniAgencyAccName_trans_df.Id,"inner")\
                                                                           .select(V_Dedupe_CniAgencyAccName_dedupe_df.Id,V_Dedupe_CniAgencyAccName_dedupe_df.V_Dedupe_CniAgencyAccName_dedupe,V_Dedupe_CniAgencyAccName_trans_df.V_Dedupe_CniAgencyAccName_trans)

    V_Dedupe_CniAgencyAccNameId_df = V_Dedupe_CniAgencyAccName_join_df.withColumn('V_Dedupe_CniAgencyAccNameId',when(V_Dedupe_CniAgencyAccName_join_df.V_Dedupe_CniAgencyAccName_dedupe.isNull(),V_Dedupe_CniAgencyAccName_join_df.V_Dedupe_CniAgencyAccName_trans)
                                                                                                  .otherwise(V_Dedupe_CniAgencyAccName_join_df.V_Dedupe_CniAgencyAccName_dedupe))\
                                                                      .select('Id','V_Dedupe_CniAgencyAccNameId')

    V_Dedupe_CniAgencyAccId_df = V_Dedupe_CniAgencyAccId_df.alias('V_Dedupe_CniAgencyAccId_df')
    V_Dedupe_CniAgencyAccNameId_df = V_Dedupe_CniAgencyAccNameId_df.alias('V_Dedupe_CniAgencyAccNameId_df')

    O_Dedupe_CniAgencyAccId_join_df = V_Dedupe_CniAgencyAccId_df.join(V_Dedupe_CniAgencyAccNameId_df,V_Dedupe_CniAgencyAccId_df.Id == V_Dedupe_CniAgencyAccNameId_df.Id,"inner")\
                                                                .select(V_Dedupe_CniAgencyAccId_df.Id,V_Dedupe_CniAgencyAccId_df.V_Dedupe_CniAgencyAccId,V_Dedupe_CniAgencyAccNameId_df.V_Dedupe_CniAgencyAccNameId)

    O_Dedupe_CniAgencyAccId_df = O_Dedupe_CniAgencyAccId_join_df.withColumn('O_Dedupe_CniAgencyAccId',when(O_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccId.isNull(),O_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccNameId)
                                                                                                  .otherwise(O_Dedupe_CniAgencyAccId_join_df.V_Dedupe_CniAgencyAccId))\
                                                                  .select('Id','O_Dedupe_CniAgencyAccId')
    
    LKP_AgencyAccount_byId_df = LKP_AgencyAccount_byId_df.alias('LKP_AgencyAccount_byId_df')
    LKP_AgencyAccount_byName_df = LKP_AgencyAccount_byName_df.alias('LKP_AgencyAccount_byName_df')

    LKP_AgencyAccount_join_df = LKP_AgencyAccount_byId_df.join(LKP_AgencyAccount_byName_df,LKP_AgencyAccount_byId_df.Id == LKP_AgencyAccount_byName_df.Id,"inner")\
                                                         .select(LKP_AgencyAccount_byId_df.Id,LKP_AgencyAccount_byId_df.LKP_AgencyAccount_byId,LKP_AgencyAccount_byName_df.LKP_AgencyAccount_byName)

    LKP_AgencyAccount_join_df = LKP_AgencyAccount_join_df.alias('LKP_AgencyAccount_join_df')
    O_Dedupe_CniAgencyAccId_df = O_Dedupe_CniAgencyAccId_df.alias('O_Dedupe_CniAgencyAccId_df')

    Atlas_AgencyAccount_Id_join_df = LKP_AgencyAccount_join_df.join(O_Dedupe_CniAgencyAccId_df,LKP_AgencyAccount_join_df.Id == O_Dedupe_CniAgencyAccId_df.Id,"inner")\
                                                             .select('LKP_AgencyAccount_join_df.*',O_Dedupe_CniAgencyAccId_df.O_Dedupe_CniAgencyAccId)

    Atlas_AgencyAccount_Id_Name_df = Atlas_AgencyAccount_Id_join_df.withColumn('Atlas_AgencyAccount_Id_Name',when(Atlas_AgencyAccount_Id_join_df.LKP_AgencyAccount_byName.isNull(),Atlas_AgencyAccount_Id_join_df.O_Dedupe_CniAgencyAccId)
                                                                                                  .otherwise(Atlas_AgencyAccount_Id_join_df.LKP_AgencyAccount_byName))\
                                                                  .select('Id','LKP_AgencyAccount_byId','Atlas_AgencyAccount_Id_Name')

    Atlas_AgencyAccount_Id_df = Atlas_AgencyAccount_Id_Name_df.withColumn('Atlas_AgencyAccount_Id',when(Atlas_AgencyAccount_Id_Name_df.LKP_AgencyAccount_byId.isNull(),Atlas_AgencyAccount_Id_Name_df.Atlas_AgencyAccount_Id_Name)
                                                                                                  .otherwise(Atlas_AgencyAccount_Id_Name_df.LKP_AgencyAccount_byId))\
                                                              .select('Id','Atlas_AgencyAccount_Id')
    
    Atlas_AgencyAccount_Id_df = Atlas_AgencyAccount_Id_df.withColumn('Publisher_Name',F.lit('CN China'))
    
    Atlas_AgencyAccount_Id_df = Atlas_AgencyAccount_Id_df.alias('Atlas_AgencyAccount_Id_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    cond = [Atlas_AgencyAccount_Id_df.Atlas_AgencyAccount_Id == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c,Atlas_AgencyAccount_Id_df.Publisher_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Publisher_Name__c]

    LKP_AtlasAgency_PubAcc_Id_df = Atlas_AgencyAccount_Id_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,cond,"left")\
                                                           .select(Atlas_AgencyAccount_Id_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('LKP_AtlasAgency_PubAcc_Id'))
    
    Src_CNIAgency_RecordTypeId_df = Src_CNIAgency_RecordTypeId_df.alias('Src_CNIAgency_RecordTypeId_df')
    LKP_AtlasAgency_PubAcc_Id_df = LKP_AtlasAgency_PubAcc_Id_df.alias('LKP_AtlasAgency_PubAcc_Id_df')

    Agency_Publisher_Account__c_join_df = Src_CNIAgency_RecordTypeId_df.join(LKP_AtlasAgency_PubAcc_Id_df,Src_CNIAgency_RecordTypeId_df.Id == LKP_AtlasAgency_PubAcc_Id_df.Id,"inner")\
                                                                      .select(Src_CNIAgency_RecordTypeId_df.Id,Src_CNIAgency_RecordTypeId_df.Src_CNIAgency_RecordTypeId,LKP_AtlasAgency_PubAcc_Id_df.LKP_AtlasAgency_PubAcc_Id)
    
    Agency_Publisher_Account__c_df = Agency_Publisher_Account__c_join_df.withColumn('Agency_Publisher_Account__c',when(Agency_Publisher_Account__c_join_df.Src_CNIAgency_RecordTypeId == '012b0000000QBYUAA4',Agency_Publisher_Account__c_join_df.LKP_AtlasAgency_PubAcc_Id)
                                                                                                        .otherwise('Null'))\
                                                                     .select('Id','Agency_Publisher_Account__c')
    
    return Agency_Publisher_Account__c_df
    
def Booked_By__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df):
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_USER_BOOKEDBY_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Booked_By__c == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_USER_BOOKEDBY_Id'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Spark_df.withColumn('IN_FAN_USERID',F.lit('005b0000001ohQTAAY'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Fan_user_df.alias('CNI_Booking__c_Fan_user_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_FAN_USER_Id_df = CNI_Booking__c_Fan_user_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Fan_user_df.IN_FAN_USERID == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Fan_user_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_FAN_USER_Id'))
    
    LKP_USER_BOOKEDBY_Id_df = LKP_USER_BOOKEDBY_Id_df.alias('LKP_USER_BOOKEDBY_Id_df')
    LKP_FAN_USER_Id_df = LKP_FAN_USER_Id_df.alias('LKP_FAN_USER_Id_df')

    Booked_By__c_join_df = LKP_USER_BOOKEDBY_Id_df.join(LKP_FAN_USER_Id_df,LKP_USER_BOOKEDBY_Id_df.Id == LKP_FAN_USER_Id_df.Id,"inner")\
                                              .select(LKP_USER_BOOKEDBY_Id_df.Id,LKP_USER_BOOKEDBY_Id_df.LKP_USER_BOOKEDBY_Id,LKP_FAN_USER_Id_df.LKP_FAN_USER_Id)
    
    Booked_By__c_df = Booked_By__c_join_df.withColumn('Booked_By__c',when(Booked_By__c_join_df.LKP_USER_BOOKEDBY_Id.isNull(),Booked_By__c_join_df.LKP_FAN_USER_Id)
                                                                    .otherwise(Booked_By__c_join_df.LKP_USER_BOOKEDBY_Id))\
                                  .select('Id','Booked_By__c')
    
    return Booked_By__c_df
    
def Booked_On_Behalf_Of__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df):
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_USER_BOOKEDBY_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Booked_By__c == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_USER_BOOKEDBY_Id'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Spark_df.withColumn('IN_FAN_USERID',F.lit('005b0000001ohQTAAY'))

    CNI_Booking__c_Fan_user_df = CNI_Booking__c_Fan_user_df.alias('CNI_Booking__c_Fan_user_df')
    LKP_SF_Target_User_data_Spark_df = LKP_SF_Target_User_data_Spark_df.alias('LKP_SF_Target_User_data_Spark_df')

    LKP_FAN_USER_Id_df = CNI_Booking__c_Fan_user_df.join(LKP_SF_Target_User_data_Spark_df,CNI_Booking__c_Fan_user_df.IN_FAN_USERID == LKP_SF_Target_User_data_Spark_df.TPS_ID__c,"left")\
                                                     .select(CNI_Booking__c_Fan_user_df.Id,col('LKP_SF_Target_User_data_Spark_df.Id').alias('LKP_FAN_USER_Id'))
    
    LKP_USER_BOOKEDBY_Id_df = LKP_USER_BOOKEDBY_Id_df.alias('LKP_USER_BOOKEDBY_Id_df')
    LKP_FAN_USER_Id_df = LKP_FAN_USER_Id_df.alias('LKP_FAN_USER_Id_df')

    Booked_On_Behalf_Of__c_join_df = LKP_USER_BOOKEDBY_Id_df.join(LKP_FAN_USER_Id_df,LKP_USER_BOOKEDBY_Id_df.Id == LKP_FAN_USER_Id_df.Id,"inner")\
                                              .select(LKP_USER_BOOKEDBY_Id_df.Id,LKP_USER_BOOKEDBY_Id_df.LKP_USER_BOOKEDBY_Id,LKP_FAN_USER_Id_df.LKP_FAN_USER_Id)
    
    Booked_On_Behalf_Of__c_df = Booked_On_Behalf_Of__c_join_df.withColumn('Booked_By__c',when(Booked_On_Behalf_Of__c_join_df.LKP_USER_BOOKEDBY_Id.isNull(),Booked_On_Behalf_Of__c_join_df.LKP_FAN_USER_Id)
                                                                    .otherwise(Booked_On_Behalf_Of__c_join_df.LKP_USER_BOOKEDBY_Id))\
                                  .select('Id','Booked_By__c')
    
    return Booked_On_Behalf_Of__c_df
    
def Sub_Category__c_ETL(Atlas_Category_df):
    
    Sub_Category__c_df = Atlas_Category_df.withColumn('Sub_Category__c',when(Atlas_Category_df.Atlas_Category.isNull(),'Other')
                                                                       .otherwise(Atlas_Category_df.Atlas_Sub_Category))\
                                          .select('Id','Sub_Category__c')
    
    return Sub_Category__c_df
    
def Parent_Client_ID__c_ETL(Tgt_Acc_Id_df,LKP_SF_Account_uksupdev_data_Spark_df):

    Tgt_Acc_Id_df = Tgt_Acc_Id_df.alias('Tgt_Acc_Id_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')
    
    Parent_Client_ID__c_df = Tgt_Acc_Id_df.join(LKP_SF_Account_uksupdev_data_Spark_df,Tgt_Acc_Id_df.Tgt_Acc_Id == LKP_SF_Account_uksupdev_data_Spark_df.Id,"left")\
                                               .select(Tgt_Acc_Id_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.ParentId').alias('Parent_Client_ID__c'))
                                               
    return Parent_Client_ID__c_df
    
def Category__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df):
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    LKP_CniAccount_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c,"left")\
                                                  .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('LKP_CniAccount_Id'))
    
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    V_CniAccountName_TransbyID_df = CNI_Booking__c_Spark_df.join(Lkp_English_Translation_df,CNI_Booking__c_Spark_df.CNI_Account__c == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                            .select(CNI_Booking__c_Spark_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniAccountName_TransbyID'))

    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')
    
    CNI_Account_Name_df = CNI_Booking__c_Spark_df.join(LKP_SF_Account_uksupdev_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Account__c == LKP_SF_Account_uksupdev_data_Spark_df.Id,"left")\
                                                 .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.Name').alias('CNI_Account_Name'))

    V_CniAccountName_TransbyName_df = CNI_Account_Name_df.join(Lkp_English_Translation_df,CNI_Account_Name_df.CNI_Account_Name == Lkp_English_Translation_df.Account_Name,"left")\
                                                         .select(CNI_Account_Name_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniAccountName_TransbyName'))

    V_CniAccountName_TransbyID_df = V_CniAccountName_TransbyID_df.alias('V_CniAccountName_TransbyID_df')
    V_CniAccountName_TransbyName_df = V_CniAccountName_TransbyName_df.alias('V_CniAccountName_TransbyName_df')

    LKP_CniAccount_Name_join_df = V_CniAccountName_TransbyID_df.join(V_CniAccountName_TransbyName_df,V_CniAccountName_TransbyID_df.Id == V_CniAccountName_TransbyName_df.Id,"inner")\
                                                               .select(V_CniAccountName_TransbyID_df.Id,V_CniAccountName_TransbyID_df.V_CniAccountName_TransbyID,V_CniAccountName_TransbyName_df.V_CniAccountName_TransbyName)

    IN_CniAccountName_df = LKP_CniAccount_Name_join_df.withColumn('IN_CniAccountName',when(LKP_CniAccount_Name_join_df.V_CniAccountName_TransbyID.isNull(),LKP_CniAccount_Name_join_df.V_CniAccountName_TransbyName)
                                                                                       .otherwise(LKP_CniAccount_Name_join_df.V_CniAccountName_TransbyID))\
                                                      .select('Id','IN_CniAccountName')

    IN_CniAccountName_df = IN_CniAccountName_df.alias('IN_CniAccountName_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    LKP_CniAccount_NameId_df = IN_CniAccountName_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,IN_CniAccountName_df.IN_CniAccountName == LKP_SF_Target_Publisher_Account_data_Spark_df.Account_Name__c,"left")\
                                                   .select(IN_CniAccountName_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('LKP_CniAccount_NameId'))
    
    V_CniAccountName_TransbyID_df = V_CniAccountName_TransbyID_df.alias('V_CniAccountName_TransbyID_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniAccountName_TransDeduped_df = V_CniAccountName_TransbyID_df.join(Lkp_Account_Dedupe_df,V_CniAccountName_TransbyID_df.V_CniAccountName_TransbyID == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                     .select(V_CniAccountName_TransbyID_df.Id,col('V_CniAccountName_TransbyID_df.V_CniAccountName_TransbyID').alias('V_CniAccountName_Trans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniAccountName_TransDeduped'))

    V_CniAccountName_TransDeduped_df = V_CniAccountName_TransDeduped_df.alias('V_CniAccountName_TransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAccId_dedupe_df = V_CniAccountName_TransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAccountName_TransDeduped_df.V_CniAccountName_TransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                  .select(V_CniAccountName_TransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAccId_dedupe'))

    V_CniAccountName_TransDeduped_df = V_CniAccountName_TransDeduped_df.alias('V_CniAccountName_TransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAccId_trans_df = V_CniAccountName_TransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAccountName_TransDeduped_df.V_CniAccountName_Trans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                 .select(V_CniAccountName_TransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAccId_trans'))

    V_Dedupe_CniAccId_dedupe_df = V_Dedupe_CniAccId_dedupe_df.alias('V_Dedupe_CniAccId_dedupe_df')
    V_Dedupe_CniAccId_trans_df = V_Dedupe_CniAccId_trans_df.alias('V_Dedupe_CniAccId_trans_df')

    V_Dedupe_CniAccId_join_df = V_Dedupe_CniAccId_dedupe_df.join(V_Dedupe_CniAccId_trans_df,V_Dedupe_CniAccId_dedupe_df.Id == V_Dedupe_CniAccId_trans_df.Id,"inner")\
                                                           .select(V_Dedupe_CniAccId_dedupe_df.Id,V_Dedupe_CniAccId_dedupe_df.V_Dedupe_CniAccId_dedupe,V_Dedupe_CniAccId_trans_df.V_Dedupe_CniAccId_trans)

    V_Dedupe_CniAccId_df = V_Dedupe_CniAccId_join_df.withColumn('V_Dedupe_CniAccId',when(V_Dedupe_CniAccId_join_df.V_Dedupe_CniAccId_dedupe.isNull(),V_Dedupe_CniAccId_join_df.V_Dedupe_CniAccId_trans)
                                                                                                  .otherwise(V_Dedupe_CniAccId_join_df.V_Dedupe_CniAccId_dedupe))\
                                                            .select('Id','V_Dedupe_CniAccId')

    V_CniAccountName_TransbyName_df = V_CniAccountName_TransbyName_df.alias('V_CniAccountName_TransbyName_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniAccountNameTransDeduped_df = V_CniAccountName_TransbyName_df.join(Lkp_Account_Dedupe_df,V_CniAccountName_TransbyName_df.V_CniAccountName_TransbyName == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                     .select(V_CniAccountName_TransbyName_df.Id,col('V_CniAccountName_TransbyName_df.V_CniAccountName_TransbyName').alias('V_CniAccountNameTrans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniAccountNameTransDeduped'))

    V_CniAccountNameTransDeduped_df = V_CniAccountNameTransDeduped_df.alias('V_CniAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAccName_dedupe_df = V_CniAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAccountNameTransDeduped_df.V_CniAccountNameTransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                   .select(V_CniAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAccName_dedupe'))

    V_CniAccountNameTransDeduped_df = V_CniAccountNameTransDeduped_df.alias('V_CniAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniAccName_trans_df = V_CniAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniAccountNameTransDeduped_df.V_CniAccountNameTrans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                  .select(V_CniAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniAccName_trans'))

    V_Dedupe_CniAccName_dedupe_df = V_Dedupe_CniAccName_dedupe_df.alias('V_Dedupe_CniAccName_dedupe_df')
    V_Dedupe_CniAccName_trans_df = V_Dedupe_CniAccName_trans_df.alias('V_Dedupe_CniAccName_trans_df')

    V_Dedupe_CniAccName_join_df = V_Dedupe_CniAccName_dedupe_df.join(V_Dedupe_CniAccName_trans_df,V_Dedupe_CniAccName_dedupe_df.Id == V_Dedupe_CniAccName_trans_df.Id,"inner")\
                                                               .select(V_Dedupe_CniAccName_dedupe_df.Id,V_Dedupe_CniAccName_dedupe_df.V_Dedupe_CniAccName_dedupe,V_Dedupe_CniAccName_trans_df.V_Dedupe_CniAccName_trans)

    V_Dedupe_CniAccNameId_df = V_Dedupe_CniAccName_join_df.withColumn('V_Dedupe_CniAccNameId',when(V_Dedupe_CniAccName_join_df.V_Dedupe_CniAccName_dedupe.isNull(),V_Dedupe_CniAccName_join_df.V_Dedupe_CniAccName_trans)
                                                                                                  .otherwise(V_Dedupe_CniAccName_join_df.V_Dedupe_CniAccName_dedupe))\
                                                          .select('Id','V_Dedupe_CniAccNameId')

    V_Dedupe_CniAccId_df = V_Dedupe_CniAccId_df.alias('V_Dedupe_CniAccId_df')
    V_Dedupe_CniAccNameId_df = V_Dedupe_CniAccNameId_df.alias('V_Dedupe_CniAccNameId_df')

    O_Dedupe_CniAccId_join_df = V_Dedupe_CniAccId_df.join(V_Dedupe_CniAccNameId_df,V_Dedupe_CniAccId_df.Id == V_Dedupe_CniAccNameId_df.Id,"inner")\
                                                    .select(V_Dedupe_CniAccId_df.Id,V_Dedupe_CniAccId_df.V_Dedupe_CniAccId,V_Dedupe_CniAccNameId_df.V_Dedupe_CniAccNameId)

    O_Dedupe_CniAccId_df = O_Dedupe_CniAccId_join_df.withColumn('O_Dedupe_CniAccId',when(O_Dedupe_CniAccId_join_df.V_Dedupe_CniAccId.isNull(),O_Dedupe_CniAccId_join_df.V_Dedupe_CniAccNameId)
                                                                                                  .otherwise(O_Dedupe_CniAccId_join_df.V_Dedupe_CniAccId))\
                                                    .select('Id','O_Dedupe_CniAccId')
    
    LKP_CniAccount_Id_df = LKP_CniAccount_Id_df.alias('LKP_CniAccount_Id_df')
    LKP_CniAccount_NameId_df = LKP_CniAccount_NameId_df.alias('LKP_CniAccount_NameId_df')

    LKP_Account_join_df = LKP_CniAccount_Id_df.join(LKP_CniAccount_NameId_df,LKP_CniAccount_Id_df.Id == LKP_CniAccount_NameId_df.Id,"inner")\
                                              .select(LKP_CniAccount_Id_df.Id,LKP_CniAccount_Id_df.LKP_CniAccount_Id,LKP_CniAccount_NameId_df.LKP_CniAccount_NameId)

    LKP_Account_join_df = LKP_Account_join_df.alias('LKP_Account_join_df')
    O_Dedupe_CniAccId_df = O_Dedupe_CniAccId_df.alias('O_Dedupe_CniAccId_df')

    Tgt_Acc_Id_join_df = LKP_Account_join_df.join(O_Dedupe_CniAccId_df,LKP_Account_join_df.Id == O_Dedupe_CniAccId_df.Id,"inner")\
                                            .select('LKP_Account_join_df.*',O_Dedupe_CniAccId_df.O_Dedupe_CniAccId)

    Tgt_Acc_Id_Name_df = Tgt_Acc_Id_join_df.withColumn('Tgt_Acc_Id_Name',when(Tgt_Acc_Id_join_df.LKP_CniAccount_NameId.isNull(),Tgt_Acc_Id_join_df.O_Dedupe_CniAccId)
                                                                                                  .otherwise(Tgt_Acc_Id_join_df.LKP_CniAccount_NameId))\
                                                                       .select('Id','LKP_CniAccount_Id','Tgt_Acc_Id_Name')

    Tgt_Acc_Id_df = Tgt_Acc_Id_Name_df.withColumn('Tgt_Acc_Id',when(Tgt_Acc_Id_Name_df.LKP_CniAccount_Id.isNull(),Tgt_Acc_Id_Name_df.Tgt_Acc_Id_Name)
                                                                                                  .otherwise(Tgt_Acc_Id_Name_df.LKP_CniAccount_Id))\
                                                              .select('Id','Tgt_Acc_Id')
    
    Tgt_Acc_Id_df = Tgt_Acc_Id_df.withColumn('Publisher_Name',F.lit('CN China'))
    
    Parent_Client_ID__c_df = Parent_Client_ID__c_ETL(Tgt_Acc_Id_df,LKP_SF_Account_uksupdev_data_Spark_df)
    
    Tgt_Acc_Id_df = Tgt_Acc_Id_df.alias('Tgt_Acc_Id_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    cond = [Tgt_Acc_Id_df.Tgt_Acc_Id == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c,Tgt_Acc_Id_df.Publisher_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Publisher_Name__c]

    Atlas_Category_df = Tgt_Acc_Id_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,cond,"left")\
                                     .select(Tgt_Acc_Id_df.Id,col('Tgt_Acc_Id_df.Tgt_Acc_Id').alias('Client__c'),col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Client_Publisher_Account__c'),col('LKP_SF_Target_Publisher_Account_data_Spark_df.Category__c').alias('Atlas_Category'),col('LKP_SF_Target_Publisher_Account_data_Spark_df.Sub_Category__c').alias('Atlas_Sub_Category'))
    
    Sub_Category__c_df = Sub_Category__c_ETL(Atlas_Category_df)
    
    Category__c_df = Atlas_Category_df.withColumn('Category__c',when(Atlas_Category_df.Atlas_Category.isNull(),'M&E - Media & Entertainment')
                                                                                           .otherwise(Atlas_Category_df.Atlas_Category))\
                                      .select('Id','Client_Publisher_Account__c','Client__c','Category__c')
    
    Category__c_df = Category__c_df.alias('Category__c_df')
    Sub_Category__c_df = Sub_Category__c_df.alias('Sub_Category__c_df')

    Category_df = Category__c_df.join(Sub_Category__c_df,Category__c_df.Id == Sub_Category__c_df.Id,"inner")\
                                            .select('Category__c_df.*',Sub_Category__c_df.Sub_Category__c)
                                            
    Category__c_df = Category__c_df.alias('Category__c_df')
    Sub_Category__c_df = Sub_Category__c_df.alias('Sub_Category__c_df')
    
    Category_df = Category__c_df.join(Parent_Client_ID__c_df,Category__c_df.Id == Parent_Client_ID__c_df.Id,"inner")\
                                            .select('Category__c_df.*',Parent_Client_ID__c_df.Parent_Client_ID__c)
    
    return Category_df
    
def Charge_To_Publisher_Account__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df):    
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    LKP_Billingaccount_Id_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Billing_Account__c == LKP_SF_Target_Publisher_Account_data_Spark_df.External_UID__c,"left")\
                                                  .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('LKP_Billingaccount_Id'))
        
        
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    Lkp_English_Translation_df = Lkp_English_Translation_df.alias('Lkp_English_Translation_df')

    V_CniBillingAccountNameTransbyID_df = CNI_Booking__c_Spark_df.join(Lkp_English_Translation_df,CNI_Booking__c_Spark_df.CNI_Billing_Account__c == Lkp_English_Translation_df.Salesforce_ID,"left")\
                                                                 .select(CNI_Booking__c_Spark_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniBillingAccountNameTransbyID'))

    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')
        
    CNI_BillingAccount_Name_df = CNI_Booking__c_Spark_df.join(LKP_SF_Account_uksupdev_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Billing_Account__c == LKP_SF_Account_uksupdev_data_Spark_df.Id,"left")\
                                                     .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.Name').alias('CNI_BillingAccount_Name'))

    V_CniBillingAccountNameTransbyName_df = CNI_BillingAccount_Name_df.join(Lkp_English_Translation_df,CNI_BillingAccount_Name_df.CNI_BillingAccount_Name == Lkp_English_Translation_df.Account_Name,"left")\
                                                                      .select(CNI_BillingAccount_Name_df.Id,ltrim(rtrim(col('Lkp_English_Translation_df.English_Translation'))).alias('V_CniBillingAccountNameTransbyName'))

    V_CniBillingAccountNameTransbyID_df = V_CniBillingAccountNameTransbyID_df.alias('V_CniBillingAccountNameTransbyID_df')
    V_CniBillingAccountNameTransbyName_df = V_CniBillingAccountNameTransbyName_df.alias('V_CniBillingAccountNameTransbyName_df')

    LKP_CNI_BillingAccount_Name_join_df = V_CniBillingAccountNameTransbyID_df.join(V_CniBillingAccountNameTransbyName_df,V_CniBillingAccountNameTransbyID_df.Id == V_CniBillingAccountNameTransbyName_df.Id,"inner")\
                                                                   .select(V_CniBillingAccountNameTransbyID_df.Id,V_CniBillingAccountNameTransbyID_df.V_CniBillingAccountNameTransbyID,V_CniBillingAccountNameTransbyName_df.V_CniBillingAccountNameTransbyName)

    IN_CNI_BillingAccount_Name_df = LKP_CNI_BillingAccount_Name_join_df.withColumn('IN_CNI_BillingAccount_Name',when(LKP_CNI_BillingAccount_Name_join_df.V_CniBillingAccountNameTransbyID.isNull(),LKP_CNI_BillingAccount_Name_join_df.V_CniBillingAccountNameTransbyName)
                                                                                                               .otherwise(LKP_CNI_BillingAccount_Name_join_df.V_CniBillingAccountNameTransbyID))\
                                                                        .select('Id','IN_CNI_BillingAccount_Name')

    IN_CNI_BillingAccount_Name_df = IN_CNI_BillingAccount_Name_df.alias('IN_CNI_BillingAccount_Name_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    LKP_Billingaccount_NameId_df = IN_CNI_BillingAccount_Name_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,IN_CNI_BillingAccount_Name_df.IN_CNI_BillingAccount_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Account_Name__c,"left")\
                                                                .select(IN_CNI_BillingAccount_Name_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c').alias('LKP_Billingaccount_NameId'))
        
    V_CniBillingAccountNameTransbyID_df = V_CniBillingAccountNameTransbyID_df.alias('V_CniBillingAccountNameTransbyID_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniBillingAccountName_TransDeduped_df = V_CniBillingAccountNameTransbyID_df.join(Lkp_Account_Dedupe_df,V_CniBillingAccountNameTransbyID_df.V_CniBillingAccountNameTransbyID == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                                 .select(V_CniBillingAccountNameTransbyID_df.Id,col('V_CniBillingAccountNameTransbyID_df.V_CniBillingAccountNameTransbyID').alias('V_CniBillingAccountName_Trans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniBillingAccountName_TransDeduped'))

    V_CniBillingAccountName_TransDeduped_df = V_CniBillingAccountName_TransDeduped_df.alias('V_CniBillingAccountName_TransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniBillingAccId_dedupe_df = V_CniBillingAccountName_TransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniBillingAccountName_TransDeduped_df.V_CniBillingAccountName_TransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                      .select(V_CniBillingAccountName_TransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniBillingAccId_dedupe'))

    V_CniBillingAccountName_TransDeduped_df = V_CniBillingAccountName_TransDeduped_df.alias('V_CniBillingAccountName_TransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniBillingAccId_trans_df = V_CniBillingAccountName_TransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniBillingAccountName_TransDeduped_df.V_CniBillingAccountName_Trans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                     .select(V_CniBillingAccountName_TransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniBillingAccId_trans'))

    V_Dedupe_CniBillingAccId_dedupe_df = V_Dedupe_CniBillingAccId_dedupe_df.alias('V_Dedupe_CniBillingAccId_dedupe_df')
    V_Dedupe_CniBillingAccId_trans_df = V_Dedupe_CniBillingAccId_trans_df.alias('V_Dedupe_CniBillingAccId_trans_df')

    V_Dedupe_CniBillingAccId_join_df = V_Dedupe_CniBillingAccId_dedupe_df.join(V_Dedupe_CniBillingAccId_trans_df,V_Dedupe_CniBillingAccId_dedupe_df.Id == V_Dedupe_CniBillingAccId_trans_df.Id,"inner")\
                                                               .select(V_Dedupe_CniBillingAccId_dedupe_df.Id,V_Dedupe_CniBillingAccId_dedupe_df.V_Dedupe_CniBillingAccId_dedupe,V_Dedupe_CniBillingAccId_trans_df.V_Dedupe_CniBillingAccId_trans)

    V_Dedupe_CniBillingAccId_df = V_Dedupe_CniBillingAccId_join_df.withColumn('V_Dedupe_CniBillingAccId',when(V_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccId_dedupe.isNull(),V_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccId_trans)
                                                                                                      .otherwise(V_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccId_dedupe))\
                                                                .select('Id','V_Dedupe_CniBillingAccId')

    V_CniBillingAccountNameTransbyName_df = V_CniBillingAccountNameTransbyName_df.alias('V_CniBillingAccountNameTransbyName_df')
    Lkp_Account_Dedupe_df = Lkp_Account_Dedupe_df.alias('Lkp_Account_Dedupe_df')

    V_CniBillingAccountNameTransDeduped_df = V_CniBillingAccountNameTransbyName_df.join(Lkp_Account_Dedupe_df,V_CniBillingAccountNameTransbyName_df.V_CniBillingAccountNameTransbyName == Lkp_Account_Dedupe_df.Market_Account_English_Name,"left")\
                                                                                  .select(V_CniBillingAccountNameTransbyName_df.Id,col('V_CniBillingAccountNameTransbyName_df.V_CniBillingAccountNameTransbyName').alias('V_CniBillingAccountNameTrans'),ltrim(rtrim(col('Lkp_Account_Dedupe_df.Atlas_Account_Name'))).alias('V_CniBillingAccountNameTransDeduped'))

    V_CniBillingAccountNameTransDeduped_df = V_CniBillingAccountNameTransDeduped_df.alias('V_CniBillingAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniBillingAccName_dedupe_df = V_CniBillingAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniBillingAccountNameTransDeduped_df.V_CniBillingAccountNameTransDeduped == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                              .select(V_CniBillingAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniBillingAccName_dedupe'))

    V_CniBillingAccountNameTransDeduped_df = V_CniBillingAccountNameTransDeduped_df.alias('V_CniBillingAccountNameTransDeduped_df')
    LKP_SF_Target_Account_data_Spark_df = LKP_SF_Target_Account_data_Spark_df.alias('LKP_SF_Target_Account_data_Spark_df')

    V_Dedupe_CniBillingAccName_trans_df = V_CniBillingAccountNameTransDeduped_df.join(LKP_SF_Target_Account_data_Spark_df,V_CniBillingAccountNameTransDeduped_df.V_CniBillingAccountNameTrans == LKP_SF_Target_Account_data_Spark_df.Name,"left")\
                                                                                .select(V_CniBillingAccountNameTransDeduped_df.Id,ltrim(rtrim(col('LKP_SF_Target_Account_data_Spark_df.Id'))).alias('V_Dedupe_CniBillingAccName_trans'))

    V_Dedupe_CniBillingAccName_dedupe_df = V_Dedupe_CniBillingAccName_dedupe_df.alias('V_Dedupe_CniBillingAccName_dedupe_df')
    V_Dedupe_CniBillingAccName_trans_df = V_Dedupe_CniBillingAccName_trans_df.alias('V_Dedupe_CniBillingAccName_trans_df')

    V_Dedupe_CniBillingAccName_join_df = V_Dedupe_CniBillingAccName_dedupe_df.join(V_Dedupe_CniBillingAccName_trans_df,V_Dedupe_CniBillingAccName_dedupe_df.Id == V_Dedupe_CniBillingAccName_trans_df.Id,"inner")\
                                                                             .select(V_Dedupe_CniBillingAccName_dedupe_df.Id,V_Dedupe_CniBillingAccName_dedupe_df.V_Dedupe_CniBillingAccName_dedupe,V_Dedupe_CniBillingAccName_trans_df.V_Dedupe_CniBillingAccName_trans)

    V_Dedupe_CniBillingAccNameId_df = V_Dedupe_CniBillingAccName_join_df.withColumn('V_Dedupe_CniBillingAccNameId',when(V_Dedupe_CniBillingAccName_join_df.V_Dedupe_CniBillingAccName_dedupe.isNull(),V_Dedupe_CniBillingAccName_join_df.V_Dedupe_CniBillingAccName_trans)
                                                                                             .otherwise(V_Dedupe_CniBillingAccName_join_df.V_Dedupe_CniBillingAccName_dedupe))\
                                                              .select('Id','V_Dedupe_CniBillingAccNameId')

    V_Dedupe_CniBillingAccId_df = V_Dedupe_CniBillingAccId_df.alias('V_Dedupe_CniBillingAccId_df')
    V_Dedupe_CniBillingAccNameId_df = V_Dedupe_CniBillingAccNameId_df.alias('V_Dedupe_CniBillingAccNameId_df')

    O_Dedupe_CniBillingAccId_join_df = V_Dedupe_CniBillingAccId_df.join(V_Dedupe_CniBillingAccNameId_df,V_Dedupe_CniBillingAccId_df.Id == V_Dedupe_CniBillingAccNameId_df.Id,"inner")\
                                                               .select(V_Dedupe_CniBillingAccId_df.Id,V_Dedupe_CniBillingAccId_df.V_Dedupe_CniBillingAccId,V_Dedupe_CniBillingAccNameId_df.V_Dedupe_CniBillingAccNameId)

    O_Dedupe_CniBillingAccId_df = O_Dedupe_CniBillingAccId_join_df.withColumn('O_Dedupe_CniBillingAccId',when(O_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccId.isNull(),O_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccNameId)
                                                                                                        .otherwise(O_Dedupe_CniBillingAccId_join_df.V_Dedupe_CniBillingAccId))\
                                                                  .select('Id','O_Dedupe_CniBillingAccId')
        
    LKP_Billingaccount_Id_df = LKP_Billingaccount_Id_df.alias('LKP_Billingaccount_Id_df')
    LKP_Billingaccount_NameId_df = LKP_Billingaccount_NameId_df.alias('LKP_Billingaccount_NameId_df')

    LKP_BillingAccount_join_df = LKP_Billingaccount_Id_df.join(LKP_Billingaccount_NameId_df,LKP_Billingaccount_Id_df.Id == LKP_Billingaccount_NameId_df.Id,"inner")\
                                                         .select(LKP_Billingaccount_Id_df.Id,LKP_Billingaccount_Id_df.LKP_Billingaccount_Id,LKP_Billingaccount_NameId_df.LKP_Billingaccount_NameId)

    LKP_BillingAccount_join_df = LKP_BillingAccount_join_df.alias('LKP_BillingAccount_join_df')
    O_Dedupe_CniBillingAccId_df = O_Dedupe_CniBillingAccId_df.alias('O_Dedupe_CniBillingAccId_df')

    TGT_BILLINGACCOUNT_Id_join_df = LKP_BillingAccount_join_df.join(O_Dedupe_CniBillingAccId_df,LKP_BillingAccount_join_df.Id == O_Dedupe_CniBillingAccId_df.Id,"inner")\
                                                              .select('LKP_BillingAccount_join_df.*',O_Dedupe_CniBillingAccId_df.O_Dedupe_CniBillingAccId)

    TGT_BILLINGACCOUNT_Id_Name_df = TGT_BILLINGACCOUNT_Id_join_df.withColumn('TGT_BILLINGACCOUNT_Id_Name',when(TGT_BILLINGACCOUNT_Id_join_df.LKP_Billingaccount_NameId.isNull(),TGT_BILLINGACCOUNT_Id_join_df.O_Dedupe_CniBillingAccId)
                                                                                                         .otherwise(TGT_BILLINGACCOUNT_Id_join_df.LKP_Billingaccount_NameId))\
                                                      .select('Id','LKP_Billingaccount_Id','TGT_BILLINGACCOUNT_Id_Name')

    TGT_BILLINGACCOUNT_Id_df = TGT_BILLINGACCOUNT_Id_Name_df.withColumn('TGT_BILLINGACCOUNT_Id',when(TGT_BILLINGACCOUNT_Id_Name_df.LKP_Billingaccount_Id.isNull(),TGT_BILLINGACCOUNT_Id_Name_df.TGT_BILLINGACCOUNT_Id_Name)
                                                                                                      .otherwise(TGT_BILLINGACCOUNT_Id_Name_df.LKP_Billingaccount_Id))\
                                                            .select('Id','TGT_BILLINGACCOUNT_Id')
        
    TGT_BILLINGACCOUNT_Id_df = TGT_BILLINGACCOUNT_Id_df.withColumn('Publisher_Name',F.lit('CN China'))
        
    TGT_BILLINGACCOUNT_Id_df = TGT_BILLINGACCOUNT_Id_df.alias('TGT_BILLINGACCOUNT_Id_df')
    LKP_SF_Target_Publisher_Account_data_Spark_df = LKP_SF_Target_Publisher_Account_data_Spark_df.alias('LKP_SF_Target_Publisher_Account_data_Spark_df')

    cond = [TGT_BILLINGACCOUNT_Id_df.TGT_BILLINGACCOUNT_Id == LKP_SF_Target_Publisher_Account_data_Spark_df.Account__c,TGT_BILLINGACCOUNT_Id_df.Publisher_Name == LKP_SF_Target_Publisher_Account_data_Spark_df.Publisher_Name__c]

    Charge_To_Publisher_Account__c_df = TGT_BILLINGACCOUNT_Id_df.join(LKP_SF_Target_Publisher_Account_data_Spark_df,cond,"left")\
                                                    .select(TGT_BILLINGACCOUNT_Id_df.Id,col('LKP_SF_Target_Publisher_Account_data_Spark_df.Id').alias('Charge_To_Publisher_Account__c'))
                                                    
    return Charge_To_Publisher_Account__c_df
    
def Client_Brand__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Client_Brand_c_data_Spark_df):
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Account_uksupdev_data_Spark_df = LKP_SF_Account_uksupdev_data_Spark_df.alias('LKP_SF_Account_uksupdev_data_Spark_df')

    LKP_CNIAccount_join_df = CNI_Booking__c_Spark_df.join(LKP_SF_Account_uksupdev_data_Spark_df,CNI_Booking__c_Spark_df.CNI_Account__c == LKP_SF_Account_uksupdev_data_Spark_df.Id,"inner")\
                                                    .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Account_uksupdev_data_Spark_df.Name').alias('CNI_Account_Name'),col('LKP_SF_Account_uksupdev_data_Spark_df.RecordTypeId').alias('LKP_CniAccount_RecordTypeId'))

    O_CniAccountBrandName_df = LKP_CNIAccount_join_df.withColumn('O_CniAccountBrandName',when(LKP_CNIAccount_join_df.LKP_CniAccount_RecordTypeId == '012b0000000QHwRAAW',LKP_CNIAccount_join_df.CNI_Account_Name)
                                                                                           .otherwise(LKP_CNIAccount_join_df.LKP_CniAccount_RecordTypeId))\
                                                     .select('Id','O_CniAccountBrandName')
    
    O_CniAccountBrandName_df = O_CniAccountBrandName_df.alias('O_CniAccountBrandName_df')
    LKP_SF_Target_Client_Brand_c_data_Spark_df = LKP_SF_Target_Client_Brand_c_data_Spark_df.alias('LKP_SF_Target_Client_Brand_c_data_Spark_df')

    Client_Brand__c_df = O_CniAccountBrandName_df.join(LKP_SF_Target_Client_Brand_c_data_Spark_df,O_CniAccountBrandName_df.O_CniAccountBrandName == LKP_SF_Target_Client_Brand_c_data_Spark_df.Name,"left")\
                                                .select(O_CniAccountBrandName_df.Id,col('LKP_SF_Target_Client_Brand_c_data_Spark_df.Id').alias('LKP_TGT_BRAND_Id'))

    return Client_Brand__c_df
    
def Committed_Date__c_ETL(CNI_Booking__c_Spark_df):
    
    Committed_Date__c_df = CNI_Booking__c_Spark_df.withColumn('Committed_Date__c',when(CNI_Booking__c_Spark_df.CNI_Status__c == 'Confirmed',CNI_Booking__c_Spark_df.LastModifiedDate)
                                                                                 .otherwise(current_timestamp()))\
                                                  .select('Id','Committed_Date__c')
    return Committed_Date__c_df
    
def Contact__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_Contact_data_Spark_df):
    
    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    LKP_SF_Target_Contact_data_Spark_df = LKP_SF_Target_Contact_data_Spark_df.alias('LKP_SF_Target_Contact_data_Spark_df')

    Contact__c_df = CNI_Booking__c_Spark_df.join(LKP_SF_Target_Contact_data_Spark_df,CNI_Booking__c_Spark_df.CNI_PrimaryContact__c == LKP_SF_Target_Contact_data_Spark_df.TPS_ID__c,"left")\
                                           .select(CNI_Booking__c_Spark_df.Id,col('LKP_SF_Target_Contact_data_Spark_df.Id').alias('Contact__c'))
    
    return Contact__c_df
    
def Contains_Digital_Lines__c_ETL(CNI_Booking__c_Spark_df):
    
    Contains_Digital_Lines__c_df = CNI_Booking__c_Spark_df.withColumn('Contains_Digital_Lines__c',when(instr(CNI_Booking__c_Spark_df.CNI_Platform__c,'Digital') != 0,'True')
                                                                                             .otherwise('False'))\
                                                          .select('Id','Contains_Digital_Lines__c')
    
    return Contains_Digital_Lines__c_df
    
def Mkt_To_Corp_Exchange_Rate__c_ETL(CNI_Booking__c_Spark_df,Lkp_FF_CNI_BOOKINGS_df,LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df):

    CNI_Booking__c_Spark_df = CNI_Booking__c_Spark_df.alias('CNI_Booking__c_Spark_df')
    Lkp_FF_CNI_BOOKINGS_df = Lkp_FF_CNI_BOOKINGS_df.alias('Lkp_FF_CNI_BOOKINGS_df')

    LKP_CNI_BLI_Start_Date__c_df = CNI_Booking__c_Spark_df.join(Lkp_FF_CNI_BOOKINGS_df,CNI_Booking__c_Spark_df.Id == Lkp_FF_CNI_BOOKINGS_df.CNI_BOOKING_Id,"left")\
                                                         .select(CNI_Booking__c_Spark_df.Id,col('Lkp_FF_CNI_BOOKINGS_df.TGT_BLI_START_DATE').alias('LKP_CNI_BLI_Start_Date__c'))

    LKP_CNI_BLI_Start_Date__c_df = LKP_CNI_BLI_Start_Date__c_df.withColumn('O_FROM_CURRENCY',F.lit('CNY'))\
                                                                .withColumn('O_TO_CURRENCY',F.lit('USD'))

    LKP_CNI_BLI_Start_Date__c_df = LKP_CNI_BLI_Start_Date__c_df.withColumn('year', substring('LKP_CNI_BLI_Start_Date__c', 7,4))\
                                                               .withColumn('month', substring('LKP_CNI_BLI_Start_Date__c', 1,2))\
                                                               .withColumn('day', substring('LKP_CNI_BLI_Start_Date__c', 4,2))

    LKP_CNI_BLI_Start_Date__c_df = LKP_CNI_BLI_Start_Date__c_df.withColumn('LKP_Start_Date',concat(LKP_CNI_BLI_Start_Date__c_df.year,F.lit('-'),LKP_CNI_BLI_Start_Date__c_df.month,F.lit('-'),LKP_CNI_BLI_Start_Date__c_df.day))

    LKP_CNI_BLI_Start_Date__c_df = LKP_CNI_BLI_Start_Date__c_df.alias('LKP_CNI_BLI_Start_Date__c_df')
    LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df = LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df.alias('LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df')

    cond = [LKP_CNI_BLI_Start_Date__c_df.LKP_Start_Date == LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df.Start_Date__c, LKP_CNI_BLI_Start_Date__c_df.O_FROM_CURRENCY == LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df.From_ISOCurrency__c, LKP_CNI_BLI_Start_Date__c_df.O_TO_CURRENCY == LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df.To_ISOCurrency__c]

    Mkt_To_Corp_Exchange_Rate__c_df = LKP_CNI_BLI_Start_Date__c_df.join(LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df,cond,"left")\
                                                                  .select(LKP_CNI_BLI_Start_Date__c_df.Id,col('LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df.Conversion_Rate__c').alias('Mkt_To_Corp_Exchange_Rate__c'))
                                                                  
    return Mkt_To_Corp_Exchange_Rate__c_df
    
if __name__ == "__main__":
    CNI_Booking__c_df = get_input_data()
    CNI_Booking__c_Spark_df = pandas_to_pyspark_df(CNI_Booking__c_df)
    LKP_SF_Target_User_data_df = LKP_SF_Target_User_data()
    LKP_SF_Target_User_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_User_data_df)
    LKP_SF_Account_uksupdev_data_df = LKP_SF_Account_uksupdev_data()
    LKP_SF_Account_uksupdev_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Account_uksupdev_data_df)
    LKP_SF_Target_Publisher_Account_data_df = LKP_SF_Target_Publisher_Account_data()
    LKP_SF_Target_Publisher_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Publisher_Account_data_df)
    LKP_SF_Target_Account_data_df = LKP_SF_Target_Account_data()
    LKP_SF_Target_Account_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Account_data_df)
    LKP_SF_Target_Contact_data_df = LKP_SF_Target_Contact_data()
    LKP_SF_Target_Contact_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Contact_data_df)
    LKP_SF_Target_Client_Brand_c_data_df = LKP_SF_Target_Client_Brand_c_data()
    LKP_SF_Target_Client_Brand_c_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_Client_Brand_c_data_df)
    LKP_SF_Target_AMS_Exchange_Rate_data_df = LKP_SF_Target_AMS_Exchange_Rate_data()
    LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df = pandas_to_pyspark_df(LKP_SF_Target_AMS_Exchange_Rate_data_df)
    Lkp_English_Translation_df = LKP_FF_English_Translation()
    Lkp_Account_Dedupe_df = LKP_FF_Account_Dedupe()
    Lkp_FF_CNI_BOOKINGS_df = LKP_FF_CNI_BOOKINGS()
    OwnerId_df = OwnerId_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df)
    CreatedById_df = CreatedById_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df)
    Agency_Publisher_Account__c_df = Agency_Publisher_Account__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df)
    Booked_By__c_df = Booked_By__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df)
    Booked_On_Behalf_Of__c_df = Booked_On_Behalf_Of__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_User_data_Spark_df)
    Category__c_df = Category__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df)
    Charge_To_Publisher_Account__c_df = Charge_To_Publisher_Account__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Publisher_Account_data_Spark_df,Lkp_English_Translation_df,Lkp_Account_Dedupe_df,LKP_SF_Target_Account_data_Spark_df)
    Client_Brand__c_df = Client_Brand__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Account_uksupdev_data_Spark_df,LKP_SF_Target_Client_Brand_c_data_Spark_df)
    Committed_Date__c_df = Committed_Date__c_ETL(CNI_Booking__c_Spark_df)
    Contact__c_df = Contact__c_ETL(CNI_Booking__c_Spark_df,LKP_SF_Target_Contact_data_Spark_df)
    Contains_Digital_Lines__c_df = Contains_Digital_Lines__c_ETL(CNI_Booking__c_Spark_df)
    Mkt_To_Corp_Exchange_Rate__c_df = Mkt_To_Corp_Exchange_Rate__c_ETL(CNI_Booking__c_Spark_df,Lkp_FF_CNI_BOOKINGS_df,LKP_SF_Target_AMS_Exchange_Rate_data_Spark_df)