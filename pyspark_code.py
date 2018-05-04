from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HHP") \
    .getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions','8')

## Claims

claims = spark.read.csv('gs://hhp-data/HHP_release3/Claims.csv',header=True).repartition(8)
claims=claims\
    .withColumn('SupLOS',claims.SupLOS.astype('short'))\
    .withColumn('MemberID',col('MemberID').astype('integer'))
claims.write.mode('overwrite').parquet('gs://hhp-data/HHP_release3/claims') # change to columnar format
claims = spark.read.parquet('gs://hhp-data/HHP_release3/claims*') # change to columnar format
claims.createOrReplaceTempView('claims')
claims = spark.sql("""
SELECT *,
    CASE 
        WHEN PayDelay = '162+' THEN 162 
        ELSE CAST(PayDelay AS integer) 
    END AS PayDelayI,
    CASE
        WHEN dsfs = '0- 1 month' THEN 1
        WHEN dsfs = '1- 2 months' THEN 2
        WHEN dsfs = '2- 3 months' THEN 3
        WHEN dsfs = '3- 4 months' THEN 4
        WHEN dsfs = '4- 5 months' THEN 5
        WHEN dsfs = '5- 6 months' THEN 6
        WHEN dsfs = '6- 7 months' THEN 7
        WHEN dsfs = '7- 8 months' THEN 8
        WHEN dsfs = '8- 9 months' THEN 9
        WHEN dsfs = '9-10 months' THEN 10
        WHEN dsfs = '10-11 months' THEN 11
        WHEN dsfs = '11-12 months' THEN 12
        WHEN dsfs IS NULL THEN NULL
    END AS dsfsI,
    CASE
        WHEN CharlsonIndex = '0' THEN 0
        WHEN CharlsonIndex = '1-2' THEN 2
        WHEN CharlsonIndex = '3-4' THEN 4
        WHEN CharlsonIndex = '5+' THEN 6
    END AS CharlsonIndexI,
    CASE
        WHEN LengthOfStay = '1 day' THEN 1
        WHEN LengthOfStay = '2 days' THEN 2
        WHEN LengthOfStay = '3 days' THEN 3
        WHEN LengthOfStay = '4 days' THEN 4
        WHEN LengthOfStay = '5 days' THEN 5
        WHEN LengthOfStay = '6 days' THEN 6
        WHEN LengthOfStay = '1- 2 weeks' THEN 11
        WHEN LengthOfStay = '2- 4 weeks' THEN 21
        WHEN LengthOfStay = '4- 8 weeks' THEN 42
        WHEN LengthOfStay = '26+ weeks' THEN 180
        WHEN LengthOfStay IS NULL THEN null
    END AS LengthOfStayI
    FROM claims
""")
claims.createOrReplaceTempView('claims')

claims_per_member = spark.sql("""
SELECT
year
,Memberid
,COUNT(*) AS no_Claims
,COUNT(DISTINCT ProviderID) AS no_Providers
,COUNT(DISTINCT Vendor) AS no_Vendors
,COUNT(DISTINCT PCP) AS no_PCPs
,COUNT(DISTINCT PlaceSvc) AS no_PlaceSvcs
,COUNT(DISTINCT Specialty) AS no_Specialities
,COUNT(DISTINCT PrimaryConditionGroup) AS no_PrimaryConditionGroups
,COUNT(DISTINCT ProcedureGroup) AS no_ProcedureGroups

,MAX(PayDelayI) AS PayDelay_max
,MIN(PayDelayI) AS PayDelay_min
,AVG(PayDelayI) AS PayDelay_ave
,(CASE WHEN COUNT(*) = 1 THEN 0 ELSE STD(PayDelayI) END) AS PayDelay_stdev

,MAX(LengthOfStayI) AS LOS_max
,MIN(LengthOfStayI) AS LOS_min
,AVG(LengthOfStayI) AS LOS_ave
,(CASE WHEN COUNT(*) = 1 THEN 0 ELSE STD(LengthOfStayI) END) AS LOS_stdev

,SUM(CASE WHEN LENGTHOFSTAY IS NULL AND SUPLOS = 0 THEN 1 ELSE 0 END) AS LOS_TOT_UNKNOWN
,SUM(CASE WHEN LENGTHOFSTAY IS NULL AND SUPLOS = 1 THEN 1 ELSE 0 END) AS LOS_TOT_SUPRESSED
,SUM(CASE WHEN LENGTHOFSTAY IS NOT NULL THEN 1 ELSE 0 END) AS LOS_TOT_KNOWN
,MAX(dsfsI) AS dsfs_max
,MIN(dsfsI) AS dsfs_min
,MAX(dsfsI) - MIN(dsfsI) AS dsfs_range
,AVG(dsfsI) AS dsfs_ave
,(CASE WHEN COUNT(*) = 1 THEN 0 ELSE STD(dsfsI) END) AS dsfs_stdev

,MAX(CharlsonIndexI) AS CharlsonIndexI_max
,MIN(CharlsonIndexI) AS CharlsonIndexI_min
,AVG(CharlsonIndexI) AS CharlsonIndexI_ave
,MAX(CharlsonIndexI) - MIN(CharlsonIndexI) AS CharlsonIndexI_range
,(CASE WHEN COUNT(*) = 1 THEN 0 ELSE STD(CharlsonIndexI) END) AS CharlsonIndexI_stdev

,SUM(CASE WHEN PrimaryConditionGroup = 'MSC2a3' THEN 1 ELSE 0 END) AS pcg1
,SUM(CASE WHEN PrimaryConditionGroup = 'METAB3' THEN 1 ELSE 0 END) AS pcg2
,SUM(CASE WHEN PrimaryConditionGroup = 'ARTHSPIN' THEN 1 ELSE 0 END) AS pcg3
,SUM(CASE WHEN PrimaryConditionGroup = 'NEUMENT' THEN 1 ELSE 0 END) AS pcg4
,SUM(CASE WHEN PrimaryConditionGroup = 'RESPR4' THEN 1 ELSE 0 END) AS pcg5
,SUM(CASE WHEN PrimaryConditionGroup = 'MISCHRT' THEN 1 ELSE 0 END) AS pcg6
,SUM(CASE WHEN PrimaryConditionGroup = 'SKNAUT' THEN 1 ELSE 0 END) AS pcg7
,SUM(CASE WHEN PrimaryConditionGroup = 'GIBLEED' THEN 1 ELSE 0 END) AS pcg8
,SUM(CASE WHEN PrimaryConditionGroup = 'INFEC4' THEN 1 ELSE 0 END) AS pcg9
,SUM(CASE WHEN PrimaryConditionGroup = 'TRAUMA' THEN 1 ELSE 0 END) AS pcg10
,SUM(CASE WHEN PrimaryConditionGroup = 'HEART2' THEN 1 ELSE 0 END) AS pcg11
,SUM(CASE WHEN PrimaryConditionGroup = 'RENAL3' THEN 1 ELSE 0 END) AS pcg12
,SUM(CASE WHEN PrimaryConditionGroup = 'ROAMI' THEN 1 ELSE 0 END) AS pcg13
,SUM(CASE WHEN PrimaryConditionGroup = 'MISCL5' THEN 1 ELSE 0 END) AS pcg14
,SUM(CASE WHEN PrimaryConditionGroup = 'ODaBNCA' THEN 1 ELSE 0 END) AS pcg15
,SUM(CASE WHEN PrimaryConditionGroup = 'UTI' THEN 1 ELSE 0 END) AS pcg16
,SUM(CASE WHEN PrimaryConditionGroup = 'COPD' THEN 1 ELSE 0 END) AS pcg17
,SUM(CASE WHEN PrimaryConditionGroup = 'GYNEC1' THEN 1 ELSE 0 END) AS pcg18
,SUM(CASE WHEN PrimaryConditionGroup = 'CANCRB' THEN 1 ELSE 0 END) AS pcg19
,SUM(CASE WHEN PrimaryConditionGroup = 'FXDISLC' THEN 1 ELSE 0 END) AS pcg20
,SUM(CASE WHEN PrimaryConditionGroup = 'AMI' THEN 1 ELSE 0 END) AS pcg21
,SUM(CASE WHEN PrimaryConditionGroup = 'PRGNCY' THEN 1 ELSE 0 END) AS pcg22
,SUM(CASE WHEN PrimaryConditionGroup = 'HEMTOL' THEN 1 ELSE 0 END) AS pcg23
,SUM(CASE WHEN PrimaryConditionGroup = 'HEART4' THEN 1 ELSE 0 END) AS pcg24
,SUM(CASE WHEN PrimaryConditionGroup = 'SEIZURE' THEN 1 ELSE 0 END) AS pcg25
,SUM(CASE WHEN PrimaryConditionGroup = 'APPCHOL' THEN 1 ELSE 0 END) AS pcg26
,SUM(CASE WHEN PrimaryConditionGroup = 'CHF' THEN 1 ELSE 0 END) AS pcg27
,SUM(CASE WHEN PrimaryConditionGroup = 'GYNECA' THEN 1 ELSE 0 END) AS pcg28
,SUM(CASE WHEN PrimaryConditionGroup IS NULL THEN 1 ELSE 0 END) AS pcg29
,SUM(CASE WHEN PrimaryConditionGroup = 'PNEUM' THEN 1 ELSE 0 END) AS pcg30
,SUM(CASE WHEN PrimaryConditionGroup = 'RENAL2' THEN 1 ELSE 0 END) AS pcg31
,SUM(CASE WHEN PrimaryConditionGroup = 'GIOBSENT' THEN 1 ELSE 0 END) AS pcg32
,SUM(CASE WHEN PrimaryConditionGroup = 'STROKE' THEN 1 ELSE 0 END) AS pcg33
,SUM(CASE WHEN PrimaryConditionGroup = 'CANCRA' THEN 1 ELSE 0 END) AS pcg34
,SUM(CASE WHEN PrimaryConditionGroup = 'FLaELEC' THEN 1 ELSE 0 END) AS pcg35
,SUM(CASE WHEN PrimaryConditionGroup = 'MISCL1' THEN 1 ELSE 0 END) AS pcg36
,SUM(CASE WHEN PrimaryConditionGroup = 'HIPFX' THEN 1 ELSE 0 END) AS pcg37
,SUM(CASE WHEN PrimaryConditionGroup = 'METAB1' THEN 1 ELSE 0 END) AS pcg38
,SUM(CASE WHEN PrimaryConditionGroup = 'PERVALV' THEN 1 ELSE 0 END) AS pcg39
,SUM(CASE WHEN PrimaryConditionGroup = 'LIVERDZ' THEN 1 ELSE 0 END) AS pcg40
,SUM(CASE WHEN PrimaryConditionGroup = 'CATAST' THEN 1 ELSE 0 END) AS pcg41
,SUM(CASE WHEN PrimaryConditionGroup = 'CANCRM' THEN 1 ELSE 0 END) AS pcg42
,SUM(CASE WHEN PrimaryConditionGroup = 'PERINTL' THEN 1 ELSE 0 END) AS pcg43
,SUM(CASE WHEN PrimaryConditionGroup = 'PNCRDZ' THEN 1 ELSE 0 END) AS pcg44
,SUM(CASE WHEN PrimaryConditionGroup = 'RENAL1' THEN 1 ELSE 0 END) AS pcg45
,SUM(CASE WHEN PrimaryConditionGroup = 'SEPSIS' THEN 1 ELSE 0 END) AS pcg46
,SUM(CASE WHEN Specialty = 'Internal' THEN 1 ELSE 0 END) AS sp1
,SUM(CASE WHEN Specialty = 'Laboratory' THEN 1 ELSE 0 END) AS sp2
,SUM(CASE WHEN Specialty = 'General Practice' THEN 1 ELSE 0 END) AS sp3
,SUM(CASE WHEN Specialty = 'Surgery' THEN 1 ELSE 0 END) AS sp4
,SUM(CASE WHEN Specialty = 'Diagnostic Imaging' THEN 1 ELSE 0 END) AS sp5
,SUM(CASE WHEN Specialty = 'Emergency' THEN 1 ELSE 0 END) AS sp6
,SUM(CASE WHEN Specialty = 'Other' THEN 1 ELSE 0 END) AS sp7
,SUM(CASE WHEN Specialty = 'Pediatrics' THEN 1 ELSE 0 END) AS sp8
,SUM(CASE WHEN Specialty = 'Rehabilitation' THEN 1 ELSE 0 END) AS sp9
,SUM(CASE WHEN Specialty = 'Obstetrics and Gynecology' THEN 1 ELSE 0 END) AS sp10
,SUM(CASE WHEN Specialty = 'Anesthesiology' THEN 1 ELSE 0 END) AS sp11
,SUM(CASE WHEN Specialty = 'Pathology' THEN 1 ELSE 0 END) AS sp12
,SUM(CASE WHEN Specialty IS NULL THEN 1 ELSE 0 END) AS sp13
,SUM(CASE WHEN ProcedureGroup = 'EM' THEN 1 ELSE 0 END ) AS pg1
,SUM(CASE WHEN ProcedureGroup = 'PL' THEN 1 ELSE 0 END ) AS pg2
,SUM(CASE WHEN ProcedureGroup = 'MED' THEN 1 ELSE 0 END ) AS pg3
,SUM(CASE WHEN ProcedureGroup = 'SCS' THEN 1 ELSE 0 END ) AS pg4
,SUM(CASE WHEN ProcedureGroup = 'RAD' THEN 1 ELSE 0 END ) AS pg5
,SUM(CASE WHEN ProcedureGroup = 'SDS' THEN 1 ELSE 0 END ) AS pg6
,SUM(CASE WHEN ProcedureGroup = 'SIS' THEN 1 ELSE 0 END ) AS pg7
,SUM(CASE WHEN ProcedureGroup = 'SMS' THEN 1 ELSE 0 END ) AS pg8
,SUM(CASE WHEN ProcedureGroup = 'ANES' THEN 1 ELSE 0 END ) AS pg9
,SUM(CASE WHEN ProcedureGroup = 'SGS' THEN 1 ELSE 0 END ) AS pg10
,SUM(CASE WHEN ProcedureGroup = 'SEOA' THEN 1 ELSE 0 END ) AS pg11
,SUM(CASE WHEN ProcedureGroup = 'SRS' THEN 1 ELSE 0 END ) AS pg12
,SUM(CASE WHEN ProcedureGroup = 'SNS' THEN 1 ELSE 0 END ) AS pg13
,SUM(CASE WHEN ProcedureGroup = 'SAS' THEN 1 ELSE 0 END ) AS pg14
,SUM(CASE WHEN ProcedureGroup = 'SUS' THEN 1 ELSE 0 END ) AS pg15
,SUM(CASE WHEN ProcedureGroup IS NULL THEN 1 ELSE 0 END ) AS pg16
,SUM(CASE WHEN ProcedureGroup = 'SMCD' THEN 1 ELSE 0 END ) AS pg17
,SUM(CASE WHEN ProcedureGroup = 'SO' THEN 1 ELSE 0 END ) AS pg18
,SUM(CASE WHEN PlaceSvc = 'Office' THEN 1 ELSE 0 END) AS ps1
,SUM(CASE WHEN PlaceSvc = 'Independent Lab' THEN 1 ELSE 0 END) AS ps2
,SUM(CASE WHEN PlaceSvc = 'Urgent Care' THEN 1 ELSE 0 END) AS ps3
,SUM(CASE WHEN PlaceSvc = 'Outpatient Hospital' THEN 1 ELSE 0 END) AS ps4
,SUM(CASE WHEN PlaceSvc = 'Inpatient Hospital' THEN 1 ELSE 0 END) AS ps5
,SUM(CASE WHEN PlaceSvc = 'Ambulance' THEN 1 ELSE 0 END) AS ps6
,SUM(CASE WHEN PlaceSvc = 'Other' THEN 1 ELSE 0 END) AS ps7
,SUM(CASE WHEN PlaceSvc = 'Home' THEN 1 ELSE 0 END) AS ps8
,SUM(CASE WHEN PlaceSvc IS NULL THEN 1 ELSE 0 END) AS ps9
FROM claims
GROUP BY year,Memberid
""")
#claims_per_member.write.saveAsTable('claims_per_member')
claims_per_member.cache()
claims_per_member.count()
claims_per_member = claims_per_member\
    .withColumn('LOS_stdev', when(isnan('LOS_stdev') ,None).otherwise(col('LOS_stdev')))\
    .withColumn('dsfs_stdev', when(isnan('dsfs_stdev') ,None).otherwise(col('dsfs_stdev')))
claims_per_member.createOrReplaceTempView('claims_per_member')

## Members

members=spark.read.csv('gs://hhp-data/HHP_release3/Members.csv',header=True)
members=members\
    .withColumn('MemberID',col('MemberID').astype('integer'))\
    .withColumnRenamed('MemberID','MemberID_M')
members.createOrReplaceTempView('members')
members=spark.sql("""
SELECT *, 
    IF(AgeAtFirstClaim = '0-9',1,0) AS age5,
    IF(AgeAtFirstClaim = '10-19',1,0) AS age15,
    IF(AgeAtFirstClaim = '20-29',1,0) AS age25,
    IF(AgeAtFirstClaim = '30-39',1,0) AS age35,
    IF(AgeAtFirstClaim = '40-49',1,0) AS age45,
    IF(AgeAtFirstClaim = '50-59',1,0) AS age55,
    IF(AgeAtFirstClaim = '60-69',1,0) AS age65,
    IF(AgeAtFirstClaim = '70-79',1,0) AS age75,
    IF(AgeAtFirstClaim = '80+',1,0) AS age85,
    IF(AgeAtFirstClaim IS NULL,1,0) AS ageMISS,
    IF(Sex = 'M',1,0) AS sexMALE,
    IF(Sex = 'F',1,0) AS sexFEMALE,
    IF(Sex IS NULL,1,0) AS sexMISS
FROM 
    members
""")
members.createOrReplaceTempView('members')

## DRUG COUNTS

drugCount=spark.read.csv('gs://hhp-data/HHP_release3/DrugCount.csv',header=True)
drugCount=drugCount\
    .withColumn('MemberID',col('MemberID').astype('integer'))
drugCount.write.mode('overwrite').parquet('gs://hhp-data/HHP_release3/DrugCount')# parquet write
drugCount=spark.read.parquet('gs://hhp-data/HHP_release3/DrugCount/*')\
    .withColumn('DrugCountI',when(col('DrugCount')=='7+',7).otherwise(col('DrugCount')))
drugCount.createOrReplaceTempView('drugCount')

DRUGCOUNT_SUMMARY= spark.sql("""
SELECT 
    memberID AS memberID_dc,
    year as year_dc,
    MAX(drugcountI) AS drugCount_max,
    MIN(drugcountI) AS drugCount_min,
    AVG(drugcountI*1.0) AS drugCount_ave,
    COUNT(*) AS drugcount_months
FROM
    drugcount
GROUP BY
    memberID,
    Year
""")
DRUGCOUNT_SUMMARY.createOrReplaceTempView('DRUGCOUNT_SUMMARY')
DRUGCOUNT_SUMMARY.cache()
DRUGCOUNT_SUMMARY.count()

## LAB COUNTS

labCount=spark.read.csv('gs://hhp-data/HHP_release3/LabCount.csv',header=True)
labCount=labCount\
    .withColumn('MemberID',col('MemberID').astype('integer'))
labCount.write.mode('overwrite').parquet('gs://hhp-data/HHP_release3/LabCountt')
labCount=spark.read.parquet('gs://hhp-data/HHP_release3/LabCountt/*')\
    .withColumn('LabCountI',when(col('LabCount')=='10+',10).otherwise(col('LabCount')))
labCount.createOrReplaceTempView('labCount')
LABCOUNT_SUMMARY=spark.sql("""
SELECT 
    memberID as memberID_lc,
    Year AS Year_lc,
    MAX(labcountI) AS labCount_max,
    MIN(labcountI) AS labCount_min,
    AVG(labcountI*1.0) AS labCount_ave,
    COUNT(*) AS labcount_months
FROM
    labcount
GROUP BY
    memberID,
    year
""")
LABCOUNT_SUMMARY.createOrReplaceTempView('LABCOUNT_SUMMARY')
LABCOUNT_SUMMARY.cache()
LABCOUNT_SUMMARY.count()
    
## Targets 

daysInHospital_Y2=spark.read.csv('gs://hhp-data/HHP_release3/DaysInHospital_Y2.csv',header=True)
daysInHospital_Y2=daysInHospital_Y2\
    .withColumn('MemberID',col('MemberID').astype('integer'))\
    .withColumn('ClaimsTruncated',col('ClaimsTruncated').astype('short'))\
    .withColumn('DaysInHospital',col('DaysInHospital').astype('short'))
daysInHospital_Y2.createOrReplaceTempView('daysInHospital_Y2')
    
daysInHospital_Y3=spark.read.csv('gs://hhp-data/HHP_release3/DaysInHospital_Y3.csv',header=True)
daysInHospital_Y3=daysInHospital_Y3\
    .withColumn('MemberID',col('MemberID').astype('integer'))\
    .withColumn('ClaimsTruncated',col('ClaimsTruncated').astype('short'))\
    .withColumn('DaysInHospital',col('DaysInHospital').astype('short'))
daysInHospital_Y3.createOrReplaceTempView('daysInHospital_Y3')
    
target=spark.read.csv('gs://hhp-data/HHP_release3/Target.csv',header=True)
target=target\
    .withColumn('MemberID',col('MemberID').astype('integer'))\
    .withColumn('ClaimsTruncated',col('ClaimsTruncated').astype('short'))\
    .withColumn('DaysInHospital',col('DaysInHospital').astype('short'))
target.createOrReplaceTempView('target')

DIH = spark.sql("""
SELECT *
FROM 
(
SELECT
    MemberID AS MemberID_mm,
    'Y1' AS YEAR_mm,
    ClaimsTruncated,
    DaysInHospital,
    1 AS trainset
FROM DaysInHospital_Y2

UNION ALL

SELECT
    MemberID AS MemberID_mm,
    'Y2' AS YEAR_mm,
    ClaimsTruncated,
    DaysInHospital,
    1 AS trainset
FROM DaysInHospital_Y3

UNION ALL

SELECT 
    MemberID AS MemberID_mm,
    'Y3' AS YEAR_mm,
    ClaimsTruncated,
    NULL AS DaysInHospital,
    0 AS trainset
FROM Target
) a

""")

DIH.cache()
DIH.count()
DIH.createOrReplaceTempView('DIH')

temp4 = spark.sql("""
SELECT a.*, b.*, c.*, d.*, e.*
FROM DIH a
LEFT JOIN members b
on a.MemberID_mm = B.Memberid_M 
LEFT JOIN claims_per_member c
on a.MemberID_mm=c.Memberid 
    AND a.YEAR_mm=c.year
LEFT JOIN DRUGCOUNT_SUMMARY d
on a.MemberID_mm=d.memberID_dc
    AND a.YEAR_mm=d.year_dc
LEFT JOIN LABCOUNT_SUMMARY e
on a.MemberID_mm=e.Memberid_lc
    AND a.YEAR_mm=e.Year_lc
""").drop('Memberid_M').drop('AgeAtFirstClaim').drop('Sex')\
    .drop('Memberid').drop('year')\
    .drop('Memberid_dc').drop('year_dc')\
    .drop('Memberid_lc').drop('year_lc')
                
feature_matrix=temp4.withColumn('labNULL',when(isnull(col('labCount_max')),1).otherwise(0))\
            .withColumn('drugNULL',when(isnull(col('drugCount_max')),1).otherwise(0))
#            .withColumn('labCount_max',when(isnull(col('labCount_max')),0).otherwise(col('labCount_max')))\
#            .withColumn('labCount_min',when(isnull(col('labCount_min')),0).otherwise(col('labCount_min')))\
#            .withColumn('labCount_ave',when(isnull(col('labCount_ave')),0).otherwise(col('labCount_ave')))\
#            .withColumn('labcount_months',when(isnull(col('labcount_months')),0).otherwise(col('labcount_months')))\
#            .withColumn('drugCount_max',when(isnull(col('drugCount_max')),0).otherwise(col('drugCount_max')))\
#            .withColumn('drugCount_min',when(isnull(col('drugCount_min')),0).otherwise(col('drugCount_min')))\
#            .withColumn('drugCount_ave',when(isnull(col('drugCount_ave')),0).otherwise(col('drugCount_ave')))\
#            .withColumn('drugcount_months',when(isnull(col('drugcount_months')),0).otherwise(col('drugcount_months')))

feature_matrix.count()
#218415
feature_matrix[feature_matrix['trainset']==1].count()
#147473
feature_matrix[feature_matrix['trainset']==1]\
                .coalesce(1)\
                .write\
                .mode('overwrite')\
                .csv('gs://hhp-data/HHP_release3/feature_matrix_train.csv')
feature_matrix[feature_matrix['trainset']==1]\
                .write\
                .mode('overwrite')\
                .parquet('gs://hhp-data/HHP_release3/feature_matrix_train')
                
#>>> data=spark.read.parquet('gs://hhp-data/HHP_release3/feature_matrix_train/*')
#>>> data.select('DaysInHospital').describe().show()
#+-------+-------------------+                                                   
#|summary|     DaysInHospital|
#+-------+-------------------+
#|  count|             147473|
#|   mean|0.45295070962142225|
#| stddev| 1.5738178814266066|
#|    min|                  0|
#|    max|                 15|
#+-------+-------------------+

## modeling gradient boosting regreesion trees using GradientBoostingRegressor
## download the dataset 
## cd /Users/linjunli/Documents/Kaggle/HHP/HHP_release3/feature_matrix
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
                
#import matplotlib.pyplot as plt
df=pd.read_csv('feature_matrix_train.csv')
df['DaysInHospital']=np.log(df['DaysInHospital']+1)
df=df.sort_values(['YEAR_mm','MemberID_mm'])
X = df.drop(['DaysInHospital','trainset','MemberID_mm','YEAR_mm'],axis=1)
X = X.fillna(-9999)
y = df['DaysInHospital']
                
X_train,X_test, y_train,y_test = train_test_split(X,
                                                  y,
                                                  test_size=0.2,
                                                  random_state=1234)
                
clf28 = GradientBoostingRegressor(loss='ls', subsample=0.5, 
                                n_estimators=1000, max_depth=4,
                                learning_rate=.03, min_samples_leaf=50)
clf28.fit(X_train,y_train)
y_test_hat28 = clf28.predict(X_test)
e28= np.sqrt(sum((y_test_hat28-y_test.values)**2)/len(y_test))
# e28
# 0.45096967439293228
                
                
                
                
                
                
                
                