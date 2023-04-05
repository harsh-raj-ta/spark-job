from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Basic") \
    .config("spark.sql.extensions", \
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",\
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.addPyFile('s3://harsh-landingzone-batch08/Delta_Lake_Jar/delta-core_2.12-0.8.0.jar')

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import lit, to_date
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import sha2
from datetime import date
import boto
import boto.s3.connection
from pyspark.sql.functions import xxhash64
from pyspark.sql.functions import concat
from delta import *

app = spark.read.option("multiline","true") \
                      .json("s3://harsh-landingzone-batch08/config/app_config.json")
app.printSchema()

class transform:
    dec=Row()
    mask=Row()
    comma=Row()
    
    def __init__(self,transform_key):
        self.dec = app.select("{}.*".format(transform_key)).collect()[0]['to_decimal']
        self.mask = app.select("{}.*".format(transform_key)).collect()[0]['mask']
        self.comma = app.select("{}.*".format(transform_key)).collect()[0]['to_comma']

    def cast_dec(self,df):
        for i in self.dec:
            df = df.withColumn(i['name'], col(i['name']).cast(i['to']))
        return df
    def cast_comma(self,df):
        for i in self.comma:
            df = df.withColumn(i['name'], concat_ws(i['to'],col(i['name'])))
        return df
    def cast_mask(self,df):
        for i in self.mask:
            df = df.withColumn(f"{i['name']}_mask", sha2(df[i['name']], 256))
        return df
    '''def export_to_staging(self,df,name):
        unique_month = df.select('month').distinct().collect()
        for i in unique_month:
            dat = df.filter(df['month']==i[0]).select('date').distinct().collect()
            print("-----------------------------month------------------------- = {}".format(i[0]))
            for j in dat:
                df.filter(df["date"] == lit(str(j[0]))).write.parquet("{}partition{}/{}/{}.parquet".format(app.select('destination_staging.*').collect()[0]['path'],name,str(i[0]),str(j[0])))
                '''

import sys
dataset_name = sys.argv[1]

x = ".parquet"
a = dataset_name
i = a[:-len(x)]

df = spark.read.parquet("{}{}.parquet".format(app.select('destination_raw.*').collect()[0]['path'],i))
data_transform = app.select("used_key.*").collect()[0]
obj = transform(data_transform['{}'.format(i)])
df = obj.cast_dec(df)
df = obj.cast_comma(df)
df = obj.cast_mask(df)
df.printSchema()
df.write.mode("overwrite").parquet("{}{}.parquet".format(app.select('destination_staging.*').collect()[0]['path'],i))
df = spark.read.parquet("{}{}.parquet".format(app.select('destination_staging.*').collect()[0]['path'],i))
df.write.partitionBy('month','date').mode("overwrite").parquet("{}partition{}".format(app.select('destination_staging.*').collect()[0]['path'],i))





if i == 'Actives':
    s3 = boto.s3.connect_to_region('ap-southeast-2',
        is_secure=True,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
    my_bucket = s3.get_bucket('harsh-stagingbucket-batch08')

    has_delta_file = False
    for s3_file in my_bucket:
        #print(s3_file.key)
        if('db_Actives' in s3_file.key):
            has_delta_file = True
            #print("innit")
            break

    if has_delta_file == False:
        df_delta = df.select('advertising_id','user_id','advertising_id_mask','user_id_mask')
        df_delta = df_delta.withColumn('Flag',lit("Active")).withColumn('start_date',lit(date.today())).withColumn('end_date',lit(None).cast('date'))
        df_delta.write.format("delta").save("{}db_Actives".format(app.select('destination_staging.*').collect()[0]['path']))

    targetDf = spark.read.format("delta").load("s3://harsh-stagingbucket-batch08/db_Actives/")
    sourceDf = df.select('advertising_id','user_id','advertising_id_mask','user_id_mask')

    joinDf = sourceDf.join(targetDf,(sourceDf.advertising_id == targetDf.advertising_id) & \
                           (sourceDf.advertising_id_mask == targetDf.advertising_id_mask) & \
                            (targetDf.Flag == "Active"),"leftouter") \
                                .select(sourceDf["*"], \
                                    targetDf.advertising_id.alias("target_advertising_id"), \
                                    targetDf.user_id.alias("target_user_id"), \
                                    targetDf.advertising_id_mask.alias("target_advertising_id_mask"), \
                                    targetDf.user_id_mask.alias("target_user_id_mask"))
    
    filterDf = joinDf.filter(xxhash64(joinDf.advertising_id,joinDf.user_id,joinDf.advertising_id_mask,joinDf.user_id_mask)
                            !=xxhash64(joinDf.target_advertising_id,joinDf.target_user_id,joinDf.target_advertising_id_mask,joinDf.target_user_id_mask))
    
    mergeDF = filterDf.withColumn("MERGEKEY",concat(filterDf.advertising_id,filterDf.advertising_id_mask))

    dummyDF = filterDf.filter("target_advertising_id is not null").withColumn("MERGEKEY",lit(None))

    scDf = mergeDF.union(dummyDF)

    current_date = date.today()

    target_table = DeltaTable.forPath(spark,'s3://harsh-stagingbucket-batch08/db_Actives/')

    target_table.alias("target").merge(
        source = scDf.alias("source"),
                            condition = "concat(target.advertising_id,target.advertising_id_mask) = source.MERGEKEY and target.Flag = 'Active'"
                        ).whenMatchedUpdate(set = 
                                            {
                                                "Flag":"'InActive'",
                                                "end_date":"current_date"
                                            }
                        ).whenNotMatchedInsert(values =
                                                {
                                                    "advertising_id":"source.advertising_id",
                                                    "user_id":"source.user_id",
                                                    "advertising_id_mask":"source.advertising_id_mask",
                                                    "user_id_mask":"source.user_id_mask",
                                                    "Flag":"'Active'",
                                                    "start_date":"current_date",
                                                    "end_date":"Null",
                                                }
                                                ).execute()


if i == 'Viewership':
    s3 = boto.s3.connect_to_region('ap-southeast-2',
        is_secure=True,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
    my_bucket = s3.get_bucket('harsh-stagingbucket-batch08')

    has_delta_file = False
    for s3_file in my_bucket:
        #print(s3_file.key)
        if('db_Viewership' in s3_file.key):
            has_delta_file = True
            print("innit")
            break
    
    if has_delta_file == False:
        df_delta = df.select('advertising_id','user_lat','advertising_id_mask','user_long')
        df_delta = df_delta.withColumn('Flag',lit("Active")).withColumn('start_date',lit(date.today())).withColumn('end_date',lit(None).cast('date'))
        df_delta.write.format("delta").save("{}db_Viewership".format(app.select('destination_staging.*').collect()[0]['path']))
    
    targetDf = spark.read.format("delta").load("s3://harsh-stagingbucket-batch08/db_Viewership/")

    sourceDf = df.select('advertising_id','user_lat','advertising_id_mask','user_long')

    joinDf = sourceDf.join(targetDf,(sourceDf.advertising_id == targetDf.advertising_id) & \
                        (sourceDf.advertising_id_mask == targetDf.advertising_id_mask) & \
                        (targetDf.Flag == "Active"),"leftouter") \
                            .select(sourceDf["*"], \
                                    targetDf.advertising_id.alias("target_advertising_id"), \
                                    targetDf.user_lat.alias("target_user_lat"), \
                                    targetDf.advertising_id_mask.alias("target_advertising_id_mask"), \
                                    targetDf.user_long.alias("target_user_long"))
    
    filterDf = joinDf.filter(xxhash64(joinDf.advertising_id,joinDf.user_lat,joinDf.advertising_id_mask,joinDf.user_long)
                            !=xxhash64(joinDf.target_advertising_id,joinDf.target_user_lat,joinDf.target_advertising_id_mask,joinDf.target_user_long))
    
    mergeDF = filterDf.withColumn("MERGEKEY",concat(filterDf.advertising_id,filterDf.advertising_id_mask))

    dummyDF = filterDf.filter("target_advertising_id is not null").withColumn("MERGEKEY",lit(None))

    scDf = mergeDF.union(dummyDF)

    current_date = date.today()

    target_table = DeltaTable.forPath(spark,'s3://harsh-stagingbucket-batch08/db_Viewership/')

    target_table.alias("target").merge(
        source = scDf.alias("source"),
                            condition = "concat(target.advertising_id,target.advertising_id_mask) = source.MERGEKEY and target.Flag = 'Active'"
                        ).whenMatchedUpdate(set = 
                                            {
                                                "Flag":"'InActive'",
                                                "end_date":"current_date"
                                            }
                        ).whenNotMatchedInsert(values =
                                                {
                                                    "advertising_id":"source.advertising_id",
                                                    "user_lat":"source.user_lat",
                                                    "advertising_id_mask":"source.advertising_id_mask",
                                                    "user_long":"source.user_long",
                                                    "Flag":"'Active'",
                                                    "start_date":"current_date",
                                                    "end_date":"Null",
                                                }
                                                ).execute()
    



#did it got commited