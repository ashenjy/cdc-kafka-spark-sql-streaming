# Imports and running findspark

import findspark
from pyspark.sql.functions import from_json

findspark.init('/home/ashen/gsb-apps/spark-3.1.1-bin-hadoop2.7')
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time

spark = SparkSession.builder.master("spark://ashen-ubuntu-os:7077").appName("PGSQL-CDC-RealTime").getOrCreate()
sc = SQLContext(spark)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("after", StringType())
])

rawDF = sc \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "postgres.public.customers, postgres.public.orders") \
    .load()

# .option("startingOffsets", "earliest") \

print("#################################################################")
print("Printing Schema of rawDF: ")
rawDF.printSchema()

# df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").select("value")

df1 = rawDF.selectExpr("CAST(value AS STRING)", "timestamp").select(from_json('value', schema).alias("data"), "timestamp")

df2 = df1.select("data.after", "timestamp")

print("Printing Schema of records_df2: ")
df2.printSchema()

commonSchema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("location", StringType()),
    StructField("item", StringType()),
    StructField("price", StringType())
])

df3 = df2.select(from_json('after', commonSchema).alias("records"))

df4 = df3.select("records.name", "records.email", "records.location", "records.item", "records.price")

# df4.createOrReplaceTempView(customerName)
# df = df.withColumn('value_str', df['value'].cast('string')).drop('value')
#
# df = df.select(from_json('value_str', schema)).alias('data')


records_write_stream = df4.writeStream \
    .format('console') \
    .start()

records_write_stream.awaitTermination()

# customerName = df4.collect()[0][0]
# print("Customer Name : " + customerName)

print("Stream Data Processing Application Completed.")
