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

customerDF = sc \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "postgres.public.customers") \
    .load()

orderDF = sc \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "postgres.public.orders") \
    .load()

# .option("startingOffsets", "earliest") \


# rawDF = customerDF.join(orderDF)
# print("#################################################################")
# print("Printing Schema of rawDF: ")
# rawDF.printSchema()
# print(rawDF)

# df = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").select("value")

df1 = customerDF.selectExpr("CAST(value AS STRING) as cname").select(from_json('cname', schema).alias("cdata"))
df2 = orderDF.selectExpr("CAST(value AS STRING) as oname").select(from_json('oname', schema).alias("odata"))

cdf = df1.select("cdata.after").alias("customer")
odf = df2.select("odata.after").alias("order")

# customerSchema = StructType([
#     StructField("id", IntegerType()),
#     StructField("name", StringType()),
#     StructField("email", StringType()),
#     StructField("location", StringType())
# ])
#
# orderSchema = StructType([
#     StructField("id", IntegerType()),
#     StructField("customer_id", IntegerType()),
#     StructField("item", StringType()),
#     StructField("price", StringType())
# ])

commonSchema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("location", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("item", StringType()),
    StructField("price", StringType())
])

cusDF = cdf.select(from_json('customer.after', commonSchema).alias("crecords")).select("crecords.id", "crecords.name", "crecords.email", "crecords.location")
# ordDF = odf.select(from_json('order.after', commonSchema).alias("orecords")).select("orecords.item", "orecords.price")

# finalDF = cusDF.join(ordDF)

# df4.createOrReplaceTempView(customerName)
# df = df.withColumn('value_str', df['value'].cast('string')).drop('value')
#
# df = df.select(from_json('value_str', schema)).alias('data')


records_write_stream = cusDF.writeStream \
    .format('console') \
    .start()
#.trigger(processingTime='2 seconds') \
# .outputMode("append") \
records_write_stream.awaitTermination()

# customerName = df4.collect()[0][0]
# print("Customer Name : " + customerName)

print("Stream Data Processing Application Completed.")
