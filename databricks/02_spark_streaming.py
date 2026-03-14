# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, current_timestamp


import os

CONFLUENT_BOOTSTRAP_SERVERS = "your_bootstrap_server"
CONFLUENT_API_KEY = "your_api_key"
CONFLUENT_API_SECRET = "your_api_secret"
KAFKA_TOPIC = "raw_news"
BRONZE_PATH = "s3a://news-intelligence-platform-bronze/bronze/news_delta/"
CHECKPOINT_PATH = "s3a://news-intelligence-platform-bronze/checkpoints/bronze/"

schema = StructType([
  StructField("title", StringType(), True),
  StructField("description", StringType(), True),
  StructField("content", StringType(), True),
  StructField("url", StringType(), True),
  StructField("source", StringType(), True),
  StructField("published_at", StringType(), True),
  StructField("author", StringType(), True)
])

kafka_stream_df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", CONFLUENT_BOOTSTRAP_SERVERS) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config",
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{CONFLUENT_API_KEY}" password="{CONFLUENT_API_SECRET}";') \
  .option("subscribe", KAFKA_TOPIC) \
  .option("startingOffsets", "earliest")\
  .load()

from pyspark.sql.functions import from_json, current_timestamp

bronze_df = kafka_stream_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

query = bronze_df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(availableNow=True) \
    .start(BRONZE_PATH)


print("Spark Streaming started! Writing raw data to Bronze Delta table...")
query.awaitTermination()

# COMMAND ----------

verify =spark.read.format('delta').load(BRONZE_PATH)
verify.count()
verify.show(5,truncate=True)

# COMMAND ----------

