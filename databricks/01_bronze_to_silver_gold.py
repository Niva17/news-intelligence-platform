# Databricks notebook source
print("Databricks is working!")
spark.version

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
  StructField("title", StringType(), True),
  StructField("description", StringType(), True),
  StructField("content", StringType(), True),
  StructField("url", StringType(), True),
  StructField("source", StringType(), True),
  StructField("published_at", StringType(), True),
  StructField("author", StringType(), True)
  ])

BRONZE_PATH = "s3a://news-intelligence-platform-bronze/bronze/news_delta/"

# df = spark.read.schema(schema).option("recursiveFileLookup", "true").json("s3a://news-intelligence-platform-bronze/bronze/news/")
df=spark.read.format("delta").load(BRONZE_PATH)
print(f"Total articles: {df.count()}")
# df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, trim, upper, when, length
from delta.tables import DeltaTable

silver_path = "s3a://news-intelligence-platform-bronze/silver/news/"

# ── Silver Layer Transformations ──

silver_df = df \
    .dropDuplicates(["url"]) \
    .filter(col("title").isNotNull() & col("url").isNotNull()) \
    .withColumn("published_at", to_timestamp(col("published_at"))) \
    .withColumn("title", trim(col("title"))) \
    .withColumn("author", when(col("author").isNull(), "Unknown").otherwise(trim(col("author")))) \
    .withColumn("source", when(col("source").isNull(), "Unknown").otherwise(trim(col("source")))) \
    .withColumn("content_length", length(col("content"))) \
    .withColumn("has_content", when(col("content").isNotNull(), True).otherwise(False))

# Check if Silver table already exists
if DeltaTable.isDeltaTable(spark, silver_path):
    # MERGE - upsert new records
    delta_table = DeltaTable.forPath(spark, silver_path)
    delta_table.alias("existing") \
        .merge(
            silver_df.alias("new"),
            "existing.url = new.url"  # match on URL
        ) \
        .whenNotMatchedInsertAll().execute()
    print(f"Silver table updated with MERGE! And the count is: {delta_table.toDF().count()}")
else:
    # First time - just write
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
    print(f"Silver table created! And the count is: {spark.read.format('delta').load(silver_path).count()}")

print(f"Bronze count: {df.count()}")
# silver_df.show(5, truncate=True)

# COMMAND ----------

from pyspark.sql.functions import avg, count, min, max, round
silver_df=spark.read.format("delta").load(silver_path)

# ── Gold Layer: Articles by Source ──

gold_by_source = silver_df \
    .groupBy("source") \
    .agg(count("*").alias("total_articles"),
          round(avg("content_length"),2).alias("avg_content_length"),
          min("published_at").alias("earliest_article"),
          max("published_at").alias("latest_article")
    ).orderBy(col("total_articles").desc())

gold_source_path = "s3a://news-intelligence-platform-bronze/gold/by_source/"

if DeltaTable.isDeltaTable(spark, gold_source_path):
    delta_table = DeltaTable.forPath(spark, gold_source_path)
    delta_table.alias("existing") \
        .merge(
            gold_by_source.alias("new"),
            "existing.source = new.source"
        ) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    print(f"Gold by_source updated with MERGE! And the count is: {delta_table.toDF().count()}")
else:
    gold_by_source.write.format("delta").mode("overwrite").save(gold_source_path)
    print(f"Gold by_source created! And the count is: {gold_by_source.count()}")

# COMMAND ----------

# Read gold_by_source and check total_articles
gold_check = spark.read.format("delta").load("s3a://news-intelligence-platform-bronze/gold/by_source/")
from pyspark.sql.functions import sum as _sum; gold_check.agg(_sum("total_articles").alias("total_articles_sum")).show()

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, round, year, month, dayofmonth, date_trunc

gold_date_path = "s3a://news-intelligence-platform-bronze/gold/by_date/"


# ── Gold Table 2: Articles by Date ──
gold_by_date = silver_df \
    .withColumn("date", date_trunc("day", col("published_at"))) \
    .groupBy("date") \
    .agg(
        count("*").alias("total_articles"),
        count("author").alias("total_authors")
    ) \
    .orderBy(col("date").desc())


# # gold_by_date_clean = gold_by_date.drop("year", "month")

if DeltaTable.isDeltaTable(spark, gold_date_path):
    delta_table = DeltaTable.forPath(spark, gold_date_path)
    delta_table.alias("existing") \
        .merge(
            gold_by_date.alias("new"),
            "existing.date = new.date"
        ) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    print(f"Gold by_date updated with MERGE! And the count is: {delta_table.toDF().count()}")
else:
    gold_by_date.write.format("delta").mode("overwrite").save(gold_date_path)
    print(f"Gold by_date created! And the count is: {gold_by_date.count()}")


# COMMAND ----------

# ── Gold Table 3: Articles by Author ──
gold_by_author = silver_df \
    .filter(col("author") != "Unknown") \
    .groupBy("author", "source") \
    .agg(
        count("*").alias("total_articles"),
        max("published_at").alias("latest_article")
    ) \
    .orderBy(col("total_articles").desc())

gold_author_path = "s3a://news-intelligence-platform-bronze/gold/by_author/"

if DeltaTable.isDeltaTable(spark, gold_author_path):
    delta_table = DeltaTable.forPath(spark, gold_author_path)
    delta_table.alias("existing") \
        .merge(
            gold_by_author.alias("new"),
            "existing.author = new.author"
        ) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    print(f"Gold by_author updated with MERGE! And the count is: {delta_table.toDF().count()}")
else:
    gold_by_author.write.format("delta").mode("overwrite").save(gold_author_path)
    print(f"Gold by_author created! And the count is: {gold_by_author.count()}")

# COMMAND ----------

