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

df = spark.read.schema(schema).option("recursiveFileLookup", "true").json("s3a://news-intelligence-platform-bronze/bronze/news/")
print(f"Total articles: {df.count()}")
# df.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, trim, upper, when, length

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

print(f"Bronze count: {df.count()}")
print(f"Silver count: {silver_df.count()}")
silver_df.show(5, truncate=True)

# COMMAND ----------

silver_path='s3a://news-intelligence-platform-bronze/silver/news/'

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("Silver Delta table saved successfully!")

silver_verify = spark.read.format("delta").load(silver_path)
print(f"Silver Delta table verified: {silver_verify.count()}")


# COMMAND ----------

from pyspark.sql.functions import avg, count, min, max, round

# ── Gold Layer: Articles by Source ──

gold_by_source = silver_df \
    .groupBy("source") \
    .agg(count("*").alias("total_articles"),
          round(avg("content_length"),2).alias("avg_content_length"),
          min("published_at").alias("earliest_article"),
          max("published_at").alias("latest_article")
    ).orderBy(col("total_articles").desc())

gold_by_source.show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import count, max, min, avg, round, year, month, dayofmonth, date_trunc

# ── Gold Table 2: Articles by Date ──
gold_by_date = silver_df \
    .withColumn("date", date_trunc("day", col("published_at"))) \
    .groupBy("date") \
    .agg(
        count("*").alias("total_articles"),
        count("author").alias("total_authors")
    ) \
    .orderBy(col("date").desc())

gold_by_date.show(10, truncate=False)


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

gold_by_author.show(10, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth

# ── Save Gold Table 1: By Source ──
gold_source_path = "s3a://news-intelligence-platform-bronze/gold/by_source/"

gold_by_source.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_source_path)

print("Gold by_source saved!")

# ── Save Gold Table 2: By Date ──
gold_date_path = "s3a://news-intelligence-platform-bronze/gold/by_date/"

gold_by_date \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(gold_date_path)

print("Gold by_date saved!")

# ── Save Gold Table 3: By Author ──
gold_author_path = "s3a://news-intelligence-platform-bronze/gold/by_author/"

gold_by_author.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_author_path)

print("Gold by_author saved!")

print("\nAll Gold tables saved successfully!")

# COMMAND ----------

