# Databricks notebook source
from pyspark.sql import functions as F

# Read from raw table
products_df = spark.table("raw.products")

# Enrich data
products_df = (
    products_df
    .withColumn("product_id", F.trim(F.col("product_id")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("sub_category", F.trim(F.col("sub_category")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("price_per_product", F.col("price_per_product").cast("decimal(18,2)"))
    .dropDuplicates(["product_id"])
)


# COMMAND ----------

# Save to enriched DB
spark.sql("CREATE DATABASE IF NOT EXISTS enriched")

products_df.write.mode("overwrite").format("delta").saveAsTable("enriched.products")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM enriched.products LIMIT 5"))

# COMMAND ----------

