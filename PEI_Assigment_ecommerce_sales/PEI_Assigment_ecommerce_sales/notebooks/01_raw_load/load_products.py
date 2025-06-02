# Databricks notebook source
# MAGIC %run ../utilities

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Load the products CSV
products_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/ecommerce/Products.csv")
)

# Inspect schema and records
products_df.printSchema()
display(products_df,5)

# COMMAND ----------

products_df_cleaned = products_df.toDF(*[clean_column_name(c) for c in products_df.columns])
products_df_cleaned.printSchema()

# COMMAND ----------

# Save to raw.products managed table
products_df_cleaned.write.mode("overwrite").format("delta").saveAsTable("raw.products")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM raw.products LIMIT 5"))