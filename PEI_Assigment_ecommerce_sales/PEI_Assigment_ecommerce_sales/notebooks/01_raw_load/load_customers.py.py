# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utilities

# COMMAND ----------

customer_df = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dataAddress", "'Worksheet'!A1")
    .load("dbfs:/FileStore/ecommerce/Customer.xlsx")
)

# Sanity check
customer_df.printSchema()
display(customer_df)

# COMMAND ----------

customer_df_cleaned = customer_df.toDF(*[clean_column_name(c) for c in customer_df.columns])

# COMMAND ----------

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS raw")

# Save the cleaned customer data as a managed table
customer_df_cleaned.write.mode("overwrite").format("delta").saveAsTable("raw.customers")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM raw.customers LIMIT 5"))