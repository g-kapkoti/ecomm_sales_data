# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utilities

# COMMAND ----------

# Load the orders JSON file
orders_df = (
    spark.read.format("json")
    .option("multiline", "true")
    .option("inferSchema", "true")
    .load("dbfs:/FileStore/ecommerce/Orders.json")
)
# Inspect schema and data
orders_df.printSchema()
display(orders_df)


# COMMAND ----------

orders_df_cleaned = orders_df.toDF(*[clean_column_name(c) for c in orders_df.columns])
orders_df_cleaned = orders_df_cleaned.withColumn("order_date", F.to_date("order_date", "d/M/yyyy")) \
                                     .withColumn("ship_date", F.to_date("ship_date", "d/M/yyyy"))

orders_df_cleaned.printSchema()
display(orders_df_cleaned)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS raw")
orders_df_cleaned.write.mode("overwrite").format("delta").saveAsTable("raw.orders")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM raw.orders LIMIT 5"))