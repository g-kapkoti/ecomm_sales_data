# Databricks notebook source
from pyspark.sql import functions as F

# Read from raw table
orders_df = spark.table("raw.orders")

# Enrich data

# COMMAND ----------

display(orders_df)

# COMMAND ----------

customers_df = spark.table("enriched.customers")
products_df  = spark.table("enriched.products")

# COMMAND ----------

# 1. Round profit to 2 decimals and Year
orders_df = orders_df.withColumn("profit", F.round(F.col("profit"), 2))\
                    .withColumn("order_year", F.year(F.to_date("order_date", "d/M/yyyy")))

# 2. Join with customers (to get customer name and country)
orders_with_customers = (
    orders_df.alias("o")
    .join(customers_df.select(
        F.col("customer_id"), 
        F.col("customer_name"), 
        F.col("country")
    ).alias("c"), 
    on=F.col("o.customer_id") == F.col("c.customer_id"), 
    how="left")
)

# 3. Join with products (to get category and sub-category)
orders_enriched_df = (
    orders_with_customers
    .join(products_df.select(
        F.col("product_id"), 
        F.col("category"), 
        F.col("sub_category")
    ).alias("p"), 
    on=F.col("o.product_id") == F.col("p.product_id"), 
    how="left")
)
# 4. Add ingestion date
orders_enriched_df = orders_enriched_df.withColumn("ingestion_date", F.current_timestamp())

# COMMAND ----------

orders_enriched_df = orders_enriched_df.select(
    F.col("o.order_id"),
    F.col("o.order_date"),
    F.col("o.order_year"),
    F.col("o.ship_date"),
    F.col("o.ship_mode"),
    F.col("o.customer_id"),
    F.col("customer_name"),
    F.col("country"),
    F.col("o.product_id"),
    F.col("category"),
    F.col("sub_category"),
    F.col("o.quantity"),
    F.col("o.price"),
    F.col("o.discount"),
    F.col("o.profit"),
    F.col("ingestion_date")
)
display(orders_enriched_df)

# COMMAND ----------

orders_enriched_df.write.mode("overwrite").format("delta").saveAsTable("enriched.orders")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM enriched.orders LIMIT 5"))

# COMMAND ----------

orders_final_df = spark.sql("select * from enriched.orders")

# COMMAND ----------

aggregated_df = (
    orders_final_df
    .groupBy("order_year", "category", "sub_category", "customer_id", "customer_name")
    .agg(
        F.sum("profit").alias("total_profit"),
        F.count("*").alias("total_orders")
    )
    .withColumn("total_profit", F.round("total_profit", 2))
)

# COMMAND ----------

display(aggregated_df)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS powered")

# COMMAND ----------

aggregated_df.write.mode("overwrite").format("delta").saveAsTable("powered.profit_summary")

# COMMAND ----------

display(spark.sql("select * from powered.profit_summary"))