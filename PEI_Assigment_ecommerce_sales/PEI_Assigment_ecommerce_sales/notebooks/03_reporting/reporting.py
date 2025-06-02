# Databricks notebook source
display(spark.sql("""
SELECT order_year, ROUND(SUM(profit), 2) AS total_profit
FROM enriched.orders
GROUP BY order_year
ORDER BY order_year
"""))


# COMMAND ----------

display(spark.sql("""
SELECT order_year, category, ROUND(SUM(profit), 2) AS total_profit
FROM enriched.orders
GROUP BY order_year, category
ORDER BY order_year, category
""")
)

# COMMAND ----------

display(spark.sql("""
SELECT customer_id, customer_name, ROUND(SUM(profit), 2) AS total_profit
FROM enriched.orders
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC
""")
)

# COMMAND ----------

display(spark.sql("""
SELECT order_year, customer_id, customer_name, ROUND(SUM(profit), 2) AS total_profit
FROM enriched.orders
GROUP BY order_year, customer_id, customer_name
ORDER BY order_year, total_profit DESC
""")
)