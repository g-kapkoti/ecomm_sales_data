# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Load from raw table
customers_df = spark.table("raw.customers")

# Clean phone number: keep digits only
customers_df = customers_df.withColumn(
    "clean_phone",
    F.regexp_replace("phone", r"x.*$", "")
)

# Remove all non-numeric characters
customers_df = customers_df.withColumn(
    "clean_phone",
    F.regexp_replace("clean_phone", r"[^0-9]", "")
)

# Add a flag for invalid phone numbers (e.g., too short)
customers_df = customers_df.withColumn(
    "is_valid_phone",
    F.when((F.length("clean_phone") >= 7) & (F.length("clean_phone") <= 15), True).otherwise(False)
)

# COMMAND ----------

# Clean customer name: remove symbols, collapse whitespace, strip
customers_df = customers_df.withColumn(
    "customer_name",
    F.trim(F.regexp_replace(F.col("customer_name"), r"[^a-zA-Z\s]", ""))
)
customers_df = customers_df.withColumn(
    "customer_name",
    F.regexp_replace("customer_name", r"\s+", " ")  # Collapse multiple spaces
)

# COMMAND ----------

# Normalize email
customers_df = customers_df.withColumn("email", F.lower(F.col("email")))
# Postal code should be string
customers_df = customers_df.withColumn("postal_code", F.col("postal_code").cast('int').cast("string"))


# COMMAND ----------

# Replace null or empty names with fallback
customers_df = customers_df.withColumn(
    "customer_name",
    F.when(
        (F.col("customer_name").isNull()) | (F.col("customer_name") == ""),
        F.concat(F.lit("Unknown Customer - "), F.col("customer_id"))
    ).otherwise(F.col("customer_name"))
)

# COMMAND ----------

# Very basic email pattern: contains @ and at least one dot after it
basic_email_regex = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

# Add is_valid_email column
customers_df = customers_df.withColumn(
    "is_valid_email",
    F.col("email").rlike(basic_email_regex)
)

# COMMAND ----------

display(customers_df)

# COMMAND ----------

# Save to enriched DB
spark.sql("CREATE DATABASE IF NOT EXISTS enriched")

customers_df.write.mode("overwrite").format("delta").saveAsTable("enriched.customers")

# COMMAND ----------

# Sanity check
display(spark.sql("SELECT * FROM enriched.customers LIMIT 5"))