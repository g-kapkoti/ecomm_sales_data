# Databricks notebook source
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utilities import clean_column_names  

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("pytest-data-pipeline") \
        .getOrCreate()


def test_clean_column_names(spark):
    df = spark.createDataFrame([(1, 2)], ["Customer ID", "Order Date"])
    result_df = clean_column_names(df)
    assert result_df.columns == ["customer_id", "order_date"]


def test_phone_number_cleanup(spark):
    df = spark.createDataFrame([("421.580.0902x9815",)], ["phone"])
    cleaned_df = df.withColumn("phone", F.regexp_replace(F.col("phone"), "[^0-9]", ""))
    assert cleaned_df.first()["phone"] == "42158009029815"


def test_profit_rounding(spark):
    df = spark.createDataFrame([(123.45678,), (45.12345,)], ["profit"])
    rounded_df = df.withColumn("profit", F.round("profit", 2))
    profits = [row["profit"] for row in rounded_df.collect()]
    assert profits == [123.46, 45.12]


def test_year_extraction(spark):
    df = spark.createDataFrame([("2023-04-15",)], ["order_date"])
    df = df.withColumn("order_date", F.to_date("order_date"))
    df = df.withColumn("year", F.year("order_date"))
    assert df.first()["year"] == 2023
