# Databricks notebook source


# COMMAND ----------

def clean_column_name(name):
    return (
        name.strip()
        .replace(" ", "_")
        .replace("\n", "_")
        .replace("-", "_")
        .lower()
    )