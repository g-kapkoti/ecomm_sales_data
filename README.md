# ecomm_sales_data
# 🛒 E-commerce Sales Data Pipeline with Databricks

## 📘 Project Overview

This project processes raw e-commerce datasets using **PySpark on Databricks**, transforming them into reliable and queryable data models to generate insights for business stakeholders.

---

## 📁 Project Structure

ecommerce/
├── dbfs:/FileStore/ecommerce/
│ ├── Customer.xlsx
│ ├── Orders.json
│ └── Products.csv
├── PEI_Assigment_ecommerce_sales/notebooks
│ ├── 01_raw_load/load_customers.py
│ ├── 01_raw_load/load_orders.py
│ ├── 01_raw_load/load_products.py
│ ├── 02_data_enrichment/enrich_customers.py
│ ├── 02_data_enrichment/enrich_products.py
│ ├── 02_data_enrichment/enrich_orders_fact.py
│ ├── 03_reporting/reporting.py
│ └── utilities.py
├── tests/
│ └── test_pipeline.py
└── README.md


---

## 📦 Features

- ✅ Raw ingestion from Excel (`.xlsx`), JSON, and CSV formats.
- ✅ Cleansing and enrichment of customer, product, and order datasets.
- ✅ Creation of an enriched orders fact table with:
  - Rounded profit
  - Customer and product information
- ✅ Profit aggregations:
  - By Year
  - By Category
  - By Customer
- ✅ Modular design with test-driven development using `pytest`.

---

# 🧹 Data Quality Checks

Ensuring high data quality is critical for trust, reporting accuracy, and downstream analysis. The pipeline implements key DQ practices:

## ✅ Implemented DQ Rules

| Dataset     | Column           | Check Type               | Description                                                                 |
|-------------|------------------|--------------------------|-----------------------------------------------------------------------------|
| Customers   | `customer_name`  | Format Standardization   | Removed special characters, digits, excessive whitespace                    |
| Customers   | `email`          | Format Validation        | Checked with regex pattern `^[^@\s]+@[^@\s]+\.[^@\s]+$` |
| Customers   | `phone`          | Format Normalization     | Extracted digits only, removed `x`, dashes, and parentheses                 |
| Customers   | `postal_code`    | Data Type Enforcement    | Cast to Integer if possible, else set to null                              |   | Orders      | `profit`         | Numeric Precision        | Rounded to two decimal places                                              |


## 🧪 Unit Testing

We use `pytest` for writing unit tests for the pipeline.
