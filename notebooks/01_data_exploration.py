# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Data Exploration & Profiling
# MAGIC
# MAGIC **Objective:** Understand the raw data before building the ETL pipeline.
# MAGIC A data engineer must always answer four questions before writing a single
# MAGIC transformation:
# MAGIC
# MAGIC 1. **What is the shape?** — row counts, column count, schema
# MAGIC 2. **What is the quality?** — nulls, duplicates, outliers
# MAGIC 3. **What are the relationships?** — keys, referential integrity
# MAGIC 4. **What does the business need?** — which fields feed the reporting mart
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schema & Shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raw.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM raw.customers;

# COMMAND ----------

display(spark.table("raw.customers").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raw.products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT product_id) AS unique_products
# MAGIC FROM raw.products;

# COMMAND ----------

display(spark.table("raw.products").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE raw.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT order_id) AS unique_orders
# MAGIC FROM raw.orders;

# COMMAND ----------

display(spark.table("raw.orders").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Assessment
# MAGIC
# MAGIC Data quality issues are the most common obstacle in data engineering.
# MAGIC We profile three categories: **nulls**, **duplicates**, and **outliers**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Null Analysis

# COMMAND ----------

from pyspark.sql import functions as F

def null_report(table_name: str):
    df = spark.table(table_name)
    total = df.count()
    null_counts = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    rows = [
        {"column": col, "null_count": cnt, "null_pct": round(cnt / total * 100, 1)}
        for col, cnt in null_counts.items()
    ]
    return spark.createDataFrame(rows).orderBy(F.col("null_count").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC **Customers — null profile**

# COMMAND ----------

display(null_report("raw.customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Orders — null profile**

# COMMAND ----------

display(null_report("raw.orders"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Finding:** `city` has ~17 % nulls in customers.
# MAGIC > `discount_pct` has ~2 % nulls in orders.
# MAGIC > The ETL must decide how to handle each: drop, impute, or keep as-is.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Duplicate Detection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Products: are there duplicate product_ids?
# MAGIC SELECT product_id, COUNT(*) AS occurrences
# MAGIC FROM raw.products
# MAGIC GROUP BY product_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY occurrences DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Finding:** Several `product_id` values appear more than once — duplicated rows
# MAGIC > were loaded from the source system. The ETL must deduplicate before building
# MAGIC > the product dimension.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Outliers & Invalid Values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders: are there negative quantities?
# MAGIC SELECT order_id, product_id, quantity, unit_price, status
# MAGIC FROM raw.orders
# MAGIC WHERE quantity <= 0
# MAGIC ORDER BY quantity;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders: unit_price sanity check
# MAGIC SELECT
# MAGIC     MIN(unit_price)  AS min_price,
# MAGIC     MAX(unit_price)  AS max_price,
# MAGIC     AVG(unit_price)  AS avg_price,
# MAGIC     COUNT(CASE WHEN unit_price <= 0 THEN 1 END) AS zero_or_negative
# MAGIC FROM raw.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. Referential Integrity

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders referencing a customer_id that does not exist in raw.customers
# MAGIC SELECT o.order_id, o.customer_id
# MAGIC FROM raw.orders o
# MAGIC LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC > **Finding:** ~15 orders reference `C9999` which does not exist.
# MAGIC > These orders cannot be linked to a customer dimension and must be
# MAGIC > either excluded from the fact table or assigned to an "Unknown" customer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Distribution Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order volume by month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_FORMAT(order_date, 'yyyy-MM') AS month,
# MAGIC     COUNT(*)                           AS order_count,
# MAGIC     ROUND(SUM(unit_price * quantity), 2) AS gross_revenue
# MAGIC FROM raw.orders
# MAGIC WHERE status = 'Completed'
# MAGIC   AND quantity > 0
# MAGIC GROUP BY month
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer distribution by country & segment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, segment, COUNT(*) AS customers
# MAGIC FROM raw.customers
# MAGIC GROUP BY country, segment
# MAGIC ORDER BY country, segment;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product category breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     category,
# MAGIC     COUNT(DISTINCT product_id) AS products,
# MAGIC     ROUND(AVG(list_price), 2)  AS avg_list_price,
# MAGIC     ROUND(AVG(cost_price), 2)  AS avg_cost_price
# MAGIC FROM raw.products
# MAGIC GROUP BY category
# MAGIC ORDER BY products DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return & cancellation rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*)                        AS orders,
# MAGIC     ROUND(COUNT(*) * 100.0
# MAGIC         / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM raw.orders
# MAGIC GROUP BY status
# MAGIC ORDER BY orders DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Summary for the Data Engineer
# MAGIC
# MAGIC Before building the mart, document what the ETL pipeline must handle:
# MAGIC
# MAGIC | # | Issue | Table | Recommended Action |
# MAGIC |---|---|---|---|
# MAGIC | 1 | ~17 % null `city` | customers | Keep null — map to `"Unknown"` in dim |
# MAGIC | 2 | 5 duplicate rows | products | Deduplicate on `product_id` (keep first) |
# MAGIC | 3 | 5 negative `quantity` | orders | Exclude from fact table |
# MAGIC | 4 | ~2 % null `discount_pct` | orders | Impute with `0` (no discount) |
# MAGIC | 5 | ~15 orders, unknown customer | orders | Assign to surrogate key `-1` (Unknown) |
# MAGIC
# MAGIC These decisions are the data contract between the raw layer and the mart layer.
# MAGIC They must be applied consistently in the ETL job.
