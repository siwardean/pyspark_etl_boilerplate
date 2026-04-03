# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — ETL Exercise: Build the RetailCo Sales Data Mart
# MAGIC
# MAGIC ## Context
# MAGIC
# MAGIC You are a data engineer at **RetailCo**, a European online retailer.
# MAGIC The analytics team needs a **sales data mart** to power their reporting
# MAGIC dashboards. They have asked you to build a **star schema** from the raw
# MAGIC operational tables you explored in notebook `01`.
# MAGIC
# MAGIC ## Target Data Model
# MAGIC
# MAGIC ```
# MAGIC                        ┌─────────────┐
# MAGIC                        │  dim_date   │
# MAGIC                        │─────────────│
# MAGIC                        │ date_key PK │
# MAGIC                        │ full_date   │
# MAGIC                        │ day_of_week │
# MAGIC                        │ month       │
# MAGIC                        │ quarter     │
# MAGIC                        │ year        │
# MAGIC                        │ is_weekend  │
# MAGIC                        └──────┬──────┘
# MAGIC                               │
# MAGIC ┌──────────────┐      ┌───────┴──────────┐      ┌──────────────┐
# MAGIC │ dim_customer │      │    fact_sales     │      │ dim_product  │
# MAGIC │──────────────│      │──────────────────-│      │──────────────│
# MAGIC │customer_key  │◄─────│ customer_key  FK  │      │ product_key  │
# MAGIC │customer_id   │      │ product_key   FK ─┼─────►│ product_id   │
# MAGIC │full_name     │      │ date_key      FK  │      │ product_name │
# MAGIC │email         │      │ order_id         │      │ category     │
# MAGIC │country       │      │ quantity         │      │ subcategory  │
# MAGIC │city          │      │ unit_price       │      │ brand        │
# MAGIC │segment       │      │ discount_pct     │      │ cost_price   │
# MAGIC │signup_date   │      │ gross_amount     │      │ list_price   │
# MAGIC └──────────────┘      │ discount_amount  │      └──────────────┘
# MAGIC                        │ net_amount       │
# MAGIC                        │ cost_amount      │
# MAGIC                        │ margin_amount    │
# MAGIC                        └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Rules agreed with the analytics team
# MAGIC
# MAGIC | # | Rule |
# MAGIC |---|---|
# MAGIC | R1 | Exclude orders with `quantity <= 0` |
# MAGIC | R2 | Exclude orders with status `Cancelled` or `Returned` |
# MAGIC | R3 | Null `discount_pct` → impute `0` |
# MAGIC | R4 | Null `city` → replace with `"Unknown"` |
# MAGIC | R5 | Deduplicate products on `product_id` (keep first occurrence) |
# MAGIC | R6 | Orders referencing unknown customers → assign `customer_key = -1` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — `dim_date`
# MAGIC
# MAGIC The date dimension is generated programmatically — it does not come from a
# MAGIC source system. It covers the full date range of the orders (2022–2024).
# MAGIC
# MAGIC **TODO:** Fill in the missing expressions below.
# MAGIC
# MAGIC *Hints:*
# MAGIC - Use `F.sequence(F.lit(start), F.lit(end), F.expr("interval 1 day"))` to
# MAGIC   generate a date array, then `F.explode()` to turn it into rows.
# MAGIC - `date_key` should be an integer in `YYYYMMDD` format:
# MAGIC   `F.date_format(col, "yyyyMMdd").cast("int")`
# MAGIC - `is_weekend` is `True` when `dayofweek` is 1 (Sunday) or 7 (Saturday).

# COMMAND ----------

from pyspark.sql.types import DateType

start_date = "2022-01-01"
end_date   = "2024-12-31"

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date")
    .withColumn("date_key",     # TODO: YYYYMMDD integer
                F.lit(None).cast("int"))
    .withColumn("day_of_week",  F.dayofweek("full_date"))
    .withColumn("day_name",     # TODO: full day name e.g. "Monday"
                F.lit(None).cast("string"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("month",        F.month("full_date"))
    .withColumn("month_name",   # TODO: full month name e.g. "January"
                F.lit(None).cast("string"))
    .withColumn("quarter",      F.quarter("full_date"))
    .withColumn("year",         F.year("full_date"))
    .withColumn("is_weekend",   # TODO: True if Saturday or Sunday
                F.lit(None).cast("boolean"))
)

display(dim_date.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — `dim_customer`
# MAGIC
# MAGIC **TODO:** Build the customer dimension from `raw.customers`.
# MAGIC
# MAGIC Requirements:
# MAGIC - Assign a surrogate `customer_key` (monotonically increasing integer, starting at 1).
# MAGIC - Concatenate `first_name` and `last_name` into a single `full_name` column.
# MAGIC - Replace null `city` with `"Unknown"` (rule R4).
# MAGIC - Add a special row with `customer_key = -1` and `customer_id = "UNKNOWN"` to
# MAGIC   absorb orders whose customer cannot be resolved (rule R6).
# MAGIC
# MAGIC *Hints:*
# MAGIC - `F.monotonically_increasing_id() + 1` for surrogate keys.
# MAGIC - `F.concat_ws(" ", "first_name", "last_name")` for full name.
# MAGIC - `F.coalesce(F.col("city"), F.lit("Unknown"))` for null replacement.
# MAGIC - Build the unknown row as a one-row DataFrame and `union` it at the end.

# COMMAND ----------

raw_customers = spark.table("raw.customers")

# TODO: build dim_customer
dim_customer = raw_customers.limit(0)   # replace this placeholder

display(dim_customer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — `dim_product`
# MAGIC
# MAGIC **TODO:** Build the product dimension from `raw.products`.
# MAGIC
# MAGIC Requirements:
# MAGIC - Deduplicate on `product_id`, keeping only the first occurrence (rule R5).
# MAGIC - Assign a surrogate `product_key`.
# MAGIC - Keep all original columns.
# MAGIC
# MAGIC *Hints:*
# MAGIC - Use a `Window` partitioned by `product_id` ordered by `product_id` and
# MAGIC   `F.row_number()`, then filter `rn == 1`.

# COMMAND ----------

raw_products = spark.table("raw.products")

# TODO: build dim_product
dim_product = raw_products.limit(0)   # replace this placeholder

display(dim_product.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — `fact_sales`
# MAGIC
# MAGIC **TODO:** Build the fact table from `raw.orders`, joining to the three
# MAGIC dimensions to resolve surrogate keys and compute financial metrics.
# MAGIC
# MAGIC Requirements:
# MAGIC - Apply R1: exclude rows where `quantity <= 0`.
# MAGIC - Apply R2: exclude `Cancelled` and `Returned` orders.
# MAGIC - Apply R3: impute null `discount_pct` with `0`.
# MAGIC - Join to `dim_customer` on `customer_id`; use `customer_key = -1` for
# MAGIC   unresolved customers (left join + coalesce).
# MAGIC - Join to `dim_product` on `product_id`.
# MAGIC - Join to `dim_date` on `order_date == full_date`.
# MAGIC - Compute the financial columns:
# MAGIC   - `gross_amount  = quantity * unit_price`
# MAGIC   - `discount_amount = gross_amount * discount_pct / 100`
# MAGIC   - `net_amount = gross_amount - discount_amount`
# MAGIC   - `cost_amount = quantity * cost_price`  (from dim_product)
# MAGIC   - `margin_amount = net_amount - cost_amount`
# MAGIC - Keep only the columns listed in the star schema diagram above.
# MAGIC
# MAGIC *Hints:*
# MAGIC - Filter before joining — it's cheaper.
# MAGIC - For the customer left join:
# MAGIC   `F.coalesce(dim_customer["customer_key"], F.lit(-1))`

# COMMAND ----------

raw_orders = spark.table("raw.orders")

# TODO: build fact_sales
fact_sales = raw_orders.limit(0)   # replace this placeholder

display(fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Persist to the `mart` schema

# COMMAND ----------

# TODO: write all four tables to the mart schema as Delta tables
# dim_date.write.format("delta").mode("overwrite").saveAsTable("mart.dim_date")
# dim_customer.write ...
# dim_product.write ...
# fact_sales.write ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Validate
# MAGIC
# MAGIC Run these checks. All assertions should pass before declaring the mart ready.

# COMMAND ----------

# MAGIC %md
# MAGIC **Row counts**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment after completing Step 5
# MAGIC -- SELECT 'dim_date'     AS tbl, COUNT(*) AS rows FROM mart.dim_date
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'dim_customer',         COUNT(*)         FROM mart.dim_customer
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'dim_product',          COUNT(*)         FROM mart.dim_product
# MAGIC -- UNION ALL
# MAGIC -- SELECT 'fact_sales',           COUNT(*)         FROM mart.fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC **Referential integrity check — every FK in fact_sales must resolve**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment after completing Step 5
# MAGIC -- SELECT COUNT(*) AS orphan_customers
# MAGIC -- FROM mart.fact_sales f
# MAGIC -- LEFT JOIN mart.dim_customer c ON f.customer_key = c.customer_key
# MAGIC -- WHERE c.customer_key IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **Sanity check — no negative margins on Completed orders**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Uncomment after completing Step 5
# MAGIC -- SELECT COUNT(*) AS negative_margin_rows
# MAGIC -- FROM mart.fact_sales
# MAGIC -- WHERE margin_amount < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Sample Reporting Queries
# MAGIC
# MAGIC Once the mart is built, these queries represent what the reporting pipeline
# MAGIC will run. They are given to you so you can verify the mart produces
# MAGIC meaningful results.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly revenue trend
# MAGIC -- SELECT d.year, d.month, d.month_name,
# MAGIC --        ROUND(SUM(f.net_amount), 2)  AS net_revenue,
# MAGIC --        ROUND(SUM(f.margin_amount), 2) AS margin
# MAGIC -- FROM mart.fact_sales f
# MAGIC -- JOIN mart.dim_date d ON f.date_key = d.date_key
# MAGIC -- GROUP BY d.year, d.month, d.month_name
# MAGIC -- ORDER BY d.year, d.month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue and margin by product category
# MAGIC -- SELECT p.category,
# MAGIC --        COUNT(DISTINCT f.order_id)    AS orders,
# MAGIC --        SUM(f.quantity)               AS units_sold,
# MAGIC --        ROUND(SUM(f.net_amount), 2)   AS net_revenue,
# MAGIC --        ROUND(SUM(f.margin_amount) * 100.0 / SUM(f.net_amount), 1) AS margin_pct
# MAGIC -- FROM mart.fact_sales f
# MAGIC -- JOIN mart.dim_product p ON f.product_key = p.product_key
# MAGIC -- GROUP BY p.category
# MAGIC -- ORDER BY net_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 customers by net revenue
# MAGIC -- SELECT c.full_name, c.country, c.segment,
# MAGIC --        ROUND(SUM(f.net_amount), 2) AS net_revenue
# MAGIC -- FROM mart.fact_sales f
# MAGIC -- JOIN mart.dim_customer c ON f.customer_key = c.customer_key
# MAGIC -- WHERE c.customer_key != -1
# MAGIC -- GROUP BY c.full_name, c.country, c.segment
# MAGIC -- ORDER BY net_revenue DESC
# MAGIC -- LIMIT 10;
