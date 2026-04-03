# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — ETL Solution: RetailCo Sales Data Mart
# MAGIC
# MAGIC > **Instructor notebook — share only after students complete the exercise.**
# MAGIC
# MAGIC This notebook is the complete, production-ready implementation of the
# MAGIC star schema pipeline defined in `02_etl_exercise`. Every design decision
# MAGIC is explained inline.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — `dim_date`
# MAGIC
# MAGIC Generated entirely in Spark — no source table needed.
# MAGIC Covers 2022-01-01 → 2024-12-31 (1 096 rows).

# COMMAND ----------

start_date = "2022-01-01"
end_date   = "2024-12-31"

dim_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), "
        f"interval 1 day)) AS full_date"
    )
    .withColumn("date_key",
                F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("day_of_week",  F.dayofweek("full_date"))          # 1=Sun … 7=Sat
    .withColumn("day_name",     F.date_format("full_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("month",        F.month("full_date"))
    .withColumn("month_name",   F.date_format("full_date", "MMMM"))
    .withColumn("quarter",      F.quarter("full_date"))
    .withColumn("year",         F.year("full_date"))
    .withColumn("is_weekend",   F.dayofweek("full_date").isin(1, 7))
    .select("date_key", "full_date", "day_of_week", "day_name",
            "week_of_year", "month", "month_name", "quarter", "year", "is_weekend")
)

print(f"dim_date: {dim_date.count()} rows")
display(dim_date.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — `dim_customer`
# MAGIC
# MAGIC Key decisions:
# MAGIC - Surrogate key starts at 1 (`monotonically_increasing_id` is not gap-free
# MAGIC   across partitions, so we use `row_number` over an ordered window for
# MAGIC   a clean sequence).
# MAGIC - Null `city` → `"Unknown"` (agreed in exploration, rule R4).
# MAGIC - A sentinel row (`customer_key = -1`) absorbs unresolvable order references.

# COMMAND ----------

raw_customers = spark.table("raw.customers")

# Surrogate key via row_number for a clean 1-based sequence
w = Window.orderBy("customer_id")

dim_customer = (
    raw_customers
    .withColumn("customer_key",
                F.row_number().over(w))
    .withColumn("full_name",
                F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("city",
                F.coalesce(F.col("city"), F.lit("Unknown")))
    .select("customer_key", "customer_id", "full_name", "email",
            "country", "city", "segment", "signup_date")
)

# Sentinel row for unresolvable customers (rule R6)
unknown_customer = spark.createDataFrame([{
    "customer_key": -1,
    "customer_id":  "UNKNOWN",
    "full_name":    "Unknown Customer",
    "email":        None,
    "country":      "Unknown",
    "city":         "Unknown",
    "segment":      "Unknown",
    "signup_date":  None,
}], schema=dim_customer.schema)

dim_customer = dim_customer.union(unknown_customer)

print(f"dim_customer: {dim_customer.count()} rows  (incl. 1 sentinel)")
display(dim_customer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — `dim_product`
# MAGIC
# MAGIC Key decision: deduplicate on `product_id` keeping the first occurrence
# MAGIC (rule R5). We use `row_number` over a window to be deterministic.

# COMMAND ----------

raw_products = spark.table("raw.products")

w_prod = Window.partitionBy("product_id").orderBy("product_id")

dim_product = (
    raw_products
    .withColumn("rn", F.row_number().over(w_prod))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("product_key",
                F.row_number().over(Window.orderBy("product_id")))
    .select("product_key", "product_id", "product_name",
            "category", "subcategory", "brand", "cost_price", "list_price")
)

print(f"dim_product: {dim_product.count()} rows  (duplicates removed)")
display(dim_product.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — `fact_sales`
# MAGIC
# MAGIC The fact table is built in four stages:
# MAGIC 1. Filter raw orders (R1, R2, R3)
# MAGIC 2. Resolve dimension surrogate keys
# MAGIC 3. Compute financial metrics
# MAGIC 4. Select final columns

# COMMAND ----------

raw_orders = spark.table("raw.orders")

# ── Stage 1: filter ────────────────────────────────────────────────────────────
orders_clean = (
    raw_orders
    .filter(F.col("quantity") > 0)                                    # R1
    .filter(~F.col("status").isin("Cancelled", "Returned"))           # R2
    .withColumn("discount_pct",
                F.coalesce(F.col("discount_pct"), F.lit(0)))          # R3
)

# ── Stage 2: resolve surrogate keys ───────────────────────────────────────────
# Customer — left join so unresolvable orders survive (rule R6)
orders_with_cust = (
    orders_clean
    .join(
        dim_customer.select("customer_key", "customer_id"),
        on="customer_id",
        how="left"
    )
    .withColumn("customer_key",
                F.coalesce(F.col("customer_key"), F.lit(-1)))
)

# Product — inner join (every order must reference a valid product)
orders_with_prod = (
    orders_with_cust
    .join(
        dim_product.select("product_key", "product_id", "cost_price"),
        on="product_id",
        how="inner"
    )
)

# Date — cast order_date to date then join on full_date
orders_with_date = (
    orders_with_prod
    .withColumn("order_date_d", F.col("order_date").cast("date"))
    .join(
        dim_date.select("date_key", "full_date"),
        on=F.col("order_date_d") == F.col("full_date"),
        how="inner"
    )
)

# ── Stage 3: compute metrics ───────────────────────────────────────────────────
fact_sales = (
    orders_with_date
    .withColumn("gross_amount",
                F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("discount_amount",
                F.round(F.col("gross_amount") * F.col("discount_pct") / 100, 2))
    .withColumn("net_amount",
                F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    .withColumn("cost_amount",
                F.round(F.col("quantity") * F.col("cost_price"), 2))
    .withColumn("margin_amount",
                F.round(F.col("net_amount") - F.col("cost_amount"), 2))
    # ── Stage 4: final column selection ───────────────────────────────────────
    .select(
        "order_id",
        "customer_key",
        "product_key",
        "date_key",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "cost_amount",
        "margin_amount",
    )
)

print(f"fact_sales: {fact_sales.count()} rows")
display(fact_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Persist to `mart` schema

# COMMAND ----------

dim_date.write.format("delta").mode("overwrite").saveAsTable("mart.dim_date")
dim_customer.write.format("delta").mode("overwrite").saveAsTable("mart.dim_customer")
dim_product.write.format("delta").mode("overwrite").saveAsTable("mart.dim_product")
fact_sales.write.format("delta").mode("overwrite").saveAsTable("mart.fact_sales")

print("All tables written to mart schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Validate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_date'     AS tbl, COUNT(*) AS rows FROM mart.dim_date
# MAGIC UNION ALL
# MAGIC SELECT 'dim_customer',         COUNT(*)         FROM mart.dim_customer
# MAGIC UNION ALL
# MAGIC SELECT 'dim_product',          COUNT(*)         FROM mart.dim_product
# MAGIC UNION ALL
# MAGIC SELECT 'fact_sales',           COUNT(*)         FROM mart.fact_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Referential integrity: no orphan customer keys
# MAGIC SELECT COUNT(*) AS orphan_customers
# MAGIC FROM mart.fact_sales f
# MAGIC LEFT JOIN mart.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE c.customer_key IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Referential integrity: no orphan product keys
# MAGIC SELECT COUNT(*) AS orphan_products
# MAGIC FROM mart.fact_sales f
# MAGIC LEFT JOIN mart.dim_product p ON f.product_key = p.product_key
# MAGIC WHERE p.product_key IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Financial sanity: net_amount = gross - discount
# MAGIC SELECT COUNT(*) AS inconsistent_rows
# MAGIC FROM mart.fact_sales
# MAGIC WHERE ABS(net_amount - (gross_amount - discount_amount)) > 0.01;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Reporting Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly revenue trend
# MAGIC SELECT
# MAGIC     d.year,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     COUNT(DISTINCT f.order_id)      AS orders,
# MAGIC     SUM(f.quantity)                 AS units_sold,
# MAGIC     ROUND(SUM(f.net_amount), 2)     AS net_revenue,
# MAGIC     ROUND(SUM(f.margin_amount), 2)  AS margin,
# MAGIC     ROUND(SUM(f.margin_amount) * 100.0 / SUM(f.net_amount), 1) AS margin_pct
# MAGIC FROM mart.fact_sales f
# MAGIC JOIN mart.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.year, d.month, d.month_name
# MAGIC ORDER BY d.year, d.month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue and margin by product category
# MAGIC SELECT
# MAGIC     p.category,
# MAGIC     COUNT(DISTINCT f.order_id)    AS orders,
# MAGIC     SUM(f.quantity)               AS units_sold,
# MAGIC     ROUND(SUM(f.net_amount), 2)   AS net_revenue,
# MAGIC     ROUND(SUM(f.margin_amount) * 100.0 / SUM(f.net_amount), 1) AS margin_pct
# MAGIC FROM mart.fact_sales f
# MAGIC JOIN mart.dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.category
# MAGIC ORDER BY net_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 customers by net revenue
# MAGIC SELECT
# MAGIC     c.full_name,
# MAGIC     c.country,
# MAGIC     c.segment,
# MAGIC     COUNT(DISTINCT f.order_id)   AS orders,
# MAGIC     ROUND(SUM(f.net_amount), 2)  AS net_revenue
# MAGIC FROM mart.fact_sales f
# MAGIC JOIN mart.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE c.customer_key != -1
# MAGIC GROUP BY c.full_name, c.country, c.segment
# MAGIC ORDER BY net_revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Weekend vs weekday revenue split
# MAGIC SELECT
# MAGIC     d.is_weekend,
# MAGIC     COUNT(DISTINCT f.order_id)   AS orders,
# MAGIC     ROUND(SUM(f.net_amount), 2)  AS net_revenue
# MAGIC FROM mart.fact_sales f
# MAGIC JOIN mart.dim_date d ON f.date_key = d.date_key
# MAGIC GROUP BY d.is_weekend;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Premium vs Standard vs Basic segment contribution
# MAGIC SELECT
# MAGIC     c.segment,
# MAGIC     COUNT(DISTINCT f.order_id)   AS orders,
# MAGIC     ROUND(SUM(f.net_amount), 2)  AS net_revenue,
# MAGIC     ROUND(SUM(f.margin_amount) * 100.0 / SUM(f.net_amount), 1) AS margin_pct
# MAGIC FROM mart.fact_sales f
# MAGIC JOIN mart.dim_customer c ON f.customer_key = c.customer_key
# MAGIC WHERE c.customer_key != -1
# MAGIC GROUP BY c.segment
# MAGIC ORDER BY net_revenue DESC;
