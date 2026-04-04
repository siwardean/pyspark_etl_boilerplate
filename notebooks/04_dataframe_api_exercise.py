# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — DataFrame API vs Spark SQL
# MAGIC ## From a query nobody wants to maintain, to code everyone can read
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why this matters
# MAGIC
# MAGIC As a data engineer you will spend as much time **reading** code as writing it.
# MAGIC SQL is great for simple queries, but as transformations grow — multiple joins,
# MAGIC window functions, conditional logic — a single SQL string becomes a wall of text
# MAGIC that is hard to read, impossible to test in parts, and painful to refactor.
# MAGIC
# MAGIC The **Spark DataFrame API** lets you express the same logic as a chain of
# MAGIC named, inspectable steps. Each step is one Python call. Each step can be
# MAGIC `display()`-ed immediately. Each step can be extracted into a reusable function.
# MAGIC
# MAGIC | | Spark SQL | DataFrame API |
# MAGIC |---|---|---|
# MAGIC | Readability | Degrades fast with CTEs | Reads top-to-bottom |
# MAGIC | Testability | Run whole query or nothing | Inspect any intermediate step |
# MAGIC | Reusability | Copy-paste strings | Python functions |
# MAGIC | IDE support | Plain string | Autocomplete + type hints |
# MAGIC | Mix Python logic | Hard | Natural |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise goal
# MAGIC
# MAGIC You are given a working but hard-to-read SQL query.
# MAGIC Your job is to rewrite it using the DataFrame API by filling in **4 TODO blocks**.
# MAGIC The surrounding code is already written — you only fill in the blanks.
# MAGIC
# MAGIC **Time budget: 30 minutes**
# MAGIC
# MAGIC > Pre-requisite: run notebooks `00` and `03` first so `mart.*` tables exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — The SQL query you inherited
# MAGIC
# MAGIC The analyst who wrote this has left the company.
# MAGIC It produces a **monthly top-3 product categories by revenue**, with margin
# MAGIC classification. Run it and study the output — this is what your DataFrame
# MAGIC version must reproduce exactly.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly_sales AS (
# MAGIC     SELECT   d.year, d.month, d.month_name, p.category,
# MAGIC              SUM(f.net_amount)                               AS net_revenue,
# MAGIC              SUM(f.margin_amount)                            AS total_margin,
# MAGIC              COUNT(DISTINCT f.order_id)                      AS orders,
# MAGIC              SUM(f.quantity)                                 AS units_sold
# MAGIC     FROM     mart.fact_sales      f
# MAGIC     JOIN     mart.dim_date        d  ON  f.date_key     = d.date_key
# MAGIC     JOIN     mart.dim_product     p  ON  f.product_key  = p.product_key
# MAGIC     JOIN     mart.dim_customer    c  ON  f.customer_key = c.customer_key
# MAGIC     WHERE    c.customer_key != -1
# MAGIC     GROUP BY d.year, d.month, d.month_name, p.category
# MAGIC ),
# MAGIC with_metrics AS (
# MAGIC     SELECT   *,
# MAGIC              ROUND(total_margin * 100.0 / NULLIF(net_revenue, 0), 1)  AS margin_pct,
# MAGIC              RANK() OVER (
# MAGIC                  PARTITION BY year, month
# MAGIC                  ORDER BY net_revenue DESC
# MAGIC              )                                                          AS revenue_rank
# MAGIC     FROM     monthly_sales
# MAGIC ),
# MAGIC with_tier AS (
# MAGIC     SELECT   *,
# MAGIC              CASE
# MAGIC                  WHEN margin_pct >= 40 THEN 'High'
# MAGIC                  WHEN margin_pct >= 25 THEN 'Medium'
# MAGIC                  ELSE                       'Low'
# MAGIC              END  AS margin_tier
# MAGIC     FROM     with_metrics
# MAGIC )
# MAGIC SELECT   year, month, month_name, category,
# MAGIC          ROUND(net_revenue, 2)   AS net_revenue,
# MAGIC          ROUND(total_margin, 2)  AS total_margin,
# MAGIC          margin_pct,
# MAGIC          margin_tier,
# MAGIC          orders,
# MAGIC          units_sold,
# MAGIC          revenue_rank
# MAGIC FROM     with_tier
# MAGIC WHERE    revenue_rank <= 3
# MAGIC ORDER BY year, month, revenue_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC Study the output. Can you tell at a glance which CTE does what?
# MAGIC Now notice: three CTEs, three `SELECT *`, one window function buried in the
# MAGIC middle — and no way to inspect intermediate results without copy-pasting.
# MAGIC
# MAGIC Let's rewrite this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — Rewrite with the DataFrame API
# MAGIC
# MAGIC The pipeline is broken into the same logical steps as the SQL CTEs —
# MAGIC but now each step is a named variable you can `display()` at any point.
# MAGIC
# MAGIC Fill in the **4 TODO blocks**. The rest is done for you.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load the mart tables — these are your inputs
fact_sales   = spark.table("mart.fact_sales")
dim_date     = spark.table("mart.dim_date")
dim_product  = spark.table("mart.dim_product")
dim_customer = spark.table("mart.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step A — Join the tables
# MAGIC
# MAGIC **TODO 1:** Replace the four `# TODO` comments with the correct `.join()` calls.
# MAGIC
# MAGIC Each join needs:
# MAGIC - the table to join
# MAGIC - the join condition (`on=`)
# MAGIC - the join type (`how=` — use `"inner"` for all four)
# MAGIC
# MAGIC The SQL equivalent is the `FROM … JOIN … JOIN … JOIN` block in the first CTE.
# MAGIC
# MAGIC ```
# MAGIC SQL                                  DataFrame API
# MAGIC ─────────────────────────────────    ──────────────────────────────────────────────
# MAGIC FROM  mart.fact_sales f              fact_sales
# MAGIC JOIN  mart.dim_date d                  .join(dim_date,    fact_sales.date_key     == dim_date.date_key,    "inner")
# MAGIC   ON  f.date_key = d.date_key          .join( ... )
# MAGIC JOIN  mart.dim_product p               .join( ... )
# MAGIC   ON  f.product_key = p.product_key    .join( ... )
# MAGIC JOIN  mart.dim_customer c
# MAGIC   ON  f.customer_key = c.customer_key
# MAGIC WHERE c.customer_key != -1
# MAGIC ```

# COMMAND ----------

joined = (
    fact_sales
    # TODO 1a: join dim_date   on  fact_sales.date_key    == dim_date.date_key
    # TODO 1b: join dim_product on  fact_sales.product_key == dim_product.product_key
    # TODO 1c: join dim_customer on fact_sales.customer_key == dim_customer.customer_key
    .filter(dim_customer["customer_key"] != -1)      # exclude the sentinel row
)

# ── sanity check — display a few rows before moving on ────────────────────────
display(joined.select(
    fact_sales["order_id"],
    dim_date["year"], dim_date["month_name"],
    dim_product["category"],
    fact_sales["net_amount"], fact_sales["margin_amount"]
).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step B — Aggregate by month and category
# MAGIC
# MAGIC **TODO 2:** Complete the `.groupBy()` and `.agg()` call.
# MAGIC
# MAGIC ```
# MAGIC SQL                                        DataFrame API
# MAGIC ──────────────────────────────────────     ────────────────────────────────────────────
# MAGIC GROUP BY d.year, d.month,                  .groupBy(
# MAGIC          d.month_name, p.category               dim_date["year"], dim_date["month"],
# MAGIC                                                  dim_date["month_name"], dim_product["category"]
# MAGIC SELECT SUM(f.net_amount) AS net_revenue    ).agg(
# MAGIC        SUM(f.margin_amount) AS tot_margin      F.round(F.sum("net_amount"),    2).alias("net_revenue"),
# MAGIC        COUNT(DISTINCT order_id) AS orders       ...
# MAGIC        SUM(f.quantity) AS units_sold             ...
# MAGIC                                                  ...
# MAGIC                                             )
# MAGIC ```

# COMMAND ----------

monthly_sales = (
    joined
    # TODO 2: .groupBy( year, month, month_name, category )
    #         .agg(
    #             net_revenue    = ROUND( SUM(net_amount),    2 )
    #             total_margin   = ROUND( SUM(margin_amount), 2 )
    #             orders         = COUNT( DISTINCT order_id  )
    #             units_sold     = SUM( quantity )
    #         )
)

display(monthly_sales.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step C — Add the revenue rank per month
# MAGIC
# MAGIC **TODO 3:** Define the window and add a `revenue_rank` column.
# MAGIC
# MAGIC A window function needs two things:
# MAGIC - **partition**: the group to rank within → partition by `year` and `month`
# MAGIC - **order**: what to rank by → order by `net_revenue` descending
# MAGIC
# MAGIC ```
# MAGIC SQL                                          DataFrame API
# MAGIC ──────────────────────────────────────────   ────────────────────────────────────────────
# MAGIC RANK() OVER (                                w = Window.partitionBy("year", "month") \
# MAGIC   PARTITION BY year, month                          .orderBy(F.col("net_revenue").desc())
# MAGIC   ORDER BY net_revenue DESC
# MAGIC )                                            .withColumn("revenue_rank", F.rank().over(w))
# MAGIC ```

# COMMAND ----------

# TODO 3: define w = Window.partitionBy(...).orderBy(...)

ranked = (
    monthly_sales
    # .withColumn("revenue_rank", F.rank().over(w))
)

display(ranked.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step D — Classify margin tier
# MAGIC
# MAGIC **TODO 4:** Add a `margin_pct` column and then a `margin_tier` column using
# MAGIC `F.when().when().otherwise()`.
# MAGIC
# MAGIC ```
# MAGIC SQL                                          DataFrame API
# MAGIC ──────────────────────────────────────────   ────────────────────────────────────────────
# MAGIC ROUND(margin*100.0/NULLIF(revenue,0), 1)     F.round(F.col("total_margin") * 100
# MAGIC   AS margin_pct                                   / F.nullif("net_revenue", 0), 1)
# MAGIC
# MAGIC CASE                                         F.when(F.col("margin_pct") >= 40, "High")
# MAGIC   WHEN margin_pct >= 40 THEN 'High'           .when(F.col("margin_pct") >= 25, "Medium")
# MAGIC   WHEN margin_pct >= 25 THEN 'Medium'         .otherwise("Low")
# MAGIC   ELSE 'Low'
# MAGIC END AS margin_tier
# MAGIC ```

# COMMAND ----------

with_tier = (
    ranked
    # TODO 4a: .withColumn("margin_pct",  ROUND( total_margin * 100 / net_revenue, 1 ) )
    # TODO 4b: .withColumn("margin_tier", F.when(...).when(...).otherwise(...) )
)

display(with_tier.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final step — Filter top 3 and sort (already done)

# COMMAND ----------

result = (
    with_tier
    .filter(F.col("revenue_rank") <= 3)
    .select("year", "month", "month_name", "category",
            "net_revenue", "total_margin", "margin_pct", "margin_tier",
            "orders", "units_sold", "revenue_rank")
    .orderBy("year", "month", "revenue_rank")
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3 — Compare the two approaches
# MAGIC
# MAGIC Run this to confirm your DataFrame result matches the SQL result exactly.

# COMMAND ----------

sql_result = spark.sql("""
    WITH monthly_sales AS (
        SELECT d.year, d.month, d.month_name, p.category,
               SUM(f.net_amount)           AS net_revenue,
               SUM(f.margin_amount)        AS total_margin,
               COUNT(DISTINCT f.order_id)  AS orders,
               SUM(f.quantity)             AS units_sold
        FROM   mart.fact_sales f
        JOIN   mart.dim_date d      ON f.date_key     = d.date_key
        JOIN   mart.dim_product p   ON f.product_key  = p.product_key
        JOIN   mart.dim_customer c  ON f.customer_key = c.customer_key
        WHERE  c.customer_key != -1
        GROUP BY d.year, d.month, d.month_name, p.category
    ),
    with_metrics AS (
        SELECT *, ROUND(total_margin*100.0/NULLIF(net_revenue,0),1) AS margin_pct,
               RANK() OVER (PARTITION BY year,month ORDER BY net_revenue DESC) AS revenue_rank
        FROM monthly_sales
    ),
    with_tier AS (
        SELECT *, CASE WHEN margin_pct>=40 THEN 'High'
                       WHEN margin_pct>=25 THEN 'Medium' ELSE 'Low' END AS margin_tier
        FROM with_metrics
    )
    SELECT year, month, month_name, category,
           ROUND(net_revenue,2) AS net_revenue, ROUND(total_margin,2) AS total_margin,
           margin_pct, margin_tier, orders, units_sold, revenue_rank
    FROM with_tier WHERE revenue_rank<=3 ORDER BY year, month, revenue_rank
""")

sort_cols = ["year", "month", "revenue_rank", "category"]
diff_count = result.orderBy(sort_cols).exceptAll(sql_result.orderBy(sort_cols)).count()

if diff_count == 0:
    print("✓ DataFrame result matches SQL result exactly.")
else:
    print(f"✗ {diff_count} rows differ — check your TODO implementations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4 — Plug it into the ETL boilerplate
# MAGIC
# MAGIC Now that the transformation works, here is how a data engineer would
# MAGIC package it as a production ETL job using the repository structure you
# MAGIC have been shown. **This cell is for reference — nothing to fill in.**
# MAGIC
# MAGIC Notice:
# MAGIC - `__init__` handles config and Spark startup — you never call `SparkSession` directly
# MAGIC - `extract()` reads from sources — one responsibility
# MAGIC - `transform()` applies the DataFrame chain — one responsibility
# MAGIC - `load()` writes the output — one responsibility
# MAGIC - `run()` owns MLflow tracking and orchestrates the three steps

# COMMAND ----------

# ── Reference implementation — do not run in this notebook ────────────────────
#
# from etl.etl_interface import ETLInterface
# from utils.toolbox import drop_nulls
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# import mlflow
#
#
# class MonthlyRevenueCategoryETL(ETLInterface):
#
#     def extract(self):
#         self.fact_sales   = self.spark.table("mart.fact_sales")
#         self.dim_date     = self.spark.table("mart.dim_date")
#         self.dim_product  = self.spark.table("mart.dim_product")
#         self.dim_customer = self.spark.table("mart.dim_customer")
#
#     def transform(self):
#         fact        = self.fact_sales
#         dim_date    = self.dim_date
#         dim_product = self.dim_product
#         dim_cust    = self.dim_customer
#
#         joined = (
#             fact
#             .join(dim_date,    fact["date_key"]     == dim_date["date_key"],      "inner")
#             .join(dim_product, fact["product_key"]  == dim_product["product_key"], "inner")
#             .join(dim_cust,    fact["customer_key"] == dim_cust["customer_key"],   "inner")
#             .filter(dim_cust["customer_key"] != -1)
#         )
#
#         monthly = (
#             joined
#             .groupBy(dim_date["year"], dim_date["month"],
#                      dim_date["month_name"], dim_product["category"])
#             .agg(
#                 F.round(F.sum("net_amount"),             2).alias("net_revenue"),
#                 F.round(F.sum("margin_amount"),          2).alias("total_margin"),
#                 F.countDistinct("order_id")               .alias("orders"),
#                 F.sum("quantity")                         .alias("units_sold"),
#             )
#         )
#
#         w = Window.partitionBy("year", "month").orderBy(F.col("net_revenue").desc())
#
#         self.final_df = (
#             monthly
#             .withColumn("revenue_rank", F.rank().over(w))
#             .withColumn("margin_pct",
#                         F.round(F.col("total_margin") * 100
#                                 / F.nullif(F.col("net_revenue"), F.lit(0)), 1))
#             .withColumn("margin_tier",
#                         F.when(F.col("margin_pct") >= 40, "High")
#                          .when(F.col("margin_pct") >= 25, "Medium")
#                          .otherwise("Low"))
#             .filter(F.col("revenue_rank") <= 3)
#             .select("year", "month", "month_name", "category",
#                     "net_revenue", "total_margin", "margin_pct", "margin_tier",
#                     "orders", "units_sold", "revenue_rank")
#             .orderBy("year", "month", "revenue_rank")
#         )
#
#     def load(self):
#         output_path = self.config.get("spark.targetfilepath") + "/monthly_revenue_by_category"
#         self.final_df.write.mode("overwrite").parquet(output_path)
#
#     def run(self):
#         with mlflow.start_run():
#             mlflow.set_tag("job", "MonthlyRevenueCategoryETL")
#             mlflow.log_param("env",       self.config.get("spark.env"))
#             mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
#             self.extract()
#             self.transform()
#             self.load()
#             mlflow.log_metric("record_count", self.final_df.count())
#             mlflow.log_artifact(self.config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key takeaways
# MAGIC
# MAGIC | Concept | SQL version | DataFrame API version |
# MAGIC |---|---|---|
# MAGIC | **Multi-step logic** | Three CTEs in one string | Three named variables — inspect any of them |
# MAGIC | **Window function** | Buried inside CTE | `Window` object defined once, reused anywhere |
# MAGIC | **Conditional column** | `CASE WHEN … END` block | `F.when().when().otherwise()` — chainable |
# MAGIC | **Aggregation** | `GROUP BY` + `SELECT` | `.groupBy().agg()` — columns explicit |
# MAGIC | **Debugging** | Re-run full query | `display(monthly_sales)` at any step |
# MAGIC | **Reuse** | Copy-paste SQL strings | Extract any step into a Python function |
# MAGIC
# MAGIC The DataFrame API does not replace SQL — it complements it.
# MAGIC Use SQL for ad-hoc exploration. Use the DataFrame API for production ETL jobs.
