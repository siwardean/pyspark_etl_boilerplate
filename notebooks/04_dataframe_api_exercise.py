# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — ETL Job: DataFrame API vs Spark SQL
# MAGIC
# MAGIC ## Your mission
# MAGIC
# MAGIC A working ETL job already exists below. It produces a **monthly top-3
# MAGIC product categories by revenue** report, complete with margin classification.
# MAGIC
# MAGIC The job follows the exact same structure as every ETL job in this repository:
# MAGIC
# MAGIC ```
# MAGIC __init__   →   extract()   →   transform()   →   load()
# MAGIC                                     ↑
# MAGIC                              this is where
# MAGIC                              you will work
# MAGIC ```
# MAGIC
# MAGIC **Your tasks — in order of difficulty:**
# MAGIC
# MAGIC | Task | What you do | Where |
# MAGIC |---|---|---|
# MAGIC | 1 | Read and understand the SQL inside `transform()` | `MonthlyRevenueJobV1` |
# MAGIC | 2 | Re-implement `transform()` using the DataFrame API | `MonthlyRevenueJobV2` |
# MAGIC | 3 | Add one MLflow metric of your choice | `MonthlyRevenueJobV2.run()` |
# MAGIC
# MAGIC > **Pre-requisite:** run notebooks `00` and `03` first so `mart.*` tables exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — config file and imports

# COMMAND ----------

# Write a minimal config file so the ETL class can be instantiated normally
import os

config_content = """
[SPARK_CONTEXT]
spark.app.name=MonthlyRevenueJob

[SPARK]
env=dev
loadMethod=full
targetfilepath=/tmp/retailco/monthly_revenue
"""

os.makedirs("/tmp/retailco", exist_ok=True)
with open("/tmp/retailco/job.properties", "w") as f:
    f.write(config_content)

CONFIG_PATH = "/tmp/retailco/job.properties"
print(f"Config written to {CONFIG_PATH}")

# COMMAND ----------

# Paste the boilerplate ETL base classes here so the notebook is self-contained.
# In a real project these would come from the repo package directly.

import sys, os
sys.path.insert(0, "/path/to/pyspark_etl_boilerplate")   # adjust to your repo path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The job as it exists today (SQL version)
# MAGIC
# MAGIC Read through `MonthlyRevenueJobV1` carefully.
# MAGIC Pay attention to `transform()` — this is the logic you will rewrite.
# MAGIC
# MAGIC Run the cell, then run the job below it. Study the output.

# COMMAND ----------

from etl.etl_interface import ETLInterface
from etl.etl_config import ETLConfig

class MonthlyRevenueJobV1(ETLInterface):
    """ETL job — monthly top-3 categories by revenue. SQL implementation."""

    # ── extract ───────────────────────────────────────────────────────────────
    def extract(self):
        self.fact_sales   = self.spark.table("mart.fact_sales")
        self.dim_date     = self.spark.table("mart.dim_date")
        self.dim_product  = self.spark.table("mart.dim_product")
        self.dim_customer = self.spark.table("mart.dim_customer")

        # Register as temp views so SQL can query them
        self.fact_sales  .createOrReplaceTempView("fact_sales")
        self.dim_date    .createOrReplaceTempView("dim_date")
        self.dim_product .createOrReplaceTempView("dim_product")
        self.dim_customer.createOrReplaceTempView("dim_customer")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self):
        self.final_df = self.spark.sql("""
            WITH monthly_sales AS (
                SELECT   d.year, d.month, d.month_name, p.category,
                         SUM(f.net_amount)           AS net_revenue,
                         SUM(f.margin_amount)        AS total_margin,
                         COUNT(DISTINCT f.order_id)  AS orders,
                         SUM(f.quantity)             AS units_sold
                FROM     fact_sales      f
                JOIN     dim_date        d  ON  f.date_key     = d.date_key
                JOIN     dim_product     p  ON  f.product_key  = p.product_key
                JOIN     dim_customer    c  ON  f.customer_key = c.customer_key
                WHERE    c.customer_key != -1
                GROUP BY d.year, d.month, d.month_name, p.category
            ),
            with_metrics AS (
                SELECT *,
                       ROUND(total_margin * 100.0 / NULLIF(net_revenue, 0), 1) AS margin_pct,
                       RANK() OVER (
                           PARTITION BY year, month
                           ORDER BY net_revenue DESC
                       ) AS revenue_rank
                FROM monthly_sales
            ),
            with_tier AS (
                SELECT *,
                       CASE
                           WHEN margin_pct >= 40 THEN 'High'
                           WHEN margin_pct >= 25 THEN 'Medium'
                           ELSE                       'Low'
                       END AS margin_tier
                FROM with_metrics
            )
            SELECT   year, month, month_name, category,
                     ROUND(net_revenue,  2) AS net_revenue,
                     ROUND(total_margin, 2) AS total_margin,
                     margin_pct,
                     margin_tier,
                     orders,
                     units_sold,
                     revenue_rank
            FROM     with_tier
            WHERE    revenue_rank <= 3
            ORDER BY year, month, revenue_rank
        """)

    # ── load ──────────────────────────────────────────────────────────────────
    def load(self):
        output_path = self.config.get("spark.targetfilepath") + "_v1"
        self.final_df.write.mode("overwrite").parquet(output_path)

    # ── run ───────────────────────────────────────────────────────────────────
    def run(self):
        with mlflow.start_run(run_name="MonthlyRevenueJobV1"):
            mlflow.set_tag("job", "MonthlyRevenueJobV1")
            mlflow.log_param("env",       self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            self.extract()
            self.transform()
            self.load()
            mlflow.log_metric("record_count", self.final_df.count())
            mlflow.log_artifact(self.config_path)

# COMMAND ----------

# Run the SQL version
job_v1 = MonthlyRevenueJobV1(CONFIG_PATH)
job_v1.run()
display(job_v1.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Questions to answer before moving on:
# MAGIC
# MAGIC 1. How many CTEs does the SQL have, and what does each one do?
# MAGIC 2. If you wanted to inspect the intermediate result after the joins, how would you do it with this SQL version?
# MAGIC 3. Where is the window function? What does `PARTITION BY year, month ORDER BY net_revenue DESC` mean in plain English?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — Your turn: rewrite `transform()` with the DataFrame API
# MAGIC
# MAGIC `MonthlyRevenueJobV2` below is identical to V1 **except `transform()`**,
# MAGIC which is a skeleton waiting for you to fill in.
# MAGIC
# MAGIC Complete the **4 TODO blocks** inside `transform()`. Do not change anything
# MAGIC else. Run the job when you are done — the validation cell will tell you
# MAGIC if your result matches V1 exactly.
# MAGIC
# MAGIC ### Quick reference card
# MAGIC
# MAGIC | SQL construct | DataFrame API equivalent |
# MAGIC |---|---|
# MAGIC | `FROM a JOIN b ON a.x = b.x` | `a.join(b, a["x"] == b["x"], "inner")` |
# MAGIC | `GROUP BY x, y` + `SUM(col)` | `.groupBy("x","y").agg(F.sum("col").alias("..."))` |
# MAGIC | `RANK() OVER (PARTITION BY x ORDER BY y DESC)` | `w = Window.partitionBy("x").orderBy(F.col("y").desc())` then `F.rank().over(w)` |
# MAGIC | `CASE WHEN a THEN x WHEN b THEN y ELSE z END` | `F.when(cond_a, x).when(cond_b, y).otherwise(z)` |
# MAGIC | `ROUND(x, 1)` | `F.round(F.col("x"), 1)` |
# MAGIC | `NULLIF(x, 0)` | `F.nullif(F.col("x"), F.lit(0))` |

# COMMAND ----------

class MonthlyRevenueJobV2(ETLInterface):
    """ETL job — monthly top-3 categories by revenue. DataFrame API implementation."""

    # ── extract ───────────────────────────────────────────────────────────────
    # Already done — do not modify.
    def extract(self):
        self.fact_sales   = self.spark.table("mart.fact_sales")
        self.dim_date     = self.spark.table("mart.dim_date")
        self.dim_product  = self.spark.table("mart.dim_product")
        self.dim_customer = self.spark.table("mart.dim_customer")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self):

        # ── TODO 1: Join the four tables ──────────────────────────────────────
        # Start from self.fact_sales and chain three .join() calls.
        # Use "inner" for all joins. Exclude the sentinel customer (key = -1).
        #
        # Hint: self.fact_sales.join(self.dim_date,
        #                            self.fact_sales["date_key"] == self.dim_date["date_key"],
        #                            "inner")
        #
        joined = self.fact_sales   # ← replace this line

        # You can inspect your join result here before continuing:
        # display(joined.limit(5))

        # ── TODO 2: Aggregate by year / month / category ──────────────────────
        # Group by: year, month, month_name (from dim_date), category (from dim_product)
        # Aggregate:
        #   net_revenue  = ROUND( SUM(net_amount),    2 )
        #   total_margin = ROUND( SUM(margin_amount), 2 )
        #   orders       = COUNT DISTINCT order_id
        #   units_sold   = SUM( quantity )
        #
        monthly = joined   # ← replace this line

        # display(monthly.limit(5))

        # ── TODO 3: Rank categories by revenue within each month ──────────────
        # Define a Window partitioned by year and month, ordered by net_revenue DESC.
        # Add a column "revenue_rank" using F.rank().over(that window).
        #
        # w = Window.partitionBy(...).orderBy(...)
        #
        ranked = monthly   # ← replace this line

        # display(ranked.limit(5))

        # ── TODO 4: Classify margin tier ─────────────────────────────────────
        # Add two columns:
        #   margin_pct  = ROUND( total_margin * 100 / net_revenue, 1 )
        #   margin_tier = "High" if margin_pct >= 40
        #                 "Medium" if margin_pct >= 25
        #                 "Low" otherwise
        #
        with_tier = ranked   # ← replace this line

        # ── Final select — already done, do not modify ────────────────────────
        self.final_df = (
            with_tier
            .filter(F.col("revenue_rank") <= 3)
            .select("year", "month", "month_name", "category",
                    "net_revenue", "total_margin", "margin_pct", "margin_tier",
                    "orders", "units_sold", "revenue_rank")
            .orderBy("year", "month", "revenue_rank")
        )

    # ── load — already done, do not modify ───────────────────────────────────
    def load(self):
        output_path = self.config.get("spark.targetfilepath") + "_v2"
        self.final_df.write.mode("overwrite").parquet(output_path)

    # ── run — already done, do not modify ────────────────────────────────────
    def run(self):
        with mlflow.start_run(run_name="MonthlyRevenueJobV2"):
            mlflow.set_tag("job", "MonthlyRevenueJobV2")
            mlflow.log_param("env",       self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            self.extract()
            self.transform()
            self.load()
            mlflow.log_metric("record_count", self.final_df.count())
            mlflow.log_artifact(self.config_path)
            # ── TASK 3: add one more MLflow metric here ───────────────────────
            # Ideas: total net_revenue, number of distinct categories, avg margin_pct
            # mlflow.log_metric("...", ...)

# COMMAND ----------

# Run your version
job_v2 = MonthlyRevenueJobV2(CONFIG_PATH)
job_v2.run()
display(job_v2.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — does your result match V1?

# COMMAND ----------

sort_cols = ["year", "month", "revenue_rank", "category"]

diff = (
    job_v2.final_df.orderBy(sort_cols)
    .exceptAll(job_v1.final_df.orderBy(sort_cols))
)
diff_count = diff.count()

if diff_count == 0:
    print("✓  Your DataFrame API result matches the SQL result exactly. Well done.")
else:
    print(f"✗  {diff_count} rows differ. Check your TODOs and re-run.")
    display(diff)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What did you just build?
# MAGIC
# MAGIC Look at the two `transform()` methods side by side. The DataFrame API version:
# MAGIC
# MAGIC - Has **no string literals** — every column name is a Python symbol your IDE can check
# MAGIC - Can be **paused and inspected** at any step with `display()`
# MAGIC - Reads **top to bottom** — each variable name tells you what it holds
# MAGIC - Lives inside a **proper ETL class** that handles config, Spark, and MLflow for you
# MAGIC
# MAGIC This class is what a production data engineering job looks like.
# MAGIC The pattern is always the same: `extract → transform → load`, tracked by MLflow,
# MAGIC configured by a `.properties` file, run by `main.py` with a single command.
