# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — ETL Job: DataFrame API vs Spark SQL
# MAGIC ### Daily Revenue by Category — with MLflow run tracking
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Model
# MAGIC
# MAGIC ### Source — Raw Layer (`raw` schema)
# MAGIC
# MAGIC ```
# MAGIC raw.customers                    raw.products                  raw.orders
# MAGIC ─────────────────────────        ──────────────────────────    ─────────────────────────────
# MAGIC customer_id   STRING  PK         product_id    STRING  PK      order_id      STRING  PK
# MAGIC first_name    STRING             product_name  STRING          customer_id   STRING  FK
# MAGIC last_name     STRING             category      STRING          product_id    STRING  FK
# MAGIC email         STRING             subcategory   STRING          order_date    DATE
# MAGIC country       STRING             brand         STRING          quantity      INT
# MAGIC city          STRING  (nulls)    cost_price    DOUBLE          unit_price    DOUBLE
# MAGIC segment       STRING             list_price    DOUBLE          discount_pct  DOUBLE  (nulls)
# MAGIC signup_date   DATE                                             status        STRING
# MAGIC ```
# MAGIC
# MAGIC ### Mart — Gold Layer (`mart` schema)
# MAGIC
# MAGIC ```
# MAGIC mart.dim_customer               mart.dim_product               mart.dim_date
# MAGIC ─────────────────────────────   ────────────────────────────   ──────────────────────────
# MAGIC customer_key  INT     PK (SK)   product_key  INT    PK (SK)   date_key     INT    PK
# MAGIC customer_id   STRING  (NK)      product_id   STRING (NK)      full_date    DATE
# MAGIC full_name     STRING            product_name STRING            year         INT
# MAGIC email         STRING            category     STRING            month        INT
# MAGIC country       STRING            subcategory  STRING            month_name   STRING
# MAGIC city          STRING            brand        STRING            quarter      INT
# MAGIC segment       STRING            cost_price   DOUBLE            week_of_year INT
# MAGIC signup_date   DATE              list_price   DOUBLE            day_of_week  INT
# MAGIC                                                                day_name     STRING
# MAGIC                                                                is_weekend   BOOLEAN
# MAGIC
# MAGIC mart.fact_sales
# MAGIC ─────────────────────────────────
# MAGIC order_id         STRING
# MAGIC customer_key     INT     FK → dim_customer
# MAGIC product_key      INT     FK → dim_product
# MAGIC date_key         INT     FK → dim_date
# MAGIC quantity         INT
# MAGIC unit_price       DOUBLE
# MAGIC discount_pct     DOUBLE
# MAGIC gross_amount     DOUBLE  quantity × unit_price
# MAGIC discount_amount  DOUBLE  gross × discount_pct / 100
# MAGIC net_amount       DOUBLE  gross − discount
# MAGIC cost_amount      DOUBLE  quantity × cost_price
# MAGIC margin_amount    DOUBLE  net − cost
# MAGIC ```
# MAGIC
# MAGIC ### Output — This Job (`mart.daily_revenue_by_category`)
# MAGIC
# MAGIC ```
# MAGIC daily_revenue_by_category
# MAGIC ──────────────────────────────────────────────
# MAGIC run_date      DATE     the date being processed
# MAGIC category      STRING   product category
# MAGIC orders        LONG     distinct order count
# MAGIC units_sold    LONG     total quantity
# MAGIC net_revenue   DOUBLE   sum of net_amount
# MAGIC total_margin  DOUBLE   sum of margin_amount
# MAGIC margin_pct    DOUBLE   total_margin / net_revenue × 100
# MAGIC margin_tier   STRING   High / Medium / Low
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your mission
# MAGIC
# MAGIC A working ETL job already exists (`DailyRevenueCategoryJobV1`). It runs daily
# MAGIC — one execution per day, processing that day's orders.
# MAGIC
# MAGIC The job follows the same structure as every ETL job in this repository:
# MAGIC
# MAGIC ```
# MAGIC __init__   →   extract()   →   transform(run_date)   →   load()
# MAGIC                                         ↑
# MAGIC                                  this is where
# MAGIC                                  you will work
# MAGIC ```
# MAGIC
# MAGIC **Your tasks — in order:**
# MAGIC
# MAGIC | Task | What you do |
# MAGIC |---|---|
# MAGIC | 1 | Run V1, read the SQL in `transform()`, understand what it does |
# MAGIC | 2 | Fill in the 4 TODOs in `DailyRevenueCategoryJobV2.transform()` using the DataFrame API |
# MAGIC | 3 | Add one MLflow metric of your choice to `run()` |
# MAGIC | 4 | Observe the MLflow run tracker and identify which days need to be rerun |
# MAGIC
# MAGIC > **Pre-requisite:** run notebooks `00` and `03` first so `mart.*` tables exist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import os
from datetime import date, timedelta

# Write a minimal config file
config_content = """
[SPARK_CONTEXT]
spark.app.name=DailyRevenueCategoryJob

[SPARK]
env=dev
loadMethod=incremental
targetfilepath=/tmp/retailco/daily_revenue
"""

os.makedirs("/tmp/retailco", exist_ok=True)
with open("/tmp/retailco/job.properties", "w") as f:
    f.write(config_content)

CONFIG_PATH = "/tmp/retailco/job.properties"
print(f"Config written to {CONFIG_PATH}")

# COMMAND ----------

import sys
sys.path.insert(0, "/path/to/pyspark_etl_boilerplate")  # adjust to your repo path

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from etl.etl_interface import ETLInterface
import mlflow

EXPERIMENT_NAME = "/RetailCo/DailyRevenueCategory"
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The job as it exists today (SQL version)
# MAGIC
# MAGIC Read `transform()` carefully — this is the logic you will rewrite.
# MAGIC Notice that `run_date` is passed as a parameter so the same job
# MAGIC can be called for any date (today, yesterday, or a backfill date).

# COMMAND ----------

class DailyRevenueCategoryJobV1(ETLInterface):
    """Daily ETL job — revenue by product category for a given date. SQL version."""

    # ── extract ───────────────────────────────────────────────────────────────
    def extract(self):
        self.fact_sales   = self.spark.table("mart.fact_sales")
        self.dim_date     = self.spark.table("mart.dim_date")
        self.dim_product  = self.spark.table("mart.dim_product")
        self.dim_customer = self.spark.table("mart.dim_customer")

        self.fact_sales  .createOrReplaceTempView("fact_sales")
        self.dim_date    .createOrReplaceTempView("dim_date")
        self.dim_product .createOrReplaceTempView("dim_product")
        self.dim_customer.createOrReplaceTempView("dim_customer")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self, run_date: str):
        self.final_df = self.spark.sql(f"""
            SELECT
                d.full_date                                              AS run_date,
                p.category,
                COUNT(DISTINCT f.order_id)                              AS orders,
                SUM(f.quantity)                                         AS units_sold,
                ROUND(SUM(f.net_amount),    2)                          AS net_revenue,
                ROUND(SUM(f.margin_amount), 2)                          AS total_margin,
                ROUND(SUM(f.margin_amount) * 100.0
                    / NULLIF(SUM(f.net_amount), 0), 1)                  AS margin_pct,
                CASE
                    WHEN SUM(f.margin_amount) * 100.0
                       / NULLIF(SUM(f.net_amount), 0) >= 40 THEN 'High'
                    WHEN SUM(f.margin_amount) * 100.0
                       / NULLIF(SUM(f.net_amount), 0) >= 25 THEN 'Medium'
                    ELSE                                            'Low'
                END                                                     AS margin_tier
            FROM     fact_sales      f
            JOIN     dim_date        d  ON  f.date_key     = d.date_key
            JOIN     dim_product     p  ON  f.product_key  = p.product_key
            JOIN     dim_customer    c  ON  f.customer_key = c.customer_key
            WHERE    d.full_date     = '{run_date}'
              AND    c.customer_key != -1
            GROUP BY d.full_date, p.category
            ORDER BY net_revenue DESC
        """)

    # ── load ──────────────────────────────────────────────────────────────────
    def load(self, run_date: str):
        output_path = self.config.get("spark.targetfilepath") + f"/run_date={run_date}"
        self.final_df.write.mode("overwrite").parquet(output_path)

    # ── run ───────────────────────────────────────────────────────────────────
    def run(self, run_date: str):
        with mlflow.start_run(run_name=f"DailyRevenueJob-{run_date}"):
            mlflow.set_tag("job",      "DailyRevenueCategoryJob")
            mlflow.set_tag("version",  "v1")
            mlflow.log_param("env",       self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            mlflow.log_param("run_date",  run_date)
            self.extract()
            self.transform(run_date)
            self.load(run_date)
            mlflow.log_metric("record_count", self.final_df.count())
            mlflow.log_artifact(self.config_path)

# COMMAND ----------

# Run V1 for a single day to see the output
sample_date = "2024-03-15"
job_v1 = DailyRevenueCategoryJobV1(CONFIG_PATH)
job_v1.run(sample_date)
display(job_v1.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Questions before moving on:
# MAGIC
# MAGIC 1. The SQL computes `margin_pct` twice — once for `CASE WHEN` and once for the column.
# MAGIC    Why is that? How does the DataFrame API avoid this?
# MAGIC 2. What would you change to run this job for yesterday instead of `2024-03-15`?
# MAGIC 3. Where in the code does the date partitioning of the output happen?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — Your turn: rewrite `transform()` with the DataFrame API
# MAGIC
# MAGIC `DailyRevenueCategoryJobV2` is identical to V1 **except `transform()`**.
# MAGIC Fill in the **4 TODO blocks**. Everything else is already done.
# MAGIC
# MAGIC ### Quick reference
# MAGIC
# MAGIC | SQL construct | DataFrame API equivalent |
# MAGIC |---|---|
# MAGIC | `FROM a JOIN b ON a.x = b.x` | `a.join(b, a["x"] == b["x"], "inner")` |
# MAGIC | `WHERE d.full_date = '2024-03-15'` | `.filter(F.col("full_date") == run_date)` |
# MAGIC | `GROUP BY x, y` + `SUM(col)` | `.groupBy("x","y").agg(F.sum("col").alias("..."))` |
# MAGIC | `NULLIF(x, 0)` | `F.nullif(F.col("x"), F.lit(0))` |
# MAGIC | `CASE WHEN a THEN x WHEN b THEN y ELSE z END` | `F.when(cond_a, x).when(cond_b, y).otherwise(z)` |
# MAGIC | `ROUND(x, 1)` | `F.round(F.col("x"), 1)` |
# MAGIC
# MAGIC > **Tip:** After each TODO, uncomment the `display()` line below it to
# MAGIC > inspect that step before moving on.

# COMMAND ----------

class DailyRevenueCategoryJobV2(ETLInterface):
    """Daily ETL job — revenue by product category for a given date. DataFrame API version."""

    # ── extract — do not modify ───────────────────────────────────────────────
    def extract(self):
        self.fact_sales   = self.spark.table("mart.fact_sales")
        self.dim_date     = self.spark.table("mart.dim_date")
        self.dim_product  = self.spark.table("mart.dim_product")
        self.dim_customer = self.spark.table("mart.dim_customer")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self, run_date: str):

        # ── TODO 1: Join the four tables and filter to run_date ───────────────
        # - Join fact_sales → dim_date, dim_product, dim_customer (all "inner")
        # - Filter: dim_date.full_date == run_date  AND  dim_customer.customer_key != -1
        #
        joined = self.fact_sales   # ← replace this line
        # display(joined.limit(5))

        # ── TODO 2: Aggregate by date and category ────────────────────────────
        # Group by: dim_date["full_date"]  and  dim_product["category"]
        # Aggregate — use aliases exactly as shown:
        #   orders       = COUNT DISTINCT order_id
        #   units_sold   = SUM( quantity )
        #   net_revenue  = ROUND( SUM(net_amount),    2 )
        #   total_margin = ROUND( SUM(margin_amount), 2 )
        #
        aggregated = joined   # ← replace this line
        # display(aggregated.limit(5))

        # ── TODO 3: Add margin_pct ────────────────────────────────────────────
        # margin_pct = ROUND( total_margin * 100 / NULLIF(net_revenue, 0), 1 )
        # Note: computing it as its own column first avoids repeating the
        # formula inside the CASE WHEN (the SQL has to repeat it twice).
        #
        with_pct = aggregated   # ← replace this line
        # display(with_pct.limit(5))

        # ── TODO 4: Classify margin tier ─────────────────────────────────────
        # margin_tier = "High"   if margin_pct >= 40
        #               "Medium" if margin_pct >= 25
        #               "Low"    otherwise
        #
        with_tier = with_pct   # ← replace this line

        # ── Final select and rename — do not modify ───────────────────────────
        self.final_df = (
            with_tier
            .withColumnRenamed("full_date", "run_date")
            .select("run_date", "category", "orders", "units_sold",
                    "net_revenue", "total_margin", "margin_pct", "margin_tier")
            .orderBy(F.col("net_revenue").desc())
        )

    # ── load — do not modify ──────────────────────────────────────────────────
    def load(self, run_date: str):
        output_path = self.config.get("spark.targetfilepath") + f"/run_date={run_date}"
        self.final_df.write.mode("overwrite").parquet(output_path)

    # ── run — do not modify except Task 3 ────────────────────────────────────
    def run(self, run_date: str):
        with mlflow.start_run(run_name=f"DailyRevenueJob-{run_date}"):
            mlflow.set_tag("job",      "DailyRevenueCategoryJob")
            mlflow.set_tag("version",  "v2")
            mlflow.log_param("env",       self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            mlflow.log_param("run_date",  run_date)
            self.extract()
            self.transform(run_date)
            self.load(run_date)
            mlflow.log_metric("record_count", self.final_df.count())
            # ── TASK 3: add one more MLflow metric here ───────────────────────
            # Ideas: total net_revenue for the day, number of categories,
            #        highest margin_pct, total units_sold
            # mlflow.log_metric("...", ...)
            mlflow.log_artifact(self.config_path)

# COMMAND ----------

# Run your version for the same date as V1
job_v2 = DailyRevenueCategoryJobV2(CONFIG_PATH)
job_v2.run(sample_date)
display(job_v2.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — does your result match V1?

# COMMAND ----------

diff = (
    job_v2.final_df
    .orderBy("category")
    .exceptAll(job_v1.final_df.orderBy("category"))
)

if diff.count() == 0:
    print("✓  DataFrame API result matches SQL result exactly.")
else:
    print(f"✗  Rows differ — check your TODOs and re-run.")
    display(diff)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — MLflow as a daily run tracker
# MAGIC
# MAGIC ### Why this matters
# MAGIC
# MAGIC A daily ETL job runs 365 times a year. Individual runs will fail —
# MAGIC network timeouts, schema changes, empty partitions. Without tracking,
# MAGIC you have no way to know which days succeeded and which need to be rerun.
# MAGIC
# MAGIC MLflow gives you that audit trail for free, because every `run()` call
# MAGIC already logs `run_date`, `status`, and `record_count`.
# MAGIC
# MAGIC The cells below simulate two weeks of daily runs with deliberate failures,
# MAGIC then show how to query MLflow to find the gaps.
# MAGIC
# MAGIC > **Nothing to fill in here** — read the code and observe the output.

# COMMAND ----------

# Simulate 14 days of daily runs.
# Days marked in FAIL_DATES raise an exception mid-run (simulating a real failure).

FAIL_DATES = {"2024-06-04", "2024-06-07", "2024-06-11"}   # 3 deliberate failures

run_dates = [
    str(date(2024, 6, 1) + timedelta(days=i))
    for i in range(14)
]

print(f"Running job for {len(run_dates)} days  |  Failures injected: {sorted(FAIL_DATES)}\n")

for run_date in run_dates:
    try:
        if run_date in FAIL_DATES:
            raise RuntimeError(f"Simulated upstream failure for {run_date}")

        job = DailyRevenueCategoryJobV2(CONFIG_PATH)
        job.run(run_date)
        print(f"  ✓  {run_date}")

    except Exception as e:
        # Log the failed run to MLflow so the gap is visible in the tracker
        with mlflow.start_run(run_name=f"DailyRevenueJob-{run_date}"):
            mlflow.set_tag("job",     "DailyRevenueCategoryJob")
            mlflow.set_tag("version", "v2")
            mlflow.log_param("run_date", run_date)
            mlflow.set_tag("mlflow.note", str(e))
        print(f"  ✗  {run_date}  — FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### View the run history in MLflow
# MAGIC
# MAGIC Open the **Experiments** tab in Databricks and navigate to
# MAGIC `/RetailCo/DailyRevenueCategory`. You will see:
# MAGIC
# MAGIC - A row for each day — green for success, red for failure
# MAGIC - `run_date` param making each row identifiable at a glance
# MAGIC - `record_count` metric showing how many category rows were produced
# MAGIC - Failed runs logged with their error message
# MAGIC
# MAGIC Now let's find the gaps programmatically.

# COMMAND ----------

# Query MLflow to find which dates succeeded and which failed
runs_df = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'DailyRevenueCategoryJob'",
    output_format="pandas"
)

# Separate successful runs from failed ones
successful = set(
    runs_df[runs_df["status"] == "FINISHED"]["params.run_date"].dropna()
)
failed = set(
    runs_df[runs_df["status"] != "FINISHED"]["params.run_date"].dropna()
)

print("=" * 50)
print(f"  Successful runs : {len(successful)}")
print(f"  Failed runs     : {len(failed)}")
print(f"\n  Dates to rerun  : {sorted(failed)}")
print("=" * 50)

# COMMAND ----------

# Rerun only the failed dates
print("Rerunning failed dates...\n")

for run_date in sorted(failed):
    try:
        job = DailyRevenueCategoryJobV2(CONFIG_PATH)
        job.run(run_date)
        print(f"  ✓  {run_date}  — backfill successful")
    except Exception as e:
        print(f"  ✗  {run_date}  — still failing: {e}")

# COMMAND ----------

# Confirm no gaps remain
runs_df_after = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'DailyRevenueCategoryJob' and status = 'FINISHED'",
    output_format="pandas"
)

completed_after = set(runs_df_after["params.run_date"].dropna())
remaining_gaps  = set(run_dates) - completed_after

if not remaining_gaps:
    print("✓  All dates successfully processed — no gaps remaining.")
else:
    print(f"✗  Still missing: {sorted(remaining_gaps)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you just saw
# MAGIC
# MAGIC ```
# MAGIC Day 1   ✓   run_date=2024-06-01   record_count=5   status=FINISHED
# MAGIC Day 2   ✓   run_date=2024-06-02   record_count=5   status=FINISHED
# MAGIC Day 3   ✓   run_date=2024-06-03   record_count=5   status=FINISHED
# MAGIC Day 4   ✗   run_date=2024-06-04                    status=FAILED
# MAGIC Day 5   ✓   run_date=2024-06-05   record_count=5   status=FINISHED
# MAGIC Day 6   ✓   run_date=2024-06-06   record_count=5   status=FINISHED
# MAGIC Day 7   ✗   run_date=2024-06-07                    status=FAILED
# MAGIC ...
# MAGIC
# MAGIC → mlflow.search_runs() finds the gaps in seconds
# MAGIC → rerun loop patches only the missing dates
# MAGIC → second search confirms full coverage
# MAGIC ```
# MAGIC
# MAGIC This pattern — **log every run, query for gaps, backfill** — is the foundation
# MAGIC of any reliable data pipeline. You get it for free because `run()` already
# MAGIC wraps every execution in `mlflow.start_run()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key takeaways
# MAGIC
# MAGIC | Concept | SQL version | DataFrame API version |
# MAGIC |---|---|---|
# MAGIC | `margin_pct` formula | Written twice (column + CASE WHEN) | Computed once, reused by name |
# MAGIC | Intermediate inspection | Re-run entire query | `display(aggregated)` at any step |
# MAGIC | Column references | Strings inside SQL | Python symbols — IDE can validate |
# MAGIC | Daily parameterisation | String interpolation in SQL | `run_date` param passed cleanly |
# MAGIC | Pipeline reliability | No built-in tracking | MLflow logs every run — gaps queryable |
