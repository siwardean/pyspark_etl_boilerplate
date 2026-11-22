# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — ETL Job: DataFrame API vs Spark SQL
# MAGIC ### Daily Booking Revenue — DreamAirlines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Model
# MAGIC
# MAGIC ### Source — Raw Layer (`dream_airlines_dw` schema)
# MAGIC
# MAGIC ```
# MAGIC dream_airlines_dw.dim_travel_booking                    dream_airlines_dw.dim_customer
# MAGIC ──────────────────────────────────────    ─────────────────────────────
# MAGIC PK_TravelBooking  INT     PK              PK_Customer    INT     PK
# MAGIC DSC_TravelAgency  STRING  → dim_travel_agency  DSC_Customer  STRING  (join key)
# MAGIC DSC_Employee      STRING                  DSC_CustomerType STRING
# MAGIC DSC_Customer      STRING  → dim_customer  DSC_Nationality  STRING
# MAGIC DSC_DestinyCountry STRING                 DSC_City         STRING
# MAGIC DSC_DestinyCity   STRING
# MAGIC DSC_TravelClass   STRING  (Economy, Business, First)
# MAGIC DSC_TravelStatus  STRING  (Confirmed, Cancelled, ...)
# MAGIC Booking_Date      INT     YYYYMMDD → join dim_date.PK_Date
# MAGIC Travel_StartDate  INT     YYYYMMDD
# MAGIC Travel_EndDate    INT     YYYYMMDD
# MAGIC
# MAGIC dream_airlines_dw.dim_date                              dream_airlines_dw.dim_travel_agency
# MAGIC ──────────────────────────────────────    ─────────────────────────────
# MAGIC PK_Date   INT  PK  (YYYYMMDD)             PK_TravelAgency  INT    PK
# MAGIC Date      DATE                            DSC_TravelAgency STRING
# MAGIC Year      INT                             DSC_City         STRING
# MAGIC Month     INT
# MAGIC Quarter   INT
# MAGIC IsWeekend BOOLEAN
# MAGIC
# MAGIC dream_airlines_dw.fact_travel_booking    dream_airlines_dw.fact_hotel_booking     dream_airlines_dw.fact_car_renting       dream_airlines_dw.fact_insurance
# MAGIC ─────────────────────────  ─────────────────────────  ─────────────────────────  ─────────────────────────
# MAGIC PK_TravelBooking INT  PK   PK_HotelBooking  INT  PK   PK_CarRenting    INT  PK   PK_Insurance  INT  PK
# MAGIC FK_date          INT  FK   FK_TravelBooking INT  FK   FK_TravelBooking INT  FK   FK_TravelBooking INT FK
# MAGIC FK_TravelAgency  INT  FK   FK_TravelAgency  INT  FK   FK_TravelAgency  INT  FK   FK_TravelAgency INT FK
# MAGIC FK_Customer      INT  FK   FK_Customer      INT  FK   FK_Customer      INT  FK   FK_Customer     INT FK
# MAGIC AMT_Travel       DBL       AMT_Accomodation DBL       AMT_Rental       DBL       AMT_Travel_Insurance DBL
# MAGIC                                                        AMT_DailyRate    DBL       AMT_Car_Insurance    DBL
# MAGIC                                                        AMT_CarInsurance DBL
# MAGIC ```
# MAGIC
# MAGIC ### Output — This Job (`analytics.daily_booking_revenue`)
# MAGIC
# MAGIC ```
# MAGIC analytics.daily_booking_revenue
# MAGIC ──────────────────────────────────────────────────────────────
# MAGIC booking_date        DATE     the date being processed
# MAGIC travel_agency       STRING   agency that made the booking
# MAGIC destination         STRING   destination country
# MAGIC travel_class        STRING   Economy / Business / First
# MAGIC customer_type       STRING   Individual / Company
# MAGIC num_bookings        LONG     distinct bookings on this date
# MAGIC travel_revenue      DOUBLE   sum of AMT_Travel
# MAGIC hotel_revenue       DOUBLE   sum of AMT_Accomodation
# MAGIC car_revenue         DOUBLE   sum of AMT_Rental
# MAGIC insurance_revenue   DOUBLE   sum of AMT_Travel_Insurance + AMT_Car_Insurance
# MAGIC total_revenue       DOUBLE   sum of all four streams
# MAGIC revenue_tier        STRING   High (≥2000) / Medium (≥500) / Low
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Your mission
# MAGIC
# MAGIC A working daily ETL job already exists (`DailyBookingRevenueJobV1`). It runs
# MAGIC each day and processes all bookings made on that date.
# MAGIC
# MAGIC **Your tasks:**
# MAGIC
# MAGIC | Task | What you do |
# MAGIC |---|---|
# MAGIC | 1 | Run V1 and read the SQL in `transform()` — note where it is painful |
# MAGIC | 2 | Fill in the 4 TODOs in `DailyBookingRevenueJobV2.transform()` |
# MAGIC | 3 | Add one MLflow metric of your choice with `self.log_metric()` |
# MAGIC | 4 | Observe the MLflow tracker: see gaps, run backfill, confirm coverage |
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
from datetime import date, timedelta
from pathlib import Path

# Resolve the project root from the notebook's own workspace path.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent   # training/ → project root
sys.path.insert(0, str(REPO_ROOT))

CONFIG_PATH = str(REPO_ROOT / "config" / "dev" / "training_daily_revenue.yaml")
print(open(CONFIG_PATH).read())

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F

from jobs import JobBatch, SparkSessionMixin

# One experiment per trainee, so everyone sees only their own runs.
_user = spark.sql("SELECT current_user()").first()[0]
EXPERIMENT_NAME = f"/Users/{_user}/dreamairlines_daily_booking_revenue"
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How a job is built
# MAGIC
# MAGIC ```
# MAGIC Job                     config · logging · MLflow run          (jobs/job.py)
# MAGIC ├── JobBatch            extract() → transform() → load()       (jobs/job_batch.py)
# MAGIC └── JobStream           source()  → transform() → sink()       (jobs/job_stream.py)
# MAGIC
# MAGIC SparkSessionMixin       adds self.spark                        (jobs/sessions/spark.py)
# MAGIC ```
# MAGIC
# MAGIC A job only writes its three steps. `run()` is inherited: it opens the MLflow
# MAGIC run, logs the parameters and the config file, times each step, captures the
# MAGIC log output and marks the run as failed if a step raises.
# MAGIC
# MAGIC | Inside a step | What it gives you |
# MAGIC |---|---|
# MAGIC | `self.spark` | the Spark session (the Databricks one is reused) |
# MAGIC | `self.config.get("job.target_path")` | a value from the YAML config |
# MAGIC | `self.params["run_date"]` | what was passed to `job.run(run_date=...)` |
# MAGIC | `self.logger.info(...)` | a log line, stored with the MLflow run |
# MAGIC | `self.log_metric("name", value)` | a metric on the MLflow run |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The job as it exists today (SQL version)
# MAGIC
# MAGIC Read `transform()` carefully. Notice:
# MAGIC - The `total_revenue` formula is repeated **twice** — once in `booking_revenue`
# MAGIC   CTE and again inside `CASE WHEN` — because SQL can't reuse an alias in the
# MAGIC   same SELECT. The DataFrame API computes it once as a named column.
# MAGIC - There is no way to inspect the join result before the aggregation runs.
# MAGIC - There is no `run()` method and no MLflow code: both come from `Job`.

# COMMAND ----------

class DailyBookingRevenueJobV1(SparkSessionMixin, JobBatch):
    """Daily ETL — total booking revenue by agency/destination/class. SQL version."""

    def extract(self):
        self.dim_travel_booking = self.spark.table("dream_airlines_dw.dim_travel_booking")
        self.dim_customer       = self.spark.table("dream_airlines_dw.dim_customer")
        self.dim_date           = self.spark.table("dream_airlines_dw.dim_date")
        self.fact_travel        = self.spark.table("dream_airlines_dw.fact_travel_booking")
        self.fact_hotel         = self.spark.table("dream_airlines_dw.fact_hotel_booking")
        self.fact_car           = self.spark.table("dream_airlines_dw.fact_car_renting")
        self.fact_insurance     = self.spark.table("dream_airlines_dw.fact_insurance")

        for name, df in [
            ("dim_travel_booking", self.dim_travel_booking),
            ("dim_customer",       self.dim_customer),
            ("dim_date",           self.dim_date),
            ("fact_travel",        self.fact_travel),
            ("fact_hotel",         self.fact_hotel),
            ("fact_car",           self.fact_car),
            ("fact_insurance",     self.fact_insurance),
        ]:
            df.createOrReplaceTempView(name)

    def transform(self):
        run_date = self.params["run_date"]
        self.final_df = self.spark.sql(f"""
            WITH booking_revenue AS (
                SELECT
                    d.Date                                          AS booking_date,
                    dtb.DSC_TravelAgency                           AS travel_agency,
                    dtb.DSC_DestinyCountry                         AS destination,
                    dtb.DSC_TravelClass                            AS travel_class,
                    c.DSC_CustomerType                             AS customer_type,
                    dtb.PK_TravelBooking,
                    COALESCE(ft.AMT_Travel,             0)         AS travel_amt,
                    COALESCE(fh.AMT_Accomodation,       0)         AS hotel_amt,
                    COALESCE(fc.AMT_Rental,             0)         AS car_amt,
                    COALESCE(fi.AMT_Travel_Insurance
                           + fi.AMT_Car_Insurance,      0)         AS insurance_amt
                FROM dim_travel_booking dtb
                JOIN dim_date      d   ON  d.PK_Date        = dtb.Booking_Date
                JOIN dim_customer  c   ON  c.DSC_Customer   = dtb.DSC_Customer
                LEFT JOIN fact_travel   ft  ON ft.PK_TravelBooking = dtb.PK_TravelBooking
                LEFT JOIN fact_hotel    fh  ON fh.FK_TravelBooking = dtb.PK_TravelBooking
                LEFT JOIN fact_car      fc  ON fc.FK_TravelBooking = dtb.PK_TravelBooking
                LEFT JOIN fact_insurance fi ON fi.FK_TravelBooking = dtb.PK_TravelBooking
                WHERE d.Date = '{run_date}'
            ),
            aggregated AS (
                SELECT
                    booking_date, travel_agency, destination, travel_class, customer_type,
                    COUNT(DISTINCT PK_TravelBooking)  AS num_bookings,
                    ROUND(SUM(travel_amt),    2)       AS travel_revenue,
                    ROUND(SUM(hotel_amt),     2)       AS hotel_revenue,
                    ROUND(SUM(car_amt),       2)       AS car_revenue,
                    ROUND(SUM(insurance_amt), 2)       AS insurance_revenue,
                    ROUND(SUM(travel_amt + hotel_amt
                              + car_amt + insurance_amt), 2) AS total_revenue
                FROM booking_revenue
                GROUP BY booking_date, travel_agency, destination, travel_class, customer_type
            )
            SELECT *,
                   CASE
                       WHEN total_revenue >= 2000 THEN 'High'
                       WHEN total_revenue >= 500  THEN 'Medium'
                       ELSE                            'Low'
                   END AS revenue_tier
            FROM aggregated
            ORDER BY total_revenue DESC
        """)

    def load(self):
        out = self.config.require("job.target_path") + f"/run_date={self.params['run_date']}"
        self.final_df.write.mode("overwrite").parquet(out)

# COMMAND ----------

sample_date = "2016-03-15"
job_v1 = DailyBookingRevenueJobV1(CONFIG_PATH)
job_v1.run(run_date=sample_date)
display(job_v1.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Questions before moving on:
# MAGIC 1. The SQL computes `total_revenue` twice — where exactly? Why can't SQL avoid this?
# MAGIC 2. How would you inspect the join result after the LEFT JOINs but before the GROUP BY?
# MAGIC 3. What does `COALESCE(fi.AMT_Travel_Insurance + fi.AMT_Car_Insurance, 0)` guard against?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — Your turn: rewrite `transform()` with the DataFrame API
# MAGIC
# MAGIC Fill in the **4 TODO blocks** inside `transform()`. Everything else is done.
# MAGIC
# MAGIC ### Quick reference
# MAGIC
# MAGIC | SQL construct | DataFrame API equivalent |
# MAGIC |---|---|
# MAGIC | `FROM a JOIN b ON a.x = b.x` | `a.join(b, a["x"] == b["x"], "inner")` |
# MAGIC | `LEFT JOIN b ON a.x = b.x` | `a.join(b, a["x"] == b["x"], "left")` |
# MAGIC | `WHERE d.Date = '2016-03-15'` | `.filter(F.col("Date") == run_date)` |
# MAGIC | `COALESCE(x, 0)` | `F.coalesce(F.col("x"), F.lit(0))` |
# MAGIC | `COALESCE(x + y, 0)` | `F.coalesce(F.col("x") + F.col("y"), F.lit(0))` |
# MAGIC | `GROUP BY x, y` + `SUM(col)` | `.groupBy("x","y").agg(F.sum("col").alias("..."))` |
# MAGIC | `x + y + z` as new column | `.withColumn("total", F.col("x") + F.col("y") + F.col("z"))` |
# MAGIC | `CASE WHEN a THEN x WHEN b THEN y ELSE z` | `F.when(a, x).when(b, y).otherwise(z)` |
# MAGIC
# MAGIC > **Tip:** After each TODO, uncomment the `display()` line to inspect that step.

# COMMAND ----------

class DailyBookingRevenueJobV2(SparkSessionMixin, JobBatch):
    """Daily ETL — total booking revenue by agency/destination/class. DataFrame API version."""

    # ── extract — do not modify ───────────────────────────────────────────────
    def extract(self):
        self.dim_travel_booking = self.spark.table("dream_airlines_dw.dim_travel_booking")
        self.dim_customer       = self.spark.table("dream_airlines_dw.dim_customer")
        self.dim_date           = self.spark.table("dream_airlines_dw.dim_date")
        self.fact_travel        = self.spark.table("dream_airlines_dw.fact_travel_booking")
        self.fact_hotel         = self.spark.table("dream_airlines_dw.fact_hotel_booking")
        self.fact_car           = self.spark.table("dream_airlines_dw.fact_car_renting")
        self.fact_insurance     = self.spark.table("dream_airlines_dw.fact_insurance")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self):
        run_date = self.params["run_date"]

        # ── TODO 1: Join dim_travel_booking with dim_date and dim_customer ────
        # - inner join dim_date    on Booking_Date == PK_Date
        # - filter to Date == run_date   ← do this right after the dim_date join
        # - inner join dim_customer on DSC_Customer == DSC_Customer
        #
        joined_dims = self.dim_travel_booking   # ← replace this line
        # display(joined_dims.select("PK_TravelBooking", "Date", "DSC_TravelAgency",
        #                            "DSC_CustomerType").limit(5))

        # ── TODO 2: Left-join all four fact tables and coalesce amounts ────────
        # - left join fact_travel   on PK_TravelBooking == PK_TravelBooking
        # - left join fact_hotel    on PK_TravelBooking == FK_TravelBooking
        # - left join fact_car      on PK_TravelBooking == FK_TravelBooking
        # - left join fact_insurance on PK_TravelBooking == FK_TravelBooking
        # Then add four columns using COALESCE:
        #   travel_amt    = COALESCE( AMT_Travel,            0 )
        #   hotel_amt     = COALESCE( AMT_Accomodation,      0 )
        #   car_amt       = COALESCE( AMT_Rental,            0 )
        #   insurance_amt = COALESCE( AMT_Travel_Insurance + AMT_Car_Insurance, 0 )
        #
        enriched = joined_dims   # ← replace this line
        # display(enriched.select("PK_TravelBooking", "travel_amt", "hotel_amt",
        #                         "car_amt", "insurance_amt").limit(5))

        # ── TODO 3: Aggregate by booking dimensions ───────────────────────────
        # Group by: Date, DSC_TravelAgency, DSC_DestinyCountry,
        #           DSC_TravelClass, DSC_CustomerType
        # Aggregate:
        #   num_bookings      = COUNT DISTINCT PK_TravelBooking
        #   travel_revenue    = ROUND( SUM(travel_amt),    2 )
        #   hotel_revenue     = ROUND( SUM(hotel_amt),     2 )
        #   car_revenue       = ROUND( SUM(car_amt),       2 )
        #   insurance_revenue = ROUND( SUM(insurance_amt), 2 )
        #
        aggregated = enriched   # ← replace this line
        # display(aggregated.limit(5))

        # ── TODO 4: Add total_revenue and revenue_tier ────────────────────────
        # total_revenue = ROUND( travel + hotel + car + insurance, 2 )
        # revenue_tier  = "High" if >= 2000, "Medium" if >= 500, else "Low"
        #
        with_tier = aggregated   # ← replace this line

        # ── Final select and rename — do not modify ───────────────────────────
        self.final_df = (
            with_tier
            .withColumnRenamed("Date",               "booking_date")
            .withColumnRenamed("DSC_TravelAgency",   "travel_agency")
            .withColumnRenamed("DSC_DestinyCountry", "destination")
            .withColumnRenamed("DSC_TravelClass",    "travel_class")
            .withColumnRenamed("DSC_CustomerType",   "customer_type")
            .select("booking_date", "travel_agency", "destination", "travel_class",
                    "customer_type", "num_bookings", "travel_revenue", "hotel_revenue",
                    "car_revenue", "insurance_revenue", "total_revenue", "revenue_tier")
            .orderBy(F.col("total_revenue").desc())
        )

    # ── load — do not modify except Task 3 ────────────────────────────────────
    def load(self):
        out = self.config.require("job.target_path") + f"/run_date={self.params['run_date']}"
        self.final_df.write.mode("overwrite").parquet(out)
        # ── TASK 3: add one more MLflow metric ────────────────────────────────
        # record_count and the step durations are already logged for you.
        # Ideas: total_revenue for the day, num_bookings, number of "High" tier rows
        # self.log_metric("...", ...)

# COMMAND ----------

job_v2 = DailyBookingRevenueJobV2(CONFIG_PATH)
job_v2.run(run_date=sample_date)
display(job_v2.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — does your result match V1?

# COMMAND ----------

diff = (
    job_v2.final_df.orderBy("travel_agency", "destination", "travel_class")
    .exceptAll(job_v1.final_df.orderBy("travel_agency", "destination", "travel_class"))
)

if diff.count() == 0:
    print("✓  DataFrame API result matches SQL result exactly.")
else:
    print(f"✗  {diff.count()} rows differ — check your TODOs and re-run.")
    display(diff)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — MLflow as a daily run tracker
# MAGIC
# MAGIC ### Why this matters
# MAGIC
# MAGIC This job runs every day. Some runs will fail — the source file didn't arrive,
# MAGIC the cluster timed out, an upstream schema changed. Without tracking, you discover
# MAGIC the gap days later when an analyst notices missing data in their dashboard.
# MAGIC
# MAGIC Because `Job.run()` opens an MLflow run for every execution, logs `run_date`
# MAGIC as a parameter and marks the run `FAILED` when a step raises, you can query
# MAGIC MLflow at any time to see exactly which dates succeeded and which need to be
# MAGIC rerun. The job itself contains no tracking code.
# MAGIC
# MAGIC > **Nothing to fill in here — read the code and observe the output.**

# COMMAND ----------

# The DreamAirlines dataset covers bookings from Oct 2015 to Sep 2018.
# We simulate two weeks of daily runs in early 2016, with 3 deliberate failures.

FAIL_DATES = {"2016-01-12", "2016-01-15", "2016-01-19"}


class FlakyUpstreamJob(DailyBookingRevenueJobV2):
    """Same job, but the source "did not arrive" on the failure dates."""

    def extract(self):
        if self.params["run_date"] in FAIL_DATES:
            raise RuntimeError(f"Simulated upstream failure for {self.params['run_date']}")
        super().extract()


run_dates = [str(date(2016, 1, 10) + timedelta(days=i)) for i in range(14)]

print(f"Running job for: {run_dates[0]} → {run_dates[-1]}")
print(f"Failures injected on: {sorted(FAIL_DATES)}\n")

for run_date in run_dates:
    try:
        FlakyUpstreamJob(CONFIG_PATH).run(run_date=run_date)
        print(f"  ✓  {run_date}")
    except Exception as e:
        # Nothing to record here: Job.run() already marked the MLflow run as FAILED.
        print(f"  ✗  {run_date}  — FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Find the gaps

# COMMAND ----------

runs_df = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'DailyBookingRevenueJob'",
    output_format="pandas"
)

successful = set(runs_df[runs_df["status"] == "FINISHED"]["params.run_date"].dropna())
failed     = set(runs_df[runs_df["status"] != "FINISHED"]["params.run_date"].dropna()) - successful

print("=" * 50)
print(f"  Successful : {len(successful)}  dates")
print(f"  Failed     : {len(failed)}  dates")
print(f"\n  Dates to rerun: {sorted(failed)}")
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why did a run fail?
# MAGIC
# MAGIC Each run keeps its error in a tag and its full log as an artifact, so the
# MAGIC cause is one query away. No need to dig through cluster logs.

# COMMAND ----------

failed_runs = runs_df[runs_df["status"] == "FAILED"].sort_values("params.run_date")
display(failed_runs[["params.run_date", "status", "tags.error"]])

print(mlflow.artifacts.load_text(f"runs:/{failed_runs.iloc[0]['run_id']}/logs/run.log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Backfill the missing dates

# COMMAND ----------

print("Backfilling failed dates...\n")
for run_date in sorted(failed):
    try:
        DailyBookingRevenueJobV2(CONFIG_PATH).run(run_date=run_date)
        print(f"  ✓  {run_date}  — backfill successful")
    except Exception as e:
        print(f"  ✗  {run_date}  — still failing: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confirm no gaps remain

# COMMAND ----------

runs_after = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'DailyBookingRevenueJob' and status = 'FINISHED'",
    output_format="pandas"
)

completed = set(runs_after["params.run_date"].dropna())
gaps      = set(run_dates) - completed

if not gaps:
    print("✓  All dates processed — no gaps remaining.")
else:
    print(f"✗  Still missing: {sorted(gaps)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key takeaways
# MAGIC
# MAGIC | | SQL version | DataFrame API version |
# MAGIC |---|---|---|
# MAGIC | `total_revenue` formula | Written twice (GROUP BY + CASE WHEN) | Computed once, reused by column name |
# MAGIC | Intermediate inspection | Impossible without restructuring query | `display(enriched.limit(5))` after any step |
# MAGIC | Multiple LEFT JOINs | All in one opaque SQL block | Each join is a named, readable line |
# MAGIC | `COALESCE` for nulls | Inside SELECT list | `.withColumn()` — explicit and testable |
# MAGIC | Daily parameterisation | String interpolation in SQL | `run_date` read from `self.params` |
# MAGIC | Pipeline reliability | MLflow logs every run — gaps queryable | Same: tracking comes from `Job.run()`, not from the job |
