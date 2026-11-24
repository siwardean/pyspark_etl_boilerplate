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
# MAGIC A working daily job already exists (`DailyBookingRevenueJobV1`). It runs each
# MAGIC day and processes all bookings made on that date. Its logic is one large SQL
# MAGIC statement.
# MAGIC
# MAGIC **Your tasks:**
# MAGIC
# MAGIC | Task | What you do |
# MAGIC |---|---|
# MAGIC | 1 | Run V1 and read the SQL in `transform()` — note where it is painful |
# MAGIC | 2 | Fill in the 4 TODOs in `DailyBookingRevenueJobV2.transform()` |
# MAGIC | 3 | Check that V2 returns the same result as V1, and look at what was tracked |
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
from datetime import date
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

from jobs import JobBatch, JobParams, JobSettings, SparkSessionMixin

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
# MAGIC | `self.settings.target_path` | a value from the `job` section of the YAML config |
# MAGIC | `self.params.run_date` | what was passed to `job.run(run_date=...)` |
# MAGIC | `self.logger.info(...)` | a log line, stored with the MLflow run |
# MAGIC | `self.log_metric("name", value)` | a metric on the MLflow run |
# MAGIC
# MAGIC `Settings` and `Params` declare what the job needs. They are checked before
# MAGIC anything runs: a missing config key or a malformed date fails immediately,
# MAGIC with a message that names the field.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The shared part of the job
# MAGIC
# MAGIC Both versions of the job read the same tables and write to the same place.
# MAGIC Only `transform()` differs, so `extract()` and `load()` live in a base class.
# MAGIC **Read it, do not modify.**

# COMMAND ----------

class DailyBookingRevenueBase(SparkSessionMixin, JobBatch):
    """Daily booking revenue by agency / destination / class. Subclasses add transform()."""

    class Settings(JobSettings):
        target_path: str

    class Params(JobParams):
        run_date: date        # job.run(run_date="2018-07-15") is parsed to a date

    def extract(self):
        self.dim_travel_booking = self.spark.table("dream_airlines_dw.dim_travel_booking")
        self.dim_customer       = self.spark.table("dream_airlines_dw.dim_customer")
        self.dim_date           = self.spark.table("dream_airlines_dw.dim_date")
        self.fact_travel        = self.spark.table("dream_airlines_dw.fact_travel_booking")
        self.fact_hotel         = self.spark.table("dream_airlines_dw.fact_hotel_booking")
        self.fact_car           = self.spark.table("dream_airlines_dw.fact_car_renting")
        self.fact_insurance     = self.spark.table("dream_airlines_dw.fact_insurance")

    def load(self):
        out = f"{self.settings.target_path}/run_date={self.params.run_date}"
        self.final_df.write.mode("overwrite").parquet(out)

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

class DailyBookingRevenueJobV1(DailyBookingRevenueBase):
    """SQL version."""

    def transform(self):
        run_date = self.params.run_date

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

# COMMAND ----------

# 2018-07-15 is the busiest day in the dataset: two bookings.
sample_date = "2018-07-15"

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
# MAGIC | `WHERE d.Date = '2018-07-15'` | `.filter(F.col("Date") == run_date)` |
# MAGIC | `COALESCE(x, 0)` | `F.coalesce(F.col("x"), F.lit(0))` |
# MAGIC | `COALESCE(x + y, 0)` | `F.coalesce(F.col("x") + F.col("y"), F.lit(0))` |
# MAGIC | `GROUP BY x, y` + `SUM(col)` | `.groupBy("x","y").agg(F.sum("col").alias("..."))` |
# MAGIC | `x + y + z` as new column | `.withColumn("total", F.col("x") + F.col("y") + F.col("z"))` |
# MAGIC | `CASE WHEN a THEN x WHEN b THEN y ELSE z` | `F.when(a, x).when(b, y).otherwise(z)` |
# MAGIC
# MAGIC > **Tip:** After each TODO, uncomment the `display()` line to inspect that step.
# MAGIC > If the job fails while you are working on it, that is expected: read the error,
# MAGIC > fix the TODO and run the cell again.

# COMMAND ----------

class DailyBookingRevenueJobV2(DailyBookingRevenueBase):
    """DataFrame API version."""

    def transform(self):
        run_date = self.params.run_date

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

# COMMAND ----------

job_v2 = DailyBookingRevenueJobV2(CONFIG_PATH)
job_v2.run(run_date=sample_date)
display(job_v2.final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — Validation: does your result match V1?

# COMMAND ----------

if job_v2.final_df is None:
    print("✗  V2 has not run successfully yet — complete the TODOs and run the cell above again.")
else:
    v1_rows, v2_rows = job_v1.final_df.count(), job_v2.final_df.count()
    only_in_v2 = job_v2.final_df.exceptAll(job_v1.final_df)
    only_in_v1 = job_v1.final_df.exceptAll(job_v2.final_df)

    if v1_rows > 0 and only_in_v2.count() == 0 and only_in_v1.count() == 0:
        print(f"✓  DataFrame API result matches SQL result exactly ({v2_rows} rows).")
    else:
        print(f"✗  Results differ (V1: {v1_rows} rows, V2: {v2_rows} rows) — check your TODOs and re-run.")
        display(only_in_v2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What was tracked while you worked
# MAGIC
# MAGIC You wrote no tracking code, yet every run of both jobs was recorded, including
# MAGIC the failed attempts. Run the cell below, then open the **Experiments** page in
# MAGIC the left sidebar to see the same runs in the MLflow UI. Notebook `05` builds on this.

# COMMAND ----------

run = mlflow.get_run(job_v2.run_id)
print("params :", run.data.params)
print("metrics:", {name: round(value, 2) for name, value in run.data.metrics.items()})
print("tags   :", {name: value for name, value in run.data.tags.items() if not name.startswith("mlflow.")})

display(mlflow.search_runs(experiment_names=[EXPERIMENT_NAME])[["start_time", "status", "params.run_date"]])

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
# MAGIC | Daily parameterisation | String interpolation in SQL | `run_date` used as a typed Python value |
# MAGIC | Tracking | Inherited from `Job.run()` | Inherited from `Job.run()` |
