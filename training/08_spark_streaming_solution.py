# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 — Solution: Spark Structured Streaming extra
# MAGIC
# MAGIC **Instructor only.** Completed version of notebook `07`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Run this cell once. It defines three helpers used below: `start_over()`,
# MAGIC `new_bookings_arrive()` and `rows_read_per_run()`.

# COMMAND ----------

import sys
from pathlib import Path

# Resolve the project root from the notebook's own workspace path.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent   # training/ → project root
sys.path.insert(0, str(REPO_ROOT))

import mlflow
from pyspark.sql import functions as F

from jobs import JobConfig, JobSettings, JobStream, SparkSessionMixin

DEMO_CONFIG      = str(REPO_ROOT / "config" / "dev" / "training_stream.yaml")
CHALLENGE_CONFIG = str(REPO_ROOT / "config" / "dev" / "training_stream_challenge.yaml")
EVENTS_TABLE     = JobConfig(DEMO_CONFIG).require("job.source_table")

# One experiment per trainee, so everyone sees only their own runs.
_user = spark.sql("SELECT current_user()").first()[0]
EXPERIMENT_NAME = f"/Users/{_user}/dreamairlines_streaming"
mlflow.set_experiment(EXPERIMENT_NAME)


def start_over(config_path):
    """Reset one job: the bookings up to 2017 have arrived and nothing is processed yet."""
    config = JobConfig(config_path)
    spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")
    spark.sql(f"DROP TABLE IF EXISTS {config.require('job.target_table')}")
    dbutils.fs.rm(config.require("job.checkpoint_path"), True)
    (spark.table("dream_airlines_dw.fact_travel_booking")
        .filter("FK_date < 20180101")
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(EVENTS_TABLE))
    print(f"{spark.table(EVENTS_TABLE).count()} bookings waiting in {EVENTS_TABLE}")


def new_bookings_arrive():
    """The 2018 bookings land in the events table."""
    new = spark.table("dream_airlines_dw.fact_travel_booking").filter("FK_date >= 20180101")
    new.write.format("delta").mode("append").saveAsTable(EVENTS_TABLE)
    print(f"{new.count()} new bookings arrived in {EVENTS_TABLE}")


def rows_read_per_run(job_name):
    """What MLflow recorded for each run of a job, oldest first."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=f"tags.job = '{job_name}' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time ASC"],
    )
    for column in ("start_time", "metrics.record_count", "metrics.duration_seconds"):
        if column not in runs:                     # no finished run yet
            runs[column] = None
    return runs[["start_time", "metrics.record_count", "metrics.duration_seconds"]].tail(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — A streaming job, in full
# MAGIC
# MAGIC **Nothing to fill in: read the class, then run the cells.** Compare it with
# MAGIC your batch job from notebook `04`. Two words changed: `readStream` and
# MAGIC `writeStream`. The transformation is ordinary DataFrame code.

# COMMAND ----------

class AgencyRevenueStreamJob(SparkSessionMixin, JobStream):
    """Running number of bookings and travel revenue per agency."""

    class Settings(JobSettings):
        source_table: str
        target_table: str
        checkpoint_path: str

    def source(self):
        # readStream instead of read: an unbounded table. New rows keep flowing in.
        self.events = self.spark.readStream.table(self.settings.source_table)

    def transform(self):
        # Exactly the DataFrame API you used in the batch job.
        self.stream_df = (
            self.events
            .groupBy(F.col("FK_TravelAgency").alias("travel_agency_id"))
            .agg(
                F.count("*").alias("num_bookings"),
                F.round(F.sum(F.coalesce(F.col("AMT_Travel"), F.lit(0))), 2).alias("travel_revenue"),
            )
        )

    def sink(self):
        return (
            self.stream_df.writeStream
            .format("delta")
            .outputMode("complete")                   # totals change, so rewrite the result
            .option("checkpointLocation", self.settings.checkpoint_path)
            .trigger(availableNow=True)               # process what is there, then stop
            .toTable(self.settings.target_table)
        )

# COMMAND ----------

start_over(DEMO_CONFIG)

AgencyRevenueStreamJob(DEMO_CONFIG).run()
display(spark.table("analytics.agency_revenue_stream").orderBy("travel_agency_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### New bookings arrive
# MAGIC
# MAGIC The 2018 bookings now land. Before you run the next cell, make a guess: how
# MAGIC many rows will the job read this time — all of them, or only the new ones?

# COMMAND ----------

new_bookings_arrive()

AgencyRevenueStreamJob(DEMO_CONFIG).run()
display(spark.table("analytics.agency_revenue_stream").orderBy("travel_agency_id"))

# COMMAND ----------

display(rows_read_per_run("AgencyRevenueStreamJob"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Three things to take away
# MAGIC
# MAGIC | Insight | What you just saw |
# MAGIC |---|---|
# MAGIC | **Streaming is not a different language.** | Same class shape, same DataFrame API. `source → transform → sink` mirrors `extract → transform → load`. |
# MAGIC | **The checkpoint is the job's memory.** | The second run read 26 rows, not 99, yet the totals cover all 99 bookings. The checkpoint stores what was already read and the running totals. |
# MAGIC | **You can see it, because every run is tracked.** | `record_count` in MLflow went from 73 to 26. No tracking code in the job: it comes from `Job.run()`, as in batch. |
# MAGIC
# MAGIC A batch job answers "process this period". A streaming job answers "process
# MAGIC whatever is new". Here the job stops when it has caught up
# MAGIC (`trigger(availableNow=True)`); the same code keeps running forever if you
# MAGIC change that one line to `trigger(processingTime="1 minute")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — Final challenge: your own streaming job
# MAGIC
# MAGIC The solution to the challenge. Design notes:
# MAGIC - **`source()`** is the same line as in the demo job.
# MAGIC - **`transform()`** is stateless: a filter and a projection, with no aggregation.
# MAGIC - **`sink()`** uses `append`. Each premium booking is a final row, so new rows
# MAGIC   are added and nothing is rewritten. `complete` would be rejected here: it is
# MAGIC   only allowed when the query aggregates.

# COMMAND ----------

class PremiumBookingsStreamJob(SparkSessionMixin, JobStream):
    """Every booking worth at least `min_amount`, appended as it arrives."""

    class Settings(JobSettings):
        source_table: str
        target_table: str
        checkpoint_path: str
        min_amount: float

    def source(self):
        self.events = self.spark.readStream.table(self.settings.source_table)

    def transform(self):
        self.stream_df = (
            self.events
            .filter(F.col("AMT_Travel") >= self.settings.min_amount)
            .select(
                F.col("PK_TravelBooking").alias("booking_id"),
                F.col("FK_TravelAgency").alias("travel_agency_id"),
                F.col("AMT_Travel").alias("travel_revenue"),
            )
        )

    def sink(self):
        return (
            self.stream_df.writeStream
            .format("delta")
            .outputMode("append")                     # each row is final: add it, never rewrite
            .option("checkpointLocation", self.settings.checkpoint_path)
            .trigger(availableNow=True)
            .toTable(self.settings.target_table)
        )

# COMMAND ----------

start_over(CHALLENGE_CONFIG)   # every attempt starts clean, so retry as often as you like

PremiumBookingsStreamJob(CHALLENGE_CONFIG).run()     # first run: the bookings up to 2017
new_bookings_arrive()
PremiumBookingsStreamJob(CHALLENGE_CONFIG).run()     # second run: only what is new

display(spark.table("analytics.premium_bookings_stream").orderBy("booking_id"))

# COMMAND ----------

expected = (
    spark.table("dream_airlines_dw.fact_travel_booking")
    .filter("AMT_Travel >= 1000")
    .selectExpr("PK_TravelBooking AS booking_id", "FK_TravelAgency AS travel_agency_id",
                "AMT_Travel AS travel_revenue")
)
runs = rows_read_per_run("PremiumBookingsStreamJob")
try:
    actual = spark.table("analytics.premium_bookings_stream")
    same = actual.exceptAll(expected).count() == 0 and expected.exceptAll(actual).count() == 0
except Exception:
    same = False

if same and list(runs["metrics.record_count"]) == [73.0, 26.0]:
    print(f"✓  Challenge complete: {expected.count()} premium bookings, no duplicates, "
          "and the second run read only the 26 new events.")
elif same:
    print("✓  The table is right.  ✗  But the two runs did not read 73 then 26 rows — run the cell above again.")
else:
    print("✗  Not yet — the table does not match the expected premium bookings. Check steps 1 to 3.")
display(runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers to the questions
# MAGIC
# MAGIC 1. The **checkpoint**. It records which source rows were already read, so the
# MAGIC    second run started after them and read only the 26 new bookings.
# MAGIC 2. A total per agency changes each time a booking arrives, so the result table
# MAGIC    must be rewritten (`complete`). A premium booking never changes once written,
# MAGIC    so it can simply be added (`append`).
# MAGIC 3. The job would forget what it had read and process every booking again. With
# MAGIC    `append`, each premium booking would be written a second time: duplicates.
# MAGIC 4. No session setup, no config parsing, no logging, no MLflow code, no error
# MAGIC    handling. That is what `Job`, `JobStream` and `SparkSessionMixin` provide.
