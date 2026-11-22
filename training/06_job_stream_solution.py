# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Solution: `JobStream` — running revenue per agency
# MAGIC
# MAGIC **Instructor only.** Completed version of notebook `05`.

# COMMAND ----------

import sys
from pathlib import Path

# Resolve the project root from the notebook's own workspace path.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent   # training/ → project root
sys.path.insert(0, str(REPO_ROOT))

import mlflow
from pyspark.sql import functions as F

from jobs import JobConfig, JobStream, SparkSessionMixin

CONFIG_PATH = str(REPO_ROOT / "config" / "dev" / "training_stream.yaml")
config = JobConfig(CONFIG_PATH)
SOURCE_TABLE    = config.require("job.source_table")
TARGET_TABLE    = config.require("job.target_table")
CHECKPOINT_PATH = config.require("job.checkpoint_path")

# One experiment per trainee, so everyone sees only their own runs.
_user = spark.sql("SELECT current_user()").first()[0]
EXPERIMENT_NAME = f"/Users/{_user}/dreamairlines_agency_revenue_stream"
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

def reset_exercise():
    """Start from a clean state: bookings up to 2017 have arrived, nothing is processed yet."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    dbutils.fs.rm(CHECKPOINT_PATH, True)
    (spark.table("dream_airlines_dw.fact_travel_booking")
        .filter("FK_date < 20180101")
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(SOURCE_TABLE))
    print(f"{SOURCE_TABLE}: {spark.table(SOURCE_TABLE).count()} booking events waiting")


reset_exercise()

# COMMAND ----------

# MAGIC %md
# MAGIC ## The job (solution)
# MAGIC
# MAGIC Design notes:
# MAGIC - **Stream-static join**: `self.events` is a stream, `self.agencies` is not. Spark
# MAGIC   joins each micro-batch against the static table. A `left` join keeps events
# MAGIC   whose agency is unknown.
# MAGIC - **`COALESCE` before `SUM`**: same data-quality rule as in the batch job.
# MAGIC - **`complete` output mode**: an aggregate changes as events arrive, so the
# MAGIC   result table is rewritten each batch. The running totals themselves live in
# MAGIC   the checkpoint's state store, which is why a rerun only reads new rows.

# COMMAND ----------

class AgencyRevenueStreamJob(SparkSessionMixin, JobStream):
    """Running number of bookings and travel revenue per agency."""

    # ── source — do not modify ────────────────────────────────────────────────
    def source(self):
        # readStream: an unbounded table. New rows appended to it keep flowing in.
        self.events = self.spark.readStream.table(self.config.require("job.source_table"))
        # A normal (static) DataFrame: joined to every micro-batch of the stream.
        self.agencies = self.spark.table("dream_airlines_dw.dim_travel_agency")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self):
        enriched = self.events.join(
            self.agencies,
            self.events["FK_TravelAgency"] == self.agencies["PK_TravelAgency"],
            "left",
        )

        self.stream_df = (
            enriched
            .groupBy("DSC_TravelAgency")
            .agg(
                F.count("*").alias("num_bookings"),
                F.round(F.sum(F.coalesce(F.col("AMT_Travel"), F.lit(0))), 2).alias("travel_revenue"),
            )
            .withColumnRenamed("DSC_TravelAgency", "travel_agency")
        )

    # ── sink — do not modify ──────────────────────────────────────────────────
    def sink(self):
        return (
            self.stream_df.writeStream
            .format("delta")
            .outputMode("complete")                  # rewrite the whole result each batch
            .option("checkpointLocation", self.config.require("job.checkpoint_path"))
            .trigger(availableNow=True)              # process what is there, then stop
            .toTable(self.config.require("job.target_table"))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## First run — the bookings up to 2017

# COMMAND ----------

AgencyRevenueStreamJob(CONFIG_PATH).run()
display(spark.table(TARGET_TABLE).orderBy("travel_agency"))

# COMMAND ----------

expected = spark.sql(f"""
    SELECT a.DSC_TravelAgency                      AS travel_agency,
           COUNT(*)                                AS num_bookings,
           ROUND(SUM(COALESCE(e.AMT_Travel, 0)), 2) AS travel_revenue
    FROM {SOURCE_TABLE} e
    LEFT JOIN dream_airlines_dw.dim_travel_agency a ON e.FK_TravelAgency = a.PK_TravelAgency
    GROUP BY a.DSC_TravelAgency
""")
actual = spark.table(TARGET_TABLE)

if actual.exceptAll(expected).count() == 0 and expected.exceptAll(actual).count() == 0:
    print("✓  The streaming result matches the batch query.")
else:
    print("✗  Results differ — check your TODOs, run reset_exercise() and try again.")
    display(expected)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second run — the 2018 bookings arrive

# COMMAND ----------

new_events = spark.table("dream_airlines_dw.fact_travel_booking").filter("FK_date >= 20180101")
new_events.write.format("delta").mode("append").saveAsTable(SOURCE_TABLE)
print(f"{new_events.count()} new booking events arrived")

AgencyRevenueStreamJob(CONFIG_PATH).run()
display(spark.table(TARGET_TABLE).orderBy("travel_agency"))

# COMMAND ----------

expected = spark.sql(f"""
    SELECT a.DSC_TravelAgency                      AS travel_agency,
           COUNT(*)                                AS num_bookings,
           ROUND(SUM(COALESCE(e.AMT_Travel, 0)), 2) AS travel_revenue
    FROM {SOURCE_TABLE} e
    LEFT JOIN dream_airlines_dw.dim_travel_agency a ON e.FK_TravelAgency = a.PK_TravelAgency
    GROUP BY a.DSC_TravelAgency
""")
actual = spark.table(TARGET_TABLE)

if actual.exceptAll(expected).count() == 0 and expected.exceptAll(actual).count() == 0:
    print("✓  The streaming result matches the batch query.")
else:
    print("✗  Results differ — check your TODOs, run reset_exercise() and try again.")
    display(expected)

# COMMAND ----------

runs = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'AgencyRevenueStreamJob'",
    order_by=["attributes.start_time ASC"],
)
display(runs[["start_time", "status", "metrics.record_count", "metrics.batch_count", "metrics.duration_seconds"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers to the questions
# MAGIC
# MAGIC 1. In the **checkpoint** (`job.checkpoint_path`): it stores the source offsets
# MAGIC    already processed and the running aggregates.
# MAGIC 2. All bookings again. The totals would still be right here, because `complete`
# MAGIC    mode rewrites the table from a fresh state — but with an `append` sink you
# MAGIC    would get duplicates.
# MAGIC 3. An appended row is final, while a running total keeps changing. Appending
# MAGIC    aggregates requires a time window and a watermark, so Spark knows when a
# MAGIC    window is complete.
# MAGIC 4. Use `trigger(processingTime="1 minute")` to keep it running;
# MAGIC    `job.stream_timeout_seconds` in the config stops it after a delay.
