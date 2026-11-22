# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — From batch to stream: `JobStream`
# MAGIC ### Running revenue per agency — DreamAirlines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC In notebook `04` the job ran once a day and reprocessed a full day of
# MAGIC bookings. Here the same framework runs a **streaming** job: bookings are
# MAGIC processed as they arrive, and only the new ones.
# MAGIC
# MAGIC ## Same shape, different words
# MAGIC
# MAGIC | | `JobBatch` (notebook 04) | `JobStream` (this notebook) |
# MAGIC |---|---|---|
# MAGIC | Read | `extract()` — `spark.table(...)` | `source()` — `spark.readStream.table(...)` |
# MAGIC | Logic | `transform()` | `transform()` — same DataFrame API |
# MAGIC | Write | `load()` — `df.write...` | `sink()` — `df.writeStream...start()` |
# MAGIC | Data | finite: the job ends by itself | unbounded: the job runs until stopped |
# MAGIC | What ran? | `record_count` in MLflow | `batch_input_rows` per micro-batch in MLflow |
# MAGIC
# MAGIC Two things are new in streaming:
# MAGIC
# MAGIC - The three methods only **describe** the pipeline. Nothing flows until
# MAGIC   `sink()` starts the query; `JobStream` then waits on it for you.
# MAGIC - A **checkpoint** remembers which rows were already processed, so the next
# MAGIC   run continues where the previous one stopped.
# MAGIC
# MAGIC ## Data flow
# MAGIC
# MAGIC ```
# MAGIC dream_airlines_dw.booking_events ──► AgencyRevenueStreamJob ──► analytics.agency_revenue_stream
# MAGIC      (bookings arriving)              source → transform → sink        travel_agency   STRING
# MAGIC                 ▲                                                      num_bookings    LONG
# MAGIC   dim_travel_agency (static) ─────────────────┘                        travel_revenue  DOUBLE
# MAGIC ```
# MAGIC
# MAGIC ## Your tasks
# MAGIC
# MAGIC | Task | What you do |
# MAGIC |---|---|
# MAGIC | 1 | Fill in the 2 TODOs in `transform()` and run the job |
# MAGIC | 2 | Let new bookings arrive and run the job again — how many rows does it process? |
# MAGIC | 3 | Compare the two runs in MLflow |
# MAGIC
# MAGIC > **Pre-requisite:** Run `00_setup_data` first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

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

# MAGIC %md
# MAGIC The stream reads from `booking_events`, a copy of the bookings that we fill in
# MAGIC two steps to simulate events arriving over time. **Run this cell again any
# MAGIC time you want to start the exercise over.**

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
# MAGIC ---
# MAGIC ## Part 1 — Write the streaming job
# MAGIC
# MAGIC Fill in the **2 TODO blocks** inside `transform()`. `self.events` is a streaming
# MAGIC DataFrame, but you transform it with the same API as in notebook `04`.
# MAGIC
# MAGIC > If you run the job before completing the TODOs, Spark refuses to start it:
# MAGIC > `complete` output mode needs an aggregation. The failed run is still recorded
# MAGIC > in MLflow, with its error.
# MAGIC >
# MAGIC > The run cell below resets the exercise first, so you can retry as often as you like.

# COMMAND ----------

class AgencyRevenueStreamJob(SparkSessionMixin, JobStream):
    """Running number of bookings and travel revenue per agency."""

    class Settings(JobSettings):
        source_table: str
        target_table: str
        checkpoint_path: str

    # ── source — do not modify ────────────────────────────────────────────────
    def source(self):
        # readStream: an unbounded table. New rows appended to it keep flowing in.
        self.events = self.spark.readStream.table(self.settings.source_table)
        # A normal (static) DataFrame: joined to every micro-batch of the stream.
        self.agencies = self.spark.table("dream_airlines_dw.dim_travel_agency")

    # ── transform ─────────────────────────────────────────────────────────────
    def transform(self):

        # ── TODO 1: Enrich each booking event with its agency name ────────────
        # - left join self.agencies on FK_TravelAgency == PK_TravelAgency
        #
        enriched = self.events   # ← replace this line

        # ── TODO 2: Aggregate per agency ──────────────────────────────────────
        # Group by: DSC_TravelAgency
        # Aggregate:
        #   num_bookings   = COUNT(*)
        #   travel_revenue = ROUND( SUM( COALESCE(AMT_Travel, 0) ), 2 )
        # Then rename DSC_TravelAgency to travel_agency.
        #
        self.stream_df = enriched   # ← replace this line

    # ── sink — do not modify ──────────────────────────────────────────────────
    def sink(self):
        return (
            self.stream_df.writeStream
            .format("delta")
            .outputMode("complete")                  # rewrite the whole result each batch
            .option("checkpointLocation", self.settings.checkpoint_path)
            .trigger(availableNow=True)              # process what is there, then stop
            .toTable(self.settings.target_table)
        )

# COMMAND ----------

reset_exercise()   # a changed transform() cannot reuse an old checkpoint: start clean on every attempt

job = AgencyRevenueStreamJob(CONFIG_PATH)
job.run()
display(spark.table(TARGET_TABLE).orderBy("travel_agency"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation — does the stream match a batch query?

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
# MAGIC ---
# MAGIC ## Part 2 — New bookings arrive
# MAGIC
# MAGIC The 2018 bookings now land in `booking_events`. Before running the job again,
# MAGIC make a guess: how many rows will it read this time?
# MAGIC
# MAGIC > **Nothing to fill in here — run the cells and observe the output.**

# COMMAND ----------

new_events = spark.table("dream_airlines_dw.fact_travel_booking").filter("FK_date >= 20180101")
new_events.write.format("delta").mode("append").saveAsTable(SOURCE_TABLE)
print(f"{new_events.count()} new booking events arrived")

# COMMAND ----------

AgencyRevenueStreamJob(CONFIG_PATH).run()
display(spark.table(TARGET_TABLE).orderBy("travel_agency"))

# COMMAND ----------

# MAGIC %md
# MAGIC Run the **Validation** cell again: the totals now cover every booking, although
# MAGIC the second run only read the new ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — What MLflow recorded
# MAGIC
# MAGIC `JobStream` reports every micro-batch to MLflow, so each run shows how much
# MAGIC data it actually processed.

# COMMAND ----------

runs = mlflow.search_runs(
    experiment_names=[EXPERIMENT_NAME],
    filter_string="tags.job = 'AgencyRevenueStreamJob'",
    order_by=["attributes.start_time ASC"],
)
display(runs[["start_time", "status", "tags.mode", "tags.engine",
              "metrics.record_count", "metrics.batch_count", "metrics.duration_seconds"]])

# COMMAND ----------

# MAGIC %md
# MAGIC Questions:
# MAGIC 1. The second run read far fewer rows than the first. Where is that knowledge stored?
# MAGIC 2. What would the second run have read if you had deleted the checkpoint folder first?
# MAGIC    What would have happened to the totals?
# MAGIC 3. The sink uses `outputMode("complete")`. Why can't an aggregation simply `append`?
# MAGIC 4. `trigger(availableNow=True)` stops when the data is exhausted. What would you
# MAGIC    change to keep the job running, and which config key stops it after a delay?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key takeaways
# MAGIC
# MAGIC | | `JobBatch` | `JobStream` |
# MAGIC |---|---|---|
# MAGIC | Steps you write | `extract` · `transform` · `load` | `source` · `transform` · `sink` |
# MAGIC | Reprocessing | Everything in scope, every run | Only what arrived since the checkpoint |
# MAGIC | State between runs | None — parameters such as `run_date` | Checkpoint: offsets and running aggregates |
# MAGIC | Tracking | Inherited from `Job.run()` | Inherited from `Job.run()`, plus per-batch metrics |
# MAGIC | Engine | `SparkSessionMixin` | `SparkSessionMixin` — or `FlinkSessionMixin` outside Databricks |
# MAGIC
# MAGIC The repository also contains the same idea on Apache Flink
# MAGIC (`jobs/examples/booking_flink_job.py`). It is not part of this training because
# MAGIC Databricks does not run Flink, but the job has the same three methods.
