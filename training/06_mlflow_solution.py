# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Solution: MLflow exercise
# MAGIC
# MAGIC **Instructor only.** Completed version of notebook `05`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import random
import sys
import time
from pathlib import Path

# Resolve the project root from the notebook's own workspace path.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent   # training/ → project root
sys.path.insert(0, str(REPO_ROOT))

import mlflow
from pydantic import Field
from pyspark.sql import functions as F

from jobs import JobBatch, JobConfig, JobParams, JobSettings, SparkSessionMixin

CONFIG_PATH  = str(REPO_ROOT / "config" / "dev" / "training_mlflow.yaml")
TARGET_TABLE = JobConfig(CONFIG_PATH).require("job.target_table")

# One experiment per trainee, so everyone sees only their own runs.
_user = spark.sql("SELECT current_user()").first()[0]
EXPERIMENT_NAME = f"/Users/{_user}/dreamairlines_monthly_agency_revenue"
mlflow.set_experiment(EXPERIMENT_NAME)

spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — Instrument the job
# MAGIC
# MAGIC TODO 1 adds two lines at the end of `load()`. A metric logged inside a step is
# MAGIC attached to the run that `Job.run()` opened; the job never imports `mlflow`.

# COMMAND ----------

class MonthlyAgencyRevenueJob(SparkSessionMixin, JobBatch):
    """Travel revenue per agency for one month."""

    class Settings(JobSettings):
        target_table: str

    class Params(JobParams):
        month: str = Field(pattern=r"^\d{4}-\d{2}$")     # e.g. "2017-06"

    def extract(self):
        self.bookings = self.spark.table("dream_airlines_dw.fact_travel_booking")
        self.agencies = self.spark.table("dream_airlines_dw.dim_travel_agency")

    def transform(self):
        month_key = int(self.params.month.replace("-", ""))        # "2017-06" → 201706
        self.final_df = (
            self.bookings
            .filter(F.floor(F.col("FK_date") / 100) == month_key)  # FK_date is YYYYMMDD
            .join(self.agencies, F.col("FK_TravelAgency") == F.col("PK_TravelAgency"), "left")
            .groupBy(F.col("DSC_TravelAgency").alias("travel_agency"))
            .agg(
                F.count("*").alias("num_bookings"),
                F.round(F.sum(F.coalesce(F.col("AMT_Travel"), F.lit(0))), 2).alias("travel_revenue"),
            )
            .withColumn("month", F.lit(self.params.month))
        )

    def load(self):
        (self.final_df.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"month = '{self.params.month}'")   # replace this month only
            .saveAsTable(self.settings.target_table))

        # ── TODO 1 (solution) ─────────────────────────────────────────────────
        total = self.final_df.agg(F.sum("travel_revenue")).first()[0] or 0
        self.log_metric("total_revenue", total)

# COMMAND ----------

job = MonthlyAgencyRevenueJob(CONFIG_PATH)
job.run(month="2016-12")
display(job.final_df)

metrics = mlflow.get_run(job.run_id).data.metrics
print({name: round(value, 2) for name, value in metrics.items()})
if "total_revenue" in metrics:
    print(f"✓  total_revenue was logged: {metrics['total_revenue']}")
else:
    print("✗  No total_revenue metric on this run — complete TODO 1 and run both cells again.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — A year in production
# MAGIC
# MAGIC The job now runs once a month for 2017. Upstream is not always reliable.
# MAGIC
# MAGIC > **Nothing to fill in here — run the cell and read its output.**

# COMMAND ----------

MONTHS = [f"2017-{m:02d}" for m in range(1, 13)]

# Three months where the source "did not arrive", and one where it arrived empty.
# They are drawn for you: finding them is the exercise.
_rng = random.Random(2017)
_FAILED = set(_rng.sample(MONTHS, 3))
_EMPTY  = _rng.choice(sorted(set(MONTHS) - _FAILED))


class UnreliableSourceJob(MonthlyAgencyRevenueJob):
    """Same job, with the upstream problems of a real year."""

    def extract(self):
        super().extract()
        if self.params.month in _FAILED:
            raise RuntimeError(f"Upstream export for {self.params.month} has not arrived")
        if self.params.month == _EMPTY:
            self.bookings = self.bookings.limit(0)      # the file arrived, but empty


spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
SIMULATION_START = int(time.time() * 1000)             # only look at runs from now on

for month in MONTHS:
    try:
        UnreliableSourceJob(CONFIG_PATH).run(month=month)
        print(f"  ran     {month}")
    except Exception:
        print(f"  ERROR   {month}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — Investigate with MLflow
# MAGIC
# MAGIC Everything you need to know about that year is in MLflow. `mlflow.search_runs`
# MAGIC returns one row per run, with `params.*`, `metrics.*` and `tags.*` columns.

# COMMAND ----------

def load_runs():
    """This year's runs of the job, newest first, as a pandas DataFrame."""
    runs = mlflow.search_runs(
        experiment_names=[EXPERIMENT_NAME],
        filter_string=(
            "tags.job = 'MonthlyAgencyRevenueJob' "
            f"and attributes.start_time >= {SIMULATION_START}"
        ),
        order_by=["attributes.start_time DESC"],
    )
    for column in ("metrics.total_revenue", "tags.error"):   # absent until a run logs them
        if column not in runs:
            runs[column] = None
    return runs


runs = load_runs()
display(runs[["params.month", "status", "metrics.record_count", "metrics.total_revenue", "tags.error"]]
        .sort_values("params.month"))

# COMMAND ----------

# ── TODO 2 (solution) ────────────────────────────────────────────────────────
failed_months = set(runs[runs["status"] == "FAILED"]["params.month"])

print("Failed months:", sorted(failed_months))
print("✓  Correct." if failed_months == _FAILED else "✗  Not yet — three months failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why did they fail?
# MAGIC
# MAGIC The error is in a tag, and the full log is stored with the run. No need to
# MAGIC search the cluster logs.

# COMMAND ----------

failed_run = runs[runs["status"] == "FAILED"].iloc[0]
print("month:", failed_run["params.month"])
print("error:", failed_run["tags.error"])

# ── TODO 3 (solution) ────────────────────────────────────────────────────────
log_text = mlflow.artifacts.load_text(f"runs:/{failed_run['run_id']}/logs/run.log")

print(log_text)
print("✓  You found the traceback." if "Traceback" in log_text else "✗  Not yet — log_text is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The run that did not fail
# MAGIC
# MAGIC A `FINISHED` status only says the code did not crash. One month ran
# MAGIC "successfully" on an empty source. The status cannot tell you — the metrics can.

# COMMAND ----------

# ── TODO 4 (solution) ────────────────────────────────────────────────────────
finished = runs[runs["status"] == "FINISHED"]
empty_months = set(finished[finished["metrics.record_count"] == 0]["params.month"])

print("Empty months:", sorted(empty_months))
print("✓  Correct." if empty_months == {_EMPTY} else "✗  Not yet — exactly one month is empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4 — Backfill

# COMMAND ----------

months_to_rerun = sorted(failed_months | empty_months)
print("Rerunning:", months_to_rerun)

# ── TODO 5 (solution) ────────────────────────────────────────────────────────
for month in months_to_rerun:
    MonthlyAgencyRevenueJob(CONFIG_PATH).run(month=month)

# COMMAND ----------

runs = load_runs()
latest = runs.drop_duplicates("params.month")            # newest run of each month
complete = latest[(latest["status"] == "FINISHED") & (latest["metrics.record_count"] > 0)]
missing = sorted(set(MONTHS) - set(complete["params.month"]))

if not missing:
    print("✓  All 12 months are complete.")
else:
    print(f"✗  Still incomplete: {missing}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The year, as MLflow saw it
# MAGIC
# MAGIC The metric you added in TODO 1 is now a monthly revenue series, built without
# MAGIC querying the data itself. Use the chart button under the table to plot
# MAGIC `total_revenue` by `month`.

# COMMAND ----------

report = (
    complete[["params.month", "metrics.total_revenue", "metrics.record_count", "metrics.duration_seconds"]]
    .rename(columns=lambda name: name.split(".")[-1])
    .sort_values("month")
)
display(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answers to the questions
# MAGIC
# MAGIC 1. The failed months would surface when the scheduler alerts, or when an analyst
# MAGIC    finds a hole. The empty month raises no error anywhere: without a
# MAGIC    `record_count` metric it is found when someone questions the numbers.
# MAGIC 2. `replaceWhere month = '...'` with `mode("overwrite")`: the job replaces its
# MAGIC    own month, so it can be rerun safely (it is idempotent).
# MAGIC 3. `record_count == 0` is a clear alert. For `total_revenue`, compare with the
# MAGIC    previous months, for example a drop of more than 50%.
# MAGIC 4. It is the audit trail: when the problem happened, why, and when it was fixed.
