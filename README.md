# PySpark ETL Boilerplate

A small framework for data jobs: batch or streaming, on Spark or Flink, with
YAML configuration, logging and MLflow tracking built in.

A job only contains its business steps. Everything around them (the MLflow
run, parameters, timings, the log file, failure status) is inherited.

---

## Architecture

```
Job                     config · logging · MLflow run            jobs/job.py
├── JobBatch            extract() → transform() → load()         jobs/job_batch.py
└── JobStream           source()  → transform() → sink()         jobs/job_stream.py

SparkSessionMixin       adds self.spark                          jobs/sessions/spark.py
FlinkSessionMixin       adds self.table_env                      jobs/sessions/flink.py
```

The **mode** (batch or stream) is chosen by inheritance, the **engine** by a
mixin. `Job` itself has no engine, so a job that needs neither Spark nor Flink
simply leaves the mixin out:

```python
class DailyRevenueJob(SparkSessionMixin, JobBatch): ...    # Spark batch
class BookingStreamJob(SparkSessionMixin, JobStream): ...  # Spark Structured Streaming
class BookingFlinkJob(FlinkSessionMixin, JobStream): ...   # Flink streaming
class ApiExportJob(JobBatch): ...                          # plain Python
```

Sessions are lazy: nothing is imported or started until `self.spark` (or
`self.table_env`) is first used. `SparkSessionMixin` reuses the active session
when there is one (notebooks, Databricks) and otherwise builds one from the
`spark` section of the config.

---

## Project structure

```
pyspark_etl_boilerplate/
├── main.py                     # Entry point: runs any job by its dotted path
├── requirements.txt            # Core dependencies
├── requirements-flink.txt      # Only for jobs using FlinkSessionMixin
├── config/
│   ├── dev/                    # example.yaml, example_stream.yaml, example_flink.yaml, training_*.yaml
│   ├── test/                   # example.yaml
│   ├── qa/
│   └── prod/
├── jobs/
│   ├── job.py                  # Job: run() template, MLflow, logging
│   ├── job_batch.py            # JobBatch
│   ├── job_stream.py           # JobStream
│   ├── config.py               # JobConfig: YAML loader with {placeholder} resolution
│   ├── tracking.py             # The only module that talks to MLflow
│   ├── logs.py                 # Console format and per-run log file
│   ├── sessions/               # SparkSessionMixin, FlinkSessionMixin
│   └── examples/               # MyBatchJob, BookingStreamJob, BookingFlinkJob
├── utils/                      # DataFrame, date and file helpers
├── tests/                      # pytest suite
├── data/dreamAirlines_DW/      # Sample data used by the examples and the training
└── training/                   # Databricks training notebooks (see training/README.md)
```

`etl/etl_interface.py` keeps the old `ETLInterface` name as a deprecated alias
of `SparkSessionMixin + JobBatch`.

---

## Quick start

```bash
pip install -r requirements.txt

# Batch: CSV → cleaned Parquet
python main.py --job jobs.examples.my_batch_job.MyBatchJob --conf config/dev/example.yaml

# Spark Structured Streaming: booking files → enriched Parquet
python main.py --job jobs.examples.booking_stream_job.BookingStreamJob --conf config/dev/example_stream.yaml

# Flink (pip install -r requirements-flink.txt): running revenue per agency
python main.py --job jobs.examples.booking_flink_job.BookingFlinkJob --conf config/dev/example_flink.yaml

# See the tracked runs
mlflow ui
```

The dev examples read `data/dreamAirlines_DW/` and write to `output/`. Run
parameters are passed with `--param`, repeatable:

```bash
python main.py --job my_package.my_module.MyJob --conf config/dev/my_job.yaml --param run_date=2016-03-15
```

---

## Writing a job

### Batch

```python
from jobs import JobBatch, SparkSessionMixin


class DailyRevenueJob(SparkSessionMixin, JobBatch):
    def extract(self):
        self.df = self.spark.read.parquet(self.config.require("job.source_path"))

    def transform(self):
        run_date = self.params["run_date"]              # from job.run(run_date=...)
        self.final_df = self.df.filter(self.df["date"] == run_date)

    def load(self):
        self.final_df.write.mode("overwrite").parquet(self.config.require("job.target_path"))
        self.log_metric("days_processed", 1)            # optional custom metric


DailyRevenueJob("config/dev/daily_revenue.yaml").run(run_date="2016-03-15")
```

### Stream

```python
from jobs import JobStream, SparkSessionMixin


class BookingStreamJob(SparkSessionMixin, JobStream):
    def source(self):
        self.events = self.spark.readStream.table(self.config.require("job.source_table"))

    def transform(self):
        self.stream_df = self.events.filter("AMT_Travel IS NOT NULL")

    def sink(self):
        return (self.stream_df.writeStream
                .option("checkpointLocation", self.config.require("job.checkpoint_path"))
                .trigger(availableNow=True)
                .toTable(self.config.require("job.target_table")))
```

`source()`, `transform()` and `sink()` only describe the pipeline. `sink()`
starts it and returns the running handle; `JobStream` then waits on it, logging
each micro-batch, until the stream ends or `job.stream_timeout_seconds` elapses.

### What every step can use

| Inside a step | What it gives you |
|---|---|
| `self.config.get("section.key", default)` | a config value, with its YAML type |
| `self.config.require("section.key")` | same, but fails clearly when the key is missing |
| `self.params["name"]` | what was passed to `job.run(name=...)` |
| `self.logger` | a standard Python logger, stored with the run |
| `self.log_metric(key, value)` / `self.set_tag(key, value)` | tracking on the current MLflow run |
| `self.spark` / `self.table_env` | the engine session, when the mixin is used |

---

## Configuration

One YAML file per job and environment. Values keep their type (booleans,
numbers, lists), and are addressed as `section.key`.

```yaml
job:
  name: DailyRevenueJob            # optional; defaults to the class name
  env: dev
  source_path: "{bucket}/bookings"
  target_path: "{bucket}/daily_revenue"
  partition_by: [country, year]
  # stream_timeout_seconds: 60     # JobStream only: stop after this long

# Keys in `storage` and `database` can be used as {placeholders} in any value.
storage:
  bucket: s3://my-data-bucket

database:
  host: localhost
  port: 5432
  dbname: my_database

# Passed as-is to the SparkSession builder.
spark:
  spark.app.name: DailyRevenueJob
  spark.master: "local[*]"
  spark.sql.shuffle.partitions: 8

# Passed as-is to the Flink TableEnvironment (FlinkSessionMixin only).
flink:
  parallelism.default: 1

mlflow:
  enabled: true                    # false turns tracking off without touching the job
  experiment: DailyRevenueJob-dev
  tags: {team: data-eng}           # optional, added to every run

logging:
  level: INFO
```

A missing file, invalid YAML or a missing required key raises `ConfigError`
with the file and key name. Credentials do not belong in these files: the MLflow
server is taken from the `MLFLOW_TRACKING_URI` environment variable (already set
on Databricks; a local `./mlruns` store is used when it is absent).

Where a Spark session already exists, cluster-level settings such as
`spark.master` are ignored and only runtime settings are applied.

---

## What is tracked

`Job.run()` records every execution as one MLflow run:

| Type | Key | Value |
|---|---|---|
| Tag | `job`, `mode`, `engine` | job name, `batch` / `stream`, `spark` / `flink` / `python` |
| Tag | `error` | exception type and message, on failed runs |
| Param | `env` | `job.env` |
| Param | *run parameters* | everything passed to `run(**params)` |
| Metric | `duration_seconds` | total run time |
| Metric | `<step>_seconds` | time spent in each step |
| Metric | `record_count` | rows produced (batch) or read (stream) |
| Metric | `batch_input_rows`, `batch_count` | per micro-batch, Spark streaming only |
| Artifact | `config/` | the config file used |
| Artifact | `logs/run.log` | everything the job logged during the run |

A step that raises marks the run `FAILED`, stores the traceback in the log
artifact and re-raises. Querying MLflow by `params.run_date` and `status` is
therefore enough to find gaps and backfill them (see training notebook 04).

Two limits: the log artifact contains the Python driver logs only, not Spark
executor or JVM logs, and it is uploaded when the run ends, so a long-running
stream is monitored live through the platform's own logs.

Because Spark is lazy, `extract_seconds` and `transform_seconds` are usually
close to zero: the work happens in `load()`. Counting rows runs the query once
more; set `track_record_count = False` on a job where that is too expensive.

---

## Running tests

```bash
pip install -r requirements.txt
pytest tests/
```

The Spark tests need Java; the Flink test is skipped unless PyFlink is installed.
