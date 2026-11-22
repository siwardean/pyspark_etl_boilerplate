# PySpark ETL Boilerplate

A small framework for data jobs: batch or streaming, on Spark or Flink, with
validated YAML configuration, logging and MLflow tracking built in.

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
│   ├── dev/                    # example.yaml, example_stream.yaml, training_*.yaml
│   ├── test/                   # example.yaml
│   ├── qa/
│   └── prod/
├── jobs/
│   ├── job.py                  # Job: run() template, MLflow, logging
│   ├── job_batch.py            # JobBatch
│   ├── job_stream.py           # JobStream
│   ├── settings.py             # JobSettings, JobParams: typed, validated (pydantic)
│   ├── config.py               # JobConfig: YAML loader with {placeholder} resolution
│   ├── tracking.py             # The only module that talks to MLflow
│   ├── logs.py                 # Console format and per-run log file
│   ├── sessions/               # SparkSessionMixin, FlinkSessionMixin
│   └── examples/               # MyBatchJob, BookingStreamJob (Spark and Flink)
├── utils/                      # DataFrame, date and file helpers
├── tests/                      # pytest suite
├── data/                       # Sample data used by the examples and the training
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

# Streaming on Spark: booking files → cleaned Parquet
python main.py --job jobs.examples.booking_stream_job.BookingSparkStreamJob --conf config/dev/example_stream.yaml

# The same streaming job on Flink (pip install -r requirements-flink.txt)
python main.py --job jobs.examples.booking_stream_job.BookingFlinkStreamJob --conf config/dev/example_stream.yaml

# See the tracked runs
mlflow ui
```

The dev examples read from `data/` and write to `output/`. Run
parameters are passed with `--param`, repeatable:

```bash
python main.py --job my_package.my_module.MyJob --conf config/dev/my_job.yaml --param run_date=2016-03-15
```

---

## Writing a job

### Batch

```python
from datetime import date

from jobs import JobBatch, JobParams, JobSettings, SparkSessionMixin


class DailyRevenueJob(SparkSessionMixin, JobBatch):

    class Settings(JobSettings):          # keys this job needs in the `job` config section
        source_path: str
        target_path: str
        partition_by: list[str] = []

    class Params(JobParams):              # what changes on every run
        run_date: date

    def extract(self):
        self.df = self.spark.read.parquet(self.settings.source_path)

    def transform(self):
        self.final_df = self.df.filter(self.df["date"] == self.params.run_date)

    def load(self):
        self.final_df.write.mode("overwrite").parquet(self.settings.target_path)
        self.log_metric("days_processed", 1)            # optional custom metric


DailyRevenueJob("config/dev/daily_revenue.yaml").run(run_date="2016-03-15")
```

`Settings` and `Params` are [pydantic](https://docs.pydantic.dev) models. They
are optional, but declaring them gives you:

- **Fail-fast validation.** A missing `target_path` raises `ConfigError` when the
  job is created, before Spark starts; `run(run_date="oops")` is rejected before
  the run begins. The message names the field.
- **Typed values.** `self.params.run_date` is a `date`, `self.settings.partition_by`
  a list, with defaults where you set them.
- **A readable contract.** The top of the class says what the job needs.

Keys you do not declare remain readable (`self.settings.anything`).

### Stream

```python
from jobs import JobSettings, JobStream, SparkSessionMixin


class BookingStreamJob(SparkSessionMixin, JobStream):

    class Settings(JobSettings):
        source_table: str
        target_table: str
        checkpoint_path: str

    def source(self):
        self.events = self.spark.readStream.table(self.settings.source_table)

    def transform(self):
        self.stream_df = self.events.filter("AMT_Travel IS NOT NULL")

    def sink(self):
        return (self.stream_df.writeStream
                .option("checkpointLocation", self.settings.checkpoint_path)
                .trigger(availableNow=True)
                .toTable(self.settings.target_table))
```

`source()`, `transform()` and `sink()` only describe the pipeline. `sink()`
starts it and returns the running handle; `JobStream` then waits on it until the
stream ends or `job.stream_timeout_seconds` elapses.

### One streaming job on Spark or Flink

`JobStream` works with either engine. What is portable between them is the
transformation, when it is written in SQL: both session mixins expose
`self.sql(query)`. Sources and sinks are connectors, which are engine-specific,
so each engine gets a thin subclass. `jobs/examples/booking_stream_job.py` shows
the pattern:

```python
class BookingStreamJob(JobStream):                 # engine-neutral
    def transform(self):
        self.stream_df = self.sql(CLEAN_BOOKINGS)  # same SQL on both engines

class BookingSparkStreamJob(SparkSessionMixin, BookingStreamJob):
    def source(self): ...                          # spark.readStream → temp view `bookings`
    def sink(self): ...                            # writeStream → Parquet

class BookingFlinkStreamJob(FlinkSessionMixin, BookingStreamJob):
    def source(self): ...                          # CREATE TABLE bookings WITH (connector...)
    def sink(self): ...                            # execute_insert → CSV
```

The engine is chosen with `--job`; the config file is the same. A test checks
that both produce identical rows. Per-batch row counts are reported to MLflow on
Spark only; a Flink run records its Flink job id.

### What every step can use

| Inside a step | What it gives you |
|---|---|
| `self.settings.<key>` | a validated value from the `job` config section |
| `self.params.<name>` | a validated value passed to `job.run(name=...)` |
| `self.config.get("section.key", default)` | any other config value, with its YAML type |
| `self.logger` | a standard Python logger, stored with the run |
| `self.log_metric(key, value)` / `self.set_tag(key, value)` | tracking on the current MLflow run |
| `self.spark` / `self.table_env` / `self.sql(query)` | the engine session, when a mixin is used |

After a run, `job.run_id` is the MLflow run id and `job.final_df` the result.

---

## Configuration

One YAML file per job and environment. Values keep their type (booleans,
numbers, lists). The `job`, `mlflow` and `logging` sections are validated when
the job is created; a typo such as `experimnt:` is rejected.

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

A missing file, invalid YAML, an invalid value or a missing required key raises
`ConfigError` with the file and field name. Credentials do not belong in these files: the MLflow
server is taken from the `MLFLOW_TRACKING_URI` environment variable (already set
on Databricks; MLflow falls back to a local store in the working directory when
it is absent).

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

## Development

```bash
pip install -r requirements.txt pre-commit
pre-commit install          # run the checks on every commit
pre-commit run --all-files  # run them on everything now
pytest tests/
```

The pre-commit hooks lint and format with [ruff](https://docs.astral.sh/ruff/)
(configured in `pyproject.toml`) and run basic file checks (YAML and TOML
syntax, trailing whitespace, merge-conflict markers, large files). The training
notebooks are linted but not reformatted.

The Spark tests need Java; the Flink tests are skipped unless PyFlink is
installed (`pip install -r requirements-flink.txt`).

CI (`.github/workflows/ci.yml`) runs on every pull request and on pushes to
`main`, in two stages: the pre-commit hooks, then the pytest suite including the
Flink tests.
