---
type: Reference
title: Configuration
description: The YAML configuration file, typed Settings and Params models, placeholders and validation.
tags: [configuration, yaml, pydantic]
---

# Configuration

One YAML file per job and environment, under `config/<env>/`.

```yaml
job:
  name: DailyRevenueJob            # optional; defaults to the class name
  env: dev
  source_path: "{bucket}/bookings"
  target_path: "{bucket}/daily_revenue"
  partition_by: [country, year]
  # stream_timeout_seconds: 60     # JobStream only

storage:                           # keys usable as {placeholders}
  bucket: s3://my-data-bucket

database:                          # keys usable as {placeholders}
  host: localhost
  port: 5432

spark:                             # passed as-is to the SparkSession builder
  spark.app.name: DailyRevenueJob
  spark.sql.shuffle.partitions: 8

flink:                             # passed as-is to the Flink TableEnvironment
  parallelism.default: 1

mlflow:
  enabled: true
  experiment: DailyRevenueJob-dev
  tags: {team: data-eng}

logging:
  level: INFO
```

## Sections

| Section | Validated | Purpose |
|---|---|---|
| `job` | yes, by the job's `Settings` model | the job's own settings |
| `mlflow` | yes; unknown keys are rejected | `enabled`, `experiment`, `tracking_uri`, `tags` |
| `logging` | yes; unknown keys are rejected | `level`: DEBUG, INFO, WARNING, ERROR or CRITICAL |
| `spark`, `flink` | no | engine settings, passed through |
| `storage`, `database` | no | values available as `{placeholders}` |

## Typed settings and parameters

A job declares what it needs as nested pydantic models:

```python
class DailyRevenueJob(SparkSessionMixin, JobBatch):

    class Settings(JobSettings):       # the `job` section
        target_path: str               # required
        partition_by: list[str] = []   # optional, with a default

    class Params(JobParams):           # arguments of run()
        run_date: date
```

- `Settings` is validated when the job is created, before any engine starts.
- `Params` is validated at the start of `run()`; `"2016-03-15"` becomes a `date`.
- Undeclared keys are kept and stay readable (`self.settings.anything`).
- Declaring the models is optional; without them everything is accepted.

## Errors

A missing file, invalid YAML, an invalid value or a missing key raises
`ConfigError` naming the file and field:

```
jobs.config.ConfigError: Invalid 'job' section in config/dev/daily_revenue.yaml:
  - target_path: Field required
```

## Placeholders

A scalar declared in `storage` or `database` can be referenced as `{key}` in
any string value. Unknown placeholders are left untouched.

## Credentials

Do not put credentials in these files. The MLflow server comes from the
`MLFLOW_TRACKING_URI` environment variable, which Databricks sets for you;
without it MLflow uses a local store in the working directory.
