---
type: Architecture
title: Architecture
description: The class structure of the framework and the reasons behind it.
tags: [architecture, design]
---

# Architecture

```mermaid
classDiagram
    class Job {
        +settings
        +params
        +config
        +logger
        +run(**params)
        +log_metric(key, value)
        #_execute()
    }
    class JobBatch {
        +extract()
        +transform()
        +load()
    }
    class JobStream {
        +source()
        +transform()
        +sink()
    }
    class SparkSessionMixin {
        +spark
        +sql(query)
    }
    class FlinkSessionMixin {
        +table_env
        +sql(query)
    }
    Job <|-- JobBatch
    Job <|-- JobStream
    SparkSessionMixin <.. JobBatch : optional
    SparkSessionMixin <.. JobStream : optional
    FlinkSessionMixin <.. JobStream : optional
```

## Two independent choices

| Choice | How it is made | Options |
|---|---|---|
| **Mode** | inheritance | `JobBatch` (bounded) or `JobStream` (unbounded) |
| **Engine** | mixin | `SparkSessionMixin`, `FlinkSessionMixin`, or none |

Both engines do both modes, so making the engine a mixin avoids one base class
per combination. `Job` itself has no engine: a job that calls an API or uses
pandas simply leaves the mixin out.

```python
class DailyRevenueJob(SparkSessionMixin, JobBatch): ...     # Spark batch
class BookingStreamJob(SparkSessionMixin, JobStream): ...   # Spark Structured Streaming
class BookingFlinkJob(FlinkSessionMixin, JobStream): ...    # Flink streaming
class ApiExportJob(JobBatch): ...                           # plain Python
```

## What `Job.run()` owns

`run()` is a template method. Subclasses provide the steps; `run()` provides
everything around them, in one place:

1. validates the run parameters into `self.params`;
2. opens the MLflow run and logs tags, parameters and the config file;
3. calls `_execute()`, which runs the steps and times each one;
4. on an exception, marks the run `FAILED`, records the error and re-raises;
5. always logs the duration and attaches the run's log file.

A concrete job therefore never imports `mlflow` or configures `logging`.

## Sessions are lazy

Nothing is imported or started until `self.spark` or `self.table_env` is first
used, so a job can be created and its config validated without the engine.
`SparkSessionMixin` reuses the active session when there is one (notebooks,
Databricks) and otherwise builds one from the `spark` config section. On an
existing session only runtime settings are applied; cluster-level ones such as
`spark.master` are ignored.

## Modules

| Module | Responsibility |
|---|---|
| `jobs/job.py` | `Job`: the `run()` template |
| `jobs/job_batch.py`, `jobs/job_stream.py` | the two lifecycles |
| `jobs/sessions/` | the Spark and Flink mixins |
| `jobs/settings.py` | pydantic models: `JobSettings`, `JobParams`, `MlflowSettings`, `LoggingSettings` |
| `jobs/config.py` | `JobConfig`: YAML loading, placeholders, validation errors |
| `jobs/tracking.py` | the only module that talks to MLflow |
| `jobs/logs.py` | console format and the per-run log file |
| `jobs/examples/` | runnable example jobs |
| `main.py` | command-line entry point |

`etl/etl_interface.py` keeps the former `ETLInterface` name as a deprecated
alias of `SparkSessionMixin + JobBatch`.
