---
type: Architecture
title: Architecture
description: The class structure of the framework and the reasons behind it.
tags: [architecture, design]
---

# Architecture

The framework exists so that every pipeline has the same shape and contains
only its business logic. The diagrams below show the classes that make that
possible.

## Core classes

```mermaid
classDiagram
    direction TB
    class Job {
        <<abstract>>
        +settings : JobSettings
        +params : JobParams
        +config : JobConfig
        +logger
        +run_id
        +run(**params) Job
        +log_metric(key, value)
        +set_tag(key, value)
        #_execute()*
    }
    class JobBatch {
        <<abstract>>
        +final_df
        +extract()*
        +transform()*
        +load()*
        +count_records()
    }
    class JobStream {
        <<abstract>>
        +stream_df
        +handle
        +source()*
        +transform()*
        +sink()*
    }
    class SparkSessionMixin {
        <<mixin>>
        +spark : SparkSession
        +sql(query)
    }
    class FlinkSessionMixin {
        <<mixin>>
        +table_env : TableEnvironment
        +sql(query)
    }
    Job <|-- JobBatch
    Job <|-- JobStream
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

## Configuration and tracking

`Job` delegates two concerns. `JobConfig` loads the YAML file and validates each
section into a typed model. `Tracker` is the only class that talks to MLflow.

```mermaid
classDiagram
    direction LR
    class Job {
        <<abstract>>
        +run(**params)
    }
    class JobConfig {
        +path
        +get(key, default)
        +require(key)
        +section(name)
        +load(section, model)
    }
    class JobSettings {
        +name
        +env
        +stream_timeout_seconds
    }
    class JobParams
    class MlflowSettings {
        +enabled
        +experiment
        +tracking_uri
        +tags
    }
    class LoggingSettings {
        +level
    }
    class Tracker {
        +run_id
        +start_run(name, tags, params)
        +log_metric(key, value)
        +set_tag(key, value)
        +log_file(path)
    }
    Job *-- JobConfig : reads the YAML file
    Job *-- JobSettings : settings
    Job *-- JobParams : params
    Job *-- Tracker : records the run
    JobConfig ..> JobSettings : validates job
    JobConfig ..> MlflowSettings : validates mlflow
    JobConfig ..> LoggingSettings : validates logging
    Tracker --> MlflowSettings
```

## Example jobs

How the jobs in `jobs/examples/` are assembled. The streaming example keeps the
transformation in an engine-neutral class and adds one thin subclass per engine.

```mermaid
classDiagram
    direction TB
    class JobBatch {
        <<abstract>>
    }
    class JobStream {
        <<abstract>>
    }
    class SparkSessionMixin {
        <<mixin>>
    }
    class FlinkSessionMixin {
        <<mixin>>
    }
    class MyBatchJob {
        +extract()
        +transform()
        +load()
    }
    class BookingStreamJob {
        +transform()
    }
    class BookingSparkStreamJob {
        +source()
        +sink()
    }
    class BookingFlinkStreamJob {
        +source()
        +sink()
    }
    JobBatch <|-- MyBatchJob
    SparkSessionMixin <|-- MyBatchJob
    JobStream <|-- BookingStreamJob
    BookingStreamJob <|-- BookingSparkStreamJob
    SparkSessionMixin <|-- BookingSparkStreamJob
    BookingStreamJob <|-- BookingFlinkStreamJob
    FlinkSessionMixin <|-- BookingFlinkStreamJob
```

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
