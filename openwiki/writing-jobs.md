---
type: Guide
title: Writing jobs
description: How to write batch and streaming jobs, and a streaming job that runs on Spark or Flink.
tags: [jobs, batch, streaming, spark, flink]
---

# Writing jobs

## Batch: `JobBatch`

Implement `extract()`, `transform()` and `load()`, and leave the result of
`transform()` in `self.final_df`.

```python
from datetime import date

from jobs import JobBatch, JobParams, JobSettings, SparkSessionMixin


class DailyRevenueJob(SparkSessionMixin, JobBatch):

    class Settings(JobSettings):          # keys needed in the `job` config section
        source_path: str
        target_path: str

    class Params(JobParams):              # what changes on every run
        run_date: date

    def extract(self):
        self.df = self.spark.read.parquet(self.settings.source_path)

    def transform(self):
        self.final_df = self.df.filter(self.df["date"] == self.params.run_date)

    def load(self):
        self.final_df.write.mode("overwrite").parquet(self.settings.target_path)


DailyRevenueJob("config/dev/daily_revenue.yaml").run(run_date="2016-03-15")
```

After `load()`, the row count of `self.final_df` is logged as `record_count`.
On Spark this runs the query once more; set `track_record_count = False` on a
job where that is too expensive.

## Stream: `JobStream`

Implement `source()`, `transform()` and `sink()`. They only describe the
pipeline: `sink()` starts it and must return the running handle (returning
nothing is an error), and `JobStream` waits on that handle until the stream ends or `job.stream_timeout_seconds`
elapses.

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

With `trigger(availableNow=True)` the job processes what is available and
stops; the checkpoint makes the next run pick up only new data.

## One streaming job on Spark or Flink

What is portable between the engines is a transformation written in SQL: both
mixins expose `self.sql(query)`. Sources and sinks are connectors, which are
engine-specific, so each engine gets a thin subclass.
`jobs/examples/booking_stream_job.py` shows the pattern:

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

The engine is chosen with `--job`; the config file is shared. A test checks
that both versions produce identical rows.

## What a step can use

| Inside a step | What it gives you |
|---|---|
| `self.settings.<key>` | a validated value from the `job` config section |
| `self.params.<name>` | a validated value passed to `job.run(name=...)` |
| `self.config.get("section.key", default)` | any other config value |
| `self.logger` | a standard Python logger, stored with the run |
| `self.log_metric(key, value)`, `self.set_tag(key, value)` | tracking on the current run |
| `self.spark`, `self.table_env`, `self.sql(query)` | the engine session, when a mixin is used |

After a run, `job.run_id` is the MLflow run id and `job.final_df` the result.

See also [Configuration](configuration.md) and
[Tracking and logging](tracking-and-logging.md).
