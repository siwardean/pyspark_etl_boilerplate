---
type: Quickstart
title: Quickstart
description: Install the project with uv, run the example jobs and write a first job.
tags: [getting-started, uv, examples]
---

# Quickstart

## Install

The project uses [uv](https://docs.astral.sh/uv/). Spark needs a Java runtime (17 or later).

```bash
uv sync                  # creates .venv from uv.lock
uv sync --extra flink    # optional: also install PyFlink
```

## Run the examples

Every job is started through `main.py` with its dotted class path and a config file.

```bash
# Batch: CSV → cleaned Parquet
uv run python main.py --job jobs.examples.my_batch_job.MyBatchJob --conf config/dev/example.yaml

# Streaming on Spark
uv run python main.py --job jobs.examples.booking_stream_job.BookingSparkStreamJob --conf config/dev/example_stream.yaml

# The same streaming job on Flink
uv run python main.py --job jobs.examples.booking_stream_job.BookingFlinkStreamJob --conf config/dev/example_stream.yaml

uv run mlflow ui         # browse the tracked runs
```

The dev examples read from `data/` and write to `output/`. Run parameters are
passed with `--param KEY=VALUE`, repeatable.

## Your first job

```python
from jobs import JobBatch, JobSettings, SparkSessionMixin


class CleanBookingsJob(SparkSessionMixin, JobBatch):

    class Settings(JobSettings):
        source_path: str
        target_path: str

    def extract(self):
        self.df = self.spark.read.option("header", "true").csv(self.settings.source_path)

    def transform(self):
        self.final_df = self.df.na.drop()

    def load(self):
        self.final_df.write.mode("overwrite").parquet(self.settings.target_path)
```

Add a `config/dev/clean_bookings.yaml` with a `job` section holding
`source_path` and `target_path`, then run it with `main.py`. The run is logged
and tracked without any further code: see
[Tracking and logging](tracking-and-logging.md).

Next: [Writing jobs](writing-jobs.md) and [Configuration](configuration.md).
