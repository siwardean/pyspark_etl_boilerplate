---
type: Wiki index
title: PySpark ETL Boilerplate wiki
description: Entry point to the documentation of the job framework and its training material.
okf_version: "0.2"
tags: [index]
---

# PySpark ETL Boilerplate wiki

A small framework for data jobs: batch or streaming, on Spark or Flink, with
validated YAML configuration, logging and MLflow tracking built in.

| Page | What it covers |
|---|---|
| [Quickstart](quickstart.md) | Install, run the example jobs, write your first job |
| [Architecture](architecture.md) | `Job`, `JobBatch`, `JobStream`, the session mixins, and why they are split this way |
| [Writing jobs](writing-jobs.md) | Batch jobs, streaming jobs, one streaming job on Spark and Flink |
| [Configuration](configuration.md) | The YAML file, typed `Settings` and `Params`, placeholders, validation errors |
| [Tracking and logging](tracking-and-logging.md) | What every run records in MLflow, the run log, limits |
| [Operations](operations.md) | uv, pre-commit, CI, the documentation rule, releases |
| [Training](training.md) | The DreamAirlines notebooks and the 2-hour agenda |

## Keeping this wiki current

A pull request that changes the framework code or its configuration must also
change this wiki or the README; CI enforces it. See
[Operations](operations.md#documentation-rule).
