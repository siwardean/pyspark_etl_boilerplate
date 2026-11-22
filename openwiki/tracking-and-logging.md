---
type: Reference
title: Tracking and logging
description: What every job run records in MLflow, how logging works, and the known limits.
tags: [mlflow, logging, observability]
---

# Tracking and logging

`Job.run()` records every execution as one MLflow run. The job contains no
tracking code.

## What is recorded

| Type | Key | Value |
|---|---|---|
| Tag | `job`, `mode`, `engine` | job name, `batch` / `stream`, `spark` / `flink` / `python` |
| Tag | `error` | exception type and message, on failed runs |
| Tag | `flink_job_id` | Flink streaming runs only |
| Param | `env` | `job.env` |
| Param | *run parameters* | everything passed to `run(**params)` |
| Metric | `duration_seconds` | total run time |
| Metric | `<step>_seconds` | time spent in each step |
| Metric | `record_count` | rows produced (batch) or read (Spark stream) |
| Metric | `batch_input_rows`, `batch_count` | per micro-batch, Spark streaming only |
| Artifact | `config/` | the config file used |
| Artifact | `logs/run.log` | everything the job logged during the run |

Add your own with `self.log_metric(key, value)` and `self.set_tag(key, value)`
inside any step.

## Failures

A step that raises marks the run `FAILED`, sets the `error` tag, stores the
traceback in `logs/run.log` and re-raises. Querying MLflow by a run parameter
and `status` is therefore enough to find gaps and backfill them:

```python
runs = mlflow.search_runs(experiment_names=[...], filter_string="tags.job = 'DailyRevenueJob'")
failed = set(runs[runs["status"] == "FAILED"]["params.run_date"])
```

A `FINISHED` status only means the code did not crash. Check `record_count`, or
a business metric of your own, to catch a run that finished on empty input.

## Logging

Every job has `self.logger`, a standard Python logger. The base class logs the
start, end and duration of each step and of the run. The level comes from
`logging.level` in the config. Everything logged during a run is also written to
a file that is attached to the MLflow run when it ends.

## Limits

- The log artifact holds the Python driver logs only, not Spark executor or JVM logs.
- It is uploaded when the run ends, so a long-running stream is monitored live
  through the platform's own logs.
- Spark is lazy: `extract_seconds` and `transform_seconds` are usually close to
  zero, because the work happens in `load()`.
- Per-batch row counts are reported on Spark only. A Flink run records its job id.

## Turning tracking off

Set `mlflow.enabled: false` in the config. The job runs unchanged.
