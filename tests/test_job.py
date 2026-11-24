"""The run() template, tested with plain-Python jobs: no engine involved."""

from datetime import date
from pathlib import Path

import mlflow
import pytest
from pydantic import ValidationError

from jobs import ConfigError, JobBatch, JobParams, JobSettings, JobStream

EXPERIMENT = "test-jobs"


def make_config(**mlflow_settings):
    return {
        "job": {"env": "test"},
        "mlflow": {"experiment": EXPERIMENT, **mlflow_settings},
    }


class NumbersJob(JobBatch):
    class Params(JobParams):
        size: int = 5

    def extract(self):
        self.numbers = list(range(self.params.size))

    def transform(self):
        self.final_df = [n * 2 for n in self.numbers]
        self.log_metric("total", sum(self.final_df))

    def load(self):
        self.logger.info("loaded %d numbers", len(self.final_df))


class BrokenJob(NumbersJob):
    def transform(self):
        raise ValueError("upstream file missing")


def read_artifact(run, path):
    return Path(mlflow.artifacts.download_artifacts(run_id=run.info.run_id, artifact_path=path)).read_text()


def test_successful_run_is_tracked(last_run):
    job = NumbersJob(make_config()).run(size=4)

    run = last_run(EXPERIMENT)
    assert job.final_df == [0, 2, 4, 6]
    assert run.info.status == "FINISHED"
    assert run.info.run_name == "NumbersJob-4"
    assert run.data.tags["job"] == "NumbersJob"
    assert run.data.tags["mode"] == "batch"
    assert run.data.tags["engine"] == "python"
    assert run.data.params == {"env": "test", "size": "4"}
    assert job.params.size == 4
    assert run.data.metrics["record_count"] == 4
    assert run.data.metrics["total"] == 12
    assert {"duration_seconds", "extract_seconds", "transform_seconds", "load_seconds"} <= set(
        run.data.metrics
    )


def test_run_log_and_config_are_stored_with_the_run(last_run):
    NumbersJob(make_config()).run()

    run = last_run(EXPERIMENT)
    log = read_artifact(run, "logs/run.log")
    assert "extract: started" in log
    assert "loaded 5 numbers" in log
    assert "Run finished" in log
    assert "experiment: test-jobs" in read_artifact(run, "config/config.yaml")


def test_config_file_is_stored_as_written(tmp_path, last_run):
    path = tmp_path / "numbers.yaml"
    path.write_text(f"# my comment\njob:\n  env: test\nmlflow:\n  experiment: {EXPERIMENT}\n")
    NumbersJob(path).run()

    assert "# my comment" in read_artifact(last_run(EXPERIMENT), "config/numbers.yaml")


def test_failed_run_is_marked_failed_and_reraised(last_run):
    with pytest.raises(ValueError, match="upstream file missing"):
        BrokenJob(make_config()).run(run_date="2016-01-12")

    run = last_run(EXPERIMENT)
    assert run.info.status == "FAILED"
    assert run.data.params["run_date"] == "2016-01-12"
    assert run.info.run_name == "BrokenJob-5-2016-01-12"
    assert run.data.tags["error"] == "ValueError: upstream file missing"
    assert "record_count" not in run.data.metrics
    assert "duration_seconds" in run.data.metrics
    log = read_artifact(run, "logs/run.log")
    assert "Run failed" in log and "Traceback" in log


def test_tracking_can_be_disabled():
    job = NumbersJob(make_config(enabled=False)).run()

    assert job.final_df == [0, 2, 4, 6, 8]
    assert mlflow.get_experiment_by_name(EXPERIMENT) is None


def test_job_name_and_extra_tags_can_come_from_config(last_run):
    config = make_config(tags={"team": "data-eng"})
    config["job"]["name"] = "DailyNumbers"
    NumbersJob(config).run()

    tags = last_run(EXPERIMENT).data.tags
    assert tags["job"] == "DailyNumbers"
    assert tags["team"] == "data-eng"


def test_stream_job_runs_source_transform_sink_then_waits(last_run):
    calls = []

    class FakeStream(JobStream):
        def source(self):
            calls.append("source")

        def transform(self):
            calls.append("transform")

        def sink(self):
            calls.append("sink")
            return "handle"

        def _await_stream(self, handle, timeout=None):
            calls.append(("await", handle, timeout))

    config = make_config()
    config["job"]["stream_timeout_seconds"] = 30
    FakeStream(config).run()

    assert calls == ["source", "transform", "sink", ("await", "handle", 30)]
    assert last_run(EXPERIMENT).data.tags["mode"] == "stream"


def test_stream_job_without_engine_explains_what_is_missing():
    class NoEngine(JobStream):
        def source(self): ...
        def transform(self): ...
        def sink(self):
            return "handle"

    with pytest.raises(NotImplementedError, match="session mixin"):
        NoEngine(make_config(enabled=False)).run()


class TypedJob(JobBatch):
    class Settings(JobSettings):
        target_path: str
        partition_by: list[str] = []

    class Params(JobParams):
        run_date: date

    def extract(self): ...
    def transform(self): ...
    def load(self): ...


def test_settings_are_validated_and_typed():
    job = TypedJob({"job": {"target_path": "{bucket}/out", "extra_key": 1}, "storage": {"bucket": "s3://b"}})

    assert job.settings.target_path == "s3://b/out"
    assert job.settings.partition_by == []
    assert job.settings.env == "dev"
    assert job.settings.extra_key == 1  # undeclared keys stay readable


def test_missing_setting_fails_when_the_job_is_created():
    with pytest.raises(ConfigError, match=r"Invalid 'job' section.*\n  - target_path: Field required"):
        TypedJob({"job": {"env": "test"}})


def test_unknown_mlflow_or_logging_key_is_rejected():
    with pytest.raises(ConfigError, match="experimnt"):
        NumbersJob({"mlflow": {"experimnt": "typo"}})
    with pytest.raises(ConfigError, match="level"):
        NumbersJob({"logging": {"level": "LOUD"}})


def test_run_params_are_validated_and_parsed(last_run):
    config = {"job": {"target_path": "/tmp/out"}, "mlflow": {"experiment": EXPERIMENT}}

    job = TypedJob(config).run(run_date="2016-03-15")
    assert job.params.run_date == date(2016, 3, 15)
    assert last_run(EXPERIMENT).data.params["run_date"] == "2016-03-15"

    with pytest.raises(ValidationError, match="run_date"):
        TypedJob(config).run(run_date="not a date")
    with pytest.raises(ValidationError, match="run_date"):
        TypedJob(config).run()


def test_stream_job_whose_sink_returns_nothing_is_rejected():
    class ForgotToReturn(JobStream):
        def source(self): ...
        def transform(self): ...
        def sink(self): ...

    with pytest.raises(ValueError, match="must start the stream and return its handle"):
        ForgotToReturn(make_config(enabled=False)).run()
