"""The Flink session mixin and example job. Skipped unless PyFlink is installed."""
import csv
from pathlib import Path

import pytest

pytest.importorskip("pyflink")

from jobs.examples.booking_stream_job import BookingFlinkStreamJob  # noqa: E402

LANDING = Path(__file__).resolve().parent.parent / "data" / "booking_events"


def read_rows(directory):
    rows = []
    for path in Path(directory).rglob("*"):
        if path.is_file() and not path.name.startswith((".", "_")):
            rows += list(csv.reader(path.open()))
    return sorted(rows, key=lambda row: int(row[0]))


def test_flink_stream_job_runs_and_is_tracked(tmp_path, last_run):
    BookingFlinkStreamJob({
        "job": {
            "env": "test",
            "source_path": str(LANDING),
            "target_path": str(tmp_path / "stream"),
        },
        "flink": {"parallelism.default": 1},
        "mlflow": {"experiment": "flink-stream"},
    }).run()

    run = last_run("flink-stream")
    assert run.info.status == "FINISHED"
    assert run.data.tags["engine"] == "flink"
    assert run.data.tags["mode"] == "stream"
    assert run.data.tags["flink_job_id"]
    rows = read_rows(tmp_path / "stream")
    assert len(rows) == 99
    assert rows[0] == ["1", "2015-10-01", "1", "180.0"]


def test_same_transform_gives_the_same_rows_on_spark_and_flink(tmp_path):
    pytest.importorskip("pyspark")
    from jobs.examples.booking_stream_job import BookingSparkStreamJob

    job_settings = {"env": "test", "source_path": str(LANDING)}
    spark_job = BookingSparkStreamJob({
        "job": {**job_settings, "target_path": str(tmp_path / "spark")},
        "spark": {"spark.master": "local[2]", "spark.ui.enabled": False},
        "mlflow": {"enabled": False},
    }).run()
    BookingFlinkStreamJob({
        "job": {**job_settings, "target_path": str(tmp_path / "flink")},
        "mlflow": {"enabled": False},
    }).run()

    spark_rows = [
        [str(r.booking_id), str(r.booking_date), str(r.travel_agency_id), str(r.travel_revenue)]
        for r in spark_job.spark.read.parquet(str(tmp_path / "spark")).orderBy("booking_id").collect()
    ]
    assert spark_rows == read_rows(tmp_path / "flink")
