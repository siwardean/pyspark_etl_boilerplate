"""The Spark session mixin and the example jobs, on a local Spark session."""
import shutil
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from jobs import JobBatch, SparkSessionMixin  # noqa: E402
from jobs.examples.booking_stream_job import BookingSparkStreamJob  # noqa: E402
from jobs.examples.my_batch_job import MyBatchJob  # noqa: E402

DATA = Path(__file__).resolve().parent.parent / "data" / "dreamAirlines_DW"
SPARK_CONF = {
    "spark.master": "local[2]",
    "spark.app.name": "tests",
    "spark.ui.enabled": False,
    "spark.sql.shuffle.partitions": 2,
}


@pytest.fixture(scope="module", autouse=True)
def stop_spark():
    yield
    session = pyspark.sql.SparkSession.getActiveSession()
    if session is not None:
        session.stop()


def test_spark_conf_comes_from_config_as_strings():
    class AnyJob(SparkSessionMixin, JobBatch):
        def extract(self): ...
        def transform(self): ...
        def load(self): ...

    job = AnyJob({"spark": SPARK_CONF, "mlflow": {"enabled": False}})

    assert job.engine == "spark"
    assert job._spark is None  # nothing started until self.spark is used
    assert job._get_spark_conf() == {
        "spark.master": "local[2]",
        "spark.app.name": "tests",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
    }


def test_batch_job_writes_parquet_and_tracks_the_run(tmp_path, last_run):
    target = tmp_path / "clean"
    job = MyBatchJob({
        "job": {
            "env": "test",
            "source_path": str(DATA / "factTravelBooking.csv"),
            "target_path": str(target),
            "partition_by": ["FK_TravelAgency"],
        },
        "spark": SPARK_CONF,
        "mlflow": {"experiment": "spark-batch"},
    }).run()

    assert job.spark.conf.get("spark.sql.shuffle.partitions") == "2"
    assert job.spark.read.parquet(str(target)).count() == 99
    assert sorted(p.name for p in target.glob("FK_TravelAgency=*")) == [
        "FK_TravelAgency=1", "FK_TravelAgency=2", "FK_TravelAgency=3",
    ]
    run = last_run("spark-batch")
    assert run.info.status == "FINISHED"
    assert run.data.tags["engine"] == "spark"
    assert run.data.metrics["record_count"] == 99


def test_stream_job_processes_new_files_only(tmp_path, last_run):
    source = tmp_path / "landing"
    source.mkdir()
    shutil.copy(DATA / "factTravelBooking.csv", source / "bookings_1.csv")
    config = {
        "job": {"env": "test", "source_path": str(source), "target_path": str(tmp_path / "stream")},
        "spark": SPARK_CONF,
        "mlflow": {"experiment": "spark-stream"},
    }

    job = BookingSparkStreamJob(config).run()

    rows = job.spark.read.parquet(str(tmp_path / "stream"))
    assert rows.count() == 99
    assert rows.columns == ["booking_id", "booking_date", "travel_agency_id", "travel_revenue"]
    assert rows.filter("booking_date IS NULL OR travel_revenue IS NULL").count() == 0
    run = last_run("spark-stream")
    assert run.info.status == "FINISHED"
    assert run.data.tags["mode"] == "stream"
    assert run.data.metrics["record_count"] == 99
    assert run.data.metrics["batch_count"] >= 1

    # Same checkpoint, no new file: the second run has nothing to do.
    BookingSparkStreamJob(config).run()
    assert last_run("spark-stream").data.metrics["record_count"] == 0

    # A new file lands: only that file is read.
    (source / "bookings_2.csv").write_text(
        "PK_TravelBooking,FK_date,FK_TravelAgency,FK_Employee,FK_HotelBooking,FK_Customer,AMT_Travel\n"
        "1000,20181001,1,1,11,99,250.00\n1001,20181002,2,1,11,98,90.00\n"
    )
    BookingSparkStreamJob(config).run()
    assert last_run("spark-stream").data.metrics["record_count"] == 2
    assert job.spark.read.parquet(str(tmp_path / "stream")).count() == 101


def test_active_session_is_reused_and_gets_runtime_settings_only():
    class AnyJob(SparkSessionMixin, JobBatch):
        def extract(self): ...
        def transform(self): ...
        def load(self): ...

    active = pyspark.sql.SparkSession.builder.config("spark.master", "local[2]").getOrCreate()
    job = AnyJob({
        "spark": {"spark.master": "yarn", "spark.sql.shuffle.partitions": 7},
        "mlflow": {"enabled": False},
    })

    assert job.spark is active
    assert active.conf.get("spark.sql.shuffle.partitions") == "7"
    assert active.conf.get("spark.master") == "local[2]"


def test_old_etl_interface_name_still_works():
    from etl.etl_interface import ETLInterface

    class OldJob(ETLInterface):
        def extract(self): ...
        def transform(self): ...
        def load(self): ...

    with pytest.warns(DeprecationWarning, match="ETLInterface is deprecated"):
        job = OldJob({"mlflow": {"enabled": False}})
    assert isinstance(job, JobBatch) and job.engine == "spark"
