"""The Flink session mixin and example job. Skipped unless PyFlink is installed."""
from pathlib import Path

import pytest

pytest.importorskip("pyflink")

from jobs.examples.booking_flink_job import BookingFlinkJob  # noqa: E402

DATA = Path(__file__).resolve().parent.parent / "data" / "dreamAirlines_DW"


def test_flink_stream_job_runs_and_is_tracked(last_run, capfd):
    BookingFlinkJob({
        "job": {"env": "test", "source_path": str(DATA / "factTravelBooking.csv")},
        "flink": {"parallelism.default": 1},
        "mlflow": {"experiment": "flink-stream"},
    }).run()

    run = last_run("flink-stream")
    assert run.info.status == "FINISHED"
    assert run.data.tags["engine"] == "flink"
    assert run.data.tags["mode"] == "stream"
    assert run.data.tags["flink_job_id"]
    # Final changelog row of each agency: 39, 30 and 30 bookings.
    output = capfd.readouterr().out
    assert "+I[1, 1," in output
    for agency, bookings in ((1, 39), (2, 30), (3, 30)):
        assert f"[{agency}, {bookings}," in output
