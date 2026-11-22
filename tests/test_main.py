import pytest

import main
from jobs import JobBatch


def test_load_job_class_from_dotted_path():
    assert main.load_job_class("jobs.job_batch.JobBatch") is JobBatch


def test_params_are_parsed_as_key_value_pairs():
    args = main.parse_args(
        ["--job", "a.B", "--conf", "c.yaml", "--param", "run_date=2016-03-15", "--param", "x=a=b"]
    )
    assert args.params == {"run_date": "2016-03-15", "x": "a=b"}


def test_malformed_param_is_rejected():
    with pytest.raises(SystemExit):
        main.parse_args(["--job", "a.B", "--conf", "c.yaml", "--param", "oops"])
