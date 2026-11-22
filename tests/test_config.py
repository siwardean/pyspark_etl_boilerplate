import pytest

from jobs import ConfigError, JobConfig

YAML = """
job:
  env: dev
  target_path: "{bucket}/daily/{dbname}"
  partition_by: [country, year]
  partitioned: true
  retries: 3
storage:
  bucket: s3://my-bucket
database:
  dbname: sales
  port: 5432
spark:
  spark.app.name: MyJob
  spark.sql.shuffle.partitions: 8
"""


@pytest.fixture
def config(tmp_path):
    path = tmp_path / "job.yaml"
    path.write_text(YAML)
    return JobConfig(path)


def test_values_keep_their_yaml_type(config):
    assert config.get("job.env") == "dev"
    assert config.get("job.partitioned") is True
    assert config.get("job.retries") == 3
    assert config.get("job.partition_by") == ["country", "year"]


def test_placeholders_are_resolved_from_storage_and_database(config):
    assert config.get("job.target_path") == "s3://my-bucket/daily/sales"


def test_unknown_placeholder_is_left_untouched():
    config = JobConfig({"job": {"path": "/data/{run_date}"}})
    assert config.get("job.path") == "/data/{run_date}"


def test_engine_keys_containing_dots(config):
    assert config.get("spark.spark.app.name") == "MyJob"
    assert config.section("spark") == {"spark.app.name": "MyJob", "spark.sql.shuffle.partitions": 8}


def test_missing_key_returns_default(config):
    assert config.get("job.nope") is None
    assert config.get("job.nope", "fallback") == "fallback"
    assert config.get("nope.nope", 1) == 1
    assert config.section("nope") == {}


def test_require_names_the_missing_key(config):
    with pytest.raises(ConfigError, match="job.nope"):
        config.require("job.nope")


def test_missing_file_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        JobConfig(tmp_path / "absent.yaml")


def test_invalid_yaml_is_an_error(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("job: [unclosed")
    with pytest.raises(ConfigError, match="Invalid YAML"):
        JobConfig(path)


def test_config_can_be_a_dict():
    config = JobConfig({"job": {"env": "test"}})
    assert config.path is None
    assert config.get("job.env") == "test"
