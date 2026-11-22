"""Typed, validated settings. Each model maps to one section of the YAML config."""
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobSettings(BaseModel):
    """The ``job`` section of the config.

    Subclass it inside a job to declare the keys that job needs. They are then
    validated when the job is created, before any engine starts, and read as
    typed attributes::

        class DailyRevenueJob(SparkSessionMixin, JobBatch):
            class Settings(JobSettings):
                target_path: str                 # required
                partition_by: list[str] = []     # optional, with a default

            def load(self):
                self.final_df.write.parquet(self.settings.target_path)

    Keys that are not declared are kept and stay readable as attributes.
    """

    model_config = ConfigDict(extra="allow")

    name: Optional[str] = Field(None, description="Job name used in logs and MLflow; defaults to the class name.")
    env: str = Field("dev", description="Environment the job runs in (dev, test, qa, prod).")
    stream_timeout_seconds: Optional[float] = Field(
        None, gt=0, description="JobStream only: stop the stream after this many seconds."
    )


class JobParams(BaseModel):
    """What is passed to ``job.run(**params)``: the values that change per run.

    Subclass it inside a job to declare and validate its run parameters::

        class DailyRevenueJob(SparkSessionMixin, JobBatch):
            class Params(JobParams):
                run_date: date                   # "2016-03-15" is parsed to a date

            def transform(self):
                self.final_df = self.df.filter(self.df["date"] == self.params.run_date)

    Undeclared parameters are accepted and readable as attributes.
    """

    model_config = ConfigDict(extra="allow")


class MlflowSettings(BaseModel):
    """The ``mlflow`` section of the config."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(True, description="Set to false to run without tracking.")
    experiment: Optional[str] = Field(None, description="Experiment the runs are filed under.")
    tracking_uri: Optional[str] = Field(
        None, description="MLflow server; normally left to MLFLOW_TRACKING_URI or the platform."
    )
    tags: dict[str, str] = Field(default_factory=dict, description="Extra tags added to every run.")


class LoggingSettings(BaseModel):
    """The ``logging`` section of the config."""

    model_config = ConfigDict(extra="forbid")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("level", mode="before")
    @classmethod
    def _upper(cls, value):
        return value.upper() if isinstance(value, str) else value
