"""Minimal batch job: CSV in, cleaned Parquet out."""
from jobs import JobBatch, JobSettings, SparkSessionMixin
from utils.file_management import save_as_parquet
from utils.toolbox import drop_nulls


class MyBatchJob(SparkSessionMixin, JobBatch):
    class Settings(JobSettings):
        source_path: str
        target_path: str
        partition_by: list[str] = []

    def extract(self):
        self.df = (
            self.spark.read.option("header", "true").option("inferSchema", "true")
            .csv(self.settings.source_path)
        )

    def transform(self):
        self.final_df = drop_nulls(self.df)

    def load(self):
        save_as_parquet(self.final_df, self.settings.target_path, self.settings.partition_by)
