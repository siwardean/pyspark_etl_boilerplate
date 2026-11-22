"""Minimal batch job: CSV in, cleaned Parquet out."""
from jobs import JobBatch, SparkSessionMixin
from utils.file_management import save_as_parquet
from utils.toolbox import drop_nulls


class MyBatchJob(SparkSessionMixin, JobBatch):
    def extract(self):
        path = self.config.require("job.source_path")
        self.df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    def transform(self):
        self.final_df = drop_nulls(self.df)

    def load(self):
        save_as_parquet(
            self.final_df,
            self.config.require("job.target_path"),
            self.config.get("job.partition_by"),
        )
