import uuid
from pyspark.sql import SparkSession


class Config:
    job_uid = str(uuid.uuid4())

    @staticmethod
    def getSparkSession(config):
        builder = SparkSession.builder
        for key, value in config.get_spark_context().items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    @staticmethod
    def getJobUid():
        return Config.job_uid
