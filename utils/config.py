import uuid
from pyspark.sql import SparkSession


class Config:
    job_uid = str(uuid.uuid4())

    @staticmethod
    def getSparkSession(config):
        builder = (
            SparkSession.builder
            .appName(config.get("spark.appname", "ETL Job"))
            .master(config.get("spark.master", "local[*]"))
        )
        jars = config.get("spark.jars")
        if jars:
            builder = builder.config("spark.jars", jars)
        return builder.getOrCreate()

    @staticmethod
    def getJobUid():
        return Config.job_uid
