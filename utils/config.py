import uuid
from pyspark.sql import SparkSession

class Config:
    job_uid = str(uuid.uuid4())

    @staticmethod
    def getSparkSession():
        return SparkSession.builder             .appName("ETL Job")             .getOrCreate()

    @staticmethod
    def getJobUid():
        return Config.job_uid
