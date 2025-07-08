from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class ETLInterface(ABC):
    def __init__(self):
        self.spark = SparkSession.builder.appName("ETL Job").getOrCreate()
        self.config_path = None
        self.final_df = None

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self):
        pass

    def run(self):
        self.extract()
        self.transform()
        self.load()
