from abc import ABC, abstractmethod
from etl.etl_config import ETLConfig
from utils.config import Config


class ETLInterface(ABC):
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = ETLConfig(config_path)
        self.spark = Config.getSparkSession(self.config)
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
