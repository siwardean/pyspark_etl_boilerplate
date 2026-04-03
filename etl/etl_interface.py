from abc import ABC, abstractmethod
from utils.config import Config

class ETLInterface(ABC):
    def __init__(self):
        self.spark = Config.getSparkSession()
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
