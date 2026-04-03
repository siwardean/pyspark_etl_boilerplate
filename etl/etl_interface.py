from abc import ABC, abstractmethod
from utils.config import Config

class ETLInterface(ABC):
    def __init__(self):
        self.spark = None
        self.config_path = None
        self.final_df = None

    def init(self):
        self.spark = Config.getSparkSession()

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
        self.init()
        self.extract()
        self.transform()
        self.load()
