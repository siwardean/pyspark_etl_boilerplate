from etl.etl_interface import ETLInterface
from etl.etl_config import ETLConfig
from utils.toolbox import drop_nulls
import mlflow


class MyETLJob(ETLInterface):
    def extract(self):
        self.config = ETLConfig(self.config_path)
        path = self.config.get("spark.targetfilepath")
        self.df = self.spark.read.option("header", "true").csv(path)

    def transform(self):
        self.final_df = drop_nulls(self.df)

    def load(self):
        output_path = self.config.get("spark.targetfilepath") + "_output"
        self.final_df.write.mode("overwrite").parquet(output_path)

    def run(self):
        with mlflow.start_run():
            mlflow.set_tag("job", "MyETLJob")
            self.extract()
            mlflow.log_param("env", self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            self.transform()
            self.load()
            mlflow.log_metric("record_count", self.final_df.count())
            mlflow.log_artifact(self.config_path)
