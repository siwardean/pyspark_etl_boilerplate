from etl.etl_interface import ETLInterface
from etl.etl_config import ETLConfig
import mlflow

class MyETLJob(ETLInterface):
    def extract(self):
        self.config = ETLConfig(self.config_path)
        path = self.config.get("spark.targetfilepath")
        self.df = self.spark.read.option("header", "true").csv(path)

    def transform(self):
        self.final_df = self.df.dropna()

    def load(self):
        with mlflow.start_run():
            mlflow.set_tag("job", "MyETLJob")
            mlflow.log_param("env", self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            mlflow.log_metric("record_count", self.final_df.count())
            output_path = self.config.get("spark.targetfilepath") + "_output"
            self.final_df.write.mode("overwrite").parquet(output_path)
            mlflow.log_artifact(self.config_path)
