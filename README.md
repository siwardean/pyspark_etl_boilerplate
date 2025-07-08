
# PySpark ETL Boilerplate

This project provides a modular and extensible PySpark ETL pipeline based on a legacy Scala Spark implementation, restructured for Python and modern data workflows with S3 and PostgreSQL.

## 📁 Project Structure

```
pyspark_etl_boilerplate/
├── main.py                        # Entry point for running ETL jobs
├── requirements.txt              # Python dependencies
├── README.md                     # Project documentation
├── .github/workflows/ci.yml      # GitHub Actions CI setup
├── .gitlab-ci.yml                # GitLab CI/CD pipeline
├── etl/
│   ├── etl_config.py             # Loads job configuration from .properties file
│   ├── etl_interface.py          # Abstract base ETL class
│   └── my_etl_job.py             # Sample ETL job implementing extract/transform/load
├── utils/
│   ├── args_management.py        # Argument parser (class name, package, config path)
│   ├── config.py                 # Global job UID, SparkSession, staging path
│   ├── date_management.py        # Date and time utilities
│   ├── file_management.py        # Read/write CSV/Parquet and incremental logic
│   ├── kafka_management.py       # Kafka writer utility (optional)
│   └── toolbox.py                # DataFrame utility functions (case, skew join, filter...)
├── config/
│   └── example.properties        # Sample configuration file
├── tests/
│   └── test_my_etl_job.py        # Unit tests using pytest
├── workflows/
│   └── domino_job.yaml           # Example Domino Workflow job config
```

## 🚀 Running a Job

You can run a job using `spark-submit`:

```bash
spark-submit   --master local[*]   main.py   --className MyETLJob   --classPackage etl   --confPath config/example.properties
```

## ⚙️ Configuration File Example

Create a file `config/example.properties` with the following content:

```properties
# Spark environment name
spark.env=dev

# Output mode (file or table)
spark.targetstorage=file
spark.targetfilepath=s3://your-bucket/output/my_data

# Load method (full or incremental)
spark.loadMethod=full

# Enable partitioning
spark.ispartitioned=true
spark.partitioncolumnlist=country,year

# Optional Kafka support
kafka.insertintokafka=false
# kafka.destinationtopic=my-topic
# kafka.brokerlist=localhost:9092
```

## 🧪 Running Tests

Make sure `pytest` is installed:

```bash
pip install -r requirements.txt
pytest tests/
```

## ⚙️ CI/CD

### GitHub Actions
Defined in `.github/workflows/ci.yml`:
- Runs tests and linting
- Validates configuration

### GitLab CI
Configured in `.gitlab-ci.yml`:
- Includes a test stage
- Easy to adapt for Domino job triggers

## 🧠 MLFlow & Domino Orchestration

This project supports MLFlow as the orchestrator:

### MLFlow Job Example

Inside your ETL job (e.g., `my_etl_job.py`):

```python
import mlflow

with mlflow.start_run():
    mlflow.set_tag("job", "MyETLJob")
    mlflow.log_param("env", ETLConfig.getEnv())
    mlflow.log_param("load_type", ETLConfig.getLoadType())
    mlflow.log_metric("record_count", finalDfToLoad.count())
    df.write.parquet(output_path)  # or any save logic
    mlflow.log_artifact(config_path)
```

### Domino Workflow YAML

Save as `workflows/domino_job.yaml`:

```yaml
apiVersion: domino/v1
kind: Run
metadata:
  name: etl-job
spec:
  title: "Run MyETLJob"
  command: "spark-submit main.py --className MyETLJob --classPackage etl --confPath config/example.properties"
  environment:
    variables:
      MLFLOW_TRACKING_URI: "https://your-domino-url.com/mlflow"
```

## 📦 Requirements

```text
pyspark
kafka-python
boto3
mlflow
```

These are listed in `requirements.txt`.

## 🛠 Development Tips

- Implement new jobs by extending `ETLInterface` in the `etl/` folder.
- Use `etl_config.py` to define and validate runtime behavior.
- Data sources currently supported: CSV/Parquet from S3, PostgreSQL (stub), Kafka (optional).

---

For additional help or contribution, feel free to open an issue or PR.
