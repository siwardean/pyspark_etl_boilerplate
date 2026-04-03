# PySpark ETL Boilerplate

A modular, extensible PySpark ETL framework with MLflow tracking and Cloud Object Storage support.

---

## Project Structure

```
pyspark_etl_boilerplate/
├── main.py                    # Entry point — dynamically loads and runs any ETL job
├── requirements.txt           # Python dependencies
├── config/
│   └── example.properties     # Configuration template
├── etl/
│   ├── etl_interface.py       # Abstract base class all ETL jobs must extend
│   ├── etl_config.py          # Config loader with COS placeholder resolution
│   └── my_etl_job.py          # Sample ETL job implementation
├── utils/
│   ├── args_management.py     # CLI argument parser
│   ├── config.py              # SparkSession factory and job UID
│   ├── date_management.py     # Date arithmetic utilities
│   ├── file_management.py     # Parquet write helpers
│   ├── kafka_management.py    # Optional Kafka sink
│   └── toolbox.py             # DataFrame transformation utilities
└── tests/
    └── test_my_etl_job.py     # Unit tests (pytest)
```

---

## ETL Execution Flow

### Startup and class loading

```
spark-submit main.py --className MyETLJob --classPackage etl --confPath config/example.properties
        │
        ▼
AppArgsManagement           parse --className / --classPackage / --confPath
        │
        ▼
importlib.import_module      dynamically import etl.myetljob
        │
        ▼
cls(conf_path)               instantiate the job — triggers __init__ chain
```

### Construction (`__init__`)

```
MyETLJob(conf_path)
        │
        └── ETLInterface.__init__(conf_path)
                │
                ├── ETLConfig(conf_path)          parse .properties file
                │       ├── read [SPARK_CONTEXT]
                │       ├── read [COS]            store bucket placeholders
                │       ├── read [DATABASE]       store database placeholders
                │       ├── read [SPARK]
                │       └── read [KAFKA]
                │
                └── Config.getSparkSession(config)
                        └── iterate [SPARK_CONTEXT] key-value pairs
                                └── builder.config(key, value) for each
                                └── builder.getOrCreate()
```

### Run (`run`)

```
etl_instance.run()
        │
        ▼
mlflow.start_run()
        │
        ├── log_param  env, load_type
        │
        ├── extract()
        │       └── config.get("spark.targetfilepath")
        │               └── _resolve()  →  {bucket} replaced by [COS] bucket value
        │               └── spark.read.csv(resolved_path)
        │
        ├── transform()
        │       └── drop_nulls(df)
        │
        ├── load()
        │       └── final_df.write.parquet(output_path)
        │
        ├── log_metric  record_count
        └── log_artifact  config file
```

---

## Configuration File

The `.properties` file is split into four sections:

```ini
# ── Spark session properties ────────────────────────────────────────────────
# Any valid Spark config key can be added here. All entries are applied
# to the SparkSession builder via builder.config(key, value).
[SPARK_CONTEXT]
spark.app.name=MyETLJob
spark.master=local[*]
spark.jars=/opt/spark/jars/postgresql-42.2.jar
spark.executor.memory=4g

# ── Cloud Object Storage ─────────────────────────────────────────────────────
# Keys defined here become placeholders usable in any other section value.
# Example: targetfilepath={bucket}/path  →  s3://my-bucket/path at runtime
[COS]
bucket=s3://my-data-bucket

# ── Database ──────────────────────────────────────────────────────────────────
# Keys defined here become placeholders usable in any other section value.
# Example: url=jdbc:postgresql://{host}:{port}/{dbname}
[DATABASE]
host=localhost
port=5432
dbname=my_database

# ── Job parameters ──────────────────────────────────────────────────────────
[SPARK]
env=dev
targetstorage=cos
targetfilepath={bucket}/input/data.csv
loadMethod=full
ispartitioned=true
partitioncolumnlist=country,year

# ── Kafka (optional) ─────────────────────────────────────────────────────────
[KAFKA]
insertintokafka=false
```

---

## Implementing a New ETL Job

Extend `ETLInterface` and implement the three abstract methods, then override `run()` to add MLflow tracking:

```python
from etl.etl_interface import ETLInterface
from utils.toolbox import drop_nulls
import mlflow


class MyNewJob(ETLInterface):
    def extract(self):
        path = self.config.get("spark.targetfilepath")  # {bucket} resolved automatically
        self.df = self.spark.read.parquet(path)

    def transform(self):
        self.final_df = drop_nulls(self.df)

    def load(self):
        out = self.config.get("spark.targetfilepath") + "_output"
        self.final_df.write.mode("overwrite").parquet(out)

    def run(self):
        with mlflow.start_run():
            mlflow.set_tag("job", "MyNewJob")
            mlflow.log_param("env", self.config.get("spark.env"))
            mlflow.log_param("load_type", self.config.get("spark.loadMethod"))
            self.extract()
            self.transform()
            self.load()
            mlflow.log_metric("record_count", self.final_df.count())
            mlflow.log_artifact(self.config_path)
```

Run it without changing `main.py`:

```bash
spark-submit main.py \
  --className MyNewJob \
  --classPackage etl \
  --confPath config/mynewjob.properties
```

---

## MLflow Tracking

Every run logs the following to the configured MLflow tracking server:

| Type | Key | Value |
|------|-----|-------|
| Tag | `job` | class name |
| Param | `env` | value of `spark.env` |
| Param | `load_type` | value of `spark.loadMethod` |
| Metric | `record_count` | number of rows written |
| Artifact | — | the `.properties` config file |

Set the tracking URI before running:

```bash
export MLFLOW_TRACKING_URI=http://your-mlflow-server:5000
```

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/
```

---

## Requirements

```
pyspark
mlflow
kafka-python
boto3
pytest
```
