from pyspark.sql import DataFrame

def save_as_parquet(df: DataFrame, path: str, partition_cols: list = []):
    if partition_cols:
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
