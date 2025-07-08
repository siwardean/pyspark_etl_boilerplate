from pyspark.sql.functions import col

def drop_nulls(df):
    return df.na.drop()

def lowercase_columns(df):
    return df.select([col(c).alias(c.lower()) for c in df.columns])
