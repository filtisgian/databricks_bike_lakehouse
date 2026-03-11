import pyspark.sql.functions as F
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


# Get row and column count
def row_col_count(df):
    print(f"Row count: {df.count()}")
    print(f"Column count: {len(df.columns)}")

#Trims leading and trailing spaces from all STRING columns in a DataFrame.
def trim_string_columns(df):
    return df.select([
        trim(col(c)).alias(c) if isinstance(df.schema[c].dataType, StringType) else col(c)
        for c in df.columns
    ])

def trim_single_column(df, column):
  return df.withColumn(column, trim(col(column) ))

def rename_columns(df, column_map):
  for old_name, new_name in column_map.items():
    df = df.withColumnRenamed(old_name, new_name)
  return df

def apply_null_numeric_cleanup(df, columns):
  for column in columns:
    df = df.withColumn(column, F.when(col(column).isNull(), F.lit(0)).otherwise(col(column)))
  return df

#    Counts NULL values for each column in a DataFrame.
def nulls_count_per_column(df):
    return df.select([F.count(F.when(col(c).isNull(), c)).alias(c) for c in df.columns])


def replace_values(df, column, value_map):
  return df.replace(to_replace=value_map, subset=[column])


def apply_numeric_cleanup(df, columns):
  for column in columns:
    df = df.withColumn(column, F.regexp_replace(column, r'[^0-9]', ''))
  return df

def apply_date_cleanup(df, columns):
  for column in columns:
    df = df.withColumn(column, F.to_date(column, 'MM/dd/yyyy'))
  return df

