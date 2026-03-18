
import pyspark.sql.functions as F
from pyspark.sql.functions import col, trim

def check_not_null(df, column):
    return df.filter(f"{column} IS NULL").count() == 0


def check_no_duplicates(df, column):
    return df.count() == df.dropDuplicates([column]).count()


def check_positive(df, column):
    return df.filter(f"{column} < 0").count() == 0


def check_not_empty(df):
    return df.count() > 0