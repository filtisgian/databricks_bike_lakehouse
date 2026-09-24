
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

def remove_duplicates(df, key):
    return df.dropDuplicates([key])

## Unit tests for the functions
def run_tests():
    results = []
    
    # Test 1: null check passes
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    passed = check_not_null(df, "id") == True
    results.append(("null check - no nulls", "PASS" if passed else "FAIL"))
    
    # Test 2: null check fails correctly
    df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "name"])
    passed = check_not_null(df, "id") == False
    results.append(("null check - with null", "PASS" if passed else "FAIL"))
    
    # Test 3: duplicates
    df = spark.createDataFrame([(1,"a"),(1,"a"),(2,"b")], ["id","name"])
    passed = remove_duplicates(df, "id").count() == 2
    results.append(("remove duplicates", "PASS" if passed else "FAIL"))

    df = spark.createDataFrame([(1, 5.0), (2, 10.0)], ["id", "amount"])
    passed = check_positive(df, "id") == True
    results.append(("check positive", "PASS" if passed else "FAIL"))
    
    for name, status in results:
        print(f"{'✅' if status == 'PASS' else '❌'} {name}: {status}")

run_tests()
