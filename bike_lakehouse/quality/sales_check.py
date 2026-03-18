from bike_lakehouse.quality.data_quality_checks import *

def check_no_duplicates_2(df, column1,column2):
    return df.count() == df.dropDuplicates([column1,column2]).count()

def validate_sales(df):

    assert check_not_empty(df), "Sales table is empty"

    assert check_not_null(df, "order_number"), "order_number contains NULLs"

    assert check_no_duplicates_2(df, "order_number","product_key"), "Duplicate order_number and product_key found"