from bike_lakehouse.quality.data_quality_checks import *

def validate_products(df):

    assert check_not_empty(df), "Products table is empty"

    assert check_not_null(df, "product_id"), "product_id contains NULLs"

    assert check_no_duplicates(df, "product_id"), "Duplicate product_id found"