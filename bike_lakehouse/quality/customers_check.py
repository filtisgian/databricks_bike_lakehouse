from bike_lakehouse.quality.data_quality_checks import *

def validate_customers(df):

    assert check_not_empty(df), "Customers table is empty"

    assert check_not_null(df, "customer_id"), "customer_id contains NULLs"

    assert check_no_duplicates(df, "customer_id"), "Duplicate customer_id found"