def run_quality_checks(df, checks):
    """
    df: DataFrame to check
    checks: list of dicts with rules
    """
    for check in checks:
        check_type = check["type"]
        column     = check["column"]

        assert df.count() > 0 , \
           "FAILED: table is empty"
        
        if check_type == "null_check":
            assert df.filter(f"{column} IS NULL").count() == 0, \
                f"FAILED: Nulls found in {column}"
                
        elif check_type == "duplicate_check":
            columns = [column] if isinstance(column, str) else column
            assert df.count() == df.dropDuplicates(columns).count(), \
                f"FAILED: Duplicates found in {columns}"
                
        elif check_type == "negative_check":
            assert df.filter(f"{column} < 0").count() == 0, \
                f"FAILED: Negative values found in {column}"
    
    print("All quality checks passed!!")



