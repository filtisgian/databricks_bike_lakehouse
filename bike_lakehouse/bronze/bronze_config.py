INGESTION_CONFIG=[
    {
        "source": "cust_info",
        "path": "/Volumes/workspace/bronze/raw_sources/source_crm/cust_info.csv",
        "table": "crm_cust_info"
    },
    {
        "source": "prd_info",
        "path": "/Volumes/workspace/bronze/raw_sources/source_crm/prd_info.csv",
        "table": "crm_prd_info"
    },
    {
        "source": "sales_details",
        "path": "/Volumes/workspace/bronze/raw_sources/source_crm/sales_details.csv",
        "table": "crm_sales_details"
    },
    {
        "source": "CUST_AZ12",
        "path": "/Volumes/workspace/bronze/raw_sources/source_erp/CUST_AZ12.csv",
        "table": "erp_cust_az12"
    },
    {
        "source": "LOC_A101",
        "path": "/Volumes/workspace/bronze/raw_sources/source_erp/LOC_A101.csv",
        "table": "erp_loc_a101"
    },
    {
        "source": "PX_CAT_G1V2",
        "path": "/Volumes/workspace/bronze/raw_sources/source_erp/PX_CAT_G1V2.csv",
        "table": "erp_px_cat_g1v2"
    }
]