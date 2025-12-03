-- ============================================================================
-- Athena External Tables for S3 Data Lake
-- 
-- Creates external tables pointing to S3 for ad-hoc querying
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS loans_raw (
    loan_id STRING,
    customer_id STRING,
    product_type STRING,
    origination_date DATE,
    maturity_date DATE,
    outstanding_balance DECIMAL(18,2),
    currency STRING,
    internal_rating STRING,
    country_code STRING
)
PARTITIONED BY (snapshot_date STRING)
STORED AS PARQUET
LOCATION 's3://bank-datalake/raw/loans/';

-- Add partitions
MSCK REPAIR TABLE loans_raw;

-- Query example:
-- SELECT * FROM loans_raw WHERE snapshot_date = '2025-12-02' LIMIT 10;
