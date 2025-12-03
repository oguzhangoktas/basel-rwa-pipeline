# glue/rwa_stage1.py
"""
Basel III RWA Calculation Pipeline - Stage 1: Data Standardization

This module performs data cleaning, validation, and standardization of source data
before RWA calculations. Implements comprehensive data quality checks.

Author: Portfolio Project
Date: December 2025
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'execution_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

execution_date = args['execution_date']

# Configure Spark
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# S3 Paths
RAW_PATH = f"s3://bank-datalake/raw"
STAGING_PATH = f"s3://bank-datalake/staging"

print(f"Starting standardization for date: {execution_date}")

# ============================================================================
# STEP 1: Read Source Data
# ============================================================================

# Read loans (largest table - 120M rows)
loans_df = spark.read.parquet(f"{RAW_PATH}/loans/date={execution_date}/")
print(f"Loans loaded: {loans_df.count()} rows")

# Read collateral
collateral_df = spark.read.parquet(f"{RAW_PATH}/collateral/date={execution_date}/")

# Read counterparty
counterparty_df = spark.read.parquet(f"{RAW_PATH}/counterparty/date={execution_date}/")

# Read external ratings (CSV from SFTP)
ratings_df = spark.read.csv(
    f"{RAW_PATH}/ratings/date={execution_date}/",
    header=True,
    inferSchema=True
)

# Read FX rates (JSON from API)
fx_rates_df = spark.read.json(f"{RAW_PATH}/market_data/date={execution_date}/")

# ============================================================================
# STEP 2: Data Quality Checks
# ============================================================================

def check_nulls(df, critical_columns, table_name):
    """Check for nulls in critical columns"""
    for col in critical_columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / df.count()) * 100
        
        if null_pct > 1.0:  # More than 1% nulls
            raise ValueError(f"{table_name}.{col} has {null_pct:.2f}% nulls")
        
        print(f"{table_name}.{col}: {null_count} nulls ({null_pct:.4f}%)")

check_nulls(loans_df, ['loan_id', 'customer_id', 'outstanding_balance'], 'loans')
check_nulls(collateral_df, ['collateral_id', 'market_value'], 'collateral')

# Check for duplicates
loan_duplicates = loans_df.groupBy('loan_id').count().filter(F.col('count') > 1)
if loan_duplicates.count() > 0:
    raise ValueError(f"Found {loan_duplicates.count()} duplicate loan_ids")

# ============================================================================
# STEP 3: Standardization & Cleansing
# ============================================================================

# Standardize loans
loans_clean = loans_df \
    .withColumn('outstanding_balance', F.col('outstanding_balance').cast('decimal(18,2)')) \
    .withColumn('currency', F.upper(F.trim(F.col('currency')))) \
    .withColumn('product_type', F.upper(F.trim(F.col('product_type')))) \
    .withColumn('country_code', F.upper(F.trim(F.col('country_code')))) \
    .withColumn('internal_rating', F.trim(F.col('internal_rating'))) \
    .filter(F.col('outstanding_balance') > 0) \
    .filter(F.col('maturity_date') > F.current_date())

# Add derived fields
loans_clean = loans_clean \
    .withColumn('days_to_maturity', 
                F.datediff(F.col('maturity_date'), F.lit(execution_date))) \
    .withColumn('loan_age_days',
                F.datediff(F.lit(execution_date), F.col('origination_date'))) \
    .withColumn('asset_class',
                F.when(F.col('product_type').isin(['MORTGAGE', 'HOME_LOAN']), 'RETAIL_MORTGAGE')
                 .when(F.col('product_type').isin(['PERSONAL_LOAN', 'CREDIT_CARD']), 'RETAIL_OTHER')
                 .when(F.col('product_type').isin(['SME_LOAN']), 'CORPORATE_SME')
                 .when(F.col('product_type').isin(['CORPORATE_LOAN']), 'CORPORATE_LARGE')
                 .otherwise('OTHER'))

# Standardize collateral
collateral_clean = collateral_df \
    .withColumn('market_value', F.col('market_value').cast('decimal(18,2)')) \
    .withColumn('haircut_percentage', F.col('haircut_percentage').cast('decimal(5,4)')) \
    .withColumn('collateral_type', F.upper(F.trim(F.col('collateral_type')))) \
    .withColumn('adjusted_collateral_value',
                F.col('market_value') * (1 - F.col('haircut_percentage'))) \
    .withColumn('days_since_valuation',
                F.datediff(F.lit(execution_date), F.col('valuation_date')))

# Flag stale valuations (> 365 days old)
collateral_clean = collateral_clean \
    .withColumn('valuation_stale_flag',
                F.when(F.col('days_since_valuation') > 365, True).otherwise(False))

# Standardize counterparty
counterparty_clean = counterparty_df \
    .withColumn('customer_type', F.upper(F.trim(F.col('customer_type')))) \
    .withColumn('total_assets', F.col('total_assets').cast('decimal(18,2)'))

# Standardize ratings - handle multiple agencies
ratings_clean = ratings_df \
    .withColumn('rating_agency', F.upper(F.trim(F.col('rating_agency')))) \
    .withColumn('rating', F.upper(F.trim(F.col('rating'))))

# Deduplicate ratings (take most recent per customer per agency)
window_spec = Window.partitionBy('customer_id', 'rating_agency').orderBy(F.col('rating_date').desc())
ratings_clean = ratings_clean \
    .withColumn('rank', F.row_number().over(window_spec)) \
    .filter(F.col('rank') == 1) \
    .drop('rank')

# Map external ratings to internal scale
rating_mapping = {
    'AAA': 1, 'AA+': 2, 'AA': 2, 'AA-': 3,
    'A+': 4, 'A': 4, 'A-': 5,
    'BBB+': 6, 'BBB': 6, 'BBB-': 7,
    'BB+': 8, 'BB': 8, 'BB-': 9,
    'B+': 10, 'B': 10, 'B-': 11,
    'CCC+': 12, 'CCC': 12, 'CCC-': 12,
    'CC': 13, 'C': 13, 'D': 14
}

rating_map_expr = F.create_map([F.lit(x) for pair in rating_mapping.items() for x in pair])
ratings_clean = ratings_clean \
    .withColumn('rating_numeric', rating_map_expr[F.col('rating')])

# Convert FX rates to standard format
fx_rates_clean = fx_rates_df \
    .withColumn('currency_pair', F.upper(F.trim(F.col('currency_pair')))) \
    .withColumn('rate', F.col('rate').cast('decimal(18,8)'))

# ============================================================================
# STEP 4: Write to Staging (Delta Lake for ACID guarantees)
# ============================================================================

# Write with partitioning and optimization
loans_clean \
    .repartition(200, 'asset_class', 'country_code') \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .partitionBy('asset_class') \
    .save(f"{STAGING_PATH}/loans_clean/")

collateral_clean \
    .repartition(50, 'collateral_type') \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('collateral_type') \
    .save(f"{STAGING_PATH}/collateral_clean/")

counterparty_clean \
    .repartition(20) \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"{STAGING_PATH}/counterparty_clean/")

ratings_clean \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"{STAGING_PATH}/ratings_clean/")

fx_rates_clean \
    .write \
    .format('delta') \
    .mode('overwrite') \
    .save(f"{STAGING_PATH}/fx_rates_clean/")

print(f"Stage 1 standardization completed for {execution_date}")

# Write summary statistics
summary = {
    'execution_date': execution_date,
    'loans_count': loans_clean.count(),
    'collateral_count': collateral_clean.count(),
    'counterparty_count': counterparty_clean.count(),
    'ratings_count': ratings_clean.count()
}

print(f"Summary: {summary}")

job.commit()