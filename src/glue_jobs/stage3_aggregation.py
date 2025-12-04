# glue/rwa_stage3_aggregation.py
"""
Basel III RWA Aggregation Layer
Creates summary tables for regulatory reporting and dashboards
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

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME", "execution_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

execution_date = args["execution_date"]

PROCESSED_PATH = "s3://bank-datalake/processed"

print(f"Starting aggregation for date: {execution_date}")

# ============================================================================
# STEP 1: Load Detail RWA Data
# ============================================================================

rwa_detail = (
    spark.read.format("delta")
    .load(f"{PROCESSED_PATH}/rwa_detail/")
    .filter(F.col("calculation_date") == execution_date)
)

# ============================================================================
# STEP 2: Summary by Asset Class & Geography
# ============================================================================

summary_asset_geo = rwa_detail.groupBy(
    "calculation_date", "asset_class", "geography", "rating_bucket"
).agg(
    F.count("loan_id").alias("loan_count"),
    F.sum("outstanding_balance_usd").alias("total_exposure"),
    F.sum("exposure_at_default").alias("total_ead"),
    F.sum("risk_weighted_assets").alias("total_rwa"),
    F.sum("regulatory_capital").alias("total_regulatory_capital"),
    F.sum("expected_loss").alias("total_expected_loss"),
    F.avg("risk_weight").alias("avg_risk_weight"),
    F.avg("probability_of_default").alias("avg_pd"),
    F.avg("loss_given_default").alias("avg_lgd"),
    F.sum(F.when(F.col("high_risk_flag") == True, 1).otherwise(0)).alias(
        "high_risk_count"
    ),
    F.avg("data_quality_score").alias("avg_data_quality"),
)

# Calculate derived metrics
summary_asset_geo = summary_asset_geo.withColumn(
    "capital_ratio", F.col("total_regulatory_capital") / F.col("total_exposure")
).withColumn("rwa_density", F.col("total_rwa") / F.col("total_exposure"))

# ============================================================================
# STEP 3: Trend Analysis (Compare with Previous Days)
# ============================================================================

# Load last 7 days for trend calculation
historical_data = (
    spark.read.format("delta")
    .load(f"{PROCESSED_PATH}/rwa_summary/")
    .filter(F.col("calculation_date") >= F.date_sub(F.lit(execution_date), 7))
)

# Calculate day-over-day changes
window_spec = Window.partitionBy("asset_class", "geography", "rating_bucket").orderBy(
    "calculation_date"
)

trend_df = (
    summary_asset_geo.unionByName(historical_data, allowMissingColumns=True)
    .withColumn("prev_total_rwa", F.lag("total_rwa", 1).over(window_spec))
    .withColumn(
        "rwa_change_pct",
        (
            (F.col("total_rwa") - F.col("prev_total_rwa"))
            / F.col("prev_total_rwa")
            * 100
        ),
    )
    .filter(F.col("calculation_date") == execution_date)
    .drop("prev_total_rwa")
)

# ============================================================================
# STEP 4: Regulatory Reporting Aggregates
# ============================================================================

# Basel III Pillar 1 - Total Capital Requirement
total_capital_req = (
    rwa_detail.agg(
        F.sum("risk_weighted_assets").alias("total_rwa"),
        F.sum("regulatory_capital").alias("total_tier1_capital_req"),
    )
    .withColumn("calculation_date", F.lit(execution_date).cast("date"))
    .withColumn("report_type", F.lit("PILLAR1_TOTAL_CAPITAL"))
)

# By Exposure Class (Basel reporting format)
exposure_class_summary = rwa_detail.groupBy("calculation_date", "asset_class").agg(
    F.sum("outstanding_balance_usd").alias("original_exposure"),
    F.sum("ead_after_crm").alias("exposure_after_crm"),
    F.sum("risk_weighted_assets").alias("rwa"),
    F.avg("risk_weight").alias("average_risk_weight"),
    F.count("*").alias("number_of_obligors"),
)

# Concentration Risk - Top 20 Customers
top_customers = (
    rwa_detail.groupBy("customer_id")
    .agg(
        F.sum("risk_weighted_assets").alias("customer_rwa"),
        F.sum("outstanding_balance_usd").alias("customer_exposure"),
        F.count("loan_id").alias("loan_count"),
    )
    .orderBy(F.desc("customer_rwa"))
    .limit(20)
    .withColumn("calculation_date", F.lit(execution_date).cast("date"))
)

# Geographic Concentration
geo_concentration = (
    rwa_detail.groupBy("geography")
    .agg(
        F.sum("risk_weighted_assets").alias("geo_rwa"),
        F.sum("outstanding_balance_usd").alias("geo_exposure"),
    )
    .withColumn("calculation_date", F.lit(execution_date).cast("date"))
)

# ============================================================================
# STEP 5: Dashboard KPIs
# ============================================================================

# Executive Dashboard Metrics
kpi_summary = spark.createDataFrame(
    [
        (
            execution_date,
            rwa_detail.agg(F.sum("risk_weighted_assets")).collect()[0][0],
            rwa_detail.agg(F.sum("regulatory_capital")).collect()[0][0],
            rwa_detail.agg(F.sum("outstanding_balance_usd")).collect()[0][0],
            rwa_detail.agg(F.count("loan_id")).collect()[0][0],
            rwa_detail.agg(F.countDistinct("customer_id")).collect()[0][0],
            rwa_detail.filter(F.col("high_risk_flag") == True).count(),
            rwa_detail.agg(F.avg("risk_weight")).collect()[0][0],
        )
    ],
    [
        "calculation_date",
        "total_rwa",
        "total_capital",
        "total_exposure",
        "total_loans",
        "total_customers",
        "high_risk_loans",
        "avg_risk_weight",
    ],
)

# ============================================================================
# STEP 6: Write Aggregated Tables
# ============================================================================

# Main summary table for Qlik Sense
trend_df.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"calculation_date = '{execution_date}'"
).partitionBy("calculation_date").save(f"{PROCESSED_PATH}/rwa_summary/")

# Regulatory reports
exposure_class_summary.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"calculation_date = '{execution_date}'"
).save(f"{PROCESSED_PATH}/regulatory_exposure_class/")

top_customers.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"calculation_date = '{execution_date}'"
).save(f"{PROCESSED_PATH}/top_customers/")

geo_concentration.write.format("delta").mode("overwrite").option(
    "replaceWhere", f"calculation_date = '{execution_date}'"
).save(f"{PROCESSED_PATH}/geographic_concentration/")

kpi_summary.write.format("delta").mode("append").save(
    f"{PROCESSED_PATH}/kpi_dashboard/"
)

# ============================================================================
# STEP 7: Export to Parquet for Redshift Load
# ============================================================================

# Prepare data for Redshift (optimized format)
trend_df.coalesce(10).write.mode("overwrite").parquet(
    f"s3://bank-datalake/redshift-staging/rwa_summary/date={execution_date}/"
)

rwa_detail.select(
    "calculation_date",
    "loan_id",
    "customer_id",
    "asset_class",
    "geography",
    "outstanding_balance_usd",
    "risk_weighted_assets",
    "regulatory_capital",
    "risk_weight",
    "rating_bucket",
    "high_risk_flag",
).coalesce(50).write.mode("overwrite").parquet(
    f"s3://bank-datalake/redshift-staging/rwa_detail/date={execution_date}/"
)

print(f"Aggregation completed for {execution_date}")
job.commit()
