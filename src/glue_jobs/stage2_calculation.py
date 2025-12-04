# glue/rwa_stage2_calculation.py
"""
Basel III Risk-Weighted Assets Calculation
Implements Standardized Approach for Credit Risk
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import json

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME", "execution_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

execution_date = args["execution_date"]

# Spark Optimization Configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Paths
STAGING_PATH = "s3://bank-datalake/staging"
PROCESSED_PATH = "s3://bank-datalake/processed"
REFERENCE_PATH = "s3://bank-datalake/reference"

print(f"Starting RWA calculation for date: {execution_date}")

# ============================================================================
# STEP 1: Load Clean Data from Stage 1
# ============================================================================

loans_df = spark.read.format("delta").load(f"{STAGING_PATH}/loans_clean/")
collateral_df = spark.read.format("delta").load(f"{STAGING_PATH}/collateral_clean/")
counterparty_df = spark.read.format("delta").load(f"{STAGING_PATH}/counterparty_clean/")
ratings_df = spark.read.format("delta").load(f"{STAGING_PATH}/ratings_clean/")
fx_rates_df = spark.read.format("delta").load(f"{STAGING_PATH}/fx_rates_clean/")

print(f"Loaded {loans_df.count():,} loans for calculation")

# ============================================================================
# STEP 2: Load Reference Data (Basel Parameters)
# ============================================================================

# Basel III Risk Weight Mapping by Asset Class and Rating
# These would normally come from a reference table maintained by Risk team
risk_weight_map = spark.createDataFrame(
    [
        ("RETAIL_MORTGAGE", "AAA", 0.35),
        ("RETAIL_MORTGAGE", "AA", 0.35),
        ("RETAIL_MORTGAGE", "A", 0.35),
        ("RETAIL_MORTGAGE", "BBB", 0.50),
        ("RETAIL_MORTGAGE", "BB", 0.75),
        ("RETAIL_MORTGAGE", "B", 1.00),
        ("RETAIL_MORTGAGE", "CCC", 1.50),
        ("RETAIL_MORTGAGE", "DEFAULT", 0.35),
        ("RETAIL_OTHER", "AAA", 0.75),
        ("RETAIL_OTHER", "AA", 0.75),
        ("RETAIL_OTHER", "A", 0.75),
        ("RETAIL_OTHER", "BBB", 0.75),
        ("RETAIL_OTHER", "BB", 0.75),
        ("RETAIL_OTHER", "B", 0.75),
        ("RETAIL_OTHER", "DEFAULT", 0.75),
        ("CORPORATE_SME", "AAA", 0.20),
        ("CORPORATE_SME", "AA", 0.30),
        ("CORPORATE_SME", "A", 0.50),
        ("CORPORATE_SME", "BBB", 0.75),
        ("CORPORATE_SME", "BB", 1.00),
        ("CORPORATE_SME", "B", 1.50),
        ("CORPORATE_SME", "CCC", 1.50),
        ("CORPORATE_SME", "DEFAULT", 1.00),
        ("CORPORATE_LARGE", "AAA", 0.20),
        ("CORPORATE_LARGE", "AA", 0.30),
        ("CORPORATE_LARGE", "A", 0.50),
        ("CORPORATE_LARGE", "BBB", 1.00),
        ("CORPORATE_LARGE", "BB", 1.00),
        ("CORPORATE_LARGE", "B", 1.50),
        ("CORPORATE_LARGE", "CCC", 1.50),
        ("CORPORATE_LARGE", "DEFAULT", 1.50),
    ],
    ["asset_class", "rating_bucket", "risk_weight"],
)

# Credit Conversion Factor (CCF) for off-balance sheet items
ccf_map = spark.createDataFrame(
    [
        ("COMMITTED_LINE", 0.50),
        ("LETTER_OF_CREDIT", 0.20),
        ("GUARANTEE", 1.00),
        ("DERIVATIVE", 1.00),
        ("REPO", 0.00),
        ("ON_BALANCE", 1.00),
    ],
    ["exposure_type", "ccf"],
)

# Loss Given Default (LGD) by collateral type
lgd_map = spark.createDataFrame(
    [
        ("REAL_ESTATE_RESIDENTIAL", 0.35),
        ("REAL_ESTATE_COMMERCIAL", 0.40),
        ("CASH_DEPOSIT", 0.00),
        ("SECURITIES_LISTED", 0.25),
        ("SECURITIES_UNLISTED", 0.50),
        ("RECEIVABLES", 0.45),
        ("INVENTORY", 0.60),
        ("EQUIPMENT", 0.55),
        ("UNCOLLATERALIZED", 0.45),
    ],
    ["collateral_type", "lgd"],
)

# Broadcast small reference tables
risk_weight_map = F.broadcast(risk_weight_map)
ccf_map = F.broadcast(ccf_map)
lgd_map = F.broadcast(lgd_map)

# ============================================================================
# STEP 3: Currency Conversion to Base Currency (USD)
# ============================================================================

# Create FX rate lookup - optimize with broadcast
fx_pivot = (
    fx_rates_df.select("currency_pair", "rate")
    .withColumn("base_ccy", F.substring("currency_pair", 1, 3))
    .withColumn("quote_ccy", F.substring("currency_pair", 4, 3))
    .select("quote_ccy", F.col("rate").alias("fx_rate"))
)

fx_pivot = F.broadcast(fx_pivot)

# Convert loan balances to USD
loans_usd = (
    loans_df.join(fx_pivot, loans_df.currency == fx_pivot.quote_ccy, "left")
    .withColumn("fx_rate", F.coalesce(F.col("fx_rate"), F.lit(1.0)))
    .withColumn(
        "outstanding_balance_usd", F.col("outstanding_balance") * F.col("fx_rate")
    )
    .drop("fx_rate", "quote_ccy")
)

# Convert collateral values to USD
collateral_usd = (
    collateral_df.join(fx_pivot, collateral_df.currency == fx_pivot.quote_ccy, "left")
    .withColumn("fx_rate", F.coalesce(F.col("fx_rate"), F.lit(1.0)))
    .withColumn(
        "adjusted_collateral_value_usd",
        F.col("adjusted_collateral_value") * F.col("fx_rate"),
    )
    .drop("fx_rate", "quote_ccy")
)

# ============================================================================
# STEP 4: Join Enrichment (Optimized with Salting for Skew)
# ============================================================================

# Identify skewed keys (large corporate borrowers with many loans)
skewed_customers = (
    loans_usd.groupBy("customer_id")
    .agg(F.count("*").alias("loan_count"))
    .filter(F.col("loan_count") > 100)
    .select("customer_id")
)

skewed_customers = F.broadcast(skewed_customers)

# Add salt column for skewed keys
loans_salted = (
    loans_usd.join(skewed_customers, "customer_id", "left")
    .withColumn(
        "is_skewed", F.when(F.col("loan_count").isNotNull(), True).otherwise(False)
    )
    .withColumn(
        "salt",
        F.when(F.col("is_skewed"), (F.rand() * 10).cast("int")).otherwise(F.lit(0)),
    )
    .withColumn(
        "customer_id_salted", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
    )
)

# Salt the dimension tables accordingly
counterparty_salted = (
    counterparty_df.join(skewed_customers, "customer_id", "inner")
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(10)])))
    .withColumn(
        "customer_id_salted", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
    )
    .unionByName(
        counterparty_df.join(skewed_customers, "customer_id", "left_anti")
        .withColumn("salt", F.lit(0))
        .withColumn("customer_id_salted", F.col("customer_id"))
    )
)

# Join with counterparty (salted to handle skew)
enriched_df = loans_salted.join(counterparty_salted, "customer_id_salted", "left").drop(
    "customer_id_salted", "salt", "is_skewed", "loan_count"
)

# Join with collateral (broadcast join - smaller table)
enriched_df = enriched_df.join(collateral_usd, "collateral_id", "left")

# Join with external ratings (broadcast)
ratings_broadcast = F.broadcast(ratings_df)
enriched_df = enriched_df.join(ratings_broadcast, "customer_id", "left")

print(f"Enrichment complete: {enriched_df.count():,} rows")

# ============================================================================
# STEP 5: Calculate Credit Risk Parameters
# ============================================================================

# 5.1: Determine Final Rating (priority: External > Internal > Default)
enriched_df = enriched_df.withColumn(
    "final_rating",
    F.coalesce(
        F.col("rating"),  # External rating
        F.col("internal_rating"),  # Internal rating
        F.lit("BBB"),  # Default rating if none available
    ),
).withColumn(
    "rating_bucket",
    F.when(F.col("final_rating").isin(["AAA", "AA+", "AA", "AA-"]), "AAA")
    .when(F.col("final_rating").isin(["A+", "A", "A-"]), "A")
    .when(F.col("final_rating").isin(["BBB+", "BBB", "BBB-"]), "BBB")
    .when(F.col("final_rating").isin(["BB+", "BB", "BB-"]), "BB")
    .when(F.col("final_rating").isin(["B+", "B", "B-"]), "B")
    .when(F.col("final_rating").isin(["CCC+", "CCC", "CCC-", "CC", "C"]), "CCC")
    .otherwise("BBB"),
)

# 5.2: Calculate Exposure at Default (EAD)
enriched_df = enriched_df.withColumn(
    "exposure_type",
    F.when(F.col("product_type").isin(["COMMITTED_LINE"]), "COMMITTED_LINE").otherwise(
        "ON_BALANCE"
    ),
)

enriched_df = (
    enriched_df.join(ccf_map, "exposure_type", "left")
    .withColumn("ccf", F.coalesce(F.col("ccf"), F.lit(1.0)))
    .withColumn("exposure_at_default", F.col("outstanding_balance_usd") * F.col("ccf"))
)

# 5.3: Calculate Credit Risk Mitigation (CRM) from Collateral
enriched_df = enriched_df.withColumn(
    "collateral_coverage_ratio",
    F.when(
        F.col("adjusted_collateral_value_usd").isNotNull(),
        F.least(
            F.col("adjusted_collateral_value_usd") / F.col("exposure_at_default"),
            F.lit(1.0),
        ),
    ).otherwise(F.lit(0.0)),
)

# Adjust EAD for collateral (simple approach - Basel allows more complex calculations)
enriched_df = enriched_df.withColumn(
    "ead_after_crm",
    F.col("exposure_at_default") * (1 - F.col("collateral_coverage_ratio")),
)

# 5.4: Join Risk Weights
enriched_df = enriched_df.join(
    risk_weight_map, ["asset_class", "rating_bucket"], "left"
).withColumn(
    "risk_weight", F.coalesce(F.col("risk_weight"), F.lit(1.0))
)  # Default 100% if not found

# 5.5: Calculate Risk-Weighted Assets (RWA)
enriched_df = enriched_df.withColumn(
    "risk_weighted_assets", F.col("ead_after_crm") * F.col("risk_weight")
)

# 5.6: Calculate Regulatory Capital Requirement (8% of RWA per Basel III)
enriched_df = enriched_df.withColumn(
    "regulatory_capital", F.col("risk_weighted_assets") * F.lit(0.08)
)

# 5.7: Calculate Probability of Default (PD) from rating
# Simplified mapping - real implementation uses detailed PD curves
pd_mapping = {
    "AAA": 0.0001,
    "AA": 0.0002,
    "A": 0.0005,
    "BBB": 0.001,
    "BB": 0.005,
    "B": 0.02,
    "CCC": 0.10,
    "DEFAULT": 1.0,
}
pd_map_expr = F.create_map([F.lit(x) for pair in pd_mapping.items() for x in pair])

enriched_df = enriched_df.withColumn(
    "probability_of_default", pd_map_expr[F.col("rating_bucket")]
)

# 5.8: Join LGD
enriched_df = enriched_df.join(lgd_map, "collateral_type", "left").withColumn(
    "loss_given_default", F.coalesce(F.col("lgd"), F.lit(0.45))
)  # Default LGD 45%

# 5.9: Calculate Expected Loss (EL) = EAD × PD × LGD
enriched_df = enriched_df.withColumn(
    "expected_loss",
    F.col("ead_after_crm")
    * F.col("probability_of_default")
    * F.col("loss_given_default"),
)

# ============================================================================
# STEP 6: Business Rules & Adjustments
# ============================================================================

# 6.1: Apply regulatory floors
enriched_df = enriched_df.withColumn(
    "risk_weight_floored",
    F.when(
        F.col("asset_class") == "RETAIL_MORTGAGE",
        F.greatest(F.col("risk_weight"), F.lit(0.10)),
    ).otherwise(  # 10% floor for mortgages
        F.col("risk_weight")
    ),
).withColumn(
    "risk_weighted_assets", F.col("ead_after_crm") * F.col("risk_weight_floored")
)

# 6.2: Flag high-risk loans for review
enriched_df = enriched_df.withColumn(
    "high_risk_flag",
    F.when(
        (F.col("risk_weight") > 1.0)
        | (F.col("probability_of_default") > 0.05)
        | (F.col("days_to_maturity") < 30),
        True,
    ).otherwise(False),
)

# 6.3: Add calculation metadata
enriched_df = (
    enriched_df.withColumn("calculation_date", F.lit(execution_date).cast("date"))
    .withColumn("calculation_method", F.lit("BASEL_III_STANDARDIZED"))
    .withColumn("basel_version", F.lit("3.0"))
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn(
        "data_quality_score",
        F.when(F.col("rating").isNotNull(), F.lit(1.0))  # Has external rating
        .when(F.col("internal_rating").isNotNull(), F.lit(0.8))  # Only internal
        .otherwise(F.lit(0.5)),
    )
)  # Default rating used

# ============================================================================
# STEP 7: Data Quality Validation
# ============================================================================

# Check for calculation anomalies
validation_df = enriched_df.agg(
    F.count("*").alias("total_loans"),
    F.sum("outstanding_balance_usd").alias("total_exposure"),
    F.sum("risk_weighted_assets").alias("total_rwa"),
    F.sum("regulatory_capital").alias("total_capital"),
    F.avg("risk_weight").alias("avg_risk_weight"),
    F.countDistinct("customer_id").alias("unique_customers"),
    F.sum(F.when(F.col("high_risk_flag") == True, 1).otherwise(0)).alias(
        "high_risk_count"
    ),
)

validation_metrics = validation_df.collect()[0]
print(
    f"""
Calculation Summary:
- Total Loans: {validation_metrics['total_loans']:,}
- Total Exposure: ${validation_metrics['total_exposure']:,.2f}
- Total RWA: ${validation_metrics['total_rwa']:,.2f}
- Total Capital: ${validation_metrics['total_capital']:,.2f}
- Avg Risk Weight: {validation_metrics['avg_risk_weight']:.4f}
- Unique Customers: {validation_metrics['unique_customers']:,}
- High Risk Loans: {validation_metrics['high_risk_count']:,}
"""
)

# Validate against business rules
assert validation_metrics["total_rwa"] > 0, "Total RWA cannot be zero"
assert validation_metrics["avg_risk_weight"] > 0, "Average risk weight must be positive"
assert (
    validation_metrics["total_rwa"] <= validation_metrics["total_exposure"] * 1.5
), "RWA exceeds 150% of exposure - possible calculation error"

# ============================================================================
# STEP 8: Write Results to Delta Lake (with Z-Ordering)
# ============================================================================

# Select final output columns
output_df = enriched_df.select(
    "calculation_date",
    "loan_id",
    "customer_id",
    "product_type",
    "asset_class",
    "country_code",
    F.col("country_code").substr(1, 2).alias("geography"),
    "outstanding_balance_usd",
    "credit_conversion_factor",
    "exposure_at_default",
    "ead_after_crm",
    "collateral_coverage_ratio",
    "final_rating",
    "rating_bucket",
    "probability_of_default",
    "loss_given_default",
    "expected_loss",
    "risk_weight",
    "risk_weight_floored",
    "risk_weighted_assets",
    "regulatory_capital",
    "calculation_method",
    "high_risk_flag",
    "data_quality_score",
    "created_timestamp",
)

# Write to Delta with partitioning
output_path = f"{PROCESSED_PATH}/rwa_detail/"

output_df.repartition(100, "asset_class", "geography").write.format("delta").mode(
    "overwrite"
).option("replaceWhere", f"calculation_date = '{execution_date}'").partitionBy(
    "calculation_date", "asset_class"
).save(
    output_path
)

# Optimize Delta table with Z-Ordering (for query performance)
deltaTable = DeltaTable.forPath(spark, output_path)
deltaTable.optimize().where(f"calculation_date = '{execution_date}'").executeZOrderBy(
    "geography", "rating_bucket"
)

print(f"RWA calculation completed successfully for {execution_date}")

# ============================================================================
# STEP 9: Generate Calculation Statistics for Monitoring
# ============================================================================

stats_df = output_df.groupBy("asset_class", "geography", "rating_bucket").agg(
    F.count("*").alias("loan_count"),
    F.sum("outstanding_balance_usd").alias("total_exposure"),
    F.sum("risk_weighted_assets").alias("total_rwa"),
    F.avg("risk_weight").alias("avg_risk_weight"),
    F.sum("regulatory_capital").alias("total_capital"),
    F.avg("probability_of_default").alias("avg_pd"),
    F.sum(F.when(F.col("high_risk_flag") == True, 1).otherwise(0)).alias(
        "high_risk_count"
    ),
)

# Write statistics for monitoring dashboard
stats_df.withColumn(
    "calculation_date", F.lit(execution_date).cast("date")
).write.format("delta").mode("overwrite").option(
    "replaceWhere", f"calculation_date = '{execution_date}'"
).save(
    f"{PROCESSED_PATH}/rwa_statistics/"
)

# Push metrics to CloudWatch
from boto3 import client

cloudwatch = client("cloudwatch")

cloudwatch.put_metric_data(
    Namespace="RWA-Pipeline",
    MetricData=[
        {
            "MetricName": "TotalRWA",
            "Value": float(validation_metrics["total_rwa"]),
            "Unit": "None",
            "Timestamp": datetime.utcnow(),
        },
        {
            "MetricName": "ProcessedLoans",
            "Value": float(validation_metrics["total_loans"]),
            "Unit": "Count",
            "Timestamp": datetime.utcnow(),
        },
        {
            "MetricName": "HighRiskLoans",
            "Value": float(validation_metrics["high_risk_count"]),
            "Unit": "Count",
            "Timestamp": datetime.utcnow(),
        },
    ],
)

job.commit()
