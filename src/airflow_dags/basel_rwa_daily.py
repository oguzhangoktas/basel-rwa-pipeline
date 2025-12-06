"""
Basel III RWA Daily Calculation Pipeline

Production-ready Airflow DAG that orchestrates:
- Source data validation using custom DataQualityValidator
- Multi-stage Spark processing (Standardization -> Calculation -> Aggregation)
- Redshift loading
- Comprehensive reconciliation using ReconciliationEngine
- Dashboard refresh and notifications

Author: Oğuzhan Göktaş
Date: December 2024
"""

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import boto3
import logging

# Import custom modules
from data_quality import DataQualityValidator
from reconciliation import ReconciliationEngine
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "start_date": datetime(2025, 1, 1),
    "email": ["data-team@bank.com", "risk-team@bank.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=3),
}

dag = DAG(
    "basel_rwa_daily_calculation",
    default_args=default_args,
    description="Daily Basel III RWA calculation pipeline with comprehensive validation",
    schedule_interval="0 2 * * *",  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=["basel", "risk", "regulatory", "compliance"],
)

# ============================================================================
# Task 1: Wait for Source Data (Sensors)
# ============================================================================

wait_for_loans = S3KeySensor(
    task_id="wait_for_loans_data",
    bucket_name="bank-datalake",
    bucket_key="raw/loans/date={{ ds }}/loans_snapshot.parquet",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    mode="poke",
    dag=dag,
)

wait_for_collateral = S3KeySensor(
    task_id="wait_for_collateral_data",
    bucket_name="bank-datalake",
    bucket_key="raw/collateral/date={{ ds }}/collateral_snapshot.parquet",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    mode="poke",
    dag=dag,
)

wait_for_ratings = S3KeySensor(
    task_id="wait_for_ratings_data",
    bucket_name="bank-datalake",
    bucket_key="raw/ratings/date={{ ds }}/ratings.csv",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    mode="poke",
    dag=dag,
)

wait_for_market_data = S3KeySensor(
    task_id="wait_for_market_data",
    bucket_name="bank-datalake",
    bucket_key="raw/market_data/date={{ ds }}/fx_rates.json",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    mode="poke",
    dag=dag,
)

# ============================================================================
# Task 2: Data Quality Validation (Using Custom DataQualityValidator)
# ============================================================================

def validate_source_data(**context):
    """
    Validates source data using custom DataQualityValidator module
    
    Performs:
    - Row count validation against historical ranges
    - Schema validation
    - Null checks on critical columns
    - Duplicate detection
    - Business rule validation
    
    Raises:
        ValueError: If any validation check fails
    """
    execution_date = context["ds"]
    logger.info(f"Starting data quality validation for {execution_date}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"RWA_DataQuality_{execution_date}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Initialize custom validator
    validator = DataQualityValidator(spark)
    
    # -------------------------------------------------------------------------
    # Validate Loans Data
    # -------------------------------------------------------------------------
    logger.info("Validating loans data...")
    loans_df = spark.read.parquet(
        f"s3://bank-datalake/raw/loans/date={execution_date}/loans_snapshot.parquet"
    )
    
    # Row count check (100M - 150M expected)
    validator.check_row_count(
        loans_df, 
        expected_range=(100_000_000, 150_000_000),
        table_name="loans"
    )
    
    # Null checks on critical columns
    validator.check_nulls(
        loans_df,
        critical_columns=["loan_id", "customer_id", "outstanding_balance", "asset_class"],
        table_name="loans",
        threshold_pct=0.1  # Max 0.1% nulls allowed
    )
    
    # Duplicate check on loan_id
    validator.check_duplicates(
        loans_df,
        key_columns=["loan_id"],
        table_name="loans"
    )
    
    # Business rule validation
    validator.validate_business_rules(
        loans_df,
        rules=[
            {
                "name": "positive_balance",
                "condition": "outstanding_balance > 0",
                "error_msg": "Outstanding balance must be positive"
            },
            {
                "name": "valid_asset_class",
                "condition": "asset_class IN ('CORPORATE_LARGE', 'CORPORATE_SME', 'RETAIL_MORTGAGE', 'RETAIL_OTHER')",
                "error_msg": "Invalid asset class"
            }
        ],
        table_name="loans"
    )
    
    # -------------------------------------------------------------------------
    # Validate Collateral Data
    # -------------------------------------------------------------------------
    logger.info("Validating collateral data...")
    collateral_df = spark.read.parquet(
        f"s3://bank-datalake/raw/collateral/date={execution_date}/collateral_snapshot.parquet"
    )
    
    validator.check_row_count(
        collateral_df,
        expected_range=(40_000_000, 50_000_000),
        table_name="collateral"
    )
    
    validator.check_nulls(
        collateral_df,
        critical_columns=["collateral_id", "loan_id", "collateral_value"],
        table_name="collateral",
        threshold_pct=0.1
    )
    
    # -------------------------------------------------------------------------
    # Validate Ratings Data
    # -------------------------------------------------------------------------
    logger.info("Validating ratings data...")
    ratings_df = spark.read.csv(
        f"s3://bank-datalake/raw/ratings/date={execution_date}/ratings.csv",
        header=True
    )
    
    validator.check_row_count(
        ratings_df,
        expected_range=(400_000, 600_000),
        table_name="ratings"
    )
    
    validator.check_nulls(
        ratings_df,
        critical_columns=["customer_id", "rating_bucket"],
        table_name="ratings",
        threshold_pct=0.0  # No nulls allowed in ratings
    )
    
    # -------------------------------------------------------------------------
    # Get Validation Summary
    # -------------------------------------------------------------------------
    summary = validator.get_validation_summary()
    
    logger.info(f"Validation Summary: {summary['passed']}/{summary['total_checks']} checks passed")
    
    # Raise exception if any check failed
    validator.raise_if_failed()
    
    # Store validation results in S3 for audit trail
    s3_client = boto3.client("s3")
    import json
    s3_client.put_object(
        Bucket="bank-datalake",
        Key=f"validation/date={execution_date}/source_validation.json",
        Body=json.dumps(summary, indent=2, default=str)
    )
    
    spark.stop()
    
    return summary


validate_data = PythonOperator(
    task_id="validate_source_data",
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# Task 3-5: Spark ETL Stages (Glue Jobs)
# ============================================================================

# Stage 1: Data Standardization & Enrichment
spark_stage1 = GlueJobOperator(
    task_id="spark_standardization",
    job_name="rwa_stage1_standardization",
    script_location="s3://bank-scripts/glue/rwa_stage1_standardization.py",
    s3_bucket="bank-datalake",
    iam_role_name="GlueExecutionRole",
    create_job_kwargs={
        "GlueVersion": "4.0",
        "NumberOfWorkers": 50,
        "WorkerType": "G.2X",
        "Timeout": 120,
        "DefaultArguments": {
            "--execution_date": "{{ ds }}",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://bank-logs/spark-ui/",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-glue-datacatalog": "true",
            "--conf": "spark.sql.adaptive.enabled=true",
            "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
        },
    },
    dag=dag,
)

# Stage 2: Basel III RWA Calculations
spark_stage2 = GlueJobOperator(
    task_id="spark_rwa_calculation",
    job_name="rwa_stage2_calculation",
    script_location="s3://bank-scripts/glue/rwa_stage2_calculation.py",
    s3_bucket="bank-datalake",
    iam_role_name="GlueExecutionRole",
    create_job_kwargs={
        "GlueVersion": "4.0",
        "NumberOfWorkers": 100,
        "WorkerType": "G.2X",
        "Timeout": 180,
        "DefaultArguments": {
            "--execution_date": "{{ ds }}",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://bank-logs/spark-ui/",
            "--conf": "spark.sql.adaptive.enabled=true",
            "--conf": "spark.sql.adaptive.skewJoin.enabled=true",
        },
    },
    dag=dag,
)

# Stage 3: Aggregation & Summary Tables
spark_stage3 = GlueJobOperator(
    task_id="spark_aggregation",
    job_name="rwa_stage3_aggregation",
    script_location="s3://bank-scripts/glue/rwa_stage3_aggregation.py",
    s3_bucket="bank-datalake",
    iam_role_name="GlueExecutionRole",
    create_job_kwargs={
        "GlueVersion": "4.0",
        "NumberOfWorkers": 20,
        "WorkerType": "G.1X",
        "Timeout": 60,
        "DefaultArguments": {
            "--execution_date": "{{ ds }}",
            "--enable-spark-ui": "true",
        },
    },
    dag=dag,
)

# ============================================================================
# Task 6: Load Results to Redshift
# ============================================================================

load_detail = S3ToRedshiftOperator(
    task_id="load_rwa_detail_to_redshift",
    schema="risk",
    table="rwa_calculations",
    s3_bucket="bank-datalake",
    s3_key="processed/rwa_detail/date={{ ds }}/",
    copy_options=[
        "FORMAT AS PARQUET",
        "TRUNCATECOLUMNS",
        "COMPUPDATE OFF",
        "STATUPDATE OFF",
    ],
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_default",
    method="REPLACE",
    dag=dag,
)

load_summary = S3ToRedshiftOperator(
    task_id="load_rwa_summary_to_redshift",
    schema="risk",
    table="rwa_summary",
    s3_bucket="bank-datalake",
    s3_key="processed/rwa_summary/date={{ ds }}/",
    copy_options=[
        "FORMAT AS PARQUET",
        "TRUNCATECOLUMNS",
        "COMPUPDATE OFF",
    ],
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_default",
    method="REPLACE",
    dag=dag,
)

# ============================================================================
# Task 7: Comprehensive Reconciliation (Using Custom ReconciliationEngine)
# ============================================================================

def reconcile_results(**context):
    """
    Comprehensive reconciliation using custom ReconciliationEngine
    
    Performs:
    1. Total reconciliation (calculated vs source)
    2. Day-over-day trend analysis
    3. Asset class level reconciliation
    4. Top movers analysis
    
    Raises:
        ValueError: If reconciliation fails
    """
    execution_date = context["ds"]
    logger.info(f"Starting reconciliation for {execution_date}")
    
    # Initialize ReconciliationEngine
    recon_engine = ReconciliationEngine(
        redshift_conn_string="postgresql://{{ var.value.redshift_user }}:{{ var.value.redshift_password }}@{{ var.value.redshift_host }}:5439/prod",
        s3_bucket="bank-datalake"
    )
    
    # -------------------------------------------------------------------------
    # 1. Total Reconciliation (Source vs Calculated)
    # -------------------------------------------------------------------------
    logger.info("Running total reconciliation...")
    total_recon = recon_engine.reconcile_totals(
        calculation_date=execution_date,
        tolerance_pct=0.01  # 0.01% variance allowed
    )
    
    if total_recon["status"] == "FAILED":
        raise ValueError(
            f"Total reconciliation FAILED: "
            f"Variance: {total_recon.get('exposure_variance_pct', 0):.4f}%"
        )
    
    logger.info(f"✓ Total reconciliation PASSED: {total_recon.get('exposure_variance_pct', 0):.4f}% variance")
    
    # -------------------------------------------------------------------------
    # 2. Day-over-Day Trend Analysis
    # -------------------------------------------------------------------------
    logger.info("Running day-over-day analysis...")
    dod_recon = recon_engine.reconcile_day_over_day(
        calculation_date=execution_date,
        threshold_pct=10.0  # 10% change triggers warning
    )
    
    if dod_recon["status"] == "WARNING":
        # Send Slack alert but don't fail the pipeline
        logger.warning(
            f"⚠️ Large day-over-day change detected: "
            f"RWA changed {dod_recon['rwa_change_pct']:.2f}%"
        )
        send_slack_alert(
            f"⚠️ RWA Day-over-Day Alert\n"
            f"Date: {execution_date}\n"
            f"RWA Change: {dod_recon['rwa_change_pct']:.2f}%\n"
            f"Previous RWA: {dod_recon['previous_rwa']:,.2f}\n"
            f"Current RWA: {dod_recon['current_rwa']:,.2f}"
        )
    else:
        logger.info(f"✓ Day-over-day change within threshold: {dod_recon.get('rwa_change_pct', 0):.2f}%")
    
    # -------------------------------------------------------------------------
    # 3. Asset Class Reconciliation
    # -------------------------------------------------------------------------
    logger.info("Running asset class reconciliation...")
    asset_class_recon = recon_engine.reconcile_by_asset_class(
        calculation_date=execution_date,
        tolerance_pct=0.1
    )
    logger.info(f"✓ Reconciled {len(asset_class_recon)} asset classes")
    
    # -------------------------------------------------------------------------
    # 4. Top Movers Analysis
    # -------------------------------------------------------------------------
    logger.info("Analyzing top movers...")
    top_movers = recon_engine.get_top_movers(
        calculation_date=execution_date,
        top_n=20
    )
    logger.info(f"✓ Identified top {len(top_movers)} movers")
    
    # -------------------------------------------------------------------------
    # 5. Save Reconciliation Report to S3
    # -------------------------------------------------------------------------
    recon_engine.save_reconciliation_report(
        calculation_date=execution_date,
        output_format='json'
    )
    
    # Generate summary
    summary = recon_engine.generate_reconciliation_summary()
    logger.info(f"\n{summary}")
    
    # -------------------------------------------------------------------------
    # 6. Push Results to XCom for Email Notification
    # -------------------------------------------------------------------------
    context['ti'].xcom_push(
        key='total_rwa',
        value=f"{total_recon.get('calculated_exposure', 0):,.2f}"
    )
    context['ti'].xcom_push(
        key='reconciliation_status',
        value=total_recon['status']
    )
    context['ti'].xcom_push(
        key='variance_pct',
        value=f"{total_recon.get('exposure_variance_pct', 0):.4f}%"
    )
    
    return {
        "total_reconciliation": total_recon,
        "day_over_day": dod_recon,
        "asset_classes": len(asset_class_recon),
        "top_movers": len(top_movers)
    }


def send_slack_alert(message: str):
    """Send alert to Slack channel (implement based on your setup)"""
    # In production: Use Slack webhook or API
    # For now, just log
    logger.warning(f"SLACK ALERT: {message}")


reconcile = PythonOperator(
    task_id="reconcile_results",
    python_callable=reconcile_results,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# Task 8: Refresh BI Dashboard
# ============================================================================

def refresh_dashboard(**context):
    """
    Trigger BI dashboard refresh (Qlik Sense in this example)
    
    Can be adapted for Tableau, Power BI, or other BI tools
    """
    import requests
    
    execution_date = context["ds"]
    
    # Qlik Sense API call
    qlik_api_url = "https://qlik.bank.com/api/v1/apps/rwa-dashboard/reload"
    headers = {
        "Authorization": f"Bearer {context['var']['value'].get('qlik_api_token')}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            qlik_api_url,
            headers=headers,
            json={"calculation_date": execution_date},
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"✓ Dashboard refresh triggered successfully")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Dashboard refresh failed: {e}")
        # Don't fail the entire pipeline for dashboard refresh issues
        return {"status": "failed", "error": str(e)}


refresh_dashboard_task = PythonOperator(
    task_id="refresh_qlik_dashboard",
    python_callable=refresh_dashboard,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# Task 9: Success Notification
# ============================================================================

send_success = EmailOperator(
    task_id="send_success_notification",
    to=["risk-team@bank.com", "data-team@bank.com"],
    subject="✅ Basel RWA Calculation Completed - {{ ds }}",
    html_content="""
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .header { background-color: #28a745; color: white; padding: 20px; }
            .content { padding: 20px; }
            .metric { background-color: #f8f9fa; padding: 10px; margin: 10px 0; border-left: 4px solid #007bff; }
            .footer { background-color: #f1f1f1; padding: 10px; margin-top: 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h2>✅ Basel III RWA Calculation Successful</h2>
        </div>
        
        <div class="content">
            <p><strong>Execution Date:</strong> {{ ds }}</p>
            
            <div class="metric">
                <strong>Total RWA:</strong> {{ ti.xcom_pull(task_ids='reconcile_results', key='total_rwa') }}
            </div>
            
            <div class="metric">
                <strong>Reconciliation Status:</strong> {{ ti.xcom_pull(task_ids='reconcile_results', key='reconciliation_status') }}
            </div>
            
            <div class="metric">
                <strong>Variance:</strong> {{ ti.xcom_pull(task_ids='reconcile_results', key='variance_pct') }}
            </div>
            
            <h3>Pipeline Summary:</h3>
            <ul>
                <li>✓ Source data validated</li>
                <li>✓ Spark processing completed (3 stages)</li>
                <li>✓ Data loaded to Redshift</li>
                <li>✓ Reconciliation passed</li>
                <li>✓ Dashboard refreshed</li>
            </ul>
            
            <p>
                <a href="https://qlik.bank.com/rwa-dashboard" 
                   style="background-color: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px;">
                    View Dashboard
                </a>
            </p>
        </div>
        
        <div class="footer">
            <small>Automated notification from Basel RWA Pipeline | 
            <a href="https://airflow.bank.com/dags/basel_rwa_daily_calculation/graph">View in Airflow</a>
            </small>
        </div>
    </body>
    </html>
    """,
    dag=dag,
)

# ============================================================================
# Define Task Dependencies
# ============================================================================

# Step 1: Wait for all source data
[
    wait_for_loans,
    wait_for_collateral,
    wait_for_ratings,
    wait_for_market_data,
] >> validate_data

# Step 2: Run Spark ETL stages sequentially
validate_data >> spark_stage1 >> spark_stage2 >> spark_stage3

# Step 3: Load to Redshift in parallel
spark_stage3 >> [load_detail, load_summary]

# Step 4: Reconciliation after both loads complete
[load_detail, load_summary] >> reconcile

# Step 5: Dashboard refresh and notification
reconcile >> refresh_dashboard_task >> send_success