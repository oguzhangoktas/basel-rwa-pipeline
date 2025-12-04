# dags/basel_rwa_daily.py
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    "owner": "data-engineering",
    "depends_on_past": True,
    "start_date": datetime(2025, 1, 1),
    "email": ["data-team@bank.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=3),
}

dag = DAG(
    "basel_rwa_daily_calculation",
    default_args=default_args,
    description="Daily Basel III RWA calculation pipeline",
    schedule_interval="0 2 * * *",  # 2 AM daily
    catchup=False,
    max_active_runs=1,
    tags=["basel", "risk", "regulatory"],
)

# Task 1: Wait for source data
wait_for_loans = S3KeySensor(
    task_id="wait_for_loans_data",
    bucket_name="bank-datalake",
    bucket_key="raw/loans/date={{ ds }}/loans_snapshot.parquet",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

wait_for_collateral = S3KeySensor(
    task_id="wait_for_collateral_data",
    bucket_name="bank-datalake",
    bucket_key="raw/collateral/date={{ ds }}/collateral_snapshot.parquet",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

wait_for_ratings = S3KeySensor(
    task_id="wait_for_ratings_data",
    bucket_name="bank-datalake",
    bucket_key="raw/ratings/date={{ ds }}/ratings.csv",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

wait_for_market_data = S3KeySensor(
    task_id="wait_for_market_data",
    bucket_name="bank-datalake",
    bucket_key="raw/market_data/date={{ ds }}/fx_rates.json",
    aws_conn_id="aws_default",
    timeout=3600,
    poke_interval=300,
    dag=dag,
)


# Task 2: Data Quality Validation
def validate_source_data(**context):
    """
    Validates row counts, schema, and business rules
    """
    from great_expectations.data_context import DataContext
    import json

    execution_date = context["ds"]
    s3_client = boto3.client("s3")

    # Check row counts vs historical average
    expected_counts = {
        "loans": (100_000_000, 150_000_000),
        "collateral": (40_000_000, 50_000_000),
        "ratings": (400_000, 600_000),
    }

    results = {}
    for table, (min_count, max_count) in expected_counts.items():
        # Read row count from metadata or query Athena
        actual_count = get_row_count(table, execution_date)

        if not (min_count <= actual_count <= max_count):
            raise ValueError(f"{table} row count {actual_count} outside expected range")

        results[table] = actual_count

    # Store validation results
    s3_client.put_object(
        Bucket="bank-datalake",
        Key=f"validation/date={execution_date}/source_validation.json",
        Body=json.dumps(results),
    )

    return results


validate_data = PythonOperator(
    task_id="validate_source_data",
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Spark ETL - Stage 1 (Standardization)
spark_stage1 = GlueJobOperator(
    task_id="spark_standardization",
    job_name="rwa_stage1_standardization",
    script_location="s3://bank-scripts/glue/rwa_stage1.py",
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
        },
    },
    dag=dag,
)

# Task 4: Spark ETL - Stage 2 (RWA Calculations)
spark_stage2 = GlueJobOperator(
    task_id="spark_rwa_calculation",
    job_name="rwa_stage2_calculation",
    script_location="s3://bank-scripts/glue/rwa_stage2.py",
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
        },
    },
    dag=dag,
)

# Task 5: Spark ETL - Stage 3 (Aggregation)
spark_stage3 = GlueJobOperator(
    task_id="spark_aggregation",
    job_name="rwa_stage3_aggregation",
    script_location="s3://bank-scripts/glue/rwa_stage3.py",
    s3_bucket="bank-datalake",
    iam_role_name="GlueExecutionRole",
    create_job_kwargs={
        "GlueVersion": "4.0",
        "NumberOfWorkers": 20,
        "WorkerType": "G.1X",
        "Timeout": 60,
    },
    dag=dag,
)

# Task 6: Load to Redshift
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
    copy_options=["FORMAT AS PARQUET"],
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_default",
    dag=dag,
)


# Task 7: Data Reconciliation
def reconcile_results(**context):
    """
    Compare results with source totals and previous day
    """
    import pandas as pd
    from sqlalchemy import create_engine

    execution_date = context["ds"]

    # Query Redshift for today's summary
    engine = create_engine("postgresql://redshift-cluster:5439/prod")

    today_summary = pd.read_sql(
        f"""
        SELECT 
            SUM(total_exposure) as total_exposure,
            SUM(total_rwa) as total_rwa,
            SUM(loan_count) as loan_count
        FROM risk.rwa_summary
        WHERE calculation_date = '{execution_date}'
    """,
        engine,
    )

    # Compare with source system totals
    source_totals = pd.read_sql(
        f"""
        SELECT 
            SUM(outstanding_balance) as total_balance,
            COUNT(*) as loan_count
        FROM loans_master
        WHERE snapshot_date = '{execution_date}'
    """,
        engine,
    )

    # Tolerance: 0.01% variance allowed
    variance = (
        abs(today_summary["total_exposure"][0] - source_totals["total_balance"][0])
        / source_totals["total_balance"][0]
    )

    if variance > 0.0001:
        raise ValueError(f"Reconciliation failed: {variance*100:.4f}% variance")

    # Compare with previous day (should not change > 10%)
    prev_day_summary = pd.read_sql(
        f"""
        SELECT SUM(total_rwa) as total_rwa
        FROM risk.rwa_summary
        WHERE calculation_date = '{execution_date}'::date - 1
    """,
        engine,
    )

    day_over_day_change = abs(
        (today_summary["total_rwa"][0] - prev_day_summary["total_rwa"][0])
        / prev_day_summary["total_rwa"][0]
    )

    if day_over_day_change > 0.1:
        # Send warning but don't fail
        send_slack_alert(
            f"⚠️ RWA changed {day_over_day_change*100:.2f}% from previous day"
        )

    return {"variance": variance, "day_over_day_change": day_over_day_change}


reconcile = PythonOperator(
    task_id="reconcile_results",
    python_callable=reconcile_results,
    provide_context=True,
    dag=dag,
)


# Task 8: Refresh Qlik Cache
def refresh_qlik_cache(**context):
    """
    Trigger Qlik Sense app reload via API
    """
    import requests

    qlik_api_url = "https://qlik.bank.com/api/v1/apps/rwa-dashboard/reload"
    headers = {"Authorization": "Bearer {{ var.value.qlik_api_token }}"}

    response = requests.post(qlik_api_url, headers=headers)
    response.raise_for_status()

    return response.json()


refresh_qlik = PythonOperator(
    task_id="refresh_qlik_dashboard",
    python_callable=refresh_qlik_cache,
    provide_context=True,
    dag=dag,
)

# Task 9: Send Success Notification
send_success = EmailOperator(
    task_id="send_success_notification",
    to=["risk-team@bank.com", "data-team@bank.com"],
    subject="✅ Basel RWA Calculation Completed - {{ ds }}",
    html_content="""
    <h3>RWA Calculation Successful</h3>
    <p>Execution Date: {{ ds }}</p>
    <p>Processing Time: {{ ti.xcom_pull(task_ids='spark_rwa_calculation', key='duration') }}</p>
    <p>Total RWA: {{ ti.xcom_pull(task_ids='reconcile_results', key='total_rwa') }}</p>
    <p><a href="https://qlik.bank.com/rwa-dashboard">View Dashboard</a></p>
    """,
    dag=dag,
)

# Define dependencies
[
    wait_for_loans,
    wait_for_collateral,
    wait_for_ratings,
    wait_for_market_data,
] >> validate_data
validate_data >> spark_stage1 >> spark_stage2 >> spark_stage3
spark_stage3 >> [load_detail, load_summary]
[load_detail, load_summary] >> reconcile >> refresh_qlik >> send_success
