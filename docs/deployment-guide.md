# Deployment Guide

## Prerequisites

- AWS Account with appropriate permissions
- Terraform installed (for infrastructure provisioning)
- Python 3.9+ installed locally
- AWS CLI configured

## Step 1: Infrastructure Setup

### Using Terraform (Recommended)

```bash
cd terraform/
terraform init
terraform plan
terraform apply
```

This creates:
- S3 buckets for data lake
- Glue jobs and catalog
- Redshift cluster
- IAM roles and policies

### Manual Setup

If not using Terraform, manually create:
1. S3 bucket: `bank-datalake`
2. Redshift cluster: `rwa-cluster`
3. Glue data catalog database: `rwa_database`

## Step 2: Deploy Code

### Glue Jobs

```bash
# Upload Glue job scripts to S3
aws s3 cp src/glue_jobs/ s3://bank-datalake/scripts/glue/ --recursive

# Create Glue jobs
aws glue create-job --name rwa_stage1_standardization \
    --role GlueExecutionRole \
    --command "Name=glueetl,ScriptLocation=s3://bank-datalake/scripts/glue/stage1_standardization.py"
```

### Airflow DAGs

```bash
# Copy DAG to Airflow DAGs folder (MWAA S3 bucket or local Airflow)
aws s3 cp src/airflow_dags/basel_rwa_daily.py s3://your-airflow-bucket/dags/
```

## Step 3: Create Redshift Tables

```bash
# Connect to Redshift and run DDL
psql -h rwa-cluster.xxx.redshift.amazonaws.com -p 5439 -U admin -d prod -f sql/redshift/create_tables.sql
```

## Step 4: Test with Sample Data

```bash
# Generate sample data
python sample_data/generate_sample_data.py

# Upload to S3
aws s3 cp sample_data/ s3://bank-datalake/raw/ --recursive --exclude "*.py"
```

## Step 5: Trigger Pipeline

Via Airflow UI or CLI:
```bash
airflow dags trigger basel_rwa_daily_calculation
```

## Monitoring

- **Airflow UI**: http://your-airflow-host/admin/
- **CloudWatch**: AWS Console → CloudWatch → Metrics
- **Spark UI**: Available via Glue console

## Troubleshooting

### Common Issues

**Issue**: Glue job fails with OOM error  
**Solution**: Increase worker count in Airflow DAG configuration

**Issue**: Redshift COPY fails  
**Solution**: Check IAM role permissions on S3 bucket

**Issue**: Data quality validation fails  
**Solution**: Review CloudWatch logs for specific validation errors

## Production Checklist

- [ ] All secrets stored in AWS Secrets Manager
- [ ] CloudWatch alarms configured
- [ ] Backup and disaster recovery tested
- [ ] Performance testing completed
- [ ] Security review passed
- [ ] Documentation updated
