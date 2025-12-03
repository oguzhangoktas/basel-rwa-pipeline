# Architecture Deep Dive

## System Overview

The Basel III RWA calculation pipeline implements a modern data engineering architecture following the medallion pattern (Bronze → Silver → Gold) with production-grade features.

## Architecture Layers

### 1. Ingestion Layer

**Purpose**: Extract data from multiple source systems and land in S3 data lake

**Components**:
- **AWS DMS**: Continuous data replication from Oracle databases
- **Lambda Functions**: Serverless ingestion for external APIs and SFTP files
- **S3 Landing Zone**: Raw data storage with date partitioning

**Data Flow**:
```
Oracle DB → AWS DMS → S3 (raw/loans/date=2025-12-02/)
SFTP Server → Lambda → S3 (raw/ratings/date=2025-12-02/)
External API → Lambda → S3 (raw/market_data/date=2025-12-02/)
```

**Key Design Decisions**:
- Used Parquet format for 40% storage reduction vs CSV
- Partitioned by date for efficient querying
- Implemented idempotent ingestion (can safely re-run)

### 2. Catalog Layer

**Purpose**: Centralized metadata management

**Components**:
- **AWS Glue Catalog**: Schema registry and metadata store
- **Glue Crawlers**: Automatic schema inference and table creation
- **Athena**: Ad-hoc SQL queries on S3 data

**Benefits**:
- Single source of truth for schema
- Enables SQL queries via Athena without loading to warehouse
- Automatic schema evolution detection

### 3. Processing Layer (Bronze → Silver → Gold)

#### Stage 1: Standardization (Bronze → Silver)
**Input**: Raw data from landing zone  
**Output**: Clean, validated data in Delta Lake  
**Processing**: PySpark on AWS Glue

**Transformations**:
1. Schema validation and type casting
2. Deduplication and null handling
3. Currency conversion to base currency (USD)
4. Data quality checks (Great Expectations)
5. Derived field calculations

**Optimization**:
- Repartition by asset_class for balanced downstream processing
- Cache reference data (FX rates, rating mappings)
- Write to Delta Lake for ACID guarantees

#### Stage 2: Calculation (Silver → Silver)
**Input**: Clean data from Stage 1  
**Output**: RWA calculations  
**Processing**: PySpark with complex business logic

**Basel III Calculation Steps**:
1. **Exposure at Default (EAD)**
   - Formula: `EAD = Outstanding Balance × CCF`
   - CCF varies by product type (on-balance = 1.0, committed line = 0.5)

2. **Credit Risk Mitigation (CRM)**
   - Adjust EAD based on eligible collateral
   - Apply haircuts to collateral values
   - Formula: `EAD_after_CRM = EAD × (1 - Collateral_Coverage)`

3. **Risk Weight Assignment**
   - Lookup based on asset_class + rating_bucket
   - Apply regulatory floors (e.g., 10% for mortgages)

4. **RWA Calculation**
   - Formula: `RWA = EAD_after_CRM × Risk_Weight`

5. **Regulatory Capital**
   - Formula: `Capital = RWA × 8%` (Basel III requirement)

6. **Expected Loss**
   - Formula: `EL = EAD × PD × LGD`

**Data Skew Handling**:
```python
# Problem: Corporate customers with 5,000+ loans cause executor imbalance
# Solution: Salting technique

# Identify skewed keys
skewed = loans.groupBy('customer_id').count().filter('count > 100')

# Add salt (0-9) to distribute across partitions
loans_salted = loans.withColumn('salt',
    when(is_skewed, (rand() * 10).cast('int')).otherwise(0))

# Replicate dimension table 10x for skewed keys
counterparty_replicated = counterparty.crossJoin(lit(0..9))
```

**Result**: 4.5 hours → 45 minutes (83% improvement)

#### Stage 3: Aggregation (Silver → Gold)
**Input**: Detail RWA calculations  
**Output**: Summary tables for reporting  
**Processing**: PySpark aggregations

**Aggregations**:
- By asset_class + geography + rating_bucket
- Calculate totals, averages, derived metrics
- Day-over-day trend analysis
- Concentration risk analysis (top customers, geographies)

**Output Format**:
- Parquet files for Redshift COPY
- Delta tables for time-travel queries
- Materialized datasets for dashboards

### 4. Storage Layer

#### Delta Lake (S3)
**Purpose**: Transactional data lake with ACID guarantees

**Features Used**:
- **ACID Transactions**: Prevent partial writes during failures
- **Time Travel**: Query historical versions for audit
- **Schema Evolution**: Safely add/modify columns
- **Z-Ordering**: Co-locate related data for faster queries

```python
# Write with Z-ordering
df.write.format('delta') \
    .mode('overwrite') \
    .partitionBy('calculation_date', 'asset_class') \
    .save(output_path)

# Optimize with Z-ordering
deltaTable.optimize() \
    .where(f"calculation_date = '{date}'") \
    .executeZOrderBy('geography', 'rating_bucket')
```

#### Amazon Redshift
**Purpose**: Data warehouse for analytical queries and BI

**Schema Design**: Kimball star schema
- **Fact Tables**:
  - `rwa_calculations` (detail level - 120M rows)
  - `rwa_summary` (aggregated - 10K rows)
- **Dimension Tables**:
  - `dim_date`, `dim_asset_class`, `dim_geography`
- **Reference Tables**:
  - `ref_risk_weights` (Basel III mappings)

**Optimization**:
```sql
-- Distribution key: Balance data across nodes
DISTKEY(asset_class)

-- Sort keys: Speed up filtering
SORTKEY(calculation_date, asset_class)

-- Compression: Reduce storage by 60%
ENCODE LZO

-- Materialized views: Pre-compute dashboard queries
CREATE MATERIALIZED VIEW mv_daily_kpis AS ...
```

**Load Process**:
```
S3 Parquet → COPY command → Redshift
- Parallel load from multiple files
- Compression reduces network transfer
- COMPUPDATE OFF (already optimal compression)
```

### 5. Orchestration Layer

**Apache Airflow**

**DAG Structure**:
```
wait_for_sources (parallel)
    ├── S3Sensor: loans
    ├── S3Sensor: collateral
    ├── S3Sensor: ratings
    └── S3Sensor: fx_rates
         ↓
    validate_data (PythonOperator)
         ↓
    spark_stage1 (GlueJobOperator)
         ↓
    spark_stage2 (GlueJobOperator)
         ↓
    spark_stage3 (GlueJobOperator)
         ↓
    load_redshift (parallel)
    ├── S3ToRedshiftOperator: detail
    └── S3ToRedshiftOperator: summary
         ↓
    reconcile (PythonOperator)
         ↓
    refresh_qlik (PythonOperator)
         ↓
    send_notification (EmailOperator)
```

**Key Features**:
- **SLA Monitoring**: Alerts if tasks exceed 3-hour target
- **Retry Logic**: 2 retries with exponential backoff
- **Dependency Management**: Clear task dependencies
- **Monitoring**: Integrated with CloudWatch and Slack

**Scheduling**:
- Daily at 2:00 AM
- Depends on previous day's success
- Max 1 concurrent run

### 6. Monitoring & Observability

#### CloudWatch
**Metrics**:
- `TotalRWA`: Sum of risk-weighted assets
- `ProcessedLoans`: Record count
- `ProcessingTime`: End-to-end duration
- `DataQualityScore`: % of records passing validation

**Dashboards**:
- Real-time pipeline status
- Performance trends (p50, p95, p99)
- Cost analysis
- Error rates

#### Alerting
**PagerDuty** (Critical):
- Pipeline failures
- Reconciliation variance >0.1%
- SLA breaches

**Slack** (Warning):
- Data quality <99%
- Processing time >3h
- Anomalies detected

**Email** (Info):
- Daily success notifications
- Weekly performance reports

#### Logging
- **Spark Event Logs**: Saved to S3 for Spark UI replay
- **Application Logs**: CloudWatch Logs with structured JSON
- **Audit Trail**: Every calculation logged with lineage

## Data Flow Diagram

```
┌─────────────┐
│   Oracle    │ ─DMS──┐
└─────────────┘       │
                      ↓
┌─────────────┐    ┌──────────────┐
│ External    │─┐  │   S3 Data    │
│ APIs/SFTP   │ ├─→│     Lake     │
└─────────────┘ │  │ (Landing)    │
                │  └──────┬───────┘
   ┌────────────┘         │
   │ Lambda               │
   │ Functions            ↓
   │              ┌───────────────┐
   │              │  Glue Catalog │
   │              │  (Metadata)   │
   │              └───────┬───────┘
   │                      │
   │                      ↓
   │              ┌───────────────────┐
   │              │   Spark on Glue   │
   │              │                   │
   │              │  Stage 1: Clean   │
   │              │  Stage 2: Calc    │
   │              │  Stage 3: Agg     │
   │              └─────────┬─────────┘
   │                        │
   │                        ↓
   │              ┌──────────────────┐
   │              │   Delta Lake     │
   │              │  (Processed)     │
   │              └─────────┬────────┘
   │                        │
   │                        ↓
   │              ┌──────────────────┐
   │              │    Redshift DWH  │
   │              │  (Star Schema)   │
   │              └─────────┬────────┘
   │                        │
   │                        ↓
   │              ┌──────────────────┐
   │              │  Qlik Sense      │
   │              │  (Dashboards)    │
   │              └──────────────────┘
   │
   └─────> Orchestrated by Airflow
          Monitored by CloudWatch
```

## Performance Optimizations

### 1. Data Skew Mitigation
- **Problem**: Large customers with 1,000s of loans
- **Solution**: Salting (distribute across 10 partitions)
- **Impact**: 83% reduction in join time

### 2. Broadcast Joins
- **Approach**: Small tables (<50MB) broadcasted to all executors
- **Applied to**: Rating mappings, CCF tables, FX rates
- **Impact**: Eliminated shuffle for dimension joins

### 3. Adaptive Query Execution
- **Enabled**: `spark.sql.adaptive.enabled=true`
- **Benefits**: Dynamic partition coalescing, skew join handling
- **Impact**: 15-20% overall runtime improvement

### 4. Redshift Optimizations
- **Distribution Keys**: Balance data across nodes
- **Sort Keys**: Speed up date/asset_class filters
- **Compression**: 60% storage reduction
- **Materialized Views**: Pre-compute dashboard aggregations

### 5. Partitioning Strategy
- **S3**: Partition by date + asset_class
- **Redshift**: Partition by calculation_date
- **Benefit**: Query only relevant partitions

## Security Architecture

### Data Encryption
- **At Rest**: S3-KMS, Redshift encryption
- **In Transit**: TLS 1.2+ for all connections

### Access Control
- **IAM Roles**: Least privilege principle
- **Redshift**: Role-based access control (RBAC)
- **VPC**: Private subnets for processing

### Audit Logging
- **CloudTrail**: All API calls logged
- **Data Lineage**: Track data transformations
- **Change Logs**: Delta Lake transaction logs

## Disaster Recovery

### Backup Strategy
- **S3**: Versioning enabled, lifecycle policies
- **Redshift**: Automated snapshots (daily)
- **Code**: Git version control

### Recovery Procedures
- **Pipeline Failure**: Automated retry logic
- **Data Corruption**: Delta Lake time travel
- **Complete Failure**: Restore from snapshot + reprocess

## Scalability Considerations

### Current Capacity
- 120M loans/day
- 2.5 hour processing time
- $1,500/month infrastructure cost

### Scaling Strategy
- **Vertical**: Increase Glue worker count (currently 100)
- **Horizontal**: Partition by additional dimensions
- **Optimization**: Further AQE tuning, caching

### Future Enhancements
- Real-time streaming for intraday calculations
- Machine learning for PD predictions
- Basel IV framework implementation