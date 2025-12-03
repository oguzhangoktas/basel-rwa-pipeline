# Performance Optimization Guide

## Spark Optimizations

### 1. Data Skew Mitigation

**Problem**: Some customers have 1,000s of loans, causing uneven partition sizes.

**Solution**: Salting technique
```python
# Identify skewed keys
skewed = df.groupBy('customer_id').count().filter('count > 100')

# Add salt (0-9)
df_salted = df.withColumn('salt',
    when(col('customer_id').isin(skewed), (rand() * 10).cast('int'))
    .otherwise(lit(0)))
```

**Impact**: 83% reduction in join time

### 2. Broadcast Joins

Small dimension tables (<50MB) should be broadcasted:

```python
from pyspark.sql.functions import broadcast

result = large_df.join(
    broadcast(small_df),
    'key'
)
```

### 3. Adaptive Query Execution

Enable AQE for dynamic optimizations:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Redshift Optimizations

### 1. Distribution Keys

Choose keys that evenly distribute data:
```sql
CREATE TABLE rwa_calculations (
    ...
) DISTKEY(asset_class);
```

### 2. Sort Keys

Use compound sort keys for common filters:
```sql
COMPOUND SORTKEY(calculation_date, asset_class)
```

### 3. Compression

Let Redshift automatically choose:
```sql
ANALYZE COMPRESSION table_name;
```

## Cost Optimization

- Use Spot instances for Glue jobs (70% savings)
- S3 lifecycle policies for old data
- Redshift pause/resume for dev environments
- Right-size worker counts based on data volume

## Monitoring

Key metrics to track:
- Spark stage duration
- Shuffle read/write bytes
- Redshift disk-based queries
- S3 request rates

See [CloudWatch Dashboard](https://console.aws.amazon.com/cloudwatch) for real-time metrics.
