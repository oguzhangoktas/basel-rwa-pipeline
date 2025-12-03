# Sample Data

Synthetic data for testing the Basel RWA pipeline.

## Files

After running `generate_sample_data.py`, you'll have:

- `loans_master.parquet` - 10,000 loan records
- `counterparty_master.parquet` - 1,000 customer records  
- `collateral_details.parquet` - 5,000 collateral records
- `external_ratings.csv` - External credit ratings
- `fx_rates.json` - FX conversion rates

## Generate Data

```bash
python generate_sample_data.py
```

## Data Characteristics

- **Realistic distributions**: Rating distribution follows typical portfolio
- **Data skew**: Some customers have 100+ loans (for testing skew handling)
- **Various asset classes**: Mix of retail, SME, and corporate
- **Multiple currencies**: USD, EUR, GBP, JPY, CNY

**⚠️ Important**: All data is synthetically generated. No real customer information.

## Using Sample Data

```bash
# Upload to S3 (for testing AWS pipeline)
aws s3 cp sample_data/ s3://bank-datalake/raw/loans/date=2025-12-02/ \
    --exclude "*.py" --exclude "*.md" --recursive

# Or test locally with PySpark
spark-submit src/glue_jobs/stage2_calculation.py \
    --input sample_data/loans_master.parquet \
    --output /tmp/rwa_output
```
