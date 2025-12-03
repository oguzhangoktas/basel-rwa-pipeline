# Data Dictionary

## Loans Master

| Column | Type | Description | Nullable | Example |
|--------|------|-------------|----------|---------|
| loan_id | VARCHAR(50) | Unique loan identifier | No | L0001234 |
| customer_id | VARCHAR(50) | Customer identifier | No | C000567 |
| outstanding_balance | DECIMAL(18,2) | Current loan balance | No | 150000.00 |
| currency | VARCHAR(3) | ISO currency code | No | USD |
| product_type | VARCHAR(20) | Loan product type | No | MORTGAGE |
| internal_rating | VARCHAR(10) | Internal credit rating | Yes | AAA |
| country_code | VARCHAR(2) | ISO country code | No | US |

## RWA Calculations (Output)

| Column | Type | Description |
|--------|------|-------------|
| calculation_date | DATE | Date of calculation |
| loan_id | VARCHAR(50) | Loan identifier |
| risk_weighted_assets | DECIMAL(18,2) | Calculated RWA |
| regulatory_capital | DECIMAL(18,2) | Required capital (8% of RWA) |
| risk_weight | DECIMAL(5,4) | Applied risk weight (e.g., 0.35 for 35%) |

See [create_tables.sql](../sql/redshift/create_tables.sql) for complete schema.
