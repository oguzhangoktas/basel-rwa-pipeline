-- ============================================================================
-- Basel III RWA Pipeline - Redshift Table Definitions
-- 
-- Creates optimized star schema for RWA calculations and reporting
-- Uses Kimball dimensional modeling approach
-- 
-- Author: Portfolio Project
-- Date: December 2025
-- ============================================================================

-- Schema for risk management tables
CREATE SCHEMA IF NOT EXISTS risk;

-- ============================================================================
-- FACT TABLE: RWA Calculations (Detail Level)
-- ============================================================================

DROP TABLE IF EXISTS risk.rwa_calculations CASCADE;

CREATE TABLE risk.rwa_calculations (
    -- Primary Keys
    calculation_date DATE NOT NULL SORTKEY,
    loan_id VARCHAR(50) NOT NULL,
    
    -- Dimensional Keys
    customer_id VARCHAR(50) NOT NULL,
    product_type VARCHAR(20),
    asset_class VARCHAR(50) DISTKEY,  -- Distribute by asset class for balanced queries
    geography VARCHAR(10),
    rating_bucket VARCHAR(20),
    
    -- Financial Measures
    outstanding_balance_usd DECIMAL(18,2) NOT NULL,
    credit_conversion_factor DECIMAL(5,4),
    exposure_at_default DECIMAL(18,2),
    ead_after_crm DECIMAL(18,2),  -- After Credit Risk Mitigation
    collateral_coverage_ratio DECIMAL(5,4),
    
    -- Risk Metrics
    probability_of_default DECIMAL(8,6),
    loss_given_default DECIMAL(5,4),
    expected_loss DECIMAL(18,2),
    risk_weight DECIMAL(5,4) NOT NULL,
    risk_weight_floored DECIMAL(5,4),  -- After regulatory floors
    risk_weighted_assets DECIMAL(18,2) NOT NULL,
    regulatory_capital DECIMAL(18,2) NOT NULL,
    
    -- Metadata
    calculation_method VARCHAR(50),
    basel_version VARCHAR(10),
    high_risk_flag BOOLEAN,
    data_quality_score DECIMAL(3,2),
    created_timestamp TIMESTAMP NOT NULL,
    
    -- Constraints
    PRIMARY KEY (calculation_date, loan_id)
)
COMPOUND SORTKEY (calculation_date, asset_class, geography)
DISTSTYLE KEY;

-- Table Comments
COMMENT ON TABLE risk.rwa_calculations IS 
    'Detail-level RWA calculations for all loans. Updated daily via Spark pipeline.';

COMMENT ON COLUMN risk.rwa_calculations.ead_after_crm IS 
    'Exposure at Default after Credit Risk Mitigation (collateral adjustment)';

COMMENT ON COLUMN risk.rwa_calculations.risk_weight_floored IS 
    'Risk weight after applying Basel III regulatory floors (e.g., 10% for mortgages)';


-- ============================================================================
-- FACT TABLE: RWA Summary (Aggregated)
-- ============================================================================

DROP TABLE IF EXISTS risk.rwa_summary CASCADE;

CREATE TABLE risk.rwa_summary (
    -- Dimensional Keys
    calculation_date DATE NOT NULL SORTKEY,
    asset_class VARCHAR(50) DISTKEY,
    geography VARCHAR(10),
    rating_bucket VARCHAR(20),
    
    -- Aggregated Metrics
    loan_count INTEGER NOT NULL,
    total_exposure DECIMAL(20,2) NOT NULL,
    total_ead DECIMAL(20,2),
    total_rwa DECIMAL(20,2) NOT NULL,
    total_regulatory_capital DECIMAL(20,2) NOT NULL,
    total_expected_loss DECIMAL(20,2),
    
    -- Average Metrics
    avg_risk_weight DECIMAL(5,4),
    avg_pd DECIMAL(8,6),
    avg_lgd DECIMAL(5,4),
    
    -- Derived Metrics
    capital_ratio DECIMAL(5,4),  -- Capital / Exposure
    rwa_density DECIMAL(5,4),     -- RWA / Exposure
    
    -- Quality Metrics
    high_risk_count INTEGER,
    avg_data_quality DECIMAL(3,2),
    
    -- Change Metrics (vs previous day)
    rwa_change_pct DECIMAL(6,2),
    
    created_timestamp TIMESTAMP DEFAULT GETDATE(),
    
    -- Constraints
    PRIMARY KEY (calculation_date, asset_class, geography, rating_bucket)
)
COMPOUND SORTKEY (calculation_date, asset_class)
DISTSTYLE KEY;

COMMENT ON TABLE risk.rwa_summary IS 
    'Aggregated RWA metrics by asset class, geography, and rating. Used for executive dashboards.';


-- ============================================================================
-- DIMENSION TABLE: Date Dimension
-- ============================================================================

DROP TABLE IF EXISTS risk.dim_date CASCADE;

CREATE TABLE risk.dim_date (
    date_key DATE NOT NULL PRIMARY KEY SORTKEY,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
)
DISTSTYLE ALL;  -- Small dimension, replicate to all nodes

COMMENT ON TABLE risk.dim_date IS 
    'Date dimension for time-series analysis and reporting';


-- ============================================================================
-- DIMENSION TABLE: Asset Class
-- ============================================================================

DROP TABLE IF EXISTS risk.dim_asset_class CASCADE;

CREATE TABLE risk.dim_asset_class (
    asset_class VARCHAR(50) PRIMARY KEY,
    asset_category VARCHAR(50),
    basel_exposure_class VARCHAR(50),
    description TEXT,
    is_retail BOOLEAN,
    default_risk_weight DECIMAL(5,4),
    created_date DATE DEFAULT CURRENT_DATE
)
DISTSTYLE ALL;

-- Populate asset classes
INSERT INTO risk.dim_asset_class VALUES
    ('RETAIL_MORTGAGE', 'Retail', 'Retail Exposures - Residential Mortgages', 
     'Secured lending against residential property', TRUE, 0.35, CURRENT_DATE),
    ('RETAIL_OTHER', 'Retail', 'Retail Exposures - Other', 
     'Unsecured retail lending (personal loans, credit cards)', TRUE, 0.75, CURRENT_DATE),
    ('CORPORATE_SME', 'Corporate', 'Corporate Exposures - SME', 
     'Small and Medium Enterprise lending', FALSE, 0.75, CURRENT_DATE),
    ('CORPORATE_LARGE', 'Corporate', 'Corporate Exposures - Large Corporate', 
     'Large corporate and institutional lending', FALSE, 1.00, CURRENT_DATE),
    ('SOVEREIGN', 'Sovereign', 'Sovereign Exposures', 
     'Exposures to national governments', FALSE, 0.00, CURRENT_DATE);

COMMENT ON TABLE risk.dim_asset_class IS 
    'Reference data for Basel III asset classification';


-- ============================================================================
-- DIMENSION TABLE: Geography
-- ============================================================================

DROP TABLE IF EXISTS risk.dim_geography CASCADE;

CREATE TABLE risk.dim_geography (
    geography_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(100),
    region VARCHAR(50),
    continent VARCHAR(50),
    is_developed_market BOOLEAN,
    basel_country_risk_weight DECIMAL(5,4),
    created_date DATE DEFAULT CURRENT_DATE
)
DISTSTYLE ALL;

-- Sample data
INSERT INTO risk.dim_geography VALUES
    ('US', 'United States', 'North America', 'Americas', TRUE, 0.00, CURRENT_DATE),
    ('GB', 'United Kingdom', 'Western Europe', 'Europe', TRUE, 0.00, CURRENT_DATE),
    ('DE', 'Germany', 'Western Europe', 'Europe', TRUE, 0.00, CURRENT_DATE),
    ('JP', 'Japan', 'East Asia', 'Asia', TRUE, 0.00, CURRENT_DATE),
    ('CN', 'China', 'East Asia', 'Asia', FALSE, 0.20, CURRENT_DATE);

COMMENT ON TABLE risk.dim_geography IS 
    'Geographic reference data with country risk classifications';


-- ============================================================================
-- REFERENCE TABLE: Risk Weights
-- ============================================================================

DROP TABLE IF EXISTS risk.ref_risk_weights CASCADE;

CREATE TABLE risk.ref_risk_weights (
    asset_class VARCHAR(50) NOT NULL,
    rating_bucket VARCHAR(20) NOT NULL,
    risk_weight DECIMAL(5,4) NOT NULL,
    basel_version VARCHAR(10),
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (asset_class, rating_bucket, effective_date)
)
DISTSTYLE ALL;

-- Populate Basel III risk weights
INSERT INTO risk.ref_risk_weights 
(asset_class, rating_bucket, risk_weight, basel_version, effective_date, is_current) VALUES
    -- Retail Mortgages
    ('RETAIL_MORTGAGE', 'AAA', 0.35, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_MORTGAGE', 'AA', 0.35, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_MORTGAGE', 'A', 0.35, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_MORTGAGE', 'BBB', 0.50, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_MORTGAGE', 'BB', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_MORTGAGE', 'B', 1.00, 'Basel III', '2013-01-01', TRUE),
    
    -- Retail Other
    ('RETAIL_OTHER', 'AAA', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_OTHER', 'AA', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_OTHER', 'A', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_OTHER', 'BBB', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_OTHER', 'BB', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('RETAIL_OTHER', 'B', 0.75, 'Basel III', '2013-01-01', TRUE),
    
    -- Corporate SME
    ('CORPORATE_SME', 'AAA', 0.20, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_SME', 'AA', 0.30, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_SME', 'A', 0.50, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_SME', 'BBB', 0.75, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_SME', 'BB', 1.00, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_SME', 'B', 1.50, 'Basel III', '2013-01-01', TRUE),
    
    -- Corporate Large
    ('CORPORATE_LARGE', 'AAA', 0.20, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_LARGE', 'AA', 0.30, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_LARGE', 'A', 0.50, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_LARGE', 'BBB', 1.00, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_LARGE', 'BB', 1.00, 'Basel III', '2013-01-01', TRUE),
    ('CORPORATE_LARGE', 'B', 1.50, 'Basel III', '2013-01-01', TRUE);

COMMENT ON TABLE risk.ref_risk_weights IS 
    'Basel III standardized approach risk weight mappings';


-- ============================================================================
-- MATERIALIZED VIEW: Executive Dashboard KPIs
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS risk.mv_daily_kpis CASCADE;

CREATE MATERIALIZED VIEW risk.mv_daily_kpis AS
SELECT 
    calculation_date,
    
    -- Portfolio Metrics
    COUNT(DISTINCT loan_id) AS total_loans,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(outstanding_balance_usd) AS total_exposure,
    
    -- RWA Metrics
    SUM(risk_weighted_assets) AS total_rwa,
    SUM(regulatory_capital) AS total_capital,
    AVG(risk_weight) AS avg_risk_weight,
    
    -- Quality Metrics
    SUM(CASE WHEN high_risk_flag THEN 1 ELSE 0 END) AS high_risk_loans,
    AVG(data_quality_score) AS avg_data_quality,
    
    -- Ratios
    SUM(regulatory_capital) / NULLIF(SUM(outstanding_balance_usd), 0) AS capital_ratio,
    SUM(risk_weighted_assets) / NULLIF(SUM(outstanding_balance_usd), 0) AS rwa_density
    
FROM risk.rwa_calculations
GROUP BY calculation_date;

COMMENT ON MATERIALIZED VIEW risk.mv_daily_kpis IS 
    'Pre-aggregated KPIs for executive dashboard. Refreshed after daily pipeline run.';


-- ============================================================================
-- GRANTS
-- ============================================================================

-- Grant read access to analysts
GRANT USAGE ON SCHEMA risk TO GROUP risk_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA risk TO GROUP risk_analysts;

-- Grant write access to ETL service account
GRANT ALL ON SCHEMA risk TO etl_service_account;
GRANT ALL ON ALL TABLES IN SCHEMA risk TO etl_service_account;


-- ============================================================================
-- ANALYZE for Query Optimization
-- ============================================================================

ANALYZE risk.rwa_calculations;
ANALYZE risk.rwa_summary;
ANALYZE risk.dim_asset_class;
ANALYZE risk.dim_geography;
ANALYZE risk.ref_risk_weights;