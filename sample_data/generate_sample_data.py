"""
Generate Synthetic Sample Data for Basel RWA Pipeline

Creates realistic but synthetic loan portfolio data for demonstration purposes.
All data is randomly generated and does not represent any real customers.

Author: Portfolio Project
Date: December 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_LOANS = 10_000
NUM_CUSTOMERS = 1_000
NUM_COLLATERAL = 5_000

print("Generating synthetic Basel RWA sample data...")
print(f"  Loans: {NUM_LOANS:,}")
print(f"  Customers: {NUM_CUSTOMERS:,}")
print(f"  Collateral: {NUM_COLLATERAL:,}")


# ============================================================================
# Generate Customers (Counterparty Master)
# ============================================================================

print("\n1. Generating customer data...")

customer_types = ['RETAIL', 'SME', 'CORPORATE', 'FINANCIAL_INSTITUTION']
industries = ['MANUFACTURING', 'RETAIL_TRADE', 'REAL_ESTATE', 'TECHNOLOGY', 
              'HEALTHCARE', 'FINANCIAL', 'ENERGY', 'CONSUMER_GOODS']
countries = ['US', 'GB', 'DE', 'FR', 'JP', 'CN', 'CA', 'AU']

customers_data = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer_type = random.choice(customer_types)
    
    # Assign total assets based on customer type
    if customer_type == 'RETAIL':
        total_assets = random.uniform(10_000, 1_000_000)
        annual_revenue = random.uniform(20_000, 200_000)
    elif customer_type == 'SME':
        total_assets = random.uniform(500_000, 50_000_000)
        annual_revenue = random.uniform(1_000_000, 100_000_000)
    elif customer_type == 'CORPORATE':
        total_assets = random.uniform(50_000_000, 10_000_000_000)
        annual_revenue = random.uniform(100_000_000, 50_000_000_000)
    else:  # Financial Institution
        total_assets = random.uniform(1_000_000_000, 100_000_000_000)
        annual_revenue = random.uniform(500_000_000, 20_000_000_000)
    
    # Probability of default correlates with rating
    internal_rating = random.choices(
        ['AAA', 'AA', 'A', 'BBB', 'BB', 'B'],
        weights=[5, 10, 20, 35, 20, 10]
    )[0]
    
    pd_mapping = {
        'AAA': 0.0001, 'AA': 0.0003, 'A': 0.0008,
        'BBB': 0.002, 'BB': 0.008, 'B': 0.025
    }
    
    customers_data.append({
        'customer_id': f'C{i:06d}',
        'customer_type': customer_type,
        'industry_code': random.choice(industries),
        'country_code': random.choice(countries),
        'total_assets': round(total_assets, 2),
        'annual_revenue': round(annual_revenue, 2),
        'internal_rating': internal_rating,
        'credit_rating': internal_rating,  # Simplified: internal = external
        'default_probability': pd_mapping[internal_rating]
    })

customers_df = pd.DataFrame(customers_data)
customers_df.to_parquet('counterparty_master.parquet', index=False)
print(f"  ✓ Created {len(customers_df):,} customers")


# ============================================================================
# Generate Collateral
# ============================================================================

print("\n2. Generating collateral data...")

collateral_types = [
    'REAL_ESTATE_RESIDENTIAL',
    'REAL_ESTATE_COMMERCIAL', 
    'CASH_DEPOSIT',
    'SECURITIES_LISTED',
    'SECURITIES_UNLISTED',
    'RECEIVABLES',
    'INVENTORY',
    'EQUIPMENT'
]

haircut_mapping = {
    'REAL_ESTATE_RESIDENTIAL': 0.15,
    'REAL_ESTATE_COMMERCIAL': 0.25,
    'CASH_DEPOSIT': 0.00,
    'SECURITIES_LISTED': 0.20,
    'SECURITIES_UNLISTED': 0.40,
    'RECEIVABLES': 0.30,
    'INVENTORY': 0.50,
    'EQUIPMENT': 0.40
}

base_date = datetime(2025, 12, 1)
collateral_data = []

for i in range(1, NUM_COLLATERAL + 1):
    collateral_type = random.choice(collateral_types)
    market_value = random.uniform(50_000, 5_000_000)
    haircut = haircut_mapping[collateral_type] + random.uniform(-0.05, 0.05)
    haircut = max(0, min(1, haircut))  # Clamp between 0 and 1
    
    # Valuation date between 1-365 days ago
    valuation_date = base_date - timedelta(days=random.randint(1, 365))
    
    collateral_data.append({
        'collateral_id': f'COL{i:07d}',
        'collateral_type': collateral_type,
        'market_value': round(market_value, 2),
        'currency': random.choices(['USD', 'EUR', 'GBP'], weights=[60, 25, 15])[0],
        'valuation_date': valuation_date.strftime('%Y-%m-%d'),
        'haircut_percentage': round(haircut, 4),
        'legal_enforceability_flag': random.choice([True, True, True, False])  # 75% enforceable
    })

collateral_df = pd.DataFrame(collateral_data)
collateral_df.to_parquet('collateral_details.parquet', index=False)
print(f"  ✓ Created {len(collateral_df):,} collateral records")


# ============================================================================
# Generate Loans
# ============================================================================

print("\n3. Generating loan data...")

product_types = [
    'MORTGAGE', 'HOME_LOAN', 'PERSONAL_LOAN', 'CREDIT_CARD',
    'SME_LOAN', 'CORPORATE_LOAN', 'TERM_LOAN', 'REVOLVING_CREDIT'
]

currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CNY']
currency_weights = [50, 25, 15, 5, 5]

loans_data = []

# Create some customers with many loans (to simulate skew)
heavy_customers = random.sample(customers_df['customer_id'].tolist(), 20)

for i in range(1, NUM_LOANS + 1):
    # 20% of loans go to heavy customers (simulating skew)
    if random.random() < 0.20:
        customer_id = random.choice(heavy_customers)
    else:
        customer_id = random.choice(customers_df['customer_id'].tolist())
    
    # Get customer details
    customer = customers_df[customers_df['customer_id'] == customer_id].iloc[0]
    
    # Product type based on customer type
    if customer['customer_type'] == 'RETAIL':
        product_type = random.choice(['MORTGAGE', 'HOME_LOAN', 'PERSONAL_LOAN', 'CREDIT_CARD'])
        balance = random.uniform(5_000, 500_000)
    elif customer['customer_type'] == 'SME':
        product_type = random.choice(['SME_LOAN', 'TERM_LOAN', 'REVOLVING_CREDIT'])
        balance = random.uniform(100_000, 5_000_000)
    else:  # Corporate or Financial
        product_type = random.choice(['CORPORATE_LOAN', 'TERM_LOAN', 'REVOLVING_CREDIT'])
        balance = random.uniform(1_000_000, 100_000_000)
    
    # Origination date in past 1-5 years
    origination_date = base_date - timedelta(days=random.randint(365, 1825))
    
    # Maturity date in future 1-10 years
    maturity_date = base_date + timedelta(days=random.randint(365, 3650))
    
    # 50% of loans have collateral
    has_collateral = random.random() < 0.50
    collateral_id = random.choice(collateral_df['collateral_id'].tolist()) if has_collateral else None
    
    # Exposure at default (simplified: same as balance for most products)
    if product_type in ['REVOLVING_CREDIT', 'CREDIT_CARD']:
        # For revolving products, exposure includes undrawn commitment
        ead = balance * random.uniform(1.2, 1.5)
    else:
        ead = balance
    
    loans_data.append({
        'loan_id': f'L{i:07d}',
        'customer_id': customer_id,
        'product_type': product_type,
        'origination_date': origination_date.strftime('%Y-%m-%d'),
        'maturity_date': maturity_date.strftime('%Y-%m-%d'),
        'outstanding_balance': round(balance, 2),
        'currency': random.choices(currencies, weights=currency_weights)[0],
        'internal_rating': customer['internal_rating'],
        'country_code': customer['country_code'],
        'collateral_id': collateral_id,
        'exposure_at_default': round(ead, 2)
    })

loans_df = pd.DataFrame(loans_data)
loans_df.to_parquet('loans_master.parquet', index=False)
print(f"  ✓ Created {len(loans_df):,} loans")


# ============================================================================
# Generate External Ratings
# ============================================================================

print("\n4. Generating external ratings...")

rating_agencies = ['S&P', 'MOODY', 'FITCH']
rating_scales = {
    'S&P': ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-'],
    'MOODY': ['Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3', 'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3', 'B1', 'B2', 'B3'],
    'FITCH': ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-']
}

ratings_data = []

# Only corporate customers get external ratings
corporate_customers = customers_df[
    customers_df['customer_type'].isin(['CORPORATE', 'FINANCIAL_INSTITUTION'])
]['customer_id'].tolist()

# 70% of corporate customers have ratings
rated_customers = random.sample(corporate_customers, int(len(corporate_customers) * 0.7))

for customer_id in rated_customers:
    # Each customer gets 1-3 agency ratings
    num_agencies = random.randint(1, 3)
    selected_agencies = random.sample(rating_agencies, num_agencies)
    
    for agency in selected_agencies:
        rating = random.choice(rating_scales[agency])
        rating_date = base_date - timedelta(days=random.randint(1, 180))
        
        ratings_data.append({
            'customer_id': customer_id,
            'rating_agency': agency,
            'rating': rating,
            'rating_date': rating_date.strftime('%Y-%m-%d'),
            'outlook': random.choice(['POSITIVE', 'STABLE', 'NEGATIVE'])
        })

ratings_df = pd.DataFrame(ratings_data)
ratings_df.to_csv('external_ratings.csv', index=False)
print(f"  ✓ Created {len(ratings_df):,} external ratings")


# ============================================================================
# Generate FX Rates
# ============================================================================

print("\n5. Generating FX rates...")

fx_pairs = ['EURUSD', 'GBPUSD', 'JPYUSD', 'CNYUSD']
fx_base_rates = {
    'EURUSD': 1.10,
    'GBPUSD': 1.27,
    'JPYUSD': 0.0067,
    'CNYUSD': 0.14
}

fx_data = []
for pair in fx_pairs:
    base_rate = fx_base_rates[pair]
    # Add small random variation
    rate = base_rate * random.uniform(0.98, 1.02)
    
    fx_data.append({
        'currency_pair': pair,
        'rate': round(rate, 6),
        'date': base_date.strftime('%Y-%m-%d'),
        'source': 'SIMULATED'
    })

fx_df = pd.DataFrame(fx_data)
fx_df.to_json('fx_rates.json', orient='records', indent=2)
print(f"  ✓ Created {len(fx_df):,} FX rates")


# ============================================================================
# Generate Summary Statistics
# ============================================================================

print("\n" + "="*60)
print("SAMPLE DATA GENERATION COMPLETE")
print("="*60)

print("\n📊 Summary Statistics:")
print(f"\nLoans:")
print(f"  Total count: {len(loans_df):,}")
print(f"  Total balance: ${loans_df['outstanding_balance'].sum():,.2f}")
print(f"  Avg balance: ${loans_df['outstanding_balance'].mean():,.2f}")
print(f"  With collateral: {loans_df['collateral_id'].notna().sum():,} ({loans_df['collateral_id'].notna().sum()/len(loans_df)*100:.1f}%)")

print(f"\nCustomers:")
print(f"  Total count: {len(customers_df):,}")
print(f"  By type:")
for ctype in customers_df['customer_type'].unique():
    count = (customers_df['customer_type'] == ctype).sum()
    print(f"    {ctype}: {count:,} ({count/len(customers_df)*100:.1f}%)")

print(f"\nRating Distribution:")
for rating in ['AAA', 'AA', 'A', 'BBB', 'BB', 'B']:
    count = (customers_df['internal_rating'] == rating).sum()
    print(f"  {rating}: {count:,} ({count/len(customers_df)*100:.1f}%)")

print(f"\nData Skew Analysis:")
loans_per_customer = loans_df.groupby('customer_id').size()
print(f"  Max loans per customer: {loans_per_customer.max()}")
print(f"  Avg loans per customer: {loans_per_customer.mean():.1f}")
print(f"  Customers with >100 loans: {(loans_per_customer > 100).sum()}")

print("\n📁 Generated Files:")
print("  ✓ counterparty_master.parquet")
print("  ✓ collateral_details.parquet")
print("  ✓ loans_master.parquet")
print("  ✓ external_ratings.csv")
print("  ✓ fx_rates.json")

print("\n✅ Sample data ready for testing!")
print("   Use these files with: python src/glue_jobs/stage2_calculation.py")