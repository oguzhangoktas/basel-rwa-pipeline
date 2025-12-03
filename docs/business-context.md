# Basel III Business Context

## What is Basel III?

Basel III is an international regulatory framework developed by the Basel Committee on Banking Supervision (BCBS) in response to the 2008 financial crisis. It establishes minimum capital requirements that banks must maintain to absorb losses and continue operations during financial stress.

## Key Concepts

### 1. Risk-Weighted Assets (RWA)

RWA is a measure of a bank's assets adjusted for their credit risk. Instead of treating all assets equally, Basel III assigns different risk weights based on the likelihood of default and potential loss.

**Example:**
- $100M government bond (very safe) → 0% risk weight → $0 RWA
- $100M AAA corporate loan (low risk) → 20% risk weight → $20M RWA
- $100M B-rated loan (high risk) → 150% risk weight → $150M RWA

### 2. Capital Adequacy Ratio

Banks must maintain capital equal to at least 8% of their RWA:

```
Minimum Capital = RWA × 8%
```

**Example:**
If a bank has $100B in RWA, it must hold at least $8B in capital.

### 3. Three Approaches to Calculate RWA

#### a) Standardized Approach (Used in This Project)
- Uses predefined risk weights from Basel III framework
- Based on external credit ratings and asset classes
- Simpler to implement
- Used by smaller banks

#### b) Foundation Internal Ratings-Based (F-IRB)
- Uses bank's own estimates of Probability of Default (PD)
- Regulator-specified Loss Given Default (LGD)
- Requires regulatory approval

#### c) Advanced Internal Ratings-Based (A-IRB)
- Bank estimates both PD and LGD
- Most risk-sensitive approach
- Requires sophisticated models and approval

**This project implements the Standardized Approach.**

## Basel III Standardized Approach Formula

### Core Calculation

```
RWA = Exposure at Default (EAD) × Risk Weight (RW)

Where:
- EAD = Outstanding Balance × Credit Conversion Factor (CCF)
- RW = Determined by asset class + rating
- Regulatory Capital = RWA × 8%
```

### Step-by-Step Example

**Given:**
- Corporate loan: $1,000,000
- Counterparty rating: BBB
- Asset class: Corporate (Large)
- Collateral: $300,000 (eligible, after haircuts)

**Calculation:**

1. **Exposure at Default (EAD)**
   ```
   EAD = $1,000,000 × 1.0 (on-balance sheet CCF) = $1,000,000
   ```

2. **Credit Risk Mitigation (Collateral)**
   ```
   Adjusted EAD = $1,000,000 - $300,000 = $700,000
   ```

3. **Risk Weight (BBB Corporate)**
   ```
   RW = 100% (per Basel III table)
   ```

4. **Risk-Weighted Assets**
   ```
   RWA = $700,000 × 100% = $700,000
   ```

5. **Required Capital**
   ```
   Capital = $700,000 × 8% = $56,000
   ```

**Result:** Bank must set aside $56,000 in capital for this $1M loan.

## Basel III Risk Weight Tables

### Corporate Exposures

| Rating | Risk Weight |
|--------|-------------|
| AAA to AA- | 20% |
| A+ to A- | 50% |
| BBB+ to BBB- | 100% |
| BB+ to BB- | 100% |
| Below BB- | 150% |
| Unrated | 100% |

### Retail Exposures

**Residential Mortgages:**
- Fully secured: 35%
- LTV > 80%: Higher weights

**Other Retail:**
- Personal loans, credit cards: 75%

### SME Exposures

Treated as corporate but may receive 25% reduction if certain criteria met.

## Credit Risk Mitigation (CRM)

### Eligible Collateral

Basel III recognizes certain collateral types that reduce exposure:

1. **Financial Collateral**
   - Cash deposits: 0% haircut
   - Listed securities: 20% haircut
   - Gold: 15% haircut

2. **Physical Collateral**
   - Residential property: 35% haircut
   - Commercial property: 40% haircut

3. **Other Collateral**
   - Receivables: 45% haircut
   - Equipment: 55% haircut

### Haircuts

Haircuts account for potential decrease in collateral value:

```
Adjusted Collateral Value = Market Value × (1 - Haircut)
```

**Example:**
- Real estate valued at $500,000
- Haircut: 35%
- Adjusted value: $500,000 × (1 - 0.35) = $325,000

## Credit Conversion Factors (CCF)

Off-balance sheet items require conversion to credit exposure:

| Product | CCF |
|---------|-----|
| Drawn balances | 100% |
| Undrawn committed lines | 50% |
| Undrawn uncommitted lines | 0% |
| Letters of credit | 20% |
| Guarantees | 100% |

**Example:**
- $10M committed credit line
- $3M drawn, $7M undrawn
- EAD = $3M × 100% + $7M × 50% = $6.5M

## Regulatory Requirements

### Capital Buffers (Beyond 8% Minimum)

1. **Capital Conservation Buffer**: Additional 2.5%
   - Total minimum: 10.5%

2. **Countercyclical Buffer**: 0-2.5% (varies by jurisdiction)
   - Applied during credit booms

3. **Systemically Important Buffer**: 1-3.5%
   - For G-SIBs (Global Systemically Important Banks)

### Total Capital Stack

```
Common Equity Tier 1 (CET1):  ≥ 4.5% of RWA
Tier 1 Capital:               ≥ 6.0% of RWA
Total Capital:                ≥ 8.0% of RWA

Plus Buffers:
Capital Conservation:         + 2.5%
Countercyclical:              + 0-2.5%
G-SIB:                        + 1-3.5%
```

## Why RWA Matters

### 1. Regulatory Compliance
- Failure to maintain adequate capital → Regulatory restrictions
- Severe violations → Fines, reputation damage, potential closure

### 2. Business Strategy
- Higher RWA → More capital needed → Lower returns
- Banks optimize portfolios to minimize RWA
- Pricing decisions factor in capital costs

### 3. Risk Management
- RWA provides common metric across asset types
- Enables portfolio-level risk assessment
- Informs capital allocation decisions

## Basel IV (Future State)

Basel IV (technically "Basel III Finalization") introduces:

1. **Output Floor**: Minimum RWA = 72.5% of standardized approach
   - Limits benefit of IRB models

2. **Revised Standardized Approach**
   - More risk-sensitive risk weights
   - New asset classes

3. **Operational Risk**
   - Standardized Measurement Approach (SMA)
   - Replaces multiple approaches

**Implementation**: Being phased in 2023-2027

## Reporting Requirements

### Regulatory Reports

1. **COREP (Common Reporting)**
   - Own funds and capital requirements
   - Large exposures
   - Leverage ratio

2. **FINREP (Financial Reporting)**
   - Balance sheet
   - Income statement
   - Asset quality

### Frequency
- Quarterly for large banks
- Semi-annual for smaller banks

### Penalties for Errors
- Late submission: Fines
- Material errors: Additional scrutiny, higher capital requirements
- Repeated violations: Regulatory enforcement actions

## Data Requirements

### Source Systems
- **Core Banking**: Loan balances, product types, terms
- **Credit Risk**: Ratings, PD/LGD estimates
- **Collateral Management**: Valuations, haircuts
- **Market Data**: FX rates, security prices
- **Reference Data**: Customer master, product hierarchy

### Data Quality Imperatives
- **Accuracy**: 99.9%+ required for regulatory submissions
- **Completeness**: No missing critical fields
- **Timeliness**: Daily updates for large exposures
- **Lineage**: Full audit trail from source to report
- **Consistency**: Cross-system reconciliation

## Audit and Compliance

### Internal Audit
- Annual review of RWA calculation methodology
- Sample testing of calculations
- IT controls assessment

### External Audit
- Validation of capital ratios in financial statements
- Testing of key controls

### Regulatory Examination
- On-site inspections
- Data quality reviews
- Model validation (for IRB banks)

## Key Takeaways

1. **RWA is not just a regulatory metric** – it drives business decisions and capital allocation

2. **Data quality is paramount** – errors in RWA calculations can have material financial and regulatory consequences

3. **Automation is essential** – manual processes cannot handle the volume and complexity at scale

4. **Transparency matters** – regulators require full lineage from source data to final reports

5. **Continuous evolution** – Basel framework constantly updated; systems must be adaptable

---

## References

- [Basel Committee on Banking Supervision - Basel III Framework](https://www.bis.org/bcbs/basel3.htm)
- [BIS - Standardised Approach for Credit Risk](https://www.bis.org/basel_framework/chapter/CRE/20.htm)
- [EBA - Implementation of Basel III in Europe (CRR/CRD IV)](https://www.eba.europa.eu/regulation-and-policy/single-rulebook)

---

*This document provides business context for the Basel RWA calculation pipeline. For technical implementation details, see [ARCHITECTURE.md](../ARCHITECTURE.md).*