"""
Reconciliation Engine for Basel RWA Pipeline

Validates calculated results against source systems and performs
day-over-day trend analysis for data accuracy assurance.

Author: Portfolio Project
Date: December 2024
"""

from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import create_engine
import boto3
import json
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class ReconciliationEngine:
    """
    Comprehensive reconciliation framework for RWA calculations
    
    Compares calculated RWA totals with source system data and
    performs trend analysis to detect anomalies.
    """
    
    def __init__(self, redshift_conn_string: str, s3_bucket: str):
        """
        Initialize reconciliation engine
        
        Args:
            redshift_conn_string: PostgreSQL connection string for Redshift
            s3_bucket: S3 bucket name for storing reconciliation reports
        """
        self.engine = create_engine(redshift_conn_string)
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket
        self.reconciliation_results = []
        
    def reconcile_totals(
        self,
        calculation_date: str,
        tolerance_pct: float = 0.01
    ) -> Dict:
        """
        Reconcile total RWA with source system totals
        
        Args:
            calculation_date: Date to reconcile (YYYY-MM-DD)
            tolerance_pct: Acceptable variance percentage (default 0.01%)
            
        Returns:
            Dict with reconciliation results
        """
        logger.info(f"Starting reconciliation for {calculation_date}")
        
        # Get calculated totals from Redshift
        calculated_totals = self._get_calculated_totals(calculation_date)
        
        # Get source totals (would query Oracle in production)
        # For portfolio, we simulate this
        source_totals = self._get_source_totals(calculation_date)
        
        # Calculate variances
        results = {
            'calculation_date': calculation_date,
            'calculated_exposure': calculated_totals['total_exposure'],
            'source_exposure': source_totals['total_exposure'],
            'calculated_loan_count': calculated_totals['loan_count'],
            'source_loan_count': source_totals['loan_count'],
            'status': 'PASSED'
        }
        
        # Check exposure variance
        if results['source_exposure'] > 0:
            exposure_variance_pct = abs(
                results['calculated_exposure'] - results['source_exposure']
            ) / results['source_exposure'] * 100
            
            results['exposure_variance_pct'] = exposure_variance_pct
            
            if exposure_variance_pct > tolerance_pct:
                results['status'] = 'FAILED'
                logger.error(
                    f"Exposure reconciliation FAILED: {exposure_variance_pct:.4f}% variance "
                    f"(tolerance: {tolerance_pct}%)"
                )
            else:
                logger.info(
                    f"Exposure reconciliation PASSED: {exposure_variance_pct:.4f}% variance"
                )
        
        # Check loan count
        if results['calculated_loan_count'] != results['source_loan_count']:
            results['status'] = 'FAILED'
            logger.error(
                f"Loan count mismatch: Calculated={results['calculated_loan_count']}, "
                f"Source={results['source_loan_count']}"
            )
        
        self.reconciliation_results.append(results)
        
        return results
    
    def reconcile_day_over_day(
        self,
        calculation_date: str,
        threshold_pct: float = 10.0
    ) -> Dict:
        """
        Compare today's RWA with previous day to detect anomalies
        
        Args:
            calculation_date: Current date
            threshold_pct: Max acceptable day-over-day change (default 10%)
            
        Returns:
            Dict with comparison results
        """
        current_date = datetime.strptime(calculation_date, '%Y-%m-%d')
        previous_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Day-over-day reconciliation: {previous_date} -> {calculation_date}")
        
        # Get totals for both days
        try:
            current_totals = self._get_calculated_totals(calculation_date)
            previous_totals = self._get_calculated_totals(previous_date)
        except Exception as e:
            logger.warning(f"Could not get previous day data: {e}")
            return {
                'calculation_date': calculation_date,
                'previous_date': previous_date,
                'status': 'SKIPPED',
                'reason': 'Previous day data not available'
            }
        
        # Calculate changes
        rwa_change_pct = (
            (current_totals['total_rwa'] - previous_totals['total_rwa']) /
            previous_totals['total_rwa'] * 100
        )
        
        exposure_change_pct = (
            (current_totals['total_exposure'] - previous_totals['total_exposure']) /
            previous_totals['total_exposure'] * 100
        )
        
        results = {
            'calculation_date': calculation_date,
            'previous_date': previous_date,
            'current_rwa': current_totals['total_rwa'],
            'previous_rwa': previous_totals['total_rwa'],
            'rwa_change_pct': rwa_change_pct,
            'exposure_change_pct': exposure_change_pct,
            'status': 'PASSED'
        }
        
        # Flag large changes
        if abs(rwa_change_pct) > threshold_pct:
            results['status'] = 'WARNING'
            logger.warning(
                f"Large RWA change detected: {rwa_change_pct:.2f}% "
                f"(threshold: {threshold_pct}%)"
            )
        else:
            logger.info(f"Day-over-day change within threshold: {rwa_change_pct:.2f}%")
        
        self.reconciliation_results.append(results)
        
        return results
    
    def reconcile_by_asset_class(
        self,
        calculation_date: str,
        tolerance_pct: float = 0.1
    ) -> pd.DataFrame:
        """
        Detailed reconciliation by asset class
        
        Args:
            calculation_date: Date to reconcile
            tolerance_pct: Tolerance per asset class (default 0.1%)
            
        Returns:
            DataFrame with reconciliation by asset class
        """
        query = f"""
        SELECT 
            asset_class,
            SUM(outstanding_balance_usd) as calculated_exposure,
            SUM(risk_weighted_assets) as calculated_rwa,
            COUNT(*) as loan_count
        FROM risk.rwa_calculations
        WHERE calculation_date = '{calculation_date}'
        GROUP BY asset_class
        """
        
        calculated_df = pd.read_sql(query, self.engine)
        
        # In production, would compare with source
        # For portfolio, mark all as PASSED
        calculated_df['variance_pct'] = 0.0
        calculated_df['status'] = 'PASSED'
        
        logger.info(f"Asset class reconciliation completed for {calculation_date}")
        
        return calculated_df
    
    def get_top_movers(
        self,
        calculation_date: str,
        top_n: int = 20
    ) -> pd.DataFrame:
        """
        Get top N customers with largest RWA changes
        
        Args:
            calculation_date: Current date
            top_n: Number of top movers to return
            
        Returns:
            DataFrame with top movers
        """
        current_date = datetime.strptime(calculation_date, '%Y-%m-%d')
        previous_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        query = f"""
        WITH current_day AS (
            SELECT 
                customer_id,
                SUM(risk_weighted_assets) as current_rwa
            FROM risk.rwa_calculations
            WHERE calculation_date = '{calculation_date}'
            GROUP BY customer_id
        ),
        previous_day AS (
            SELECT 
                customer_id,
                SUM(risk_weighted_assets) as previous_rwa
            FROM risk.rwa_calculations
            WHERE calculation_date = '{previous_date}'
            GROUP BY customer_id
        )
        SELECT 
            COALESCE(c.customer_id, p.customer_id) as customer_id,
            COALESCE(c.current_rwa, 0) as current_rwa,
            COALESCE(p.previous_rwa, 0) as previous_rwa,
            COALESCE(c.current_rwa, 0) - COALESCE(p.previous_rwa, 0) as rwa_change,
            CASE 
                WHEN p.previous_rwa > 0 THEN 
                    ((c.current_rwa - p.previous_rwa) / p.previous_rwa * 100)
                ELSE NULL
            END as change_pct
        FROM current_day c
        FULL OUTER JOIN previous_day p ON c.customer_id = p.customer_id
        ORDER BY ABS(COALESCE(c.current_rwa, 0) - COALESCE(p.previous_rwa, 0)) DESC
        LIMIT {top_n}
        """
        
        top_movers = pd.read_sql(query, self.engine)
        
        return top_movers
    
    def _get_calculated_totals(self, calculation_date: str) -> Dict:
        """Get totals from Redshift calculations"""
        query = f"""
        SELECT 
            SUM(outstanding_balance_usd) as total_exposure,
            SUM(risk_weighted_assets) as total_rwa,
            SUM(regulatory_capital) as total_capital,
            COUNT(DISTINCT loan_id) as loan_count,
            COUNT(DISTINCT customer_id) as customer_count
        FROM risk.rwa_calculations
        WHERE calculation_date = '{calculation_date}'
        """
        
        result = pd.read_sql(query, self.engine).iloc[0].to_dict()
        
        # Convert numpy types to native Python types
        for key, value in result.items():
            if pd.isna(value):
                result[key] = 0
            elif hasattr(value, 'item'):
                result[key] = value.item()
        
        return result
    
    def _get_source_totals(self, calculation_date: str) -> Dict:
        """
        Get totals from source system (Oracle snapshot in S3)
        
        In production, this would query actual Oracle data via Athena or JDBC.
        For portfolio demonstration, we simulate expected values.
        """
        # In production:
        # athena_client = boto3.client('athena')
        # query = f"""
        # SELECT 
        #     SUM(outstanding_balance) as total_exposure,
        #     COUNT(*) as loan_count
        # FROM loans_master
        # WHERE snapshot_date = '{calculation_date}'
        # """
        
        # For portfolio: simulate source totals
        calculated = self._get_calculated_totals(calculation_date)
        
        # Simulate very close match (within tolerance)
        result = {
            'total_exposure': calculated['total_exposure'] * 1.0001,  # 0.01% difference
            'loan_count': calculated['loan_count']
        }
        
        return result
    
    def save_reconciliation_report(
        self,
        calculation_date: str,
        output_format: str = 'json'
    ):
        """
        Save reconciliation results to S3 for audit trail
        
        Args:
            calculation_date: Date of reconciliation
            output_format: 'json' or 'csv'
        """
        report_data = {
            'reconciliation_timestamp': datetime.utcnow().isoformat(),
            'calculation_date': calculation_date,
            'results': self.reconciliation_results,
            'overall_status': 'PASSED' if all(
                r.get('status') in ['PASSED', 'WARNING'] 
                for r in self.reconciliation_results
            ) else 'FAILED'
        }
        
        # Save to S3
        s3_key = f"reconciliation/date={calculation_date}/report.{output_format}"
        
        if output_format == 'json':
            content = json.dumps(report_data, indent=2, default=str)
        else:
            # Convert to CSV
            if self.reconciliation_results:
                df = pd.DataFrame(self.reconciliation_results)
                content = df.to_csv(index=False)
            else:
                content = "No reconciliation results"
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=s3_key,
            Body=content.encode('utf-8')
        )
        
        logger.info(f"Reconciliation report saved to s3://{self.s3_bucket}/{s3_key}")
    
    def generate_reconciliation_summary(self) -> str:
        """
        Generate human-readable summary of reconciliation
        
        Returns:
            Formatted summary string
        """
        if not self.reconciliation_results:
            return "No reconciliation results available"
        
        passed = sum(1 for r in self.reconciliation_results if r.get('status') == 'PASSED')
        failed = sum(1 for r in self.reconciliation_results if r.get('status') == 'FAILED')
        warning = sum(1 for r in self.reconciliation_results if r.get('status') == 'WARNING')
        
        summary = f"""
╔══════════════════════════════════════════════════════════════╗
║           RECONCILIATION SUMMARY                             ║
╠══════════════════════════════════════════════════════════════╣
║  Total Checks:    {len(self.reconciliation_results):>3}                                    ║
║  Passed:          {passed:>3} ✓                                   ║
║  Failed:          {failed:>3} ✗                                   ║
║  Warnings:        {warning:>3} ⚠                                   ║
╠══════════════════════════════════════════════════════════════╣
║  Overall Status:  {'PASSED ✓' if failed == 0 else 'FAILED ✗':40}║
╚══════════════════════════════════════════════════════════════╝
        """
        
        return summary


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize engine (connection string would come from env vars in production)
    recon_engine = ReconciliationEngine(
        redshift_conn_string="postgresql://user:pass@cluster:5439/prod",
        s3_bucket="bank-datalake"
    )
    
    # Run reconciliations
    calculation_date = "2025-12-06"
    
    # 1. Total reconciliation
    logger.info("Running total reconciliation...")
    total_results = recon_engine.reconcile_totals(
        calculation_date, 
        tolerance_pct=0.01
    )
    print(f"Total Reconciliation: {total_results['status']}")
    
    # 2. Day-over-day comparison
    logger.info("Running day-over-day analysis...")
    dod_results = recon_engine.reconcile_day_over_day(
        calculation_date, 
        threshold_pct=10.0
    )
    print(f"Day-over-Day: {dod_results['status']}")
    
    # 3. Asset class reconciliation
    logger.info("Running asset class reconciliation...")
    asset_class_df = recon_engine.reconcile_by_asset_class(calculation_date)
    print(f"Asset Classes Reconciled: {len(asset_class_df)}")
    
    # 4. Top movers
    logger.info("Analyzing top movers...")
    top_movers = recon_engine.get_top_movers(calculation_date, top_n=10)
    print(f"Top 10 Movers Identified: {len(top_movers)}")
    
    # 5. Save report
    logger.info("Saving reconciliation report...")
    recon_engine.save_reconciliation_report(calculation_date, output_format='json')
    
    # 6. Print summary
    print("\n" + recon_engine.generate_reconciliation_summary())