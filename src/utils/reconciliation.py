"""
Data Quality Validation Utilities

Provides comprehensive data quality checks for the RWA pipeline including:
- Schema validation
- Business rule validation
- Statistical anomaly detection
- Reconciliation checks

Author: Portfolio Project
Date: December 2024
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import logging

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Comprehensive data quality validation framework
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.validation_results = []
        
    def validate_schema(
        self, 
        df: DataFrame, 
        expected_schema: StructType,
        table_name: str
    ) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame schema matches expected schema
        
        Args:
            df: Input DataFrame
            expected_schema: Expected StructType schema
            table_name: Name of table for logging
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        actual_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        # Check for missing columns
        missing_cols = set(expected_fields.keys()) - set(actual_fields.keys())
        if missing_cols:
            errors.append(f"{table_name}: Missing columns: {missing_cols}")
        
        # Check for unexpected columns
        extra_cols = set(actual_fields.keys()) - set(expected_fields.keys())
        if extra_cols:
            logger.warning(f"{table_name}: Extra columns found: {extra_cols}")
        
        # Check data types
        for col in set(actual_fields.keys()) & set(expected_fields.keys()):
            if actual_fields[col] != expected_fields[col]:
                errors.append(
                    f"{table_name}.{col}: Type mismatch. "
                    f"Expected {expected_fields[col]}, got {actual_fields[col]}"
                )
        
        is_valid = len(errors) == 0
        self._log_validation("schema_validation", table_name, is_valid, errors)
        
        return is_valid, errors
    
    def check_nulls(
        self,
        df: DataFrame,
        critical_columns: List[str],
        table_name: str,
        threshold_pct: float = 1.0
    ) -> Tuple[bool, Dict[str, float]]:
        """
        Check for null values in critical columns
        
        Args:
            df: Input DataFrame
            critical_columns: List of columns that should not have nulls
            table_name: Table name for logging
            threshold_pct: Maximum acceptable null percentage (default 1%)
            
        Returns:
            Tuple of (is_valid, dict of column: null_percentage)
        """
        null_stats = {}
        total_rows = df.count()
        
        for col in critical_columns:
            if col not in df.columns:
                logger.warning(f"{table_name}: Column {col} not found")
                continue
                
            null_count = df.filter(F.col(col).isNull()).count()
            null_pct = (null_count / total_rows) * 100 if total_rows > 0 else 0
            null_stats[col] = null_pct
            
            if null_pct > threshold_pct:
                logger.error(
                    f"{table_name}.{col}: {null_pct:.2f}% nulls "
                    f"(threshold: {threshold_pct}%)"
                )
        
        is_valid = all(pct <= threshold_pct for pct in null_stats.values())
        self._log_validation("null_check", table_name, is_valid, null_stats)
        
        return is_valid, null_stats
    
    def check_duplicates(
        self,
        df: DataFrame,
        key_columns: List[str],
        table_name: str
    ) -> Tuple[bool, int]:
        """
        Check for duplicate records based on key columns
        
        Args:
            df: Input DataFrame
            key_columns: Columns that define uniqueness
            table_name: Table name for logging
            
        Returns:
            Tuple of (is_valid, duplicate_count)
        """
        duplicate_count = df.groupBy(key_columns) \
            .count() \
            .filter(F.col('count') > 1) \
            .count()
        
        is_valid = duplicate_count == 0
        
        if not is_valid:
            logger.error(f"{table_name}: Found {duplicate_count} duplicate records")
        
        self._log_validation("duplicate_check", table_name, is_valid, 
                           {"duplicate_count": duplicate_count})
        
        return is_valid, duplicate_count
    
    def validate_business_rules(
        self,
        df: DataFrame,
        rules: List[Dict],
        table_name: str
    ) -> Tuple[bool, Dict[str, int]]:
        """
        Validate business rules
        
        Args:
            df: Input DataFrame
            rules: List of rule dictionaries with 'name', 'condition', 'error_msg'
            table_name: Table name for logging
            
        Example rules:
            [
                {
                    'name': 'positive_balance',
                    'condition': 'outstanding_balance > 0',
                    'error_msg': 'Balance must be positive'
                },
                {
                    'name': 'future_maturity',
                    'condition': 'maturity_date > current_date()',
                    'error_msg': 'Maturity date must be in future'
                }
            ]
            
        Returns:
            Tuple of (is_valid, dict of rule_name: violation_count)
        """
        violations = {}
        
        for rule in rules:
            rule_name = rule['name']
            condition = rule['condition']
            
            # Count records that VIOLATE the rule (negate the condition)
            violation_count = df.filter(f"NOT ({condition})").count()
            violations[rule_name] = violation_count
            
            if violation_count > 0:
                logger.error(
                    f"{table_name}: Rule '{rule_name}' violated by "
                    f"{violation_count} records - {rule.get('error_msg', '')}"
                )
        
        is_valid = all(count == 0 for count in violations.values())
        self._log_validation("business_rules", table_name, is_valid, violations)
        
        return is_valid, violations
    
    def check_row_count(
        self,
        df: DataFrame,
        expected_range: Tuple[int, int],
        table_name: str
    ) -> Tuple[bool, int]:
        """
        Validate row count is within expected range
        
        Args:
            df: Input DataFrame
            expected_range: Tuple of (min_count, max_count)
            table_name: Table name for logging
            
        Returns:
            Tuple of (is_valid, actual_count)
        """
        actual_count = df.count()
        min_count, max_count = expected_range
        
        is_valid = min_count <= actual_count <= max_count
        
        if not is_valid:
            logger.error(
                f"{table_name}: Row count {actual_count:,} outside expected range "
                f"[{min_count:,}, {max_count:,}]"
            )
        else:
            logger.info(f"{table_name}: Row count {actual_count:,} ✓")
        
        self._log_validation("row_count_check", table_name, is_valid, 
                           {"actual": actual_count, "expected_range": expected_range})
        
        return is_valid, actual_count
    
    def detect_anomalies(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        table_name: str,
        std_threshold: float = 3.0
    ) -> Tuple[bool, Dict[str, Dict]]:
        """
        Detect statistical anomalies using standard deviation
        
        Args:
            df: Input DataFrame
            numeric_columns: Columns to check for anomalies
            table_name: Table name for logging
            std_threshold: Number of std deviations for anomaly (default 3)
            
        Returns:
            Tuple of (is_valid, dict of column: {mean, std, anomaly_count})
        """
        anomaly_stats = {}
        
        for col in numeric_columns:
            if col not in df.columns:
                continue
            
            # Calculate statistics
            stats = df.agg(
                F.mean(col).alias('mean'),
                F.stddev(col).alias('std')
            ).collect()[0]
            
            mean_val = stats['mean']
            std_val = stats['std']
            
            if std_val is None or std_val == 0:
                continue
            
            # Count values beyond threshold
            lower_bound = mean_val - (std_threshold * std_val)
            upper_bound = mean_val + (std_threshold * std_val)
            
            anomaly_count = df.filter(
                (F.col(col) < lower_bound) | (F.col(col) > upper_bound)
            ).count()
            
            anomaly_stats[col] = {
                'mean': float(mean_val),
                'std': float(std_val),
                'anomaly_count': anomaly_count,
                'bounds': (float(lower_bound), float(upper_bound))
            }
            
            if anomaly_count > 0:
                logger.warning(
                    f"{table_name}.{col}: {anomaly_count} anomalies detected "
                    f"(>{std_threshold}σ from mean)"
                )
        
        is_valid = all(stats['anomaly_count'] == 0 for stats in anomaly_stats.values())
        self._log_validation("anomaly_detection", table_name, is_valid, anomaly_stats)
        
        return is_valid, anomaly_stats
    
    def reconcile_with_source(
        self,
        target_df: DataFrame,
        source_total: float,
        target_column: str,
        tolerance_pct: float = 0.01
    ) -> Tuple[bool, float]:
        """
        Reconcile target totals with source system
        
        Args:
            target_df: Calculated DataFrame
            source_total: Total from source system
            target_column: Column to sum in target
            tolerance_pct: Acceptable variance percentage (default 0.01%)
            
        Returns:
            Tuple of (is_valid, variance_percentage)
        """
        target_total = target_df.agg(F.sum(target_column)).collect()[0][0]
        
        if target_total is None:
            logger.error("Target total is NULL")
            return False, 100.0
        
        variance_pct = abs(target_total - source_total) / source_total * 100
        
        is_valid = variance_pct <= tolerance_pct
        
        if not is_valid:
            logger.error(
                f"Reconciliation failed: {variance_pct:.4f}% variance "
                f"(tolerance: {tolerance_pct}%)\n"
                f"Source: {source_total:,.2f}, Target: {target_total:,.2f}"
            )
        else:
            logger.info(f"Reconciliation passed: {variance_pct:.4f}% variance ✓")
        
        self._log_validation("reconciliation", "totals", is_valid, 
                           {"variance_pct": variance_pct, "tolerance": tolerance_pct})
        
        return is_valid, variance_pct
    
    def _log_validation(self, check_type: str, table_name: str, 
                       is_valid: bool, details: any):
        """Internal method to log validation results"""
        self.validation_results.append({
            'check_type': check_type,
            'table_name': table_name,
            'is_valid': is_valid,
            'details': details
        })
    
    def get_validation_summary(self) -> Dict:
        """
        Get summary of all validation checks
        
        Returns:
            Dictionary with validation summary
        """
        total_checks = len(self.validation_results)
        passed_checks = sum(1 for r in self.validation_results if r['is_valid'])
        
        return {
            'total_checks': total_checks,
            'passed': passed_checks,
            'failed': total_checks - passed_checks,
            'pass_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            'details': self.validation_results
        }
    
    def raise_if_failed(self):
        """Raise exception if any validation failed"""
        summary = self.get_validation_summary()
        if summary['failed'] > 0:
            raise ValueError(
                f"Data quality validation failed: {summary['failed']} checks failed"
            )


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    
    # Sample data
    data = [
        (1, "L001", 100000, "2025-12-31", "AAA"),
        (2, "L002", -5000, "2024-01-01", "BBB"),  # Violations
        (3, "L003", 50000, "2026-06-30", "A"),
    ]
    df = spark.createDataFrame(data, 
                               ["id", "loan_id", "balance", "maturity", "rating"])
    
    # Initialize validator
    validator = DataQualityValidator(spark)
    
    # Run checks
    validator.check_nulls(df, ["loan_id", "balance"], "loans_table")
    validator.check_duplicates(df, ["loan_id"], "loans_table")
    validator.validate_business_rules(df, [
        {
            'name': 'positive_balance',
            'condition': 'balance > 0',
            'error_msg': 'Balance must be positive'
        }
    ], "loans_table")
    
    # Get summary
    print(validator.get_validation_summary())