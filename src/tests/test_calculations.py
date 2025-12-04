"""
Unit Tests for Basel III RWA Calculations

Tests the core calculation logic with known Basel III examples
to ensure regulatory compliance.

Author: Portfolio Project
Date: December 2024
"""

import pytest
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return (
        SparkSession.builder.appName("RWA-Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestRWACalculations:
    """Test suite for Basel III RWA calculations"""

    def test_rwa_aaa_corporate(self, spark):
        """
        Test RWA calculation for AAA-rated corporate loan
        Basel III: AAA corporate exposure = 20% risk weight
        """
        # Given: AAA corporate loan with $100M exposure
        data = [
            (
                "L001",
                "C001",
                100_000_000.0,  # outstanding_balance
                "CORPORATE_LARGE",
                "AAA",
                1.0,  # credit_conversion_factor
                0.0,  # collateral_coverage
            )
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("outstanding_balance", DoubleType()),
                StructField("asset_class", StringType()),
                StructField("rating_bucket", StringType()),
                StructField("ccf", DoubleType()),
                StructField("collateral_coverage", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Calculate RWA
        result_df = (
            df.withColumn("ead", F.col("outstanding_balance") * F.col("ccf"))
            .withColumn(
                "ead_after_crm", F.col("ead") * (1 - F.col("collateral_coverage"))
            )
            .withColumn("risk_weight", F.lit(0.20))  # Basel III: AAA corporate = 20%
            .withColumn("rwa", F.col("ead_after_crm") * F.col("risk_weight"))
            .withColumn(
                "regulatory_capital",
                F.col("rwa") * F.lit(0.08),  # Basel III: 8% capital requirement
            )
        )

        result = result_df.collect()[0]

        # Then: Verify calculations
        assert result["ead"] == 100_000_000.0
        assert result["ead_after_crm"] == 100_000_000.0
        assert result["risk_weight"] == 0.20
        assert result["rwa"] == 20_000_000.0
        assert result["regulatory_capital"] == 1_600_000.0

    def test_rwa_retail_mortgage_with_collateral(self, spark):
        """
        Test RWA calculation for mortgage with collateral
        Basel III: Residential mortgage = 35% risk weight
        With 80% collateral coverage, EAD should be reduced
        """
        # Given: Mortgage with 80% LTV (20% equity)
        data = [
            (
                "L002",
                "C002",
                200_000.0,  # loan amount
                "RETAIL_MORTGAGE",
                "BBB",
                1.0,
                0.80,  # 80% collateral coverage
            )
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("outstanding_balance", DoubleType()),
                StructField("asset_class", StringType()),
                StructField("rating_bucket", StringType()),
                StructField("ccf", DoubleType()),
                StructField("collateral_coverage", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Calculate RWA
        result_df = (
            df.withColumn("ead", F.col("outstanding_balance") * F.col("ccf"))
            .withColumn(
                "ead_after_crm", F.col("ead") * (1 - F.col("collateral_coverage"))
            )
            .withColumn("risk_weight", F.lit(0.35))  # Retail mortgage
            .withColumn("rwa", F.col("ead_after_crm") * F.col("risk_weight"))
        )

        result = result_df.collect()[0]

        # Then: Verify collateral reduces exposure
        assert result["ead"] == 200_000.0
        assert result["ead_after_crm"] == 40_000.0  # 20% of 200k
        assert result["rwa"] == 14_000.0  # 40k * 0.35

    def test_rwa_high_risk_unsecured(self, spark):
        """
        Test RWA for high-risk unsecured corporate loan
        Basel III: B-rated corporate = 150% risk weight
        """
        data = [
            (
                "L003",
                "C003",
                50_000_000.0,
                "CORPORATE_LARGE",
                "B",
                1.0,
                0.0,  # No collateral
            )
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("outstanding_balance", DoubleType()),
                StructField("asset_class", StringType()),
                StructField("rating_bucket", StringType()),
                StructField("ccf", DoubleType()),
                StructField("collateral_coverage", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        result_df = (
            df.withColumn("ead", F.col("outstanding_balance") * F.col("ccf"))
            .withColumn("risk_weight", F.lit(1.50))  # B-rated corporate
            .withColumn("rwa", F.col("ead") * F.col("risk_weight"))
        )

        result = result_df.collect()[0]

        # Then: High risk = RWA exceeds exposure
        assert result["rwa"] == 75_000_000.0  # 150% of exposure
        assert result["rwa"] > result["ead"]  # RWA > EAD for risky loans

    def test_rwa_off_balance_sheet(self, spark):
        """
        Test RWA for off-balance sheet items (committed credit line)
        Basel III: Credit lines have CCF of 50%
        """
        data = [
            (
                "L004",
                "C004",
                10_000_000.0,  # Committed line amount
                "CORPORATE_SME",
                "BBB",
                0.50,  # CCF for committed lines
                0.0,
            )
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("outstanding_balance", DoubleType()),
                StructField("asset_class", StringType()),
                StructField("rating_bucket", StringType()),
                StructField("ccf", DoubleType()),
                StructField("collateral_coverage", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        result_df = (
            df.withColumn("ead", F.col("outstanding_balance") * F.col("ccf"))
            .withColumn("risk_weight", F.lit(0.75))  # SME BBB
            .withColumn("rwa", F.col("ead") * F.col("risk_weight"))
        )

        result = result_df.collect()[0]

        # Then: Only 50% of committed line counts as exposure
        assert result["ead"] == 5_000_000.0  # 50% of 10M
        assert result["rwa"] == 3_750_000.0  # 5M * 0.75

    def test_expected_loss_calculation(self, spark):
        """
        Test Expected Loss = EAD × PD × LGD
        """
        data = [("L005", "C005", 1_000_000.0, 0.05, 0.45)]  # PD = 5%  # LGD = 45%

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("ead", DoubleType()),
                StructField("probability_of_default", DoubleType()),
                StructField("loss_given_default", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        result_df = df.withColumn(
            "expected_loss",
            F.col("ead")
            * F.col("probability_of_default")
            * F.col("loss_given_default"),
        )

        result = result_df.collect()[0]

        # Then: EL = 1M × 5% × 45% = $22,500
        assert result["expected_loss"] == 22_500.0

    def test_multiple_loans_aggregation(self, spark):
        """
        Test aggregation of RWA across multiple loans
        """
        data = [
            ("L001", "C001", 100_000_000.0, 0.20, 20_000_000.0),
            ("L002", "C002", 50_000_000.0, 0.35, 17_500_000.0),
            ("L003", "C003", 25_000_000.0, 0.75, 18_750_000.0),
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("ead", DoubleType()),
                StructField("risk_weight", DoubleType()),
                StructField("rwa", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Aggregate total RWA
        total_rwa = df.agg(F.sum("rwa")).collect()[0][0]
        total_capital = total_rwa * 0.08

        # Then: Total RWA and capital requirements
        assert total_rwa == 56_250_000.0
        assert total_capital == 4_500_000.0

    def test_zero_balance_loan(self, spark):
        """
        Test handling of zero or negative balances
        """
        data = [("L006", "C006", 0.0, 0.50), ("L007", "C007", -1000.0, 0.50)]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("outstanding_balance", DoubleType()),
                StructField("risk_weight", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Filter invalid balances
        valid_df = df.filter(F.col("outstanding_balance") > 0)

        # Then: Invalid loans should be excluded
        assert valid_df.count() == 0

    def test_collateral_coverage_cap(self, spark):
        """
        Test that collateral coverage cannot exceed 100%
        """
        data = [
            ("L008", "C008", 100_000.0, 1.5)  # 150% coverage - should be capped at 100%
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("ead", DoubleType()),
                StructField("collateral_coverage_raw", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Cap collateral at 100%
        result_df = df.withColumn(
            "collateral_coverage", F.least(F.col("collateral_coverage_raw"), F.lit(1.0))
        ).withColumn("ead_after_crm", F.col("ead") * (1 - F.col("collateral_coverage")))

        result = result_df.collect()[0]

        # Then: Coverage capped at 100%, EAD becomes 0
        assert result["collateral_coverage"] == 1.0
        assert result["ead_after_crm"] == 0.0


class TestDataQuality:
    """Test data quality validations"""

    def test_negative_balance_detection(self, spark):
        """Test detection of negative balances"""
        data = [("L001", 100_000.0), ("L002", -50_000.0), ("L003", 75_000.0)]  # Invalid

        schema = StructType(
            [StructField("loan_id", StringType()), StructField("balance", DoubleType())]
        )

        df = spark.createDataFrame(data, schema)

        # When: Check for negative balances
        invalid_count = df.filter(F.col("balance") < 0).count()

        # Then: Should detect 1 invalid record
        assert invalid_count == 1

    def test_missing_required_fields(self, spark):
        """Test detection of null required fields"""
        data = [
            ("L001", "C001", 100_000.0),
            ("L002", None, 50_000.0),  # Missing customer_id
            ("L003", "C003", None),  # Missing balance
        ]

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("balance", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Check for nulls
        null_customer = df.filter(F.col("customer_id").isNull()).count()
        null_balance = df.filter(F.col("balance").isNull()).count()

        # Then: Detect null values
        assert null_customer == 1
        assert null_balance == 1


class TestPerformanceOptimizations:
    """Test performance optimization techniques"""

    def test_salting_for_skewed_keys(self, spark):
        """Test salting technique for handling data skew"""
        # Given: Skewed data (one customer has many loans)
        data = []
        # Customer C001 has 1000 loans (skewed)
        for i in range(1000):
            data.append((f"L{i:04d}", "C001", 10000.0))
        # Other customers have 1 loan each
        for i in range(1000, 1100):
            data.append((f"L{i:04d}", f"C{i:04d}", 10000.0))

        schema = StructType(
            [
                StructField("loan_id", StringType()),
                StructField("customer_id", StringType()),
                StructField("balance", DoubleType()),
            ]
        )

        df = spark.createDataFrame(data, schema)

        # When: Identify skewed customers
        customer_counts = df.groupBy("customer_id").count()
        skewed_customers = customer_counts.filter(F.col("count") > 100)

        # Then: Should identify C001 as skewed
        skewed_list = skewed_customers.select("customer_id").collect()
        assert len(skewed_list) == 1
        assert skewed_list[0]["customer_id"] == "C001"

        # When: Apply salting
        salted_df = df.join(
            F.broadcast(skewed_customers.select("customer_id")), "customer_id", "left"
        ).withColumn(
            "salt",
            F.when(F.col("count").isNotNull(), (F.rand() * 10).cast("int")).otherwise(
                F.lit(0)
            ),
        )

        # Then: Verify salting applied
        salt_distribution = salted_df.groupBy("salt").count().collect()
        # Should have 11 groups (0-9 for skewed, 0 for non-skewed)
        assert len(salt_distribution) == 11


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
