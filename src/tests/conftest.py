"""
Pytest Configuration and Fixtures

Author: Portfolio Project
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for tests"""
    spark = SparkSession.builder \
        .appName("RWA-Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_loan_data():
    """Sample loan data for testing"""
    return [
        {
            "loan_id": "L001",
            "customer_id": "C001",
            "outstanding_balance": 100000.0,
            "asset_class": "CORPORATE_LARGE",
            "rating": "AAA"
        },
        {
            "loan_id": "L002",
            "customer_id": "C002",
            "outstanding_balance": 50000.0,
            "asset_class": "RETAIL_MORTGAGE",
            "rating": "BBB"
        }
    ]
