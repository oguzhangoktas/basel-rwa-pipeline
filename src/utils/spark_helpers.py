"""
Spark Helper Utilities

Common Spark functions and utilities used across the pipeline.

Author: Portfolio Project
Date: December 2024
"""

from pyspark.sql import functions as F
from typing import Dict, List


def get_spark_config() -> Dict[str, str]:
    """
    Return standard Spark configuration for RWA pipeline
    """
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.partitions": "400",
        "spark.sql.autoBroadcastJoinThreshold": "50MB",
    }


def optimize_dataframe_partitions(
    df, partition_columns: List[str], num_partitions: int = 200
):
    """
    Repartition DataFrame for optimal processing

    Args:
        df: Input DataFrame
        partition_columns: Columns to partition by
        num_partitions: Target number of partitions
    """
    return df.repartition(num_partitions, *partition_columns)


# Add more helper functions as needed
