"""
This file is run by pytest before all tests are discovered.
Therefore, any fixtures defined here are available to all tests.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Sets up the Spark session by using a fixture decorator.
    """
    return (
        SparkSession.builder.appName("Unit testing")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 30)
        .getOrCreate()
    )
