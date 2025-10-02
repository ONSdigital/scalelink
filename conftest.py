"""
This file is run by pytest before all tests are discovered.
Therefore, any fixtures defined here are available to all tests.
"""

from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Sets up the Spark session by using a fixture decorator.
    """
    spark_session = (
        SparkSession.builder.appName("Unit testing")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.maxExecutors", 30)
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="function")
def spark_mock():
    # From https://bargsten.org/python/mocking-spark-instance/
    spark_mock = Mock()
    type(spark_mock).write = spark_mock
    type(spark_mock).read = spark_mock
    spark_mock.table.return_value = spark_mock
    spark_mock.format.return_value = spark_mock
    spark_mock.option.return_value = spark_mock
    spark_mock.mode.return_value = spark_mock
    spark_mock.save.return_value = None
    return spark_mock
