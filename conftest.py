"""
This file is run by pytest before all tests are discovered.
Therefore, any fixtures defined here are available to all tests.
"""

import unittest
from unittest.mock import Mock

import pyspark
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T


@pytest.fixture(scope="module")
def spark() -> pyspark.sql.SparkSession:
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
def spark_mock() -> unittest.mock.MagicMock:
    """
    Sets up a mock version of Spark.

    Copied from https://bargsten.org/python/mocking-spark-instance/
    """
    spark_mock = Mock()
    type(spark_mock).write = spark_mock
    type(spark_mock).read = spark_mock
    spark_mock.table.return_value = spark_mock
    spark_mock.format.return_value = spark_mock
    spark_mock.option.return_value = spark_mock
    spark_mock.mode.return_value = spark_mock
    spark_mock.save.return_value = None
    return spark_mock


@pytest.fixture(scope="module")
def compare_deltas_output(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    """
    Sets up the expected output for test_compare_deltas, which is also the
    test input for test_calculate_njklm_values.
    """
    return spark.createDataFrame(
        [
            (0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1),
            (1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1),
            (1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0),
            (0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1),
            (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
            (0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ],
        T.StructType(
            [
                T.StructField("N_sex_1_sex_1", T.IntegerType(), False),
                T.StructField("N_sex_1_sex_2", T.IntegerType(), False),
                T.StructField("N_sex_1_forename_1", T.IntegerType(), False),
                T.StructField("N_sex_1_forename_2", T.IntegerType(), False),
                T.StructField("N_sex_1_forename_3", T.IntegerType(), False),
                T.StructField("N_sex_2_sex_1", T.IntegerType(), False),
                T.StructField("N_sex_2_sex_2", T.IntegerType(), False),
                T.StructField("N_sex_2_forename_1", T.IntegerType(), False),
                T.StructField("N_sex_2_forename_2", T.IntegerType(), False),
                T.StructField("N_sex_2_forename_3", T.IntegerType(), False),
                T.StructField("N_forename_1_sex_1", T.IntegerType(), False),
                T.StructField("N_forename_1_sex_2", T.IntegerType(), False),
                T.StructField("N_forename_1_forename_1", T.IntegerType(), False),
                T.StructField("N_forename_1_forename_2", T.IntegerType(), False),
                T.StructField("N_forename_1_forename_3", T.IntegerType(), False),
                T.StructField("N_forename_2_sex_1", T.IntegerType(), False),
                T.StructField("N_forename_2_sex_2", T.IntegerType(), False),
                T.StructField("N_forename_2_forename_1", T.IntegerType(), False),
                T.StructField("N_forename_2_forename_2", T.IntegerType(), False),
                T.StructField("N_forename_2_forename_3", T.IntegerType(), False),
                T.StructField("N_forename_3_sex_1", T.IntegerType(), False),
                T.StructField("N_forename_3_sex_2", T.IntegerType(), False),
                T.StructField("N_forename_3_forename_1", T.IntegerType(), False),
                T.StructField("N_forename_3_forename_2", T.IntegerType(), False),
                T.StructField("N_forename_3_forename_3", T.IntegerType(), False),
            ]
        ),
    )
