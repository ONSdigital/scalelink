"""
Utility function unit test.

Methods:
  create_spark_session

This utility function requires no other Spark sessions to be concurrently
running in the environment. Else, a new Spark session will not be made,
expected_outputs will reflect the prior Spark session and the test will fail.
Some of the other utility function tests use the spark fixture, which has
modular scope. Hence, this test has to be held in a separate module from them.
"""

import pytest

from scalelink.utils import utils as ut


@pytest.mark.parametrize(
    "test_input, expected_output",
    [
        pytest.param(
            "s",
            {
                "spark.executor.memory": "1g",
                "spark.yarn.executor.memoryOverhead": "1g",
                "spark.executor.cores": "1",
                "spark.dynamicAllocation.maxExecutors": "3",
                "spark.sql.shuffle.partitions": "12",
                "spark.shuffle.service.enabled": "false",
                "spark.ui.showConsoleProgress": "false",
            },
            id="small_session",
        ),
        pytest.param(
            "m",
            {
                "spark.executor.memory": "6g",
                "spark.yarn.executor.memoryOverhead": "1g",
                "spark.executor.cores": "3",
                "spark.dynamicAllocation.maxExecutors": "3",
                "spark.sql.shuffle.partitions": "18",
                "spark.shuffle.service.enabled": "false",
                "spark.ui.showConsoleProgress": "false",
            },
            id="medium_session",
        ),
        pytest.param(
            "l",
            {
                "spark.executor.memory": "10g",
                "spark.yarn.executor.memoryOverhead": "1g",
                "spark.executor.cores": "5",
                "spark.dynamicAllocation.maxExecutors": "5",
                "spark.sql.shuffle.partitions": "200",
                "spark.shuffle.service.enabled": "false",
                "spark.ui.showConsoleProgress": "false",
            },
            id="large_session",
        ),
        pytest.param(
            "xl",
            {
                "spark.executor.memory": "20g",
                "spark.yarn.executor.memoryOverhead": "2g",
                "spark.executor.cores": "5",
                "spark.dynamicAllocation.maxExecutors": "12",
                "spark.sql.shuffle.partitions": "240",
                "spark.shuffle.service.enabled": "false",
                "spark.ui.showConsoleProgress": "false",
            },
            id="extra_large_session",
        ),
    ],
)
def test_create_spark_session(test_input: str, expected_output: dict[str, str]) -> None:
    """
    Tests that create_spark_session() gives the correct output when supplied
    with appropriate inputs.

    Dependencies:
      No other Spark sessions currently running in the environment.
    """
    # Act
    spark_session = ut.create_spark_session(
        spark_session_name=test_input,
        spark_session_size=test_input,
    )
    test_output = dict(spark_session.sparkContext.getConf().getAll())
    spark_session.stop()

    # Assert
    for key in expected_output.keys():
        assert expected_output[key] == test_output[key]
