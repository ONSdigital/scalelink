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

from scalelink.utils import utils as ut


def test_create_spark_session():
    """
    Tests that create_spark_session() gives the correct output when supplied
    with appropriate inputs.

    Dependencies:
      No other Spark sessions currently running in the environment.
    """
    # Arrange
    test_inputs = {
        1: ["test_1", "s"],
        2: ["test_2", "m"],
        3: ["test_3", "l"],
        4: ["test_4", "xl"],
    }

    expected_outputs = {
        "s": {
            "spark.executor.memory": "1g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": "1",
            "spark.dynamicAllocation.maxExecutors": "3",
            "spark.sql.shuffle.partitions": "12",
            "spark.shuffle.service.enabled": "true",
            "spark.ui.showConsoleProgress": "false",
        },
        "m": {
            "spark.executor.memory": "6g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": "3",
            "spark.dynamicAllocation.maxExecutors": "3",
            "spark.sql.shuffle.partitions": "18",
            "spark.shuffle.service.enabled": "true",
            "spark.ui.showConsoleProgress": "false",
        },
        "l": {
            "spark.executor.memory": "10g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": "5",
            "spark.dynamicAllocation.maxExecutors": "5",
            "spark.sql.shuffle.partitions": "200",
            "spark.shuffle.service.enabled": "true",
            "spark.ui.showConsoleProgress": "false",
        },
        "xl": {
            "spark.executor.memory": "20g",
            "spark.yarn.executor.memoryOverhead": "2g",
            "spark.executor.cores": "5",
            "spark.dynamicAllocation.maxExecutors": "12",
            "spark.sql.shuffle.partitions": "240",
            "spark.shuffle.service.enabled": "true",
            "spark.ui.showConsoleProgress": "false",
        },
    }

    # Act
    test_outputs = {}
    for count, i in enumerate(test_inputs, 1):
        spark_session = ut.create_spark_session(
            spark_session_name=test_inputs[count][0],
            spark_session_size=test_inputs[count][1],
        )
        test_output = dict(spark_session.sparkContext.getConf().getAll())
        test_outputs.update({test_inputs[count][1]: test_output})
        spark_session.stop()

    # Assert
    for count, size in enumerate(expected_outputs.keys()):
        for key in expected_outputs[size].keys():
            assert (
                expected_outputs[size][key] == test_outputs[size][key]
            ), f"for {size}:\n expected_output {key} is {expected_outputs[size][key]}\n test_output {key} is {test_outputs[size][key]}"
