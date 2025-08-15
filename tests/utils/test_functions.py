"""Utility function unit tests.

Methods:
  define_binary_agreement_vars

  cartesian_join_dataframes - needs updating - reads/writes from HDFS

  create_spark_session - test shell only, currently

  define_K

  define_kj

  define_p

  define_partial_agreement_vars

  format_cutpoints

  get_input_variables - test shell only, currently

  get_s - needs updating - function updated but test not

  read_configs
"""

import configparser as cp
import os

import pytest

from scalelink.utils import functions as ut


def test_define_binary_agreement_vars():
    """
    Tests that define_binary_agreement_vars gives the gives the correct output
    when supplied with appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = ["sex", "dob"]

    # Act
    test_output = ut.define_binary_agreement_vars(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


@pytest.mark.skip(reason="needs updating")
def test_cartesian_join_dataframes(spark, hdfs_test_path):
    """
    Tests that cartesian_join_dataframes() gives the gives the correct output
    when supplied with appropriate inputs.

    Dependencies:
        configparser as cp
        subprocess
    """
    pass


#  # Arrange
#  test_input_df1 = spark.createDataFrame(
#    [
#      (1, 'GREGORY',   'PARKIN'),
#      (2, 'ELIZABETH', 'CARTER'),
#      (3, 'ALFONSO',    None)
#    ],
#    ['id_df1', 'fn_df1', 'sn_df1']
#  )
#
#  test_input_df1.coalesce(1).write.mode('overwrite')\
#  .parquet(hdfs_test_path + 'df1')
#
#  test_input_df2 = spark.createDataFrame(
#    [
#      (1,  None,   'SAYED'),
#      (2, 'AMICA', 'MAGNUSSON'),
#      (3, 'LIZ',   'CARTER-JONES')
#    ],
#    ['id_df2', 'fn_df2', 'sn_df2']
#  )
#
#  test_input_df2.coalesce(1).write.mode('overwrite')\
#  .parquet(hdfs_test_path + 'df2')
#
#  expected_output = spark.createDataFrame(
#    [
#      (1, 'GREGORY',   'PARKIN', 1,  None,   'SAYED'),
#      (2, 'ELIZABETH', 'CARTER', 1,  None,   'SAYED'),
#      (3, 'ALFONSO',    None,    1,  None,   'SAYED'),
#      (1, 'GREGORY',   'PARKIN', 2, 'AMICA', 'MAGNUSSON'),
#      (2, 'ELIZABETH', 'CARTER', 2, 'AMICA', 'MAGNUSSON'),
#      (3, 'ALFONSO',    None,    2, 'AMICA', 'MAGNUSSON'),
#      (1, 'GREGORY',   'PARKIN', 3, 'LIZ',   'CARTER-JONES'),
#      (2, 'ELIZABETH', 'CARTER', 3, 'LIZ',   'CARTER-JONES'),
#      (3, 'ALFONSO',    None,    3, 'LIZ',   'CARTER-JONES'),
#    ],
#    ['id_df1', 'fn_df1', 'sn_df1', 'id_df2', 'fn_df2', 'sn_df2']
#  )
#
#  # Act
#  test_output = ut.cartesian_join_dataframes(
#    df1_path = hdfs_test_path + 'df1',
#    df2_path = hdfs_test_path + 'df2',
#    spark = spark
#  )
#
#  # Assert
#  ch.assert_df_equality(
#    df1 = test_output,
#    df2 = expected_output,
#    ignore_row_order = True
#  )
#
#  # Cleanup
#  for i in ['df1', 'df2']:
#    cmd = f'hdfs dfs -rm -r -skipTrash {hdfs_test_path + i}'
#    p = subprocess.run(cmd, shell = True)


@pytest.mark.skip(reason="test shell")
def test_create_spark_session():
    pass


def test_define_K():
    """
    Tests that define_K() gives the gives the gives the correct output when
    supplied with appropriate inputs.
    """
    # Arrange
    test_input = {"fn": 2, "sn": 4, "sex": 2}

    expected_output = 8

    # Assert
    test_output = ut.define_K(kj=test_input)

    # Act
    assert test_output == expected_output


def test_define_kj():
    """
    Tests that define_kj() gives the gives the correct output when supplied with
    appropriate inputs.
    """
    # Arrange
    test_input = {
        "fn": [0.5],
        "sn": [0.7, 0.9, 0.95],
        "sex": None,
    }

    expected_output = {"fn": 2, "sn": 4, "sex": 2}

    # Act
    test_output = ut.define_kj(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


def test_define_p():
    """
    Tests that define_p() gives the gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input_1 = ["fn", "sn", "sex"]
    test_input_2 = ["fn", "mn", "sn", "sex", "dob"]

    expected_output_1 = 3
    expected_output_2 = 5

    # Act
    test_output_1 = ut.define_p(linkage_vars=test_input_1)
    test_output_2 = ut.define_p(linkage_vars=test_input_2)

    # Assert
    assert test_output_1 == expected_output_1
    assert test_output_2 == expected_output_2


def test_define_partial_agreement_vars():
    """
    Tests that define_partial_agreement_vars() gives the gives the correct
    output when provided with appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = ["fn", "sn"]

    # Act
    test_output = ut.define_partial_agreement_vars(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


def test_format_cutpoints():
    """
    Tests that format_cutpoints() gives the gives the correct output when
    provided with appropriate inputs.

    Dependencies:
      configparser as cp
    """
    # Arrange
    test_input = ["fn", "sn", "sex", "dob"]

    test_config_path = "test_config.ini"
    config = cp.ConfigParser()
    config["cutpoints"] = {
        "fn_cutpoints": "0.4, 0.7, 0.9",
        "sn_cutpoints": "0.75, 0.9",
        "sex_cutpoints": "None",
        "dob_cutpoints": "None",
    }
    with open(test_config_path, "w") as configfile:
        config.write(configfile)

    config.read(test_config_path)
    test_configs = config["cutpoints"]

    expected_output = {
        "fn": [0.4, 0.7, 0.9],
        "sn": [0.75, 0.9],
        "sex": None,
        "dob": None,
    }

    # Act
    test_output = ut.format_cutpoints(linkage_vars=test_input, configs=test_configs)

    # Assert
    assert test_output == expected_output

    # Cleanup
    os.remove("test_config.ini")


@pytest.mark.skip(reason="test shell")
def test_get_input_variables():
    pass


@pytest.mark.skip(reason="needs updating")
def test_get_s(spark):
    """
    Tests that define_s() gives the gives the correct output when provided with
    appropriate inputs.
    """
    pass


#  # Arrange
#  test_input = spark.createDataFrame(
#    [
#      (1, 'GREGORY',   'PARKIN', 1,  None,   'SAYED'),
#      (2, 'ELIZABETH', 'CARTER', 1,  None,   'SAYED'),
#      (3, 'ALFONSO',    None,    1,  None,   'SAYED'),
#      (1, 'GREGORY',   'PARKIN', 2, 'AMICA', 'MAGNUSSON'),
#      (2, 'ELIZABETH', 'CARTER', 2, 'AMICA', 'MAGNUSSON'),
#      (3, 'ALFONSO',    None,    2, 'AMICA', 'MAGNUSSON'),
#      (1, 'GREGORY',   'PARKIN', 3, 'LIZ',   'CARTER-JONES'),
#      (2, 'ELIZABETH', 'CARTER', 3, 'LIZ',   'CARTER-JONES'),
#      (3, 'ALFONSO',    None,    3, 'LIZ',   'CARTER-JONES'),
#    ],
#    ['id_df1', 'fn_df1', 'sn_df1', 'id_df2', 'fn_df2', 'sn_df2']
#  )
#
#  expected_output = 9
#
#  # Assert
#  test_output = ut.define_s(df = test_input)
#  define_s(input_variables, df_cartesian_join)
#
#  # Act
#  assert test_output == expected_output


def test_read_configs():
    """
    Tests that read_configs gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      configparser installed as cp
    """
    # Arrange
    test_config_path = "test_config.ini"
    config = cp.ConfigParser()
    config["section_1"] = {"variables": "a, b, c, d"}
    with open(test_config_path, "w") as configfile:
        config.write(configfile)

    # Act
    test_output = ut.read_configs(
        config_path=test_config_path, config_section="section_1"
    )

    # Assert
    assert test_output["variables"] == "a, b, c, d"

    # Cleanup
    os.remove("test_config.ini")
