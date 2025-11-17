"""Utility function unit tests.

Methods:
  define_binary_agreement_vars

  cartesian_join_dataframes

  define_K

  define_kj

  define_p

  define_partial_agreement_vars

  format_cutpoints

  get_input_variables

  get_s

  read_configs
"""

import configparser as cp
import os
import unittest
from unittest.mock import Mock, patch

import chispa as ch
import pyspark

from scalelink.utils import utils as ut


def test_define_binary_agreement_vars() -> None:
    """
    Tests that define_binary_agreement_vars gives the correct output when
    supplied with appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = ["sex", "dob"]

    # Act
    test_output = ut.define_binary_agreement_vars(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


def test_cartesian_join_dataframes(
    spark: pyspark.sql.SparkSession, spark_mock: unittest.mock.MagicMock
) -> None:
    """
    Tests that cartesian_join_dataframes() gives the correct output when supplied
    with appropriate inputs.
    """
    # Arrange
    mock_path = Mock()

    mock_df1 = spark.createDataFrame(
        [(1, "GREGORY", "PARKIN"), (2, "ELIZABETH", "CARTER"), (3, "ALFONSO", None)],
        ["id_df1", "fn_df1", "sn_df1"],
    )

    mock_df2 = spark.createDataFrame(
        [(1, None, "SAYED"), (2, "AMICA", "MAGNUSSON"), (3, "LIZ", "CARTER-JONES")],
        ["id_df2", "fn_df2", "sn_df2"],
    )

    spark_mock.read.parquet.side_effect = [
        mock_df1,
        mock_df2,
    ]

    expected_output = spark.createDataFrame(
        [
            (1, "GREGORY", "PARKIN", 1, None, "SAYED"),
            (2, "ELIZABETH", "CARTER", 1, None, "SAYED"),
            (3, "ALFONSO", None, 1, None, "SAYED"),
            (1, "GREGORY", "PARKIN", 2, "AMICA", "MAGNUSSON"),
            (2, "ELIZABETH", "CARTER", 2, "AMICA", "MAGNUSSON"),
            (3, "ALFONSO", None, 2, "AMICA", "MAGNUSSON"),
            (1, "GREGORY", "PARKIN", 3, "LIZ", "CARTER-JONES"),
            (2, "ELIZABETH", "CARTER", 3, "LIZ", "CARTER-JONES"),
            (3, "ALFONSO", None, 3, "LIZ", "CARTER-JONES"),
        ],
        ["id_df1", "fn_df1", "sn_df1", "id_df2", "fn_df2", "sn_df2"],
    )

    # Act
    test_output = ut.cartesian_join_dataframes(
        df1_path=mock_path, df2_path=mock_path, spark=spark_mock
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_define_K() -> None:
    """
    Tests that define_K() gives the correct output when supplied with appropriate
    inputs.
    """
    # Arrange
    test_input = {"fn": 2, "sn": 4, "sex": 2}

    expected_output = 8

    # Act
    test_output = ut.define_K(kj=test_input)

    # Assert
    assert test_output == expected_output


def test_define_kj() -> None:
    """
    Tests that define_kj() gives the correct output when supplied with appropriate
    inputs.
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


def test_define_p() -> None:
    """
    Tests that define_p() gives the correct output when provided with appropriate
    inputs.
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


def test_define_partial_agreement_vars() -> None:
    """
    Tests that define_partial_agreement_vars() gives the correct output when
    provided with appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = ["fn", "sn"]

    # Act
    test_output = ut.define_partial_agreement_vars(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


def test_format_cutpoints() -> None:
    """
    Tests that format_cutpoints() gives the correct output when provided with
    appropriate inputs.

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


@patch("scalelink.utils.utils.define_K")
@patch("scalelink.utils.utils.define_kj")
@patch("scalelink.utils.utils.define_p")
@patch("scalelink.utils.utils.define_partial_agreement_vars")
@patch("scalelink.utils.utils.define_binary_agreement_vars")
@patch("scalelink.utils.utils.format_cutpoints")
@patch("scalelink.utils.utils.read_configs")
def test_get_input_variables(
    mock_read_configs: unittest.mock.MagicMock,
    mock_format_cutpoints: unittest.mock.MagicMock,
    mock_define_binary_agreement_vars: unittest.mock.MagicMock,
    mock_define_partial_agreement_vars: unittest.mock.MagicMock,
    mock_define_p: unittest.mock.MagicMock,
    mock_define_kj: unittest.mock.MagicMock,
    mock_define_K: unittest.mock.MagicMock,
) -> None:
    """
    Tests that get_input_variables() gives the correct output when provided with
    appropriate inputs.

      Dependencies:
        configparser as cp
    """
    # Arrange
    test_input_filepath = "folder/subfolder/config_file.yaml"
    test_input_config = cp.ConfigParser()
    test_input_config["run_spec"] = {"spark_session_size": "m"}
    test_input_config["filepaths"] = {
        "bucket_name": "my_bucket",
        "ssl_file": "my_ssl_file",
        "df1_path": "folder/subfolder/df1",
        "df2_path": "folder/subfolder/df2",
        "df_candidates_path": "folder/subfolder/df_candidates",
        "checkpoint_path": "folder/subfolder/checkpoints/",
        "output_path": "folder/subfolder/output/",
    }
    test_input_config["variables"] = {
        "df1_id": "df1_id",
        "df2_id": "df2_id",
        "linkage_vars": "forename, surname, sex, dob",
        "df1_suffix": "_df1",
        "df2_suffix": "_df2",
    }
    test_input_config["cutpoints"] = {
        "fn_cutpoints": "0.5, 0.8",
        "mn_cutpoints": "0.5, 0.8",
        "sn_cutpoints": "0.5, 0.8",
        "dob_cutpoints": "None",
        "sex_cutpoints": "None",
        "pc1_cutpoints": "0.9",
        "pc2_cutpoints": "0.9",
    }
    test_input_formatted_linkage_vars = ["forename", "surname", "sex", "dob"]
    test_input_formatted_cutpoints = {
        "fn_cutpoints": [0.5, 0.8],
        "mn_cutpoints": [0.5, 0.8],
        "sn_cutpoints": [0.5, 0.8],
        "dob_cutpoints": None,
        "sex_cutpoints": None,
        "pc1_cutpoints": [0.9],
        "pc2_cutpoints": [0.9],
    }
    test_input_kj = {"fn": 3, "mn": 3, "sn": 3, "dob": 2, "sex": 2, "pc1": 2, "pc2": 2}

    mock_read_configs.return_value = test_input_config
    mock_format_cutpoints.return_value = test_input_formatted_cutpoints
    mock_define_kj.return_value = test_input_kj

    # Act
    _ = ut.get_input_variables(config_path=test_input_filepath)

    # Assert
    mock_read_configs.assert_called_once_with(config_path=test_input_filepath)
    mock_format_cutpoints.assert_called_once_with(
        linkage_vars=test_input_formatted_linkage_vars,
        configs=test_input_config["cutpoints"],
    )
    mock_define_binary_agreement_vars.assert_called_once_with(
        cutpoints=test_input_formatted_cutpoints
    )
    mock_define_partial_agreement_vars.assert_called_once_with(
        cutpoints=test_input_formatted_cutpoints
    )
    mock_define_p.assert_called_once_with(
        linkage_vars=test_input_formatted_linkage_vars
    )
    mock_define_kj.assert_called_once_with(cutpoints=test_input_formatted_cutpoints)
    mock_define_K.assert_called_once_with(kj=test_input_kj)


def test_get_s(spark: pyspark.sql.SparkSession) -> None:
    """
    Tests that get_s() gives the correct output when provided with appropriate
    inputs.
    """
    # Arrange
    test_input_df = spark.createDataFrame(
        [
            (1, "GREGORY", "PARKIN", 1, None, "SAYED"),
            (2, "ELIZABETH", "CARTER", 1, None, "SAYED"),
            (3, "ALFONSO", None, 1, None, "SAYED"),
            (1, "GREGORY", "PARKIN", 2, "AMICA", "MAGNUSSON"),
            (2, "ELIZABETH", "CARTER", 2, "AMICA", "MAGNUSSON"),
            (3, "ALFONSO", None, 2, "AMICA", "MAGNUSSON"),
            (1, "GREGORY", "PARKIN", 3, "LIZ", "CARTER-JONES"),
            (2, "ELIZABETH", "CARTER", 3, "LIZ", "CARTER-JONES"),
            (3, "ALFONSO", None, 3, "LIZ", "CARTER-JONES"),
        ],
        ["id_df1", "fn_df1", "sn_df1", "id_df2", "fn_df2", "sn_df2"],
    )
    test_input_variables = {}

    expected_output = {"s": 9}

    # Act
    test_output = ut.get_s(
        input_variables=test_input_variables, df_cartesian_join=test_input_df
    )

    # Assert
    assert test_output == expected_output


def test_read_configs() -> None:
    """
    Tests that read_configs gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      configparser as cp
    """
    # Arrange
    test_config_path = "test_config.ini"
    config = cp.ConfigParser()
    config["section_1"] = {"variables": "a, b, c, d"}
    with open(test_config_path, "w") as configfile:
        config.write(configfile)

    # Act
    test_output = ut.read_configs(config_path=test_config_path)

    # Assert
    assert test_output["section_1"]["variables"] == "a, b, c, d"

    # Cleanup
    os.remove("test_config.ini")
