"""Indicator matrix function unit tests.

Methods:
  test_calculate_agreement_states

  test_calculate_deltas

  test_calculate_sorensen_dice

  test_compare_deltas

  test_compute_normalised_levenshtein

  test_get_deltas

  test_make_bigrams
"""

import unittest
from unittest.mock import patch

import chispa as ch
import pyspark
import pytest

from scalelink.indicator_matrix import indicator_matrix as im


def test_calculate_agreement_states(
    spark: pyspark.sql.SparkSession,
    sorensen_dice_output_df: pyspark.sql.DataFrame,
    agreement_states_output_df: pyspark.sql.DataFrame,
) -> None:
    """
    Tests that calculate_agreement_states gives the correct output when provided
    with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = sorensen_dice_output_df
    expected_output = agreement_states_output_df

    # Act
    test_output = im.calculate_agreement_states(
        df=test_input,
        binary_agreement_cols=["sex"],
        df_suffixes=["_df1", "_df2"],
    )

    # Assert
    ch.assert_df_equality(
        df1=test_output,
        df2=expected_output,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_calculate_deltas(
    spark: pyspark.sql.SparkSession,
    agreement_states_output_df: pyspark.sql.DataFrame,
    calculate_deltas_output_df: pyspark.sql.DataFrame,
) -> None:
    """
    Tests that calculate_deltas gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = agreement_states_output_df
    expected_output = calculate_deltas_output_df

    # Act
    test_output = im.calculate_deltas(
        df=test_input,
        cutpoints={"sex": None, "forename": [0.5, 0.8]},
        agreement_col_suffix="_agr_state",
        string_similarity_suffix="_sorensen_dice",
    )

    # Assert
    ch.assert_df_equality(
        df1=test_output,
        df2=expected_output,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_calculate_sorensen_dice(
    spark: pyspark.sql.SparkSession,
    sorensen_dice_input_df: pyspark.sql.DataFrame,
    sorensen_dice_output_df: pyspark.sql.DataFrame,
) -> None:
    """
    Tests that calculate_sorensen_dice gives the correct output when provided
    with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = sorensen_dice_input_df
    expected_output = sorensen_dice_output_df

    # Act
    test_output = im.calculate_sorensen_dice(
        df=test_input,
        col1="forename_df1",
        col2="forename_df2",
        new_col="forename_sorensen_dice",
        decimal_places=4,
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_compare_deltas(
    spark: pyspark.sql.SparkSession, compare_deltas_output: pyspark.sql.DataFrame
) -> None:
    """
    Tests that compare_deltas gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH", True, 0.8889, False, True, False, False, True),
            (
                1,
                2,
                "ALEESHA",
                "ALEESHA",
                False,
                1.0000,
                True,
                False,
                False,
                False,
                True,
            ),
            (2, 1, "TOM", "GRACE", False, 0.0000, True, False, True, False, False),
            (None, 1, "RITA", "RITER", True, 0.5714, False, True, False, True, False),
            (
                2,
                None,
                "BILAL",
                "BILLALL",
                True,
                0.8000,
                False,
                True,
                False,
                False,
                True,
            ),
            (None, None, "Q", "HUI", None, 0.0000, False, False, True, False, False),
            (1, 1, None, "YUSUF", True, None, False, True, False, False, False),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
            "sex_agr_state",
            "forename_sorensen_dice",
            "di_sex_1",
            "di_sex_2",
            "di_forename_1",
            "di_forename_2",
            "di_forename_3",
        ],
    )

    expected_output = compare_deltas_output

    # Act
    test_output = im.compare_deltas(
        df=test_input, linkage_vars=["sex", "forename"], delta_col_prefix="di_"
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


@pytest.mark.parametrize(
    "test_input_df1_rows, test_input_df1_cols, test_input_df2_rows, test_input_df2_cols, expected_output_df_rows, expected_output_df_cols",
    [
        pytest.param(
            [
                ("1", "string", "string"),
                ("2", "stringed", "str"),
                ("3", "tring", "tri"),
                ("4", "ttri", "str"),
                ("5", None, "string"),
                ("6", "string", None),
                ("7", None, None),
            ],
            ("id", "string_1", "string_2"),
            None,
            None,
            [
                ("1", "string", "string", 1.0),
                ("2", "stringed", "str", 0.375),
                ("3", "tring", "tri", 0.6),
                ("4", "ttri", "str", 0.5),
                ("5", None, "string", None),
                ("6", "string", None, None),
                ("7", None, None, None),
            ],
            ("id", "string_1", "string_2", "normalised_levenshtein_output"),
            id="compare_cols_from_one_df",
        ),
        pytest.param(
            [
                ("1", "string"),
                ("2", None),
                ("3", "str"),
                ("4", None),
                ("5", "string"),
            ],
            ("l_id", "string_1"),
            [
                ("1", "string"),
                ("2", "str"),
                ("3", None),
                ("4", None),
                ("5", "st"),
                ("6", "ttri"),
            ],
            ("r_id", "string_2"),
            [
                ("1", "string", "1", "string"),
                ("5", "string", "1", "string"),
                ("3", "str", "2", "str"),
            ],
            ("l_id", "string_1", "r_id", "string_2"),
            id="compare_cols_from_two_dfs",
        ),
    ],
)
def test_compute_normalised_levenshtein(
    test_input_df1_rows: list[tuple[str | None]],
    test_input_df1_cols: tuple[str],
    test_input_df2_rows: list[tuple[str | None]],
    test_input_df2_cols: tuple[str],
    expected_output_df_rows: list[tuple[str | None]],
    expected_output_df_cols: tuple[str],
    spark: pyspark.sql.SparkSession,
) -> None:
    """
    Tests that compute_normalised_levenshtein gives the correct output when
    provided with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Assert
    test_input_1 = spark.createDataFrame(test_input_df1_rows, test_input_df1_cols)

    if test_input_df2_rows is not None:
        test_input_2 = spark.createDataFrame(test_input_df2_rows, test_input_df2_cols)

    expected_output = spark.createDataFrame(
        expected_output_df_rows, expected_output_df_cols
    )

    # Act
    if test_input_df2_rows is None:
        test_output = test_input_1.withColumn(
            "normalised_levenshtein_output",
            im.compute_normalised_levenshtein(
                test_input_1[test_input_df1_cols[1]],
                test_input_1[test_input_df1_cols[2]],
            ),
        )
    else:
        test_output = test_input_1.join(
            test_input_2,
            (
                im.compute_normalised_levenshtein(
                    test_input_1[test_input_df1_cols[1]],
                    test_input_2[test_input_df2_cols[1]],
                )
                > 0.7
            ),
            how="inner",
        )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


@patch("scalelink.indicator_matrix.indicator_matrix.calculate_deltas")
@patch("scalelink.indicator_matrix.indicator_matrix.calculate_agreement_states")
@patch("scalelink.indicator_matrix.indicator_matrix.calculate_sorensen_dice")
def test_get_deltas(
    mock_calculate_sorensen_dice: unittest.mock.MagicMock,
    mock_calculate_agreement_states: unittest.mock.MagicMock,
    mock_calculate_deltas: unittest.mock.MagicMock,
    spark_mock: unittest.mock.MagicMock,
    sorensen_dice_input_df: pyspark.sql.DataFrame,
    sorensen_dice_output_df: pyspark.sql.DataFrame,
    agreement_states_output_df: pyspark.sql.DataFrame,
) -> None:
    """
    Tests that get_deltas gives the correct output when provided with appropriate
    inputs.
    """
    # Arrange
    test_input_df = sorensen_dice_input_df
    test_input_dict = {
        "df1_suffix": "_df1",
        "df2_suffix": "_df2",
        "binary_agreement_vars": ["sex"],
        "partial_agreement_vars": ["forename"],
        "cutpoints": {"sex": None, "forename": [0.5, 0.8]},
    }

    mock_calculate_sorensen_dice.return_value = sorensen_dice_output_df
    mock_calculate_agreement_states.return_value = spark_mock.sql.DataFrame
    spark_mock.sql.DataFrame.checkpoint.return_value = agreement_states_output_df

    # Act
    test_output = im.get_deltas(
        df_cartesian_join=test_input_df, input_variables=test_input_dict
    )

    # Assert
    mock_calculate_sorensen_dice.assert_called_once_with(
        df=test_input_df,
        col1="forename_df1",
        col2="forename_df2",
        new_col="forename_sorensen_dice",
        decimal_places=4,
    )
    mock_calculate_agreement_states.assert_called_once_with(
        df=sorensen_dice_output_df,
        binary_agreement_cols=test_input_dict["binary_agreement_vars"],
        df_suffixes=[test_input_dict["df1_suffix"], test_input_dict["df2_suffix"]],
    )
    mock_calculate_deltas.assert_called_once_with(
        df=agreement_states_output_df,
        cutpoints=test_input_dict["cutpoints"],
        agreement_col_suffix="_agr_state",
        string_similarity_suffix="_sorensen_dice",
    )
    assert test_output is mock_calculate_deltas.return_value


def test_make_bigrams(spark: pyspark.sql.SparkSession) -> None:
    """
    Tests that make_bigrams gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            (1, "SARAH"),
            (2, "ALEESHA"),
            (3, "TOM"),
            (4, "RITA"),
            (5, "BILAL"),
            (6, "Q"),
            (7, ""),
            (8, None),
        ],
        ["id", "fn"],
    )

    expected_output = spark.createDataFrame(
        [
            (1, "SARAH", ["S A", "A R", "R A", "A H"]),
            (2, "ALEESHA", ["A L", "L E", "E E", "E S", "S H", "H A"]),
            (3, "TOM", ["T O", "O M"]),
            (4, "RITA", ["R I", "I T", "T A"]),
            (5, "BILAL", ["B I", "I L", "L A", "A L"]),
            (6, "Q", None),
            (7, "", None),
            (8, None, None),
        ],
        ["id", "fn", "fn_bigrams"],
    )

    # Act
    test_output = im.make_bigrams(df=test_input, col="fn")

    # Assert
    ch.assert_df_equality(
        df1=test_output,
        df2=expected_output,
        ignore_row_order=True,
        ignore_nullable=True,
    )
