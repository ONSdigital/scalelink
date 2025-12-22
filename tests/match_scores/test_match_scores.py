"""Match score function unit tests.

Methods:
  test_assign_match_score

  test_assign_weights

  test_get_match_scores
"""

from typing import Dict
from unittest.mock import patch

import chispa as ch
import pyspark

from scalelink.match_scores import match_scores as ms


def test_assign_match_score(spark: pyspark.sql.SparkSession) -> None:
    """
    Tests that assign_match_score() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            ("1-01", "2-01", 75.0, 91.0),
            ("1-02", "2-02", 0.0, 33.0),
            ("1-03", "2-03", 0.0, 0.0),
        ],
        ["id_df1", "id_df2", "sex_weight", "forename_weight"],
    )

    expected_output = spark.createDataFrame(
        [
            ("1-01", "2-01", 75.0, 91.0, 166.0),
            ("1-02", "2-02", 0.0, 33.0, 33.0),
            ("1-03", "2-03", 0.0, 0.0, 0.0),
        ],
        ["id_df1", "id_df2", "sex_weight", "forename_weight", "match_score"],
    )

    # Act
    test_output = ms.assign_match_score(df_with_weights=test_input)

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_assign_weights(
    spark: pyspark.sql.SparkSession,
    assign_weights_input_df: pyspark.sql.DataFrame,
    assign_weights_input_x_star: Dict[str, float],
    assign_weights_output_df: pyspark.sql.DataFrame,
) -> None:
    """
    Tests that assign_weights() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
        chispa as ch
    """
    # Arrange
    test_input_df = assign_weights_input_df
    test_input_x_star = assign_weights_input_x_star
    expected_output = assign_weights_output_df

    # Act
    test_output = ms.assign_weights(
        df_with_deltas=test_input_df,
        df1_id="id_df1",
        df2_id="id_df2",
        cutpoints={"sex": None, "forename": [0.5, 0.8]},
        x_star_scaled=test_input_x_star,
        spark=spark,
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


@patch("scalelink.match_scores.match_scores.assign_match_score")
@patch("scalelink.match_scores.match_scores.assign_weights")
def test_get_match_scores(
    mock_assign_weights,
    mock_assign_match_score,
    spark_mock,
    assign_weights_input_df,
    assign_weights_input_x_star,
    assign_weights_output_df,
) -> None:
    """
    Tests that get_match_scores() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input_df = assign_weights_input_df
    test_input_x_star = assign_weights_input_x_star
    test_input_ids = ["df1_id", "df2_id"]
    test_input_cutpoints = {"sex": None, "forename": [0.5, 0.8]}

    mock_assign_weights.return_value = assign_weights_output_df
    mock_assign_match_score.return_value = spark_mock.sql.DataFrame

    # Act
    _ = ms.get_match_scores(
        df_deltas=test_input_df,
        x_star_scaled_labelled=test_input_x_star,
        input_variables={
            "df1_id": test_input_ids[0],
            "df2_id": test_input_ids[1],
            "cutpoints": test_input_cutpoints,
        },
        spark=spark_mock,
    )

    # Assert
    mock_assign_weights.assert_called_once_with(
        df_with_deltas=test_input_df,
        df1_id=test_input_ids[0],
        df2_id=test_input_ids[1],
        cutpoints=test_input_cutpoints,
        x_star_scaled=test_input_x_star,
        spark=spark_mock,
    )
    mock_assign_match_score.assert_called_once_with(
        df_with_weights=assign_weights_output_df
    )
