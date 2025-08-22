"""Match score function unit tests.

Methods:
  test_assign_match_score

  test_assign_weights

  test_get_match_scores - test shell only, currently
"""

import chispa as ch
import pytest

from scalelink.match_scores import match_scores as msf


def test_assign_match_score(spark):
    """
    Tests that assign_match_score() gives the correct output when provided with
    suitable inputs.

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
    test_output = msf.assign_match_score(df_with_weights=test_input)

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_assign_weights(spark):
    """
    Tests that assign_weights() gives the correct output when provided with
    suitable inputs.

    Dependencies:
        chispa as ch
    """
    # Arrange
    test_input_df = spark.createDataFrame(
        [
            (
                "1-01",
                "2-01",
                1,
                1,
                "SARAH",
                "SAARAH",
                True,
                0.8889,
                False,
                True,
                False,
                False,
                True,
            ),
            (
                "1-02",
                "2-02",
                1,
                2,
                "ALEECHA",
                "ALEESHA",
                False,
                0.6667,
                True,
                False,
                False,
                True,
                False,
            ),
            (
                "1-03",
                "2-03",
                2,
                1,
                "TOM",
                "GRACE",
                False,
                0.0000,
                True,
                False,
                True,
                False,
                False,
            ),
        ],
        [
            "id_df1",
            "id_df2",
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

    test_input_x_star = {
        "sex_disagree": 0.0,
        "sex_agree": 75.0,
        "forename_disagree": 0.0,
        "forename_partially_agree_1": 33.0,
        "forename_agree": 91.0,
    }

    expected_output = spark.createDataFrame(
        [
            ("1-01", "2-01", 75.0, 91.0),
            ("1-02", "2-02", 0.0, 33.0),
            ("1-03", "2-03", 0.0, 0.0),
        ],
        ["id_df1", "id_df2", "sex_weight", "forename_weight"],
    )

    # Act
    test_output = msf.assign_weights(
        df_with_deltas=test_input_df,
        df1_id="id_df1",
        df2_id="id_df2",
        cutpoints={"sex": None, "forename": [0.5, 0.8]},
        x_star_scaled=test_input_x_star,
        spark=spark,
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


@pytest.mark.skip(reason="test shell")
def test_get_match_scores():
    pass
