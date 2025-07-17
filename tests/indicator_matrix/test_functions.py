"""Indicator matrix function unit tests.

Methods:
  test_calculate_agreement_states

  test_calculate_deltas

  test_calculate_sorensen_dice

  test_compare_deltas

  test_compute_normalised_levenshtein

  test_get_deltas - test shell only, currently

  test_make_bigrams
"""

import chispa as ch
import pytest
from pyspark.sql import types as T

from scalelink.scalelink.indicator_matrix import functions as im


def test_calculate_agreement_states(spark):
    """
    Tests that calculate_agreement_states gives the correct output when provided
    with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            (12, 12, 2000, "EC1A1BB", 12, 12, 2000, "EC1A1BB"),
            (9, 3, 1971, "W1A0AX", 9, 3, 1971, "EC1A1BB"),
            (27, 6, 1959, "M11AE", 12, 12, 2000, "EC1A1BB"),
            (None, 10, 1996, "B338TH", None, 10, 1996, "B338TH"),
            (13, None, 1947, "CR26XH", None, 10, 1947, "B338TH"),
            (21, 8, None, "DN551PT", 9, 3, 1971, "W1A0AX"),
            (1, 4, 1983, None, None, 4, 1983, None),
            (None, None, None, None, 27, 6, 1959, "M11AE"),
            (None, None, None, None, None, None, None, None),
        ],
        [
            "dob_day_df1",
            "dob_month_df1",
            "dob_year_df1",
            "postcode_df1",
            "dob_day_df2",
            "dob_month_df2",
            "dob_year_df2",
            "postcode_df2",
        ],
    )

    expected_output = spark.createDataFrame(
        [
            (12, 12, 2000, "EC1A1BB", 12, 12, 2000, "EC1A1BB", True, True, True, True),
            (9, 3, 1971, "W1A0AX", 9, 3, 1971, "EC1A1BB", True, True, True, False),
            (27, 6, 1959, "M11AE", 12, 12, 2000, "EC1A1BB", False, False, False, False),
            (
                None,
                10,
                1996,
                "B338TH",
                None,
                10,
                1996,
                "B338TH",
                True,
                True,
                True,
                True,
            ),
            (
                13,
                None,
                1947,
                "CR26XH",
                None,
                10,
                1947,
                "B338TH",
                True,
                True,
                True,
                False,
            ),
            (21, 8, None, "DN551PT", 9, 3, 1971, "W1A0AX", False, False, True, False),
            (1, 4, 1983, None, None, 4, 1983, None, True, True, True, True),
            (None, None, None, None, 27, 6, 1959, "M11AE", True, True, True, True),
            (None, None, None, None, None, None, None, None, True, True, True, True),
        ],
        [
            "dob_day_df1",
            "dob_month_df1",
            "dob_year_df1",
            "postcode_df1",
            "dob_day_df2",
            "dob_month_df2",
            "dob_year_df2",
            "postcode_df2",
            "dob_day_agr_state",
            "dob_month_agr_state",
            "dob_year_agr_state",
            "postcode_agr_state",
        ],
    )

    # Act
    test_output = im.calculate_agreement_states(
        df=test_input,
        binary_agreement_cols=["dob_day", "dob_month", "dob_year", "postcode"],
        df_suffixes=["_df1", "_df2"],
    )

    # Assert
    ch.assert_df_equality(
        df1=test_output,
        df2=expected_output,
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_calculate_deltas(spark):
    """
    Tests that calculate_deltas gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH", True, 0.8889),
            (1, 2, "ALEESHA", "ALEESHA", False, 1.0000),
            (2, 1, "TOM", "GRACE", False, 0.0000),
            (None, 1, "RITA", "RITER", True, 0.5714),
            (2, None, "BILAL", "BILLALL", True, 0.8000),
            (None, None, "Q", "HUI", None, 0.0000),
            (1, 1, None, "YUSUF", True, None),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
            "sex_agr_state",
            "forename_sorensen_dice",
        ],
    )

    expected_output = spark.createDataFrame(
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


def test_calculate_sorensen_dice(spark):
    """
    Tests that calculate_sorensen_dice gives the correct output when supplied
    with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    # Arrange
    test_input = spark.createDataFrame(
        [
            ("SARAH", "SAARAH"),
            ("ALEESHA", "ALEESHA"),
            ("TOM", "GRACE"),
            ("RITA", "RITER"),
            ("BILAL", "BILLALL"),
            ("Q", "HUI"),
            ("", "YUSUF"),
            (None, None),
        ],
        ["fn_df1", "fn_df2"],
    )

    expected_output = spark.createDataFrame(
        [
            ("SARAH", "SAARAH", 0.8889),
            ("ALEESHA", "ALEESHA", 1.0000),
            ("TOM", "GRACE", 0.0000),
            ("RITA", "RITER", 0.5714),
            ("BILAL", "BILLALL", 0.8000),
            ("Q", "HUI", 0.0000),
            ("", "YUSUF", 0.0000),
            (None, None, 0.0000),
        ],
        ["fn_df1", "fn_df2", "fn_sorensen_dice"],
    )

    # Act
    test_output = im.calculate_sorensen_dice(
        df=test_input,
        col1="fn_df1",
        col2="fn_df2",
        new_col="fn_sorensen_dice",
        decimal_places=4,
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_compare_deltas(spark):
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

    expected_output = spark.createDataFrame(
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

    # Act
    test_output = im.compare_deltas(
        df=test_input, linkage_vars=["sex", "forename"], delta_col_prefix="di_"
    )

    # Assert
    ch.assert_df_equality(df1=test_output, df2=expected_output, ignore_row_order=True)


def test_compute_normalized_levenshtein(spark):
    """
    Tests that compute_normalized_levenshtein gives the correct output when
    supplied with appropriate inputs.

    Dependencies:
      chispa as ch
    """
    test_input_1 = spark.createDataFrame(
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
    )

    test_input_2 = spark.createDataFrame(
        [
            ("1", "name", "string"),
            ("2", "name", None),
            ("3", "name", "str"),
            ("4", "name", None),
            ("5", "name", "string"),
        ],
        ("l_id", "first_name_1", "string_1"),
    )

    test_input_3 = spark.createDataFrame(
        [
            ("1", "name", "string"),
            ("2", "name", "str"),
            ("3", "name", None),
            ("4", "name", None),
            ("5", "name", "st"),
            ("6", "name", "ttri"),
        ],
        ("r_id", "first_name_2", "string_2"),
    )

    expected_output_1 = spark.createDataFrame(
        [
            ("1", "string", "string", 1.0),
            ("2", "stringed", "str", 0.375),
            ("3", "tring", "tri", 0.6),
            ("4", "ttri", "str", 0.5),
            ("5", None, "string", None),
            ("6", "string", None, None),
            ("7", None, None, None),
        ],
        ("id", "string_1", "string_2", "normalized_levenshtein_output"),
    )

    expected_output_2 = spark.createDataFrame(
        [
            ("1", "name", "string", "1", "name", "string"),
            ("5", "name", "string", "1", "name", "string"),
            ("3", "name", "str", "2", "name", "str"),
        ],
        ("l_id", "first_name_1", "string_1", "r_id", "first_name_2", "string_2"),
    )

    # Act
    test_output_1 = test_input_1.withColumn(
        "normalized_levenshtein_output",
        im.compute_normalized_levenshtein(test_input_1.string_1, test_input_1.string_2),
    )

    test_output_2 = test_input_2.join(
        test_input_3,
        (
            (
                im.compute_normalized_levenshtein(
                    test_input_2.string_1, test_input_3.string_2
                )
                > 0.7
            )
            & (test_input_2.first_name_1 == test_input_3.first_name_2)
        ),
        how="inner",
    )

    # Assert
    ch.assert_df_equality(
        df1=test_output_1, df2=expected_output_1, ignore_row_order=True
    )
    ch.assert_df_equality(
        df1=test_output_2, df2=expected_output_2, ignore_row_order=True
    )


@pytest.mark.skip(reason="test shell")
def test_get_deltas():
    pass


def test_make_bigrams(spark):
    """
    Tests that make_bigrams gives the correct output when supplied with
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
            (6, "Q", ["Q #"]),
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
