import pyspark
import pytest


@pytest.fixture(scope="module")
def sorensen_dice_input_df(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH"),
            (1, 2, "ALEESHA", "ALEESHA"),
            (2, 1, "TOM", "GRACE"),
            (None, 1, "RITA", "RITER"),
            (2, None, "BILAL", "BILLALL"),
            (None, None, "Q", "HUI"),
            (1, 1, None, "YUSUF"),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
        ],
    )


@pytest.fixture(scope="module")
def sorensen_dice_output_df(spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH", 0.8889),
            (1, 2, "ALEESHA", "ALEESHA", 1.0),
            (2, 1, "TOM", "GRACE", 0.0),
            (None, 1, "RITA", "RITER", 0.5714),
            (2, None, "BILAL", "BILLALL", 0.8),
            (None, None, "Q", "HUI", 0.0),
            (1, 1, None, "YUSUF", 0.0),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
            "forename_sorensen_dice",
        ],
    )


@pytest.fixture(scope="module")
def agreement_states_output_df(
    spark: pyspark.sql.SparkSession,
) -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH", 0.8889, True),
            (1, 2, "ALEESHA", "ALEESHA", 1.0, False),
            (2, 1, "TOM", "GRACE", 0.0, False),
            (None, 1, "RITA", "RITER", 0.5714, True),
            (2, None, "BILAL", "BILLALL", 0.8, True),
            (None, None, "Q", "HUI", 0.0, True),
            (1, 1, None, "YUSUF", 0.0, True),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
            "forename_sorensen_dice",
            "sex_agr_state",
        ],
    )


@pytest.fixture(scope="module")
def calculate_deltas_output_df(
    spark: pyspark.sql.SparkSession,
) -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (1, 1, "SARAH", "SAARAH", 0.8889, True, False, True, False, False, True),
            (1, 2, "ALEESHA", "ALEESHA", 1.0, False, True, False, False, False, True),
            (2, 1, "TOM", "GRACE", 0.0, False, True, False, True, False, False),
            (None, 1, "RITA", "RITER", 0.5714, True, False, True, False, True, False),
            (2, None, "BILAL", "BILLALL", 0.8, True, False, True, False, False, True),
            (None, None, "Q", "HUI", 0.0, True, False, True, True, False, False),
            (1, 1, None, "YUSUF", 0.0, True, False, True, True, False, False),
        ],
        [
            "sex_df1",
            "sex_df2",
            "forename_df1",
            "forename_df2",
            "forename_sorensen_dice",
            "sex_agr_state",
            "di_sex_1",
            "di_sex_2",
            "di_forename_1",
            "di_forename_2",
            "di_forename_3",
        ],
    )
