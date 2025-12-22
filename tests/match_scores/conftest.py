import pytest


@pytest.fixture(scope="module")
def assign_weights_input_df(spark):
    return spark.createDataFrame(
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


@pytest.fixture(scope="module")
def assign_weights_input_x_star():
    return {
        "sex_disagree": 0.0,
        "sex_agree": 75.0,
        "forename_disagree": 0.0,
        "forename_partially_agree_1": 33.0,
        "forename_agree": 91.0,
    }


@pytest.fixture(scope="module")
def assign_weights_output_df(spark):
    return spark.createDataFrame(
        [
            ("1-01", "2-01", 75.0, 91.0),
            ("1-02", "2-02", 0.0, 33.0),
            ("1-03", "2-03", 0.0, 0.0),
        ],
        ["id_df1", "id_df2", "sex_weight", "forename_weight"],
    )
