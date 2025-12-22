import pytest
from pyspark.sql import types as T


@pytest.fixture(scope="module")
def calculate_njklm_values_input(spark):
    return spark.createDataFrame(
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


@pytest.fixture(scope="module")
def calculate_njklm_values_output(spark):
    return
