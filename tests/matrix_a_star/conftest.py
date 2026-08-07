import numpy as np
import pandas as pd
import pytest


@pytest.fixture(scope="module")
def calculate_njklm_values_output() -> pd.DataFrame:
    """
    Sets up the expected output for test_calculate_njklm_values, which is
    also the test input for test_make_matrix_a.
    """
    return pd.DataFrame(
        [[2, 0, 1, 0, 1, 0, 4, 0, 1, 2, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 1, 2, 0, 0, 3]],
        columns=[
            "N_sex_1_sex_1",
            "N_sex_1_sex_2",
            "N_sex_1_forename_1",
            "N_sex_1_forename_2",
            "N_sex_1_forename_3",
            "N_sex_2_sex_1",
            "N_sex_2_sex_2",
            "N_sex_2_forename_1",
            "N_sex_2_forename_2",
            "N_sex_2_forename_3",
            "N_forename_1_sex_1",
            "N_forename_1_sex_2",
            "N_forename_1_forename_1",
            "N_forename_1_forename_2",
            "N_forename_1_forename_3",
            "N_forename_2_sex_1",
            "N_forename_2_sex_2",
            "N_forename_2_forename_1",
            "N_forename_2_forename_2",
            "N_forename_2_forename_3",
            "N_forename_3_sex_1",
            "N_forename_3_sex_2",
            "N_forename_3_forename_1",
            "N_forename_3_forename_2",
            "N_forename_3_forename_3",
        ],
    )


@pytest.fixture(scope="module")
def make_matrix_a_output() -> np.array:
    """
    Sets up the expected output for make_matrix_a, which is also the test input
    for and used by test_get_matrix_a_star.
    """
    return np.array(
        [
            [0.5, 0.0, -0.25, 0.0, -0.25],
            [0.0, 1.0, 0.0, -0.25, -0.5],
            [-0.25, 0.0, 0.5, 0.0, 0.0],
            [0.0, -0.25, 0.0, 0.25, 0.0],
            [-0.25, -0.5, 0.0, 0.0, 0.75],
        ]
    )


@pytest.fixture(scope="module")
def make_input_matrix_a_star() -> np.array:
    """
    Sets up the input matrix_a_star for test_solve_for_x_star, which is also
    one of the test inputs for test_get_scaled_labelled_x_star.
    """
    return np.array([[2, 1], [1, 1]])


@pytest.fixture(scope="module")
def make_input_b() -> list[int]:
    """
    Sets up the input b vector for test_solve_for_x_star, which is also
    one of the test inputs for test_get_scaled_labelled_x_star.
    """
    return [5, 3]


@pytest.fixture(scope="module")
def make_solve_for_x_star_output() -> list[float]:
    """
    Sets up the expected output for test_solve_for_x_star, which is also
    one of the test inputs for test_get_scaled_labelled_x_star.
    """
    return [2, 1]
