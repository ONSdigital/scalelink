"""Matrix A* functions unit tests.

Methods:
  calculate_b

  calculate_njklm_values - test shell only, currently

  calculate_q

  calculate_r

  get_matrix_a_star

  get_scaled_labelled_x_star - test shell only, currently

  label_x_star

  make_matrix_a

  make_matrix_a_star

  multiply_vectors_by_s

  scale_x_star

  solve_for_x_star
"""

import numpy as np
import pandas as pd
import pytest

from scalelink.matrix_a_star import matrix_a_star as ma


def test_calculate_b() -> None:
    """
    Tests that calculate_b() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input = 8

    expected_output = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]

    # Act
    test_output = ma.calculate_b(K=test_input)

    # Assert
    assert test_output == expected_output


@pytest.mark.skip(reason="test shell")
def test_calculate_njklm_values() -> None:
    pass


def test_calculate_q() -> None:
    """
    Tests that calculate_q() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = [1, 0, 0, 0, 1, 0, 0, 1, 0, 1, 0]

    # Act
    test_output = ma.calculate_q(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


def test_calculate_r() -> None:
    """
    Tests that calculate_r() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input = {"fn": [0.4, 0.7, 0.9], "sn": [0.75, 0.9], "sex": None, "dob": None}

    expected_output = [0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1]

    # Act
    test_output = ma.calculate_r(cutpoints=test_input)

    # Assert
    assert test_output == expected_output


@pytest.mark.skip(reason="test shell")
def test_get_matrix_a_star() -> None:
    pass


def test_label_x_star() -> None:
    """
    Tests that label_x_star() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input_x_star = [
        0.01,
        0.34,
        0.02,
        0.92,
        0.05,
        0.47,
        0.81,
        0.11,
        0.86,
        0.29,
        0.80,
    ]

    test_input_cutpoints = {
        "fn": [0.4, 0.7, 0.9],
        "sn": [0.75, 0.9],
        "sex": None,
        "dob": None,
    }

    expected_output = {
        "fn_disagree": 0.01,
        "fn_partially_agree_1": 0.34,
        "fn_partially_agree_2": 0.02,
        "fn_agree": 0.92,
        "sn_disagree": 0.05,
        "sn_partially_agree_1": 0.47,
        "sn_agree": 0.81,
        "sex_disagree": 0.11,
        "sex_agree": 0.86,
        "dob_disagree": 0.29,
        "dob_agree": 0.80,
    }

    # Act
    test_output = ma.label_x_star(
        x_star=test_input_x_star, cutpoints=test_input_cutpoints
    )

    # Assert
    assert test_output == expected_output


def test_make_matrix_a() -> None:
    """
    Tests that make_matrix_a() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
        numpy as np
        pandas as pd
    """
    # Arrange
    test_input_Njklm = pd.DataFrame(
        data=[
            [2, 0, 1, 0, 1, 0, 4, 0, 1, 2, 1, 0, 2, 0, 0, 0, 1, 0, 1, 0, 1, 2, 0, 0, 3]
        ],
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

    test_input_K = 5
    test_input_p = 2

    expected_output = np.array(
        [
            [0.5, 0.0, -0.25, 0.0, -0.25],
            [0.0, 1.0, 0.0, -0.25, -0.5],
            [-0.25, 0.0, 0.5, 0.0, 0.0],
            [0.0, -0.25, 0.0, 0.25, 0.0],
            [-0.25, -0.5, 0.0, 0.0, 0.75],
        ]
    )

    # Act
    test_output = ma.make_matrix_a(
        Njklm=test_input_Njklm, K=test_input_K, p=test_input_p
    )

    # Assert
    np.testing.assert_array_equal(test_output, expected_output)


def test_make_matrix_a_star() -> None:
    """
    Tests that make_matrix_a_star() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
        numpy as np
    """
    # Arrange
    test_input_matrix_a = np.array(
        [
            [0.5, 0, -0.25, 0, -0.25],
            [0, 1, 0, -0.25, -0.5],
            [-0.25, 0, 0.5, 0, 0],
            [0, -0.25, 0, 0.25, 0],
            [-0.25, -0.5, 0, 0, 0.75],
        ]
    )

    test_input_q = [1, 0, 1, 0, 0]
    test_input_r = [0, 1, 0, 0, 1]

    expected_output = np.array(
        [
            [1, 0, -0.5, 0, -0.5, -1, 0],
            [0, 2, 0, -0.5, -1, 0, -1],
            [-0.5, 0, 1, 0, 0, -1, 0],
            [0, -0.5, 0, 0.5, 0, 0, 0],
            [-0.5, -1, 0, 0, 1.5, 0, -1],
            [1, 0, 1, 0, 0, 0, 0],
            [0, 1, 0, 0, 1, 0, 0],
        ]
    )

    # Act
    test_output = ma.make_matrix_a_star(
        matrix_a=test_input_matrix_a, q=test_input_q, r=test_input_r
    )

    # Assert
    np.testing.assert_array_equal(test_output, expected_output)


def test_multiply_vectors_by_s() -> None:
    """
    Tests that multiply_vectors_by_s() gives the correct output when provided
    with appropriate inputs.
    """
    # Arrange
    test_input_vector = [0, 0, 0, 1, 0, 0, 1, 0, 1, 0, 1]

    test_input_s = 5000

    expected_output = [0, 0, 0, 5000, 0, 0, 5000, 0, 5000, 0, 5000]

    # Act
    test_output = ma.multiply_vectors_by_s(vector=test_input_vector, s=test_input_s)

    # Assert
    assert test_output == expected_output


def test_scale_x_star() -> None:
    """
    Tests that scale_x_star() gives the correct output when provided with
    appropriate inputs.
    """
    # Arrange
    test_input = {
        "fn_disagree": 0.01,
        "fn_partially_agree_1": 0.34,
        "fn_partially_agree_2": 0.02,
        "fn_agree": 0.92,
        "sn1_disagree": 0.05,
        "sn1_partially_agree_1": 0.47,
        "sn1_agree": 0.81,
        "sn2_disagree": 0.12,
        "sn2_partially_agree_1": 0.37,
        "sn2_agree": 0.56,
        "sex_disagree": 0.11,
        "sex_agree": 0.86,
        "dob_disagree": 0.29,
        "dob_agree": 0.80,
    }

    expected_output = {
        "fn_disagree": 0.00,
        "fn_partially_agree_1": 33.00,
        "fn_partially_agree_2": 1.00,
        "fn_agree": 91.00,
        "sn1_disagree": 0.00,
        "sn1_partially_agree_1": 42.00,
        "sn1_agree": 76.00,
        "sn2_disagree": 0.00,
        "sn2_partially_agree_1": 25.00,
        "sn2_agree": 44.00,
        "sex_disagree": 0.00,
        "sex_agree": 75.00,
        "dob_disagree": 0.00,
        "dob_agree": 51.00,
    }

    # Act
    test_output = ma.scale_x_star(x_star_labelled=test_input)

    # Assert
    assert test_output == expected_output


def test_solve_for_x_star() -> None:
    """
    Tests that solve_for_x_star() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
        numpy as np
    """
    # Arrange
    test_input_matrix_a_star = np.array([[2, 1], [1, 1]])

    test_input_b = [5, 3]

    expected_output = [2, 1]

    # Act
    test_output = ma.solve_for_x_star(
        matrix_a_star=test_input_matrix_a_star, b=test_input_b
    )

    # Assert
    assert test_output == expected_output
