"""Matrix A* functions unit tests.

Methods:
  calculate_b

  calculate_njklm_values

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

from unittest.mock import call, patch

import numpy as np
import pandas as pd

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


def test_calculate_njklm_values(
    spark: pyspark.sql.SparkSession, compare_deltas_output: pyspark.sql.DataFrame, calculate_njklm_values_output: pd.DataFrame
) -> None:
    """
    Tests that calculate_njklm_values() gives the correct output when
    provided with appropriate inputs.
    """
    # Arrange
    test_input = compare_deltas_output

    expected_output = calculate_njklm_values_output

    # Act
    test_output = ma.calculate_njklm_values(test_input)

    # Assert
    pd.testing.assert_frame_equal(test_output, expected_output)


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


@patch("scalelink.matrix_a_star.matrix_a_star.make_matrix_a_star")
@patch("scalelink.matrix_a_star.matrix_a_star.calculate_r")
@patch("scalelink.matrix_a_star.matrix_a_star.multiply_vectors_by_s")
@patch("scalelink.matrix_a_star.matrix_a_star.calculate_q")
@patch("scalelink.matrix_a_star.matrix_a_star.make_matrix_a")
@patch("scalelink.matrix_a_star.matrix_a_star.calculate_njklm_values")
def test_get_matrix_a_star(
    mock_calculate_njklm_values: unittest.mock.MagicMock,
    mock_make_matrix_a: unittest.mock.MagicMock,
    mock_calculate_q: unittest.mock.MagicMock,
    mock_multiply_vectors_by_s: unittest.mock.MagicMock,
    mock_calculate_r: unittest.mock.MagicMock,
    mock_make_matrix_a_star: unittest.mock.MagicMock,
    spark_mock: unittest.mock.MagicMock,
    compare_deltas_output: pyspark.sql.DataFrame,
    calculate_njklm_values_output: pd.DataFrame,
    make_matrix_a_output: np.array,
) -> None:
    """
    Tests that get_matrix_a_star() gives the correct output when provided
    with appropriate inputs.
    """
    # Arrange
    test_input_df = compare_deltas_output
    test_input_dict = {
        "cutpoints": {"sex": None, "fn": [0.75, 0.9]},
        "p": 2,
        "K": 5,
        "s": 7,
    }
    test_qs_input = [1, 0, 1, 0, 0]
    test_qs_output = [7, 0, 7, 0, 0]
    test_rs_input = [0, 1, 0, 0, 1]
    test_rs_output = [0, 7, 0, 0, 7]

    mock_calculate_njklm_values.return_value = calculate_njklm_values_output
    mock_make_matrix_a.return_value = make_matrix_a_output
    mock_calculate_q.return_value = test_qs_input
    mock_multiply_vectors_by_s.side_effect = [test_qs_output, test_rs_output]
    mock_calculate_r.return_value = test_rs_input

    # Act
    _ = ma.get_matrix_a_star(
        df_delta_comparisons=test_input_df, input_variables=test_input_dict
    )

    # Assert
    mock_calculate_njklm_values.assert_called_once_with(df=compare_deltas_output)
    mock_make_matrix_a.assert_called_once_with(
        Njklm=calculate_njklm_values_output,
        K=test_input_dict["K"],
        p=test_input_dict["p"],
    )
    mock_calculate_q.assert_called_once_with(cutpoints=test_input_dict["cutpoints"])
    mock_multiply_vectors_by_s.assert_has_calls(
        [
            call(vector=test_qs_input, s=test_input_dict["s"]),
            call(vector=test_rs_input, s=test_input_dict["s"]),
        ],
        any_order=False,
    )
    mock_calculate_r.assert_called_once_with(cutpoints=test_input_dict["cutpoints"])
    mock_make_matrix_a_star.assert_called_once_with(
        matrix_a=make_matrix_a_output, q=test_qs_output, r=test_rs_output
    )


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


def test_make_matrix_a(calculate_njklm_values_output, make_matrix_a_output):
    """
    Tests that make_matrix_a() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      numpy as np
      pandas as pd
    """
    # Arrange
    test_input_Njklm = calculate_njklm_values_output

    test_input_K = 5
    test_input_p = 2

    expected_output = make_matrix_a_output

    # Act
    test_output = ma.make_matrix_a(
        df_Njklm=test_input_Njklm, K=test_input_K, p=test_input_p
    )

    # Assert
    np.testing.assert_array_equal(test_output, expected_output)


def test_make_matrix_a_star(make_matrix_a_output):
    """
    Tests that make_matrix_a_star() gives the correct output when provided with
    appropriate inputs.

    Dependencies:
      numpy as np
    """
    # Arrange
    test_input_matrix_a = make_matrix_a_output

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
