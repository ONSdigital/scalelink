"""Matrix A* functions.

A series of functions that together produce Matrix A* and vector x*, as defined
in the 2017 paper by Goldstein et al.

Methods:
  calculate_b:
    A method to calculate the Scalelink variable b, a vector containing K+1
    zeros followed by a single one.

  calculate_njklm_values:
    A method to calculate the Scalelink Njklm values, the sums of the delta
    comparisons.

  calculate_q:
    A method to calculate the Scalelink variable q, a vector containing zeros
    and ones representing the scenario where the agreement state of every
    linkage variable is 'disagree'.

  calculate_r:
    A method to calculate the Scalelink variable r, a vector containing zeros
    and ones representing the scenario where the agreement state of every
    linkage variable is 'agree'.

  get_matrix_a_star:
    A method to produce the Scalelink variable Matrix A*. Works by running,
    in order:
     - calculate_njklm_values
     - make_matrix_a
     - calculate_q
     - multiply_vectors_by_s
     - calculate_r
     - multiply_vectors_by_s
     - make_matrix_a_star

  get_scaled_labelled_x_star:
    A method to produce a scaled, labelled version of the Scalelink variable
    vector x* from Matrix A*. Works by running, in order:
     - calculate_b
     - multiply_vectors_by_s
     - solve_for_x_star
     - label_x_star
     - scale_x_star

  label_x_star:
    A method to label the Scalelink variable x* with linkage variables and
    agreement states, to aid user comprehension.

  make_matrix_a:
    A method to make the Scalelink variable Matrix A.

  make_matrix_a_star:
    A method to make the Scalelink variable Matrix A*.

  multiply_vectors_by_s:
    A method that converts a Scalelink vector (e.g. b, q or r) into its s-
    multiplied equivalent (i.e. b_s, q_s or r_s).

  scale_x_star:
    A method that scales vector x* so that all of its values are between 0 and
    100.

  solve_for_x_star:
    A method that solves the matrix multiplication of Scalelink variables Matrix
    A* and vector b (or b_s) to produce vector x*.
"""

import collections
import itertools
import re
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import pyspark


def calculate_b(K: int) -> List[int]:
    """
    Takes the Scalelink variable K. From this, calculates the Scalelink vector
    b.

    Args:
      K:
        The total number of agreement states across all linkage variables.

    Dependencies:
      itertools

    Returns:
      b:
        The Scalelink vector b, a list of length K+2. It consists of K+1 0s
        followed by a single 1. It is necessary for deriving weights from
        Matrix A* by matrix multiplication.
    """
    b = []
    b = list(itertools.repeat(0, int(K + 1)))
    b.append(1)
    return b


def calculate_njklm_values(df: pyspark.sql.DataFrame) -> pd.DataFrame:
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked that
    has had the Scalelink deltas and their comparisons calculated. Calculates
    Njklm values from these delta comparisons.

    Args:
      df:
        The dataframe containing the deltas comparisons from which the Njklm
        values will be calculated.

    Dependencies:
      pandas as pd

    Returns:
      df_Njklm:
        A dataframe containing the delta comparison column names as column names
        and a single row consisting of the Njklm values (i.e. the sums of these
        delta comparison columns).
    """
    df_Njklm = df.groupBy().sum()

    # Convert column names from 'sum(col1)' to 'col1'
    for col in df_Njklm.columns:
        df_Njklm = df_Njklm.withColumnRenamed(col, col[4 : (len(col) - 1)])

    df_Njklm = df_Njklm.toPandas()

    return df_Njklm


def calculate_q(cutpoints: Dict[str, Union[List[float], None]]) -> List[int]:
    """
    Takes a dictionary containing linkage variable names and the string
    comparison cutpoints for those variables. From this, calculates the
    Scalelink vector q.

    Args:
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of the string comparison cutpoints for those
        variables.

    Dependencies:
      itertools

    Returns:
      q:
        The Scalelink vector q, a list containing 0s and 1s representing the
        scenario when the agreement state of every variable is 'disagree'.
    """
    q = []

    for value in cutpoints.values():
        if value is not None:
            # Partial agreement - add 1 (disagree) then 0s (the other states)
            extendor = [1] + list(itertools.repeat(0, len(value)))
            q = q + extendor
        else:
            # Binary agreement - add 1 (disagree) then 0 (agree)
            q = q + [1, 0]

    return q


def calculate_r(cutpoints: Dict[str, Union[List[float], None]]) -> List[int]:
    """
    Takes a dictionary containing linkage variable names and the string
    comparison cutpoints for those variables. From this, calculates the
    Scalelink vector r.

    Args:
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of the string comparison cutpoints for those
        variables.

    Dependencies:
      itertools

    Returns:
      r:
        The Scalelink vector r, a list containing 0s and 1s representing the
        scenario when the agreement state of every variable is 'agree'.
    """
    r = []

    for value in cutpoints.values():
        if value is not None:
            # Partial agreement - add 0s (the other states) then 1 (agree)
            extendor = list(itertools.repeat(0, len(value))) + [1]
            r = r + extendor
        else:
            # Binary agreement - add 0 (disagree) then 1 (agree)
            r = r + [0, 1]

    return r


def get_matrix_a_star(
    df_delta_comparisons: pyspark.sql.DataFrame, input_variables: Dict[str, Any]
) -> np.array:
    """
    Takes a dataframe containing the delta comparisons for the two dataframes to
    be linked. Also takes the input variables, stored in a dictionary. From
    this, creates a Numpy array consisting of Matrix A*, as defined in
    Goldstein et al. (2017).

    Args:
      df_delta_comparisons:
        A dataframe containing the delta comparison columns for the two
        dataframes to be linked. Delta comparisons are 1 when both deltas are
        True and are 0 in all other circumstances.
      input_variables:
        A dictionary containing the other input variables required for the
        scaling algorithm. The keys are the name of the input variables and the
        values are the variables themselves. Produced by the utils function
        get_inputs().

    Dependencies:
      pandas as pd
      numpy as np

    Returns:
      matrix_a_star:
        An array containing Matrix A*, as per Goldstein et al (2017), i.e.
        Matrix A with additional rows and columns added that are derived from
        vectors q and r (or q_s and q_r if Matrix A* is expected to not be
        well conditioned).
    """
    df_Njklm = calculate_njklm_values(df=df_delta_comparisons)

    matrix_a = make_matrix_a(
        df_Njklm=df_Njklm, K=input_variables["K"], p=input_variables["p"]
    )

    q_s = multiply_vectors_by_s(
        vector=calculate_q(cutpoints=input_variables["cutpoints"]),
        s=input_variables["s"],
    )

    r_s = multiply_vectors_by_s(
        vector=calculate_r(cutpoints=input_variables["cutpoints"]),
        s=input_variables["s"],
    )

    matrix_a_star = make_matrix_a_star(matrix_a=matrix_a, q=q_s, r=r_s)

    return matrix_a_star


def get_scaled_labelled_x_star(
    matrix_a_star: np.array, input_variables: Dict[str, Any]
) -> Dict[str, float]:
    """
    Takes a Numpy array containing Matrix A*, as defined in Goldstein et al.
    (2017). Also takes the input variables, stored in a dictionary. From this,
    creates a dictionary consisting of scaled, labelled weights.

    Args:
      matrix_a_star:
        An array containing Matrix A*, as per Goldstein et al (2017).
      input_variables:
        A dictionary containing the other input variables required for the
        scaling algorithm. The keys are the name of the input variables and the
        values are the variables themselves. Produced by the utils function
        get_inputs().

    Dependencies:
      itertools
      numpy as np
      re

    Returns:
      x_star_scaled_labelled:
        A dictionary with keys consisting of linkage variable names and states
        and the values consisting of the scaled weights calculated for those
        variables and states.
    """
    b_s = multiply_vectors_by_s(
        vector=calculate_b(K=input_variables["K"]), s=input_variables["s"]
    )

    x_star = solve_for_x_star(matrix_a_star=matrix_a_star, b=b_s)

    x_star_labelled = label_x_star(
        x_star=x_star, cutpoints=input_variables["cutpoints"]
    )

    x_star_scaled_labelled = scale_x_star(x_star_labelled=x_star_labelled)

    return x_star_scaled_labelled


def label_x_star(
    x_star: List[float], cutpoints: Dict[str, Union[List[float], None]]
) -> Dict[str, float]:
    """
    Takes Scalelink vector x* and a list of linkage variable names and their
    string comparison cutpoints. From this, converts x* into a dictionary,
    adding labels so it is clear which variable and agreement state each weight
    relates to.

    Args:
      x_star:
        A list containing x*, i.e. the unscaled weights resulting from the
        scaling algorithm.
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of lists containing the string comparison cutpoints
        for those variables. This should include both variables with binary
        agreement state (for which the key should be Null) and variables with
        partial agreement states (for which the key should be a list of float).

    Dependencies:
      collections

    Returns:
      x_star_labelled:
        A dictionary with keys consisting of linkage variable names and states
        and the values consisting of the raw weights calculated for those
        variables and states.
    """
    # Create labels
    labels = []
    for key, value in cutpoints.items():
        if value is None:
            labels.extend([f"{key}_disagree", f"{key}_agree"])
        else:
            labels.extend(
                [
                    f"{key}_disagree",
                    *[
                        f"{key}_partially_agree_{count}"
                        for count, _ in enumerate(value, 1)
                    ][:-1],
                    f"{key}_agree",
                ]
            )

    # Add labels to x*
    x_star_labelled = collections.OrderedDict()
    for count, i in enumerate(labels):
        x_star_labelled[i] = x_star[count]

    return x_star_labelled


def make_matrix_a(df_Njklm: pd.DataFrame, K: int, p: int) -> np.array:
    """
    Takes Scalelink Njklm values, variable K and variable p. From this, makes
    Scalelink Matrix A.

    Args:
      df_Njklm:
        A dataframe containing the delta comparison column names as column names
        and a single row consisting of the Njklm values (i.e. the sums of these
        delta comparison columns).
      K:
        The total number of agreement states across all linkage variables.
      p:
        The total number of linkage variables.

    Dependencies:
      numpy as np
      pandas as pd

    Returns:
      matrix_a:
        An array containing multiples of the Njk and Njklm values, as per
        Goldstein et al (2017).
    """
    # Declare vector of rows for Njklm values
    Njklm_rows = [None] * K

    # Fill vector of rows, row by row, with Njklm values
    for i in range(0, K):
        if i == 0:
            Njklm_rows[i] = df_Njklm.values.tolist()[0][i : (i + K)]
        else:
            Njklm_rows[i] = df_Njklm.values.tolist()[0][(i * K) : ((i * K) + K)]

    # Convert vector of rows to Numpy array
    Njklm_array = -np.array(Njklm_rows)

    # Correct the Njk values, as these should not be negative and should be
    # multiplied by (p-1)
    np.fill_diagonal(Njklm_array, (p - 1) * -(Njklm_array.diagonal()))

    # Multiply the matrix by 1/(p^2) to get Matrix A
    matrix_a = (1 / (p**2)) * Njklm_array

    return matrix_a


def make_matrix_a_star(matrix_a: np.array, q: List[int], r: List[int]) -> np.array:
    """
    Takes Scalelink Matrix A, Vector q and vector r. From this, makes Scalelink
    Matrix A*.

    Args:
      matrix_a:
        An array containing multiples of the Njk and Njklm values, as per
        Goldstein et al (2017).
      q:
        The Scalelink vector q, a list containing 0s and 1s representing the
        scenario when the agreement state of every variable is 'disagree'. If
        Matrix A* is expected to not be well conditioned, use q_s instead, i.e.,
        q multiplied element-wise by s.

      r:
        The Scalelink vector r, a list containing 0s and 1s representing the
        scenario when the agreement state of every variable is 'agree'. If
        Matrix A* is expected to not be well conditioned, use r_s instead, i.e.,
        r multiplied element-wise by s.

    Dependencies:
      numpy as np

    Returns:
      matrix_a_star:
        An array containing Matrix A*, as per Goldstein et al (2017), i.e.
        Matrix A with additional rows and columns added that are derived from
        vectors q and r (or q_s and q_r if Matrix A* is expected to not be well
        conditioned).
    """
    # Define additional rows and columns
    additional_columns = [[i * -1 for i in q], [i * -1 for i in r]]
    additional_rows = [q + [0, 0], r + [0, 0]]

    # Add additional columns
    matrix_a_with_rows = matrix_a * 2
    for i in range(2):
        matrix_a_with_rows = np.c_[
            np.array(matrix_a_with_rows), np.array(np.asarray(additional_columns[i]))
        ]

    # Add additional rows
    matrix_a_star = matrix_a_with_rows
    for i in range(2):
        matrix_a_star = np.vstack([matrix_a_star, np.asarray(additional_rows[i])])

    return matrix_a_star


def multiply_vectors_by_s(vector: List[int], s: int) -> List[int]:
    """
    Takes Scalelink vector (q, r or b) and the Scalelink variable s. Multiplies
    the vector by s, element-wise, to give q_s, r_s or b_s. These are used in
    place of q, r and b when Matrix A* is not well conditioned.

    Args:
      vector:
        A Scalelink vector (q, r or b) consisting of a list containing 0s and
        1s.
      s:
        A Scalelink variable consisting of the total number of candidate pairs
        entering the scaling algorithm.

    Returns:
      vector_by_s:
        The Scalelink vector with every value multiplied by s.
    """
    vector_by_s = [(i * s) for i in vector]

    return vector_by_s


def scale_x_star(x_star_labelled: Dict[str, float]) -> Dict[str, float]:
    """
    Takes the labelled version of the Scalelink vector x* and scales it. This is
    a two-step process. First, on a linkage variable basis, the disagreement
    weight is subtracted from each weight. Next, each weight is multiplied by
    100. This results in weights scaled to between 0 and 100.

    Args:
      x_star_labelled:
        A dictionary with keys consisting of linkage variable names and states
        and the values consisting of the raw weights calculated for those
        variables and states.

    Dependencies:
      collections
      re

    Returns:
      x_star_scaled:
        A dictionary with keys consisting of linkage variable names and states
        and the values consisting of the scaled weights calculated for those
        variables and states.
    """
    x_star_scaled = collections.OrderedDict()
    for x_star_key, x_star_value in x_star_labelled.items():
        # Identify which linkage column the weight in x* is from
        column_name = re.sub(
            r"_[0-9]+$",
            "",
            re.sub(r"_disagree|_agree|_partially_agree", "", x_star_key),
        )

        # Scale, using column name to select the appropriate disagreement weight
        x_star_scaled.update(
            {
                x_star_key: round(
                    (x_star_value - x_star_labelled[f"{column_name}_disagree"]) * 100, 2
                )
            }
        )

    return x_star_scaled


def solve_for_x_star(matrix_a_star: np.array, b: List[int]) -> List[float]:
    """
    Takes Scalelink Matrix A* and vector b. From this, solves the matrix to make
    Scalelink vector x*.

    Args:
      matrix_a_star:
        An array containing Matrix A*, as per Goldstein et al (2017), i.e.
        Matrix A with additional rows and columns added that are derived from
        vectors q and r (or q_s and q_r if Matrix A* is expected to not be well
        conditioned).
      b:
        The Scalelink vector b, a list of length K where every element is 0
        except the last element. If Matrix A* is expected to not be well
        conditioned, use b_s instead, i.e., b multiplied element-wise by s.

    Dependencies:
      numpy as np

    Returns:
      x_star:
        A list containing x*, i.e. the unscaled weights resulting from the
        scaling algorithm.
    """
    inverse_matrix_a_star = np.linalg.inv(matrix_a_star)
    x_star = np.dot(inverse_matrix_a_star, np.asarray(b)).tolist()
    return x_star
