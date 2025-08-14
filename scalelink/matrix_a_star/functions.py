"""Matrix A* functions.

A series of functions that together produce matrix A* and vector x*, as
defined in the 2017 paper by Goldstein et al.

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

  label_x_star:
    A method to label the Scalelink variable x* with linkage variables and
    agreement states, to aid user comprehension.


"""

import collections
import itertools


def calculate_b(K):
    """
    Takes the Scalelink variable K. From this, calculates the Scalelink vector b.

    Args:
        K (int):
            The total number of agreement states across all linkage variables.

    Dependencies:
        itertools

    Returns:
        b (list of int):
            The Scalelink vector b, a list of length K+2. It consists of K+1 0s
            followed by a single 1. It is necessary for deriving weights from
            Matrix A* by matrix multiplication.
    """
    b = []
    b = list(itertools.repeat(0, int(K + 1)))
    b.append(1)
    return b


def calculate_njklm_values(df):
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked that
        has had the Scalelink deltas and their comparisons calculated. Calculates
        Njklm values from these delta comparisons.

    Args:
        df (Spark DataFrame):
            The dataframe containing the deltas comparisons from which the Njklm
            values will be calculated.

    Dependencies:
        pandas as pd

    Returns:
        df_njklm (Pandas DataFrame):
            A dataframe containing the delta comparison column names as column
            names and a single row consisting of the Njklm values (i.e. the sums
            of these delta comparison columns).
    """
    df_njklm = df.groupBy().sum()

    # Convert column names from 'sum(col1)' to 'col1'
    for col in df_njklm.columns:
        df_njklm = df_njklm.withColumnRenamed(col, col[4 : (len(col) - 1)])

    df_njklm = df_njklm.toPandas()

    return df_njklm


def calculate_q(cutpoints):
    """
    Takes a dictionary containing linkage variable names and the string
        comparison cutpoints for those variables. From this, calculates the
        Scalelink vector q.

    Args:
        cutpoints (dict of str: float):
            A dictionary with keys consisting of the linkage variable names and
            values consisting of the string comparison cutpoints for those
            variables.

    Dependencies:
        itertools

    Returns:
        q (list of int):
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


def calculate_r(cutpoints):
    """
    Takes a dictionary containing linkage variable names and the string
        comparison cutpoints for those variables. From this, calculates the
        Scalelink vector r.

    Args:
        cutpoints (dict of str: float):
            A dictionary with keys consisting of the linkage variable names and
            values consisting of the string comparison cutpoints for those
            variables.

    Dependencies:
        itertools

    Returns:
        r (list of int):
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


def label_x_star(x_star, cutpoints):
    """
    Takes Scalelink vector x* and a list of linkage variable names and their
        string comparison cutpoints. From this, converts x* into a dictionary,
        adding labels so it is clear which variable and agreement state each
        weight relates to.

    Args:
        x_star (list of float):
            A list containing x*, i.e. the unscaled weights resulting from the
            scaling algorithm.
        cutpoints (dict of str: list of float):
            A dictionary with keys consisting of the linkage variable names and
            values consisting of lists containing the string comparison cutpoints
            for those variables. This should include both variables with binary
            agreement state (for which the key should be Null) and variables with
            partial agreement states (for which the key should be a list of
            float).

    Dependencies:
        collections

    Returns:
        x_star_labelled (dict of str: float):
            A dictionary with keys consisting of linkage variable names and
            states and the values consisting of the raw weights calculated for
            those variables and states.
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
