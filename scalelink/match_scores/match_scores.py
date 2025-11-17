"""Match score functions.

A series of functions for calculating Scalelink match weights and Scalelink
match scores, given existing Scalelink deltas as defined in the 2017 paper
by Goldstein et al.

Methods:
  assign_match_score:
    A method to calculate Scalelink match scores, given Scalelink match weights
    as input. Acts on the output of assign_weights.

  assign_weights:
    A method to calculate Scalelink match weights for each ID column pair,
    given as inputs a set of Scalelink deltas (agreement state values for ID
    pairs), and a scaled vector x* (agreement state weights by matching
    variable). Acts on the outputs of calculate_deltas, scale_x_star, and
    get_input_variables.

  get_match_scores:
    A method to produce a dataframe containing final Scalelink match scores and
    match weights. Works by running, in order:
     - assign_weights
     - assign_match_score
"""

from typing import Any, Dict, List

import spark
from pyspark.sql import functions as F


def assign_match_score(df_with_weights: spark.sql.DataFrame) -> spark.sql.DataFrame:
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked, with
        Scalelink weights assigned and calculates match score for each row.

    Args:
        df_with_weights:
            A dataframe consisting of a row ID for the first dataset that was
            joined, a row ID for the second dataset that was joined and a column
            for each Scalelink weight for each linkage variable.

    Returns:
        df_with_match_score:
            A dataframe consisting of df_with_weights with an additional column
            called match_score which contains the sum of the weights, row-wise.
    """
    weight_cols = [col for col in df_with_weights.columns if col.endswith("_weight")]

    # Make match score
    df_with_match_score = df_with_weights.withColumn("match_score", F.lit(0))

    # Fill match score
    for col in weight_cols:
        df_with_match_score = df_with_match_score.withColumn(
            "match_score", F.col(col) + F.col("match_score")
        )

    return df_with_match_score


def assign_weights(
    df_with_deltas: spark.sql.DataFrame,
    df1_id: str,
    df2_id: str,
    cutpoints: Dict[str, List[float] | None],
    x_star_scaled: Dict[str, float],
    spark: spark.sql.SparkSession,
) -> spark.sql.DataFrame:
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked, with
        Scalelink deltas (an indicator matrix calculated for the agreement states
        of the linkage variables) calculated. Also takes the pre-calculated
        Scalelink weights derived from that dataframe. Returns the ID columns
        from that dataframe, plus the weights assigned to each linkage variable
        for each row (i.e., for each ID column pair).

    Args:
        df_with_deltas (Spark DataFrame):
            A dataframe produced by the calculate_deltas() function.
        df1_id:
            The name of the column in df_with_deltas that contains the row ID
            for the first dataset that was joined.
        df2_id:
            The name of the column in df_with_deltas that contains the row ID
            for the second dataset that was joined.
        cutpoints:
            A dictionary with keys consisting of the linkage variable names and
            values consisting of the string comparison cutpoints for those
            variables.
        x_star_scaled:
            A dictionary with keys consisting of linkage variable names and
            states and the values consisting of the scaled weights calculated for
            those variables and states.
        spark (PySpark SparkSession):
            Name of the Spark session being used.

    Returns:
        df_with_weights:
            A dataframe consisting of df1_id and df2_id from df_with_deltas with
            weight columns (named after the linkage variables, suffixed with
            '_weight') containing appropriate weights from x_star_scaled for
            each linkage variable and row.
    """
    # Define variables
    df_with_weights = df_with_deltas
    linkage_cols = list(cutpoints.keys())

    # Make then fill weight columns
    for col in linkage_cols:

        df_with_weights = df_with_weights.withColumn(col + "_weight", F.lit(None))

        for key in [x for x in x_star_scaled.keys() if x.startswith(col)]:
            if "_disagree" in key:
                df_with_weights = df_with_weights.withColumn(
                    col + "_weight",
                    F.when(
                        F.col("di_" + col + "_1"), F.lit(x_star_scaled[key])
                    ).otherwise(F.col(col + "_weight")),
                )
            elif "_partially_agree_" in key:
                df_with_weights = df_with_weights.withColumn(
                    col + "_weight",
                    F.when(
                        F.col(
                            "di_"
                            + col
                            + "_"
                            + str(int(key.replace(col + "_partially_agree_", "")) + 1)
                        ),
                        F.lit(x_star_scaled[key]),
                    ).otherwise(F.col(col + "_weight")),
                )
            elif ("_agree" in key) & (cutpoints[col] is None):
                df_with_weights = df_with_weights.withColumn(
                    col + "_weight",
                    F.when(
                        F.col("di_" + col + "_2"), F.lit(x_star_scaled[key])
                    ).otherwise(F.col(col + "_weight")),
                )
            else:
                df_with_weights = df_with_weights.withColumn(
                    col + "_weight",
                    F.when(
                        F.col("di_" + col + "_" + str(len(cutpoints[col]) + 1)),
                        F.lit(x_star_scaled[key]),
                    ).otherwise(F.col(col + "_weight")),
                )

    # Drop unnecessary columns
    df_with_weights = df_with_weights.select(
        [df1_id, df2_id]
        + [col for col in df_with_weights.columns if col.endswith("_weight")]
    )

    return df_with_weights


def get_match_scores(
    df_deltas: spark.sql.DataFrame,
    x_star_scaled_labelled: Dict[str, float],
    input_variables: Dict[str, Any],
    spark: spark.sql.SparkSession,
) -> spark.sql.DataFrame:
    """
    Takes a dataframe containing a Cartesian join of the two dataframes to be
        compared, with deltas calculated. Also takes the scaled, labelled weights
        (x*) for this dataframe and the input variables (stored in a dictionary).
        From this, returns the IDs from each row of the incoming dataframe with
        weights and matchscores appropriately assigned.

    Args:
        df_deltas:
            A dataframe consisting of df_cartesian_join with additional Boolean
            columns containing Scalelink deltas (i.e, an indicator matrix) for
            the linkage variables.
        x_star_scaled_labelled:
            A dictionary with keys consisting of linkage variable names and
            states and the values consisting of the scaled weights calculated for
            those variables and states.
        input_variables:
            A dictionary containing the other input variables required for
            the scaling algorithm, including ID column names and string comparator
            cut-points. Dictionary keys are the names of the input variables and
            dictionary values are the values of the variables themselves.
            This may be produced by the utils function get_input_variables().
        spark:
            Name of the Spark session being used.

    Returns:
        df_weights_match_scores:
            A dataframe consisting of df1_id and df2_id from df_with_deltas with
            weight columns (named after the linkage variables, suffixed with
            '_weight') containing appropriate weights from x_star_scaled for
            each linkage variable and row, plus a column called match_score which
            contains the sum of the weights, row-wise.
    """
    df_weights = assign_weights(
        df_with_deltas=df_deltas,
        df1_id=input_variables["df1_id"],
        df2_id=input_variables["df2_id"],
        cutpoints=input_variables["cutpoints"],
        x_star_scaled=x_star_scaled_labelled,
        spark=spark,
    )

    df_weights_match_scores = assign_match_score(df_with_weights=df_weights)

    return df_weights_match_scores
