"""Indicator matrix functions.

A series of functions that together produce an indicator matrix from the two
datasets to be joined. As with all mathematics involving or based on Multiple
Correspondence Analysis (MCA), Scalelink starts by producing an indicator
matrix which is then used for subsequent steps. In the 2017 paper by Goldstein
et al., the process of producing an indicator matrix is described as the
process of creating and comparing deltas.

Methods:
  calculate_agreement_states:
    A method to calculate agreement states for each of the linkage variables
    with binary agreement state. Acts on a cartesian join of the two datasets
    to be linked.

  calculate_deltas:
    A method to calculate Scalelink deltas for all linkage variables. Acts on
    the output of calculate_agreement_states.

  calculate_sorensen_dice:
    A method to compare two string columns and calculate the Sorensen-Dice
    coefficient for them.

  compare_deltas:
    A method to compare Scalelink deltas from the output of get_deltas.

  compute_normalised_levenshtein:
    A method to compare two string columns and calculate the normalised
    Levenshtein edit distance for them.

  get_deltas:
    A method to calculate Scalelink deltas from a cartesian join of the two
    datasets to be linked. Works by running, in order:
     - calculate_sorensen_dice
     - calculate_agreement_states
     - calculate_deltas

  make_bigrams:
    A method to calculate bigrams for a string column.


Typical usage example:

from scalelink.indicator_matrix import functions as im

my_deltas = im.get_deltas(
  df_cartesian_join = my_df,
  input_variables = my_input_variables,
)

my_indicator_matrix = im.compare_deltas(
  df = my_deltas,
  linkage_vars = ['forename', 'surname', 'dob', 'sex', 'postcode'],
  delta_col_prefix = 'prefix_',
)
"""

from pyspark.ml.feature import NGram
from pyspark.sql import functions as F


def calculate_agreement_states(df, binary_agreement_cols, df_suffixes):
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked and
        calculates the agreement state for each of the variables with binary
        agreement state in the dataframe.

    Args:
        df (Spark DataFrame):
            The dataframe containing the data for which agreement states will be
            calculated.
        binary_agreement_cols (list of str):
            A list containing the names of the pairs of columns containing the
            linkage variables with binary agreement state. These columns must
            follow the naming rules outlined in the dependencies.
        df_suffixes (list of str):
            A list containing two strings: the suffix applied to each linkage
            variable of the first and second dataframe used to make df.

    Dependencies:
        The pairs of columns containing linkage variables for the two Cartesian
            joined datasets must have identical names, except for a suffix that
            identifies which dataset the column is from. E.g., comparing a pair
            of columns in df called 'forename_df1' and 'forename_df2' would be
            require binary_agreement_cols as ['forename'] and df_suffixes as
            ['_df1', '_df2'].
        The pairs of columns listed in binary_agreement_cols must not contain any
            Nulls. This is because there is no missingness-handling in this
            function. If Nulls are present, Null:value and Null:Null comparisons
            will erroneously be assigned the agreement state True.

    Returns:
        df_agreement (Spark DataFrame):
            A dataframe consisting of df, plus an additional Boolean column for
            each pair of columns in binary_agreement_cols. The Boolean column
            contains the agreement state for the corresponding pair of linkage
            columns where True = agree, False = disagree.
    """

    df_agreement = df

    for i in binary_agreement_cols:
        df_agreement = df_agreement.withColumn(
            i + "_agr_state",
            F.when(
                F.col(i + df_suffixes[0]) != F.col(i + df_suffixes[1]), False
            ).otherwise(True),
        )

    return df_agreement


def calculate_deltas(df, cutpoints, agreement_col_suffix, string_similarity_suffix):
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked that
        has had the agreement state for each variable with binary agreement state
        calculated. From this, calculates Scalelink deltas (i.e., an indicator
        matrix of agreement states) for all linkage variables, regardless of if
        their agreement state is binary or not.

    Args:
        df (Spark DataFrame):
            The dataframe containing the IDs, linkage variables and agreement
            states for  from which the
            deltas will be calculated.
        cutpoints (dict of str: list of float):
            A dictionary with keys consisting of the linkage variable names and
            values consisting of lists containing the string comparison cutpoints
            for those variables. This should include both variables with binary
            agreement state (for which the key should be Null) and variables with
            partial agreement states (for which the key should be a list of
            float).
        agreement_col_suffix (str):
            The suffix that identifies agreement state columns. E.g., if the
            columns 'forename_df1' and 'forename_df2' were compared and their
            binary agreement state was stored in the column 'forename_agr_state',
            the agreement_col_suffix would be '_agr_state'.
        string_similarity_suffix (str):
            The suffix that identifies columns containing string similarity
            metrics, e.g., Levenshtein edit distance, Sorensen-Dice coefficient.
            E.g., if the columns 'forename_df1' and 'forename_df2' were compared
            by calculating the Sorensen-Dice coefficient, with the result stored
            in the column 'forename_coeff', the string_similarity_suffix would be
            '_coeff'.

    Dependencies:
        The agreement state columns in df must all have the same suffix, which is
            specified in agreement_col_suffix.
        The string similarity metric columns in df must all have the same suffix,
            which is specified in string_similarity_suffix.
        The cutpoints specified as keys in the dictionary cutpoints must be
            normalised to within the bounds 0-1. However, they should not include
            the bounds. E.g., if 'forename_df1' and 'forename_df2' were compared
            using standardised Levenshtein edit distance, the cutpoints could be
            [0.6, 0.8] but must not be specified as [0, 0.6, 0.8, 1].
        The agreement columns identified by agreement_col_suffix and the string
            similarity metric columns identified by string_similarity_suffix must
            not contain any Nulls. If Nulls are present, all deltas for that
            column and row will be false, which will cause errors downstream in
            the Scalelink method.

    Returns:
        df_with_deltas (Spark DataFrame):
            A dataframe consisting of df with one additional column per Scalelink
            delta. The number of additional columns corresponds to the Scalelink
            variable K. Each delta column will be a Boolean column and will have
            a name with format 'di_col_name_int' where 'col_name' is the name of
            the linkage variable being compared (e.g., 'forename') and 'int' is
            the delta number.
            For columns with binary agreement state, delta 1 is True when the
            agreement state is False and delta 2 is True when the agreement state
            is True.
            For columns with partial agreement, delta 1 is True when the string
            comparison metric is equal to or greater than the bottom bound and
            less than the lowest cutpoint, delta 2 is True when the string
            comparison metric is equal to or greater than the lowest cutpoint and
            less than the next cutpoint, etc.
    """
    df_with_deltas = df

    for col_name, cutpoints in cutpoints.items():
        if cutpoints is None:
            df_with_deltas = df_with_deltas.withColumn(
                "di_" + col_name + "_1",
                F.when(
                    not F.col(col_name + agreement_col_suffix), F.lit(True)
                ).otherwise(F.lit(False)),
            ).withColumn(
                "di_" + col_name + "_2",
                F.when(F.col(col_name + agreement_col_suffix), F.lit(True)).otherwise(
                    F.lit(False)
                ),
            )
        else:
            # Add an upper and lower bound to the cutpoints. These are the bounds
            # of the actual cutpoint range. They permit calculation of whether a
            # string comparison metric is below the lowest inputted cutpoint or above
            # the highest.
            cutpoints_bounded = [-0.1] + cutpoints + [1.1]

            for count in range(len(cutpoints_bounded[:-1])):
                df_with_deltas = df_with_deltas.withColumn(
                    "di_" + col_name + "_" + str(count + 1),
                    F.when(
                        (
                            F.col(col_name + string_similarity_suffix)
                            >= F.lit(cutpoints_bounded[count])
                        )
                        & (
                            F.col(col_name + string_similarity_suffix)
                            < F.lit(cutpoints_bounded[count + 1])
                        ),
                        F.lit(True),
                    ).otherwise(F.lit(False)),
                )

    return df_with_deltas


def calculate_sorensen_dice(df, col1, col2, new_col, decimal_places):
    """
    Takes two string columns, calculates the Sorensen-Dice coefficient for them
    and returns it in in a new column.

    Args:
        df (Spark DataFrame):
            The dataframe containing the two columns that the Sorensen-Dice
            coefficent will be calculated for.
        col1 (str):
            The name of the first string column to be compared.
        col2 (str):
            The name of the second string column to be compared.
        new_col (str):
            The name to be given to the column containing the Sorensen-Dice
            coefficients for the comparison of col1 and col2.
        decimal_places (int):
            The number of decimal places new_col should have. Must be a positive
            number.

    Returns:
        df_compared (Spark DataFrame):
            A dataframe consisting of df with an additional float column called
            new_col which contains the Sorensen-Dice coefficients for the
            comparison of col1 and col2.

    Provisos:
        Duplicate bigrams are not counted distinctly. Therefore, 'AA' and 'AAAA'
            will have a Sorensen-Dice coefficient of 1.
        As pyspark.sql.functions.round() is used, the Sorensen-Dice coefficients
            are rounded using the half-up method.

    Dependencies:
        The feature transformer NGram from pyspark.ml.feature.
        The function make_bigram_column.

    Attribution:
        This code is derived from the Python function at en.wikibooks.org/wiki/
        Algorithm_Implementation/Strings/Dice%27s_coefficient#Python
    """
    # Rename df
    df_compared = df

    # Make bigrams
    for i in [col1, col2]:
        df_compared = make_bigrams(df=df_compared, col=i)

    # Calculate bigram array intersection
    df_compared = df_compared.withColumn(
        "overlap",
        F.when(F.col(col1 + "_bigrams").isNull(), F.lit(0))
        .when(F.col(col2 + "_bigrams").isNull(), F.lit(0))
        .otherwise(F.size(F.array_intersect(col1 + "_bigrams", col2 + "_bigrams"))),
    )

    # Calculate and round Sorensen-Dice coefficent
    df_compared = df_compared.withColumn(
        new_col,
        F.round(
            (F.col("overlap") * 2)
            / ((F.size(col1 + "_bigrams")) + (F.size(col2 + "_bigrams"))),
            decimal_places,
        ),
    )

    # Drop unnecessary columns
    df_compared = df_compared.drop(col1 + "_bigrams", col2 + "_bigrams", "overlap")

    return df_compared


def compare_deltas(df, linkage_vars, delta_col_prefix):
    """
    Takes a dataframe made by Cartesian join of two datasets to be linked that
        has had the Scalelink deltas calculated and compares the status of these
        deltas, resulting in a comparison column for each possible pair of
        deltas.

    Args:
        df (Spark DataFrame):
            The dataframe containing the deltas from which the delta comparisons
            will be calculated.
        linkage_vars (list of str):
            The names of the linkage variables.
        delta_col_prefix (str):
            The prefix that identifies delta columns. E.g., if the columns
            'forename_df1' and 'forename_df2' were compared, resulting in a delta
            column called 'di_forename_1', the delta_col_prefix would be 'di_'.

    Dependencies:
        The delta columns must be Boolean columns and must not be nullable.

    Returns:
        df_delta_comparisons (Spark DataFrame):
            A dataframe containing the delta comparison columns calculated from
            df only - none of the columns derived from df are present. Each delta
            comparison column will be a non-nullable integer column and will have
            a name with format 'N_col_name1_int1_col_name2_int2' where
            'col_name1' and 'int1' refer to the first delta of the comparison
            (e.g., 'forename_1') and 'col_name2' and 'int2' refer to the second
            delta of the comparison.
            Delta comparisons are 1 when both deltas are True and are 0 in all
            other circumstances.
    """
    delta_columns = [x for x in df.columns if x.startswith(delta_col_prefix)]

    df_delta_comparisons = df.select(delta_columns)

    for delta_1 in delta_columns:
        for delta_2 in delta_columns:
            df_delta_comparisons = df_delta_comparisons.withColumn(
                "N_"
                + delta_1[len(delta_col_prefix) : len(delta_1)]
                + "_"
                + delta_2[len(delta_col_prefix) : len(delta_2)],
                F.when(
                    (F.col(delta_1) == F.lit(True))
                    & (F.col(delta_1) == F.col(delta_2)),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )

    df_delta_comparisons = df_delta_comparisons.drop(*delta_columns)

    return df_delta_comparisons


def compute_normalized_levenshtein(string1, string2):
    """
    Applies the normalized Levenshtein distance to two strings, reporting a score
        normalised to between 0 and 1.

    Args:
        string1 (Pyspark Data Frame col: str)
            string to be compared to string2.
        string2 (Pyspark Data Frame col: str)
            string to be compared to string1.

    Returns:
        Normalized levenshtein distance between 0 and 1.
    """
    levenshtein_normalized_distance = 1 - (
        (F.levenshtein(string1, string2))
        / (F.greatest(F.length(string1), F.length(string2)))
    )

    return levenshtein_normalized_distance


def get_deltas(df_cartesian_join, input_variables):
    """
    Takes the dataframe created by Cartesian join of the two dataframes to be
        linked. Also takes the input variables, stored in a dictionary. From
        this, calculates deltas (i.e., an indicator matrix) for all linkage
        variables.

    Args:
        df_cartesian_join (Spark DataFrame):
            A Spark DataFrame consisting of the Cartesian join of the two
            dataframes to be linked.
        input_variables (dict of str, misc):
            A dictionary containing the other input variables required for
            the scaling algorithm. The keys are the name of the input variables
            and the values are the variables themselves. Produced by the utils
            function get_inputs().

    Provisos:
        Currently, the only string comparison available is Sorensen-Dice
        coefficient.

    Returns:
        df_deltas (Spark DataFrame):
            A dataframe consisting of df_cartesian_join with additional Boolean
            columns containing Scalelink deltas (i.e, an indicator matrix) for
            the linkage variables.
            For columns with binary agreement state, delta 1 is True when the
            agreement state is False and delta 2 is True when the agreement state
            is True.
            For columns with partial agreement, delta 1 is True when the string
            comparison metric is equal to or greater than the bottom bound and
            less than the lowest cutpoint, delta 2 is True when the string
            comparison metric is equal to or greater than the lowest cutpoint and
            less than the next cutpoint, etc.

    """
    df = df_cartesian_join

    for i in input_variables["partial_agreement_vars"]:
        df = calculate_sorensen_dice(
            df=df,
            col1=i + input_variables["df1_suffix"],
            col2=i + input_variables["df2_suffix"],
            new_col=i + "_sorensen_dice",
            decimal_places=4,
        )

    df_agreement_states = calculate_agreement_states(
        df=df,
        binary_agreement_cols=input_variables["binary_agreement_vars"],
        df_suffixes=[input_variables["df1_suffix"], input_variables["df2_suffix"]],
    )

    df_agreement_states = df_agreement_states.checkpoint()

    df_deltas = calculate_deltas(
        df=df_agreement_states,
        cutpoints=input_variables["cutpoints"],
        agreement_col_suffix="_agr_state",
        string_similarity_suffix="_sorensen_dice",
    )

    return df_deltas


def make_bigrams(df, col):
    """
    Takes a dataframe containing a string column and returns it with a new
        column containing the bigrams of that string column.

    Args:
        df (Spark DataFrame):
            The dataframe containing the two columns that the Sorensen-Dice
            coefficent will be calculated for.
        col (str):
            The name of the first string column to be compared.

    Returns:
        df_compared (Spark DataFrame):
            A dataframe consisting of df with an additional column containing a
            list of strings called 'col_bigrams', which contains the bigrams of
            col.
            Where the string in col was empty or Null, col_bigrams will be Null.
            Where the string in col was of length 1, col_bigrams will be
            calculated by appending a '#' onto the string to make it length 2.

    Dependencies:
        The feature transformer NGram from pyspark.ml.feature.
        col must not contain the character '#'.
    """
    # Split df (Null or empty string in col breaks ngram.transform())
    df_no_nulls = df.filter((F.col(col).isNotNull()) & (F.col(col) != ""))
    df_nulls = df.filter((F.col(col).isNull()) | (F.col(col) == ""))

    # Extend strings of length 1, to enable bigrams to be made from them
    df_no_nulls = df_no_nulls.withColumn(
        col,
        F.when(F.length(F.col(col)) == 1, F.concat(F.col(col), F.lit("#"))).otherwise(
            F.col(col)
        ),
    )

    # Split columns
    df_no_nulls = df_no_nulls.withColumn(col + "_split", F.split(col, ""))

    # Set up bigrams
    ngram = NGram(n=2)
    ngram.setInputCol(col + "_split")
    ngram.setOutputCol(col + "_bigrams")

    # Make bigrams
    df_no_nulls = (
        ngram.transform(df_no_nulls)
        .withColumn(col + "_bigrams_length", (F.size((col + "_bigrams")) - 1))
        .withColumn(
            col + "_bigrams",
            F.expr(
                "slice(" + col + "_bigrams" + ", 1," + col + "_bigrams_length" + ")"
            ),
        )
        .drop(col + "_split", col + "_bigrams_length")
    )

    # Remove # from strings that were length 1, reverting them back to length 1
    df_no_nulls = df_no_nulls.withColumn(
        col, F.regexp_replace(col, "^([0-9A-Za-z]{1})([#]{1})$", "$1")
    )

    # Join Nulls back on
    df_with_bigrams = df_no_nulls.unionByName(
        df_nulls.withColumn(col + "_bigrams", F.lit(None))
    )

    return df_with_bigrams
