"""
Run script for Scalelink.
"""

import boto3
import raz_client
from rdsa_utils.cdp.helpers.s3_utils import delete_folder

from scalelink.indicator_matrix import indicator_matrix as im
from scalelink.match_scores import match_scores as ms
from scalelink.matrix_a_star import matrix_a_star as ma
from scalelink.utils import utils as ut


def run_scalelink(config_path: str = "scalelink/configs.ini") -> None:
    """
    Takes a path for the location of the config file. From this, runs the entire
      scaling method as per Goldstein et al. (2017) on the specified datasets,
      using the specified linkage variables.

    Args:
      config_path:
        The filepath for the config file. The default is a file called
        configs.ini in the head of this repo. The contents should follow the
        template found at scalelink/configs_template.ini in this repo.

    Dependencies:
      boto3
      pyspark.sql.DataFrame
      raz_client
      delete_folder from rdsa_utils.cdp.helpers.s3_utils

    Returns:
      None.
      However, writes a linked dataframe either derived from the Cartesian join of
        the two input dataframes or derived from the dataset located at
        df_candidates_path.
        The columns present on this dataset are:
          - The ID column of each dataset.
          - Weight columns (named after the linkage variables, suffixed with
            '_weight') containing weights for each linkage variable and row.
          - A column called match_score which contains the sum of the weights,
            row-wise.
        It is written to the output_path specified in the config file.
    """
    input_variables = ut.get_input_variables(config_path=config_path)

    spark = ut.create_spark_session(
        spark_session_name="Scalelink",
        spark_session_size=input_variables["spark_session_size"],
    )

    if input_variables["df_candidates_path"] == "":
        df_candidate_pairs = ut.cartesian_join_dataframes(
            df1_path="s3a://"
            + input_variables["bucket_name"]
            + input_variables["df1_path"],
            df2_path="s3a://"
            + input_variables["bucket_name"]
            + input_variables["df2_path"],
            spark=spark,
        )
    else:
        df_candidate_pairs = spark.read.parquet(
            "s3a://"
            + input_variables["bucket_name"]
            + input_variables["df_candidates_path"]
        )

    input_variables = ut.get_s(
        input_variables=input_variables, df_cartesian_join=df_candidate_pairs
    )

    spark.sparkContext.setCheckpointDir(
        "s3a://" + input_variables["bucket_name"] + input_variables["checkpoint_path"]
    )

    df_deltas = im.get_deltas(
        df_cartesian_join=df_candidate_pairs, input_variables=input_variables
    )

    df_delta_comparisons = im.compare_deltas(
        df=df_deltas,
        linkage_vars=input_variables["linkage_vars"],
        delta_col_prefix="di_",
    )

    print("Deltas have been calculated")

    matrix_a_star = ma.get_matrix_a_star(
        df_delta_comparisons=df_delta_comparisons, input_variables=input_variables
    )

    print("Matrix A* has been calculated")

    x_star_scaled_labelled = ma.get_scaled_labelled_x_star(
        matrix_a_star=matrix_a_star, input_variables=input_variables
    )

    df_weights_match_scores = ms.get_match_scores(
        df_deltas=df_deltas,
        x_star_scaled_labelled=x_star_scaled_labelled,
        input_variables=input_variables,
        spark=spark,
    )

    print("Match scores have been calculated")

    df_weights_match_scores.write.mode("overwrite").parquet(
        "s3a://" + input_variables["bucket_name"] + input_variables["output_path"]
    )

    print("Your linked dataset has been written to:", input_variables["output_path"])

    client = boto3.client("s3")
    raz_client.configure_ranger_raz(client, ssl_file=input_variables["ssl_file"])
    delete_folder(
        client, input_variables["bucket_name"], input_variables["checkpoint_path"]
    )

    print("Your checkpoint files have been tidied up")
    print("The Scalelink linkage is now complete")


if __name__ == "__main__":
    run_scalelink()
