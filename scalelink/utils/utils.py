"""Utility functions.

A series of functions that work to prepare data for the first step of the
Scalelink algorithm (making the indicator matrix) or are used in multiple steps
of the Scalelink algorithm.

Methods:
  define_binary_agreement_vars:
    A method that takes user-defined linkage variables and cutpoints to create
    a list of the linkage variables that have binary agreement state, i.e.
    "disagree" or "agree" only.

  cartesian_join_dataframes:
    A method to Cartesian join two dataframes.

  create_spark_session:
    A method to create a Spark session of a specified size.

  define_K:
    A method to define the Scalelink variable K, the total number of agreement
    states across all linkage variables.

  define_kj:
    A method to define the Scalelink variable kj, the number of agreement states
    for each linkage variable.

  define_p:
    A method to define the Scalelink variable p, the total number of linkage
    variables.

  define_partial_agreement_vars:
    A method that takes user-defined linkage variables and cutpoints to create
    a list of the linkage variables that have partial agreement state, i.e. have
    more agreement states than just "disagree" and "agree".

  format_cutpoints:
    A method that formats the user-inputted cutpoints for use by other methods.

  get_input_variables:
    A method to produce a dictionary of input variables for the rest of the
    Scalelink algorithm. Works by running, in order:
     - read_configs
     - format_cutpoints
     - define_binary_agreement_vars
     - define_partial_agreement_vars
     - define_p
     - define_kj
     - define_K

  get_s:
    A method to define the Scalelink variable s, the total number of candidate
    pairs entering the scaling algorithm, and add it to the input variable
    dictionary.

  read_configs:
    A method that reads in configs from a .ini file at a specified filepath.
"""

import collections
import configparser as cp
from typing import Any

import pyspark
from pyspark.sql import SparkSession


def define_binary_agreement_vars(
    cutpoints: dict[str, list[float] | None],
) -> list[str]:
    """
    Defines the linkage variables that will have binary agreement state.

    Args:
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of the string comparison cutpoints for those
        variables.

    Returns:
      binary_agreement_vars:
        A list containing the names of the linkage variables that will have
        binary agreement state.
    """
    binary_agreement_cutpoints = {
        key: value for key, value in cutpoints.items() if value is None
    }
    binary_agreement_vars = list(binary_agreement_cutpoints.keys())
    return binary_agreement_vars


def cartesian_join_dataframes(
    df1_path: str, df2_path: str, spark: pyspark.sql.SparkSession
) -> pyspark.sql.DataFrame:
    """
    Takes the filepaths to two dataframes. Reads these in, Cartesian joins them
    and returns the product of this join.

    Args:
      df1_path:
        The filepath for the first dataframe.
      df2_path:
        The filepath for the second dataframe.
      spark:
        The Spark session being used.

    Dependencies
      pyspark.sql.SparkSession

    Returns:
      cartesian_join_df:
        A dataframe containing the two input dataframes Cartesian joined
        together.
    """
    df1 = spark.read.parquet(df1_path)
    df2 = spark.read.parquet(df2_path)
    cartesian_join_df = df1.crossJoin(other=df2)
    return cartesian_join_df


def create_spark_session(
    spark_session_name: str, spark_session_size: str
) -> pyspark.sql.SparkSession:
    """
    Creates a Spark Session.

    Args:
      spark_session_name:
        The name to give the Spark session.
      spark_session_size:
        The size of session to create - small (s), medium (m), large (l) or
        extra-large (xl).

    Dependencies:
      pyspark.sql.SparkSession

    Returns:
      spark:
        A Spark session of the specified size and name.
    """
    session_configs = {
        "s": {
            "spark.executor.memory": "1g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": 1,
            "spark.dynamicAllocation.maxExecutors": 3,
            "spark.sql.shuffle.partitions": 12,
        },
        "m": {
            "spark.executor.memory": "6g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": 3,
            "spark.dynamicAllocation.maxExecutors": 3,
            "spark.sql.shuffle.partitions": 18,
        },
        "l": {
            "spark.executor.memory": "10g",
            "spark.yarn.executor.memoryOverhead": "1g",
            "spark.executor.cores": 5,
            "spark.dynamicAllocation.maxExecutors": 5,
            "spark.sql.shuffle.partitions": 200,
        },
        "xl": {
            "spark.executor.memory": "20g",
            "spark.yarn.executor.memoryOverhead": "2g",
            "spark.executor.cores": 5,
            "spark.dynamicAllocation.maxExecutors": 12,
            "spark.sql.shuffle.partitions": 240,
        },
    }

    if spark_session_size not in session_configs:
        raise ValueError(
            f"{spark_session_size} is not a valid SparkSession, use one of [{', '.join(list(session_configs.keys()))}]"
        )

    session_config = session_configs[spark_session_size]
    spark_builder = (
        SparkSession.builder.appName(spark_session_name)
        .config("spark.shuffle.service.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
    )

    for k, v in session_config.items():
        spark_builder.config(k, v)

    spark = spark_builder.getOrCreate()

    return spark


def define_K(kj: dict[str, int]) -> int:
    """
    Defines the variable K, as per Goldstein et al. (2017). This variable is the
    total number of agreement states across all linkage variables.

    Args:
      kj:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of kj (number of agreement states) for each linkage
        variable.

    Returns:
      K:
        The total number of agreement states across all linkage variables.
    """
    K = 0
    for value in kj.values():
        K = K + value
    return K


def define_kj(cutpoints: dict[str, list[float] | None]) -> dict[str, int]:
    """
    Defines the variable kj for each linkage variable, as per Goldstein et al.
    (2017). This variable is the number of agreement states.

    Args:
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of the string comparison cutpoints for those
        variables.

    Dependencies:
      collections

    Returns:
      kj:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of kj (number of agreement states) for each linkage
        variable.
    """
    kj = collections.OrderedDict()
    for key, value in cutpoints.items():
        if value is None:
            kj[key] = 2
        else:
            kj[key] = len(value) + 1
    return kj


def define_p(linkage_vars: list[str]) -> int:
    """
    Defines the variable p, as per Goldstein et al (2017). This variable is the
    total number of linkage variables.

    Args:
      linkage_vars:
        The names of the linkage variables.

    Returns:
      p:
        The total number of linkage variables.
    """
    p = len(linkage_vars)
    return p


def define_partial_agreement_vars(
    cutpoints: dict[str, list[float] | None],
) -> list[str]:
    """
    Defines the linkage variables that will have partial agreement state.

    Args:
      cutpoints:
        A dictionary with keys consisting of the linkage variable names and
        values consisting of the string comparison cutpoints for those
        variables.

    Returns:
      partial_agreement_vars:
        A list containing the names of the linkage variables that will have
        partial agreement state.
    """
    partial_agreement_cutpoints = {
        key: value for key, value in cutpoints.items() if value is not None
    }
    partial_agreement_vars = list(partial_agreement_cutpoints.keys())
    return partial_agreement_vars


def format_cutpoints(
    linkage_vars: list[str], configs: cp.ConfigParser
) -> dict[str, float]:
    """
    Formats the string comparison cutpoints to be used for each linkage variable.

    Args:
      linkage_vars:
        The names of the linkage variables.
      configs:
        The variable containing the imported config section.

    Dependencies:
      collections
      configparser as cp

    Returns:
      cutpoints_formatted:
        An ordered dictionary with keys consisting of the linkage variable
        names and values consisting of the string comparison cutpoints for
        those variables.
    """
    cutpoints_formatted = collections.OrderedDict()
    for i in linkage_vars:
        if configs[i + "_cutpoints"] == "None":
            cutpoints_formatted.update({i: None})
        else:
            cutpoints_formatted.update(
                {i: [float(x) for x in configs[i + "_cutpoints"].split(", ")]}
            )
    return cutpoints_formatted


def get_input_variables(config_path: str) -> dict[str, Any]:
    """
    Takes the filepath to a config file. From this, returns a dictionary
    containing all of the input variables required for Scalelink.

    Args:
      config_path:
        The filepath for the config file. The default is the location of the
        config.ini file in this package.

    Dependencies:
      configparser as cp

    Returns:
      input_variables:
        A dictionary containing the other input variables required Scalelink.
        The keys are the name of the input variables and the values are the
        variables themselves, i.e.:
          spark_session_size (str):
            The specified Spark session size for this run. Can be 's', 'm', 'l'
            or 'xl'.
          bucket_name (str):
            The name of the S3 bucket where the various filepaths can be found.
            Must not include the "s://" prefix.
          ssl_file (str):
            The path, including file name and extension, for the SSL Certificate
            to be used by the Boto3 client.
          df1_path (str):
            The filepath for df1, excluding the S3 bucket name.
          df2_path (str):
            The filepath for df2, excluding the S3 bucket name.
          df_candidates_path (str):
            The filepath for the dataset of candidate pairs, excluding the S3
            bucket name.
            Only use if a specific set of candidate pairs (e.g. from blocking) is
            to be used instead of getting candidate pairs by Cartesian join of
            df1 and df2.
          checkpoint_path (str):
            The filepath where checkpoints will be written, excluding the S3
            bucket name.
          output_path (str):
            The filepath where the linked dataset will be written, excluding the
            S3 bucket name.
          df1_id (str):
            The name of the ID variable in df1.
          df2_id (str):
            The name of the ID variable in df2.
          linkage_vars (list of str):
            A list containing the names of the linkage variables, excluding their
            suffixes.
          df1_suffix (str):
            The suffix given to all linkage variables in df1.
          df2_suffix (str):
            The suffix given to all linkage variables in df2.
          cutpoints (dict of str: float):
            A ordered dictionary with keys consisting of the linkage variable
            names and values consisting of the string comparison cutpoints for
            those variables.
          binary_agreement_vars (list of str):
            A list containing the names of the linkage variables that will have
            binary agreement state, excluding their suffixes.
          partial_agreement_vars (list of str):
            A list containing the names of the linkage variables that will have
            partial agreement state, excluding their suffixes.
          p (int):
            The total number of linkage variables.
          kj (dict of str: int):
            A dictionary with keys consisting of the linkage variable names and
            values consisting of kj (number of agreement states) for each linkage
            variable.
          K (int):
            The total number of agreement states across all linkage variables.
          s (int):
            The total number of candidate pairs entering the scaling algorithm.
    """
    configs = read_configs(config_path=config_path)

    run_spec_configs = configs["run_spec"]
    filepath_configs = configs["filepaths"]
    variable_configs = configs["variables"]
    cutpoint_configs = configs["cutpoints"]

    spark_session_size = run_spec_configs["spark_session_size"]

    bucket_name = filepath_configs["bucket_name"]

    ssl_file = filepath_configs["ssl_file"]

    df1_path = filepath_configs["df1_path"]

    df2_path = filepath_configs["df2_path"]

    df_candidates_path = filepath_configs["df_candidates_path"]

    checkpoint_path = filepath_configs["checkpoint_path"]

    output_path = filepath_configs["output_path"]

    df1_id = variable_configs["df1_id"]

    df2_id = variable_configs["df2_id"]

    linkage_vars = variable_configs["linkage_vars"].split(", ")

    df1_suffix = variable_configs["df1_suffix"]

    df2_suffix = variable_configs["df2_suffix"]

    cutpoints = format_cutpoints(linkage_vars=linkage_vars, configs=cutpoint_configs)

    binary_agreement_vars = define_binary_agreement_vars(cutpoints=cutpoints)

    partial_agreement_vars = define_partial_agreement_vars(cutpoints=cutpoints)

    p = define_p(linkage_vars=linkage_vars)

    kj = define_kj(cutpoints=cutpoints)

    K = define_K(kj=kj)

    input_variables = {
        "spark_session_size": spark_session_size,
        "bucket_name": bucket_name,
        "ssl_file": ssl_file,
        "df1_path": df1_path,
        "df2_path": df2_path,
        "df_candidates_path": df_candidates_path,
        "checkpoint_path": checkpoint_path,
        "output_path": output_path,
        "df1_id": df1_id,
        "df2_id": df2_id,
        "linkage_vars": linkage_vars,
        "df1_suffix": df1_suffix,
        "df2_suffix": df2_suffix,
        "cutpoints": cutpoints,
        "binary_agreement_vars": binary_agreement_vars,
        "partial_agreement_vars": partial_agreement_vars,
        "p": p,
        "kj": kj,
        "K": K,
    }

    return input_variables


def get_s(
    input_variables: dict[str, Any], df_cartesian_join: pyspark.sql.DataFrame
) -> dict[str, Any]:
    """
    Takes a dictionary of input variables and a dataframe consisting of the
    Cartesian join of the two dataframes to be linked. From this, calculates the
    Scalelink variable s and adds it to input_variables.

    Args:
      input_variables:
        A dictionary containing the other input variables required for the
        scaling algorithm. The keys are the name of the input variables and
        the values are the variables themselves. Produced by the utils function
        get_input_variables().
      df_cartesian_join:
        A Spark DataFrame consisting of the Cartesian join of the two dataframes
        to be linked.

    Returns:
      input_variables_with_s:
        A dictionary consisting of input_variables with an extra key:value pair.
        The key in this pair is 's' and the value is the Scalelink variable s,
        the total number of candidate pairs entering the scaling algorithm.
    """
    input_variables_with_s = input_variables
    input_variables_with_s["s"] = df_cartesian_join.count()

    return input_variables_with_s


def read_configs(config_path: str) -> cp.ConfigParser:
    """
    Reads in configs.

    Args:
      config_path:
        The filepath for the config file.

    Dependencies:
      configparser as cp

    Returns:
      configs:
        The configs from the config file.
    """
    configs = cp.ConfigParser()
    configs.read(config_path)
    return configs
