# User guide

## Pre-requisites

See [README][readme].

## Installation

`scalelink` is available for installation via [PyPI][pypi] and can also be found on [GitHub Releases][github-releases] for direct downloads and version history.

To install via `pip`, run: `pip install scalelink`

## Use

To link two datasets together using `scalelink`, simply run a `.py` script containing the following lines:

```python
from scalelink import run_scalelink

run_scalelink(config_path = "<absolute/path/to/config/file.ini")
```

This requires an appropriately set-up config file.

### Configs

The config file for `scalelink` is a `.ini` file. You can name it whatever you choose and store it wherever you choose, provided that you reference that file name and
path in the `config_path` argument when using the `run_scalelink` function. A [config file template][config-template] is provided to guide you in creating your config
file, in addition to the below instructions.

The config file must contain four sections:

 - `run_spec`
 - `filepaths`
 - `variables`
 - `cutpoints`

The configs must be inputted as follows:

| Section   | Config             | Required? | Contents                                                                                                                                                                                                                                                                                                                               |
| --------- | ------------------ | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| run_spec  | spark_session_size | Y         | `s`, `m`, `l` or `xl`, depending on the Spark session size you want. Full Spark configs for each size are detailed in the [Spark Session Sizes](#spark-session-sizes) subsection.                                                                                                                                                      |
| filepaths | bucket_name        | Y         | The name of your AWS S3 bucket, excluding the `s3a://` prefix. All configs ending `_path` will be concatenated onto this to create full filepaths.                                                                                                                                                                                     |
| filepaths | ssl_file           | Y         | The relative filepath to your local SSL Certificate file, ending `.crt`. For `raz_client`, to permit use of `boto3`.                                                                                                                                                                                                                   |
| filepaths | df1_path           | N*        | The relative filepath to the first dataset you wish to link. This must be a parquet file in your AWS S3 bucket.                                                                                                                                                                                                                        |
| filepaths | df2_path           | N*        | The relative filepath to the second dataset you wish to link. This must be a parquet file in your AWS S3 bucket.                                                                                                                                                                                                                       |
| filepaths | df_candidates_path | N*        | The relative filepath to the Cartesian product of the two datasets you wish to link. This must be a parquet file in your AWS S3 bucket.                                                                                                                                                                                                |
| filepaths | checkpoint_path    | Y         | The relative filepath to the folder you wish checkpoint files to be saved to during the linkage. This must be a location within your AWS S3 bucket. It **must be a unique location** as `run_scalelink` will delete it once the run is finished.                                                                                       |
| filepaths | output_path        | Y         | The relative filepath to the folder you wish the linked dataset to be saved to. This must be a location within your AWS S3 bucket.                                                                                                                                                                                                     |
| variables | df1_id             | Y         | The full name of the column in `df1` that contains the row ID.                                                                                                                                                                                                                                                                         |
| variables | df2_id             | Y         | The full name of the column in `df2` that contains the row ID.                                                                                                                                                                                                                                                                         |
| variables | linkage_vars       | Y         | A comma-separated string of the variables to use for linkage. These must have identical names in both datasets, but different prefixes. In this list, the prefixes must not be present. See [Configs Example](#configs-example).                                                                                                       |
| variables | df1_suffix         | Y         | The suffix present on all linkage variables in `df1`.                                                                                                                                                                                                                                                                                  |
| variables | df2_suffix         | Y         | The suffix present on all linkage variables in `df2`.                                                                                                                                                                                                                                                                                  |
| cutpoints | \<see contents\>   | Y         | There must be the same number of configs in this section as there are linkage variables. Each must be named `<linkage_var>_cutpoints`. If the linkage variable has cutpoints, these must be entered smallest to largest as a comma-separated string. If the linkage variable does not have cutpoints, the config value must be `None`. |

\* You can either use `df1_path` and `df2_path` to supply the two datasets to be linked or you can supply these datasets already joined using `df_candidates_path`. If you
do the former, `run_scalelink` will Cartesian-join your datasets before calculating Scalelink match scores. If you do the latter, you can provide an alternative, e.g. a
blocked set of candidate pairs. However, note that it is currently unknown how blocking interacts with the Scalelink method.

### Configs example

Imagine you had an AWS S3 bucket named `s3a://my-aws-s3-bucket`. Inside this bucket are two parquet files at `/my_project/dataframe1` and `/my_project/dataframe2`.
You decide to let the `run_scalelink` use these directly, rather than joining them yourself (e.g. using blocking criteria) and running Scalelink on the candidate pairs you
have produced.

Additionally, there is a folder called `/checkpoints` where you want checkpoint files to be stored and a folder called `/outputs` where you want your linked dataset
to be saved. Locally, your SSL Certificate is located at `/local/folder/containing/cert_file.crt`.

Within `dataframe1` there is a row ID called `id1` and within `dataframe2` there is a row ID called `id2`. You want to link on forename, surname, sex and postcode plus day,
month and year of birth. To do this, you've formatted these linkage variables appropriately: in `dataframe1` they are called `forename_df1`, `surname_df1`, `sex_df1`,
`postcode_df1`, `dob_d_df1`, `dob_m_df1` and `dob_y_df1` whereas in `dataframe2` they are called `forename_df2`, `surname_df2`, `sex_df2`, `postcode_df2`, `dob_d_df2`,
`dob_m_df2` and `dob_y_df2`.

Forename, surname and postcode are all string columns and you wish to use various cutpoints with them when they are compared using the Sorensen-Dice coefficient. The other
columns (including `sex`) are numeric and do not have cutpoints.

You've taken a look at the [Spark Session Sizes](#spark-session-sizes) and think a medium session is best to try first.

In this circumstance, you would create a config file that contains the following lines:

```ini
[run_spec]
spark_session_size = m

[filepaths]
bucket_name = my-aws-s3-bucket
ssl_file = /relative/path/to/local/cert_file.crt
df1_path = /my_project/dataframe1
df2_path = /my_project/dataframe2
df_candidates_path =
checkpoint_path = /checkpoints
output_path = /outputs

[variables]
df1_id = id1
df2_id = id2
linkage_vars = forename, surname, sex, postcode, dob_d, dob_m, dob_y
df1_suffix = _df1
df2_suffix = _df2

[cutpoints]
forename_cutpoints = 0.8, 1.0
surname_cutpoints = 0.8, 1.0
sex_cutpoints = None
postcode_cutpoints = 0.85
dob_d_cutpoints = None
dob_m_cutpoints = None
dob_y_cutpoints = None
```

### Spark Session Sizes

The `run_scalelink` function comes with four sizes of Spark session pre-coded: `s`, `m`, `l` and `xl`. The Spark configs for each of these session sizes are as follows:

| Config | Small | Medium | Large | Extra-large |
| ------ | ----- | ------ | ----- | ----------- |
| `spark.executor.memory` | 1g | 6g | 10g | 20g |
| `spark.yarn.executor.memoryOverhead` | 1g | 1g | 1g | 2g |
| `spark.executor.cores` | 1 | 3 | 5 | 5 |
| `spark.dynamicAllocation.maxExecutors` | 3 | 3 | 5 | 12 |
| `spark.sql.shuffle.partitions` | 12 | 18 | 200 | 240 |
| `spark.shuffle.service.enabled` | False | False | False | False |
| `spark.ui.showConsoleProgress` | False | False | False | False |

## Assumptions

This implementation of Scalelink is experimental and does not yet have functionality to permit use in a variety of environments and scenarios. Currently, the following
assumptions are made:

- Your files are stored in a single AWS S3 bucket and can be manipulated using `boto3`.
- You have a local SSL Certificate and you know the relative filepath to it.
- Your input datasets are saved as parquet files in a location within your AWS S3 bucket.
- Your linkage variables are numeric or string. Linkage variables that are any other type (including date, timestamp or boolean) are not currently permitted.
  - All numeric linkage variables are compared on a binary basis - i.e. are they exactly the same or not?
  - All string linkage variables are compared using the Sorensen-Dice coefficient only. Other string comparison methods are not currently available.
- Your categorical linkage variables (e.g. `sex`) have been label-encoded to turn them into numeric variables.
- Your linkage variables do not contain any missingness (either as `None` or as a place-holder, e.g. a string consisting of `"None"`, `"N/A"`, etc.).
- You want your linked dataset to be outputted as a parquet file saved to a location within your AWS S3 bucket.

[config-template]: /scalelink/configs_template.ini
[github-releases]: https://github.com/ONSdigital/scalelink/releases
[pypi]: https://pypi.org/project/scalelink/
[readme]: /README.md
