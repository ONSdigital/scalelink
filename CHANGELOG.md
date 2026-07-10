# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Indicator matrix functions and their unit tests.
- Utility functions and their unit tests.
- Matrix A* functions and their unit tests.
- Match score functions and their unit tests.
- New unit tests in:
  - `tests/indicator_matrix/test_indicator_matrix.py`: `test_get_deltas`.
  - `tests/match_scores/test_match_scores.py`: `test_get_match_scores`.
  - `tests/matrix_a_star/test_matrix_a_star.py`: `test_get_matrix_a_star`, `test_calculate_njklm_values`, `test_get_scaled_labelled_x_star`.
  - `tests/utils/test_utils.py`: `test_cartesian_join_dataframes`, `test_get_input_variables`.
  - `tests/utils/test_utils_create_spark_session.py`: `test_create_spark_session`.
- Run script, `main.py`, with updates to work on Spark 3.5.1.
- Configs template, with updates to reflect updated `main.py`.
- GitHub Action that creates releases from tags.
- Python 3.11 support.
- Type hints for all functions.
- GitHub Action workflow to deploy repo as package in PyPI.
- User Guide.

### Changed

- Dependabot updates including:
  - In GitHub Actions, bump actions/checkout from v4 to v6.
  - In GitHub Actions, bump actions/setup-python from v5 to v6.
  - In GitHub Actions, bump mathieudutour/github-tag-action from v6.1 to v6.2.
  - Unpin `pytest` version.
  - Change `pyspark` version requirements (specify as requiring >=3.0.0, <4.0.0).
- Function `create_spark_session` in `scalelink/utils/utils.py`, to make it less verbose.
- Code contribution guidelines, to clarify that we are only accepting contributions from ONSdigital users currently.
- Dependabot config, so that version updates are targeted to `develop` not `main`.
- Unit tests:
  - `test_create_spark_session` in `tests/utils/test_utils_create_spark_session.py`.
  - `test_get_input_variables` in `tests/utils/test_utils.py`.
  - Additional assert statements in unit tests of parent functions.
- `README.md`:
  - Added installation and use, contact, dedication and icons.
  - Edited pre-requisites and acknowledgements.
- Branch and Deploy Guide:
  - Updated description of GitHub Actions.
  - Updated deployment instructions.
- Build method, from using `setuptools` with `setup.py`, `setup.cfg` and `requirements.txt` to using `hatchling` with `pyproject.toml`.
- Location of config template, so it is included in build.
- Minor phrasing changes to User Guide (`docs/user_guide.md`) and pull request template (`.github/pull_request_template.md`).
- Changed all third-party reusable workflows in GitHub Actions workflows to pin to latest release's long commit hash.

### Deprecated

### Fixed

- GitHub Action that increments release version - fixed typos.
- Unit tests:
  - `test_cartesian_join_dataframes` in `tests/utils/test_utils.py`.
  - `test_get_s` in `tests/utils/test_utils.py`.
- Formatting of all docstrings.

### Removed

- Python 3.8 and 3.9 support.
- Use of `bump2version`, as it's no longer supported.
- Use of `hotfix` branches by this project. Updated Branch and Deploy Guide and Pull Request GitHub Action workflow to reflect this.

## [0.1.1] 2025-07-18

### Added

- Config for bump2version: `.bumpversion.cfg`.

### Changed

- GitHub Action that increments release version, fixing typo.

### Deprecated

### Fixed

### Removed

## [0.1.0] 2025-07-10

### Added

- The `scalelink` and `tests` folders.
- Various helper files:
  - Git: `.gitignore`, `pull_request_template.md`.
  - Packaging: `pyproject.toml`, `setup.cfg`, `setup.py`.
  - CI/CD: `.pre-commit-config.yaml`, `dependabot.yml`, `pull_request_workflow.yaml`,
    `increment_version_workflow.yaml`.
- Various documentation files:
  - Basic information: `README.md`, `CHANGELOG.md`.
  - Authors: `CODEOWNERS`, `CONTRIBUTING.md`.
  - Guidance: `branch_and_deploy_guide.md`.
- Various Dependabot updates, primarily to ensure package versions, GitHub Actions etc.
  are up-to-date.

### Changed

### Deprecated

### Fixed

### Removed
