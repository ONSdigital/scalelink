# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Indicator matrix functions and unit tests.
- Utility functions and their unit tests.
- Matrix A* functions and their unit tests.
- Match score functions and their unit tests.
- Unit test for `get_s` in `tests/utils/utils.py`

### Changed

- GitHub Action that increments release version, fixing typos.

### Deprecated

### Fixed

### Removed

## [0.1.1] 2025-07-18

### Added

- Config for bump2version: `.bumpversion.cfg`

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
