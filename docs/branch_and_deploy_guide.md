# Branching and deployment guide

## Overview

Our branching strategy is designed to support Continuous Integration and Continuous Deployment (CI/CD),
ensuring smooth transitions between development, testing and production.

This framework aims to maintain a stable codebase and streamline our workflow and collaboration, making
it easier to integrate new features, fix bugs and release updates promptly.
It does this by separating in-progress work from production-ready content and using [semantic versioning][sem-ver]
to provide clarity regarding update content.

## Branches

Our repository has two permanent branches:

- **`main`** - stable codebase reflecting the current production state. Only pull requests from `develop` or
  `hotfix` branches are accepted.
- **`develop`** - active development branch containing new features, bug fixes and improvements. All feature
  branch pull requests, except `hotfix` branches, should be made here.

## Development workflow

1. **Feature branches:**
  - All new features and bugfixes are developed in separate branches created from the `develop` branch.
  - Any hotfixes are developed in separate branches created from the `main` branch.
  - Branch naming conventions:
    -`docs/<documentation-description>` - for updates to documentation only.
    - `feat/<feature-description>` - feature branches, for introducing new features.
    - `fix/<bug-description>` - bugfixes, for resolving bugs.
    - `hotfix/<issue-description>` - hotfixes, for urgent fixes that go straight to production.
  - [Conventional commit][commits] messages, including the following types:
    - `build` - for changes that affect the build system or external dependencies.
    - `ci` - for changes to CI configuration files and scripts, e.g. GitHub Actions, Dependabot.
    - `docs` - for documentation-only changes.
    - `feat` - for new features.
    - `fix` - for bugfixes and hotfixes.
    - `perf` - for changes that improve performance only.
    - `refactor` - for code changes that neither add a feature, fix a bug nor improve performance.
    - `style` - for changes that do not affect code meaning (e.g. removing whitespace, standardising quote type).
    - `test` - for changes that add missing tests or correct existing tests.

2. **Merging to development:**
   - Once a feature is complete and tested, it is merged into the `develop` branch via a pull request.
   - Pull requests must undergo peer review.
   - Approval for the most recent commit on the branch must be given by the peer reviewer prior to merge.
   - Remember to update the changelog.

3. **Version bumping:**
   - Before merging `develop` into `main`, update the package version following [semantic versioning principles][sem-ver].
   - Use `bump2version` to bump the `scalelink` package version. E.g. `bump2version patch` for a patch update,
     `bump2version minor` for a minor update or `bump2version major` for a major update.
   - Rememver to update the changelog.

4. **Merging to main:**
   - After a set of features is finalised in the `develop` vranch and the package version is bumped, merge `develop`
     into `main`.
   - This action triggers the automated deployment process through GitHub Actions.

5. **Post-merge update:**
   - After merging into `main`, update the `develop` branch with the latest `main` branch changes using `git pull`.
     This ensures the `develop` branch is aligned with production.

## Pull request process using GitHub Actions

### Overview

[GitHub Actions][github-actions] are triggered on pull request from any branch, including feature branches. This CI/CD pipeline
ensures code does not enter the `develop` or `main` branches unless it has had certain checks. The repository is set up so
that pull requests cannot be merged if these GitHub Actions fail.

### Steps in the pull request pipeline

1. **Trigger:**
   - The pipeline is triggered when a `merge` is detected.

2. **Changelog check:**
   - The changelog is checked for updates.

3. **Pre-commit hooks:**
   - All pre-commit hooks are run.

4. **Unit tests:**
   - All unit tests are run.

## Deployment process using GitHub Actions

### Overview

The deployment process is automated using [GitHub Actions][github-actions]. This CI/CD pipeline is triggered upon merging
changes into the `main` branch.

### Steps in the deployment pipeline

1. **Trigger:**
   - The pipeline is triggered when a `merge` into `main` is detected.

2. **Increment version:**
   - The version of the package is incremented.
   - The new version tag is pushed.
  
Further deployment steps will be added in due course.

[commits]: https://www.markdownguide.org/basic-syntax/#links
[sem-ver]: https://semver.org/
[github-actions]: https://github.com/features/actions
