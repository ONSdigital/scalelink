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
  - [Conventional branch][branches] naming conventions:
    - `feat/<feature-description>` - feature branches, for introducing new features.
    - `fix/<bug-description>` - bugfixes, for resolving bugs.
    - `hotfix/<issue-description>` - hotfixes, for urgent fixes that go straight to production.
    - `release/<release-number>` - for preparing a release.
    - `chore/<chore-description>` - for non-code tasks, e.g. dependency or documentation updates.
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
   - Before merging `develop` into `main`, manually update the package version in `pyproject.toml` following [semantic versioning principles][sem-ver].
   - Remember to update the changelog.

4. **Merging to main:**
   - After a set of features is finalised in the `develop` branch and the package version is bumped, merge `develop`
     into `main`.
   - This action triggers the automated deployment process through GitHub Actions.

5. **Post-merge update:**
   - After merging into `main`, update the `develop` branch with the latest `main` branch changes using `git pull`.
     This ensures the `develop` branch is aligned with production.

## Pull request process using GitHub Actions

### Overview

Certain [GitHub Actions][github-actions] are triggered on merging to any branch. This CI/CD pipeline ensures code does not enter
any parent branches unless it has had certain checks.

### Pull request workflow steps

1. **Trigger:**
   - When a `merge` is detected.

2. **Check branch:**
   - Check the base branch for the pull request.
   - If the base branch is `main`, check if the branch is `develop` or has a name starting with `hotfix`.

3. **Changelog:**
   - Check that `CHANGELOG.md` has been updated.

4. **Pre-commit:**
   - Run all pre-commit hooks.

5. **Test:**
   - Run all unit tests on all versions of Python supported by the repo.

## Deployment process using GitHub Actions

### Overview

The deployment process is automated using [GitHub Actions][github-actions]. This CI/CD pipeline ensures code does not enter `main`
without the version being incremented and a release being published on PyPI.

### Increment version and deploy workflow steps

1. **Trigger:**
   - When a `push` to `main` is detected.

2. **Extract repo version:**
   - Extract the version of the repo from the location specified in `setup.py`.

3. **Push version tag:**
   - Push a new tag containing the repo version.
   
4. **Create GitHub release:**
   - Create a new GitHub release using the new tag and changelog.

5. **Build and verify package:**
   - Use `hynek/build-and-inspect-python-package` to:
      - Build the package.
      - Upload the built wheel and the source distribution as GitHub Actions artifacts.
      - Lint the wheel contents using `check-wheel-contents`.
      - Lint the PyPI README using `Twine` and upload it as a GitHub Actions artifact.
      - Print the tree of both SDist and `wheel`, allowing manual checking of the content list.
      - Print and upload the packaging metadata as a GitHub Actions artifact.

6. **Download built package:**
   - Download the built package from GitHub Actions artifacts to `dist/`.

7. **Upload package to PyPI:**
   - Upload the package from `dist` to PyPI, using Trusted Publishing.

## Merging develop to main: A guide for maintainers

As `scalelink` maintainers, ensuring a seamless transition from `develop` to `main` branch is essential. This process extends beyond mere code managing: it encompasses careful preparation, version management and detailed documentation to preserve the codebase's integrity and reliability. Below is a straightforward guide on the procedure.

### Preparation

1. **Initiate merge request:**
   - Navigate to the GitHub repository's page and access the "Pull Requests" section.
   - Click on "New Pull Request" to start the merging process. Select the `develop` branch as the source and the `main` branch as the target.
   - Title the merge request with a relevant name that succinctly describes the set of features, fixes or improvements being merged. Example: "Release 1.2.0: Feature Enhancements and Bug Fixes".
   - Add a suitable description, using the Pull Request Template.
  
### Carry out test build

1. **Build and lint the package locally:**
   - Ensure the `build` dependencies are installed, by opening the terminal and running: `pip install .[build]`.
   - Change directory to the repo by running: `cd scalelink`.
   - Build the package locally by running: `python -m build`.
   - Lint the built wheel using [`check-wheel-contents`][check-wheel-contents] by running: `check-wheel-contents dist/<wheel filename>`.
      - If this returns 'OK', the wheel has passed all checks and you can continue.
      - Else, a message will be printed for each check that has failed (plus, if applicable, a list of filepaths that caused the failure). In this instance, backtrack and carry out the necessary bugfixes until this passes.
   - Lint the PyPI README using [`twine`][twine] by running: `twine check dist/*`.
      - If this returns 'PASSED' for both the `.whl` and `.tar.gz` files in `dist/`, you can continue.
      - Else, backtrack and bugfix the README until this passes.

2. **Upload the test build to Test PyPI:**
   - Upload by running: `twine upload -r testpypi dist/*`.
   - When prompted, input your Test PyPI API token.
   
3. **Check the package styling:**
   - Check the uploaded package on Test PyPI by following the link provided in the terminal.
   - Review the styling of the information from `README.md`. Make a note of any changes that need to be implemented prior to uploading to PyPI.

3. **Download from Test PyPI and test:**
   - In your local environment, download the test build from Test PyPI by running: `pip install -i https://test.pypi.org/simple/ scalelink==<version>`.
   - Test that the package runs correctly using a script containing the following:
   
    ```python
    from scalelink import run_scalelink
    output = run_scalelink(config_path = "<filepath/to/config/file>")
    ```

   - Again, make a note of any changes that need to be implemented prior to uploading to PyPI.

5. **Fix build issues:**
   - If there are any build issues, make a new feature branch and address them.
   - Once this feature branch is QA'd and merged to `develop`, repeat the [Carry out test build](#carry-out-test-build) instructions until no build issues remain.
   - Only once no build issues remain can you move on to the next section.

### Review and approval

These steps must be carried out by someone other than the pull request initiator.

1. **Review changes:**
   - Utilise GitHub's User Interface (UI) to review the changes introduced. This is critical for spotting any potential issues before they make it into `main` branch.
   - Cross-reference the changes against the `CHANGELOG.md` file to ensure all updates, fixes and new features are properly documented.
   - Ensure all checks via GitHub Actions pass.

2. **Approve changes:**
   - Once satisfied with the review, click on the "Review changes" button in GitHub and select "Approve" from the options. This indicates that the changes have been reviewed and are considered ready for merging. If you're reviewing multiple files, click on the "Viewed" checkbox for each file as you review them. This helps manage and streamline the review process by marking files that have already been checked.

### Version management and documentation

1. **Bump version:**
   - Before merging, it's essential to update the package version.
   - Check out and pull the `develop` branch to your local environment.
   - Manually update the package version in `pyproject.toml` following [semantic versioning principles][sem-ver].

2. **Update `CHANGELOG.md`:**
   - Continue to work in the `develop` branch.
   - In the `CHANGELOG.md` file, create a new header/section for the newly bumped version.
   - Move all entries from the "Unreleased" section to the new version section. This action effectively transfers the documentation of changes from being pending release to being part of the new version's official changelog.
   - Ensure the "Unreleased" section is left empty after this process, ready for documenting future changes.
   - Update the "Release links" section at the bottom of the document. Add links to the new version's GitHub Release page and its PyPi listing, following the existing format. **Note: this repo does not currently have a PyPi listing.**
     This step ensures users and developers can easily find and access the specific versions of `scalelink` through their respective release pages and download links, maintaining comprehensive and navigable documentation.
   - Commit and push all changes to the remote `develop` branch.

3. **Final review:**
   - Arrange for the reviewer to review the changes one more time, ensuring that the version bump and `CHANGELOG.md` updates are correctly applied.

### Merging and deployment

1. **Merge to main:**
   - With all preparations complete and changes reviewed, proceed to merge the `develop` branch into the `main` branch.
   - This action can be done through the GitHub UI by completing the pull request initiated in the Preparation section of this guide.
   - Merging to `main` automatically triggers the GitHub Actions workflow for deployment.
  
### Synchronising develop branch post-merge

After the pull request from `develop` to `main` has merged, it is crucial to synchronise the `develop` branch with the changes in `main`. Perform the following steps in your local environment to ensure that `develop` stays up-to-date:

1. **Switch to `develop` branch:**
   - Use `git checkout develop` to switch from your current feature branch to the `develop` branch.
 
2. **Merge `main` into `develop`:**
   - Run `git merge main` whilst on the `develop` branch to merge the changes from the `main` branch into `develop`.
 
3. **Push updated `develop`:**
   - After merging, push the updated `develop` branch back to the remote repository using `git push origin develop`.

By adhering to these steps, you'll make the transition from development to production smooth and efficient, ensuring the codebase remains stable and the release process flows seamlessly. As maintainers, your pivotal role guarantees the
`scalelink` package's reliability and efficiency for all users.

## Post-merge feature branch synchronisation: All developers

1. **Pull changes from `main`:**
   - Ensure your feature branch is checked out, using `git checkout <my-feature-branch>`.
   - Execute `git pull origin main` to fetch and merge the latest changes from the `main` branch to your current feature branch.
   - If you are currently working on more than one feature branch, use `git checkout <my-feature-branch>` to switch to your next feature branch. Then, execute `git pull origin main` to fetch and merge the latest changes from the `main` 
     branch to it. Repeat this until all of your current feature branches have been updated.

## Git workflow diagram

Below is a visual representation of our Git workflow, illustrating the process from feature development through to deployment.

```mermaid
graph TD
    Start1([Start or continue feature development or bugfix])
    Start2([Start or continue hotfix])
    
    Feat1[Create feature branch from develop branch]
    Feat2[Develop feature or bugfix in feature branch]
    Feat3{Feature branch: complete and tested?}
    Feat4[Raise pull request to merge feature branch into develop branch]
    Feat5[Trigger automated checks via GitHub Actions]
    Feat6[Review pull request]
    Feat7{Feature branch: approve pull request?}
    Feat8[Merge pull request]
    
    Dev1{Develop branch: Ready for release?}
    Dev2[Update package version - semver major or minor update]
    Dev3[Raise pull request to merge develop branch into main branch]
    Dev4[Trigger automated checks via GitHub Actions]
    Dev5[Review pull request]
    Dev6{Develop branch: approve pull request?}
    Dev7[Merge pull request]
    
    Deploy1[Trigger automated deployment via GitHub Actions]
    Deploy2[Create GitHub Release with version tag]
    Deploy3[Update develop branch with main]
    Deploy4[Build and test scalelink package]
    Deploy5[Publish to PyPI]
    
    Hotfix1[Create hotfix branch from main branch]
    Hotfix2[Develop hotfix in hotfix branch]
    Hotfix3{Hotfix branch: complete and tested?}
    Hotfix4[Raise pull request to merge hotfix branch into main branch]
    Hotfix5[Trigger automated checks via GitHub Actions]
    Hotfix6[Review pull request]
    Hotfix7{Hotfix branch: approve pull request?}
    Hotfix8[Update package version - semver patch update]
    Hotfix9[Merge pull request]

    Start1 --> Feat1
    Start2 --> Hotfix1

    Feat1 --> Feat2
    Feat2 --> Feat3
    Feat3 -- No --> Feat2
    Feat3 -- Yes --> Feat4
    Feat4 --> Feat5
    Feat5 --> Feat6
    Feat6 --> Feat7
    Feat7 -- No --> Feat2
    Feat7 -- Yes ---> Feat8
    Feat8 --> Dev1

    Dev1 -- No --> Start1
    Dev1 -- Yes --> Dev2
    Dev2 --> Dev3
    Dev3 --> Dev4
    Dev4 --> Dev5
    Dev5 --> Dev6
    Dev6 -- No --> Start1
    Dev6 -- Yes ---> Dev7
    Dev7 --> Deploy1
    
    Deploy1 --> Deploy2
    Deploy2 --> Deploy3
    Deploy3 --> Deploy4
    Deploy4 --> Deploy5
    Deploy5 ------------> Start1
    
    Hotfix1 --> Hotfix2
    Hotfix2 --> Hotfix3
    Hotfix3 -- No --> Hotfix2
    Hotfix3 -- Yes --> Hotfix4
    Hotfix4 --> Hotfix5
    Hotfix5 --> Hotfix6
    Hotfix6 --> Hotfix7
    Hotfix7 -- No --> Hotfix2
    Hotfix7 -- Yes --> Hotfix8
    Hotfix8 --> Hotfix9
    Hotfix9 ----------> Deploy1
```

[branches]: https://conventional-branch.github.io/
[check-wheel-contents]: https://pypi.org/project/check-wheel-contents/
[commits]: https://www.markdownguide.org/basic-syntax/#links
[github-actions]: https://github.com/features/actions
[sem-ver]: https://semver.org/
[twine]: https://twine.readthedocs.io/en/stable/
