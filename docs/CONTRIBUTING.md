# Contribution guide

We welcome contributions to the `scalelink` repository. To contribute, please follow these guidelines.

## Submitting changes

All pull requests should be made to the `develop` branch, excepting hotfixes.

Before submitting a pull request, please ensure that:

- All code follows [house style](#house-style).
- All new code has unit tests included, wherever possible.
- All existing unit tests, plus any new ones, pass with your code changes.
- All pre-commit hooks have been run successfully on the most recent version of your branch.

Please also:

- If you are making a significant change to the codebase, update the documentation to reflect the changes.
- If you are adding new functionality, provide examples of how to use it in the project's documentation or in
  a separate `README` file.
- If you are fixing a bug, include a description of the bug and how your changes address it.
- If you are adding a new dependency, please include a brief explanation of why it is necessary and what it does.
- If you are making significant changes to the project's architecture or design, please discuss your ideas with
  the project maintainers first to ensure they align with the project's goals and vision.

When submitting a pull request, please ensure that the pull request template is used and completed.

> **Note:** pull requests will not be merged without first passing peer review.

## House style

When writing code in `scalelink`, please follow these guidelines to ensure clarity, consistency and ease 
of use for others.

### Git guidance

- Use [GitFlow workflow][gitflow].
- Follow naming conventions for branches:
  - `docs/<documentation-description>` - for updates to documentation only.
  - `feat/<feature-description>` - feature branches, for introducing new features.
  - `fix/<bug-description>` - bugfixes, for resolving bugs.
  - `hotfix/<issue-description>` - hotfixes, for urgent fixes that go straight to production.
- Use [conventional commits][commits] when writing commit messages, including the following types:
  - `build` - for changes that affect the build system or external dependencies.
  - `ci` - for changes to CI configuration files and scripts, e.g. GitHub Actions, Dependabot.
  - `docs` - for documentation-only changes.
  - `feat` - for new features.
  - `fix` - for bugfixes and hotfixes.
  - `perf` - for changes that improve performance only.
  - `refactor` - for code changes that neither add a feature, fix a bug nor improve performance.
  - `style` - for changes that do not affect code meaning (e.g. removing whitespace, standardising quote type).
  - `test` - for changes that add missing tests or correct existing tests.

### General code guidance

- Adhere to the [PEP 8][pep8] style guide for Python code.
- Do not use wildcard imports.
- Import `pyspark.sql.functions` as `F`.
- Import `pyspark.sql.types` as `T`.
- Alias other imports in lowercase only.
- Provide well-documented, easy-to-understand code, including clear variable and function names as well as
  explanatory comments where necessary.
- Keep lists of package dependencies in [`pyproject.toml`][pyproject] up-to-date, splitting depedencies into those required
  by all users versus developers.
- Maintain unit test coverage of >=80%.
- Use linting and formatting:
  - Lint all code with `ruff` and format all code with `black`.
  - This is most easily done by installing `pre-commit` and using the [repo's pre-commit configs][pre-commit].

### Functions

- Use type hints in the function signature to clearly indicate input and output types. Avoid repeating type
  information in the docstring - this reduces duplication and keeps documentation clean.
- Follow the [Google Python Style Guide][style-guide] format for docstrings, with clearly defined `Args` and
  `Returns` or `Yields` sections, plus `Raises` and `Dependencies` sections as necessary.
- Use clear, descriptive and self-explanatory function, argument and variable names - avoid overly abbreviated
  or cryptic names.
- If a function is complex, split it into smaller helper functions where possible. Ensure that each part has a
  clear purpose.
- Keep explanatory comments focused and purposeful. Comment on the **why** rather than the **what** and only
  when the code's intent isn't obvious. Where possible, use meaningful function, argument and variable names to
  avoid the need for comments.
- Ensure that the function is readable, maintainable and easy for other to understand. Readability is preferred
  over overly clever one-liners.

> **Note:** If your function raises any exceptions, make sure to include a `Raises` section in the docstring
> describing the exception type and when it is triggered.

### Function checklist

| Requirement | Check before submitting |
| --- | --- |
| Type hints uses in function signature | Clearly specify input and output types; no duplication in docstring. |
| Docstring format | Follow Google Python Style Guide format with `Args` and `Returns` or `Yields`. |
| Function, argument and variable names are descriptive | Avoid overly abbreviated or cryptic names. |
| Complex logic is broken into helper functions | Keep functions focused and small where possible. |
| Comments explain **why** not **what** | Only use comments where intent isn't obvious even with good naming. |
| Code prioritises readability | Prefer clear structure over clever one-liners. |

## Unit tests

- Use `pytest`.
- Use the arrange, act, assert, (tidy up) format.
- Name tests using the `test_` prefix that are clearly inherited from the function (e.g. `test_deduplicate_sample`
  to test the function `deduplicate_sample`).
- Include a one-line descriptive docstring per test.
- Write separate tests for each scenario using `@pytest.mark.parametrize`. Use the `ids` argument to explain each
  scenario being tested. Longer explanations are fine if needed.
- Use fixtures where suitable to reduce duplication for similar scenarios.
- Use assertions that clearly show expected versus actual results.
- Aim for full test coverage, including edge cases, to ensure robustness and handle unexpected inputs.

### Unit test checklist

| Requirement | Check before submitting |
| --- | --- |
| House format used | `pytest` used; test has clear arrange, act, assert and (if applicable) tidy up steps. |
| Clear test naming | `test_` prefix is used; name clearly relates to the function being tested. |
| Test functions are clear and focused | One scenario per set of parametrized values; scenario clearly described using `ids` argument. |
| Suitable fixtures | Used to reduce duplication. |
| Assertions are clear and meaningful | Show expected versus actual results. |
| Edge cases are covered | Aim for full coverage, not just testing low-hanging fruit. |

[gitflow]: https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow
[commits]: https://www.markdownguide.org/basic-syntax/#links
[pre-commit]: .pre-commit-config.yaml
[pep8]: https://peps.python.org/pep-0008/
[pyproject]: pyproject.toml
[style-guide]: https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings
