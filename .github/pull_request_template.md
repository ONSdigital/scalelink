# Request

This section must be completed by the person making the pull request.

## Description

Please **replace this text** with a summary of the changes, using the following prompts:

  - What has been changed?
  - Why was it changed?
  - How has the change been tested?
  - Does the change close or contribute to closing any issues? Use [GitHub keywords][keywords] and
    [cite][issue-citation] relevant issues.

## Type of change

Please check all items that are applicable:

- [ ] New feature - change that adds new documentation, functionality, etc.
- [ ] Refactor - change that alters existing files, but not to fix an error.
- [ ] Bugfix - change that alters existing files to fix an error.
- [ ] Breaking change - backwards incompatible change, changes expected behaviour, etc.
- [ ] Non-user-facing change e.g. structural change, CI/CD change, etc.

## Checklist:

Please check all items that have been completed. If any are not completed, provide an explanation in the description.

- [ ] **Assignee:** I have marked myself as the assignee of this pull request.
- [ ] **Code:** I have performed a self-review of my own code.
- [ ] **Tests: I have:**
  - [ ] Added tests that prove my fix is effective or that my feature works.
  - [ ] Checked that new and existing unit tests pass locally with my changes.
- [ ] **Documentation:** I have:
  - [ ] Made corresponding changes to the documentation (comments, docstring, etc.).
  - [ ] Commented my code appropriately, focusing on explaining my design decisions (explain why, not how).
- [ ] **Style:** I have followed the [contribution guide][contribution-guide].
- [ ] **Admin:** I have:
  - [ ] Updated the change log.
  - [ ] Labelled this pull request appropriately.

#  Review

This section must be completed by the person or persons reviewing the pull request.

## Checklist

Please check all items that have been completed:

-   [ ] **Reviewer**: I have marked myself as the reviewer on this pull request.
-   [ ] **Code**: I confirm that code changes:
    - Work as expected.
    - Are not overly complex.
    - Are appropriately documented.
    - Handle edge cases and exceptions.
-   [ ] **Tests**: I confirm that unit tests:
    - Are present where appropriate.
    - Cover essential functions for a reasonable range of inputs and conditions.
    - Pass locally (both new and existing tests).
-   [ ] **Documentation**: I confirm that documentation files:
    - Have been added/updated, as applicable.
    - Make sense.
    - Are not overly complex.
-   [ ] **Style**: I confirm that all changes conform to the [contribution guide][contribution-guide].

Where items in this checklist are not met, suggestions for improvement must be made using review comments.

## Review comments

Suggestions should be tailored to the code that you are reviewing. Provide context. Be critical and clear, but not mean.
Ask questions and set actions.

Suggestions may include:

-   **Documentation improvements**:
    - Improve clarity.
    - Minimise complexity.
    - Ensure all necessary information is covered accurately.
-   **Code improvements**:
    - Ensure correct functioning.
    - Minimise complexity.
    - Ensure all code is documented sufficiently.
    - Ensure edge cases and exceptions are handled.
-   **Test improvements**:
    - Ensure sufficient coverage of both functions and inputs/conditions.
    - Ensure all tests pass.
-   **Style improvements**:
    - Ensure the [contribution guide][contribution-guide] is followed.

[contribution-guide]: /scalelink/docs/contribution_guide.md
[issue-citation]: https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/autolinked-references-and-urls#issues-and-pull-requests
[keywords]: https://docs.github.com/en/get-started/writing-on-github/working-with-advanced-formatting/using-keywords-in-issues-and-pull-requests
