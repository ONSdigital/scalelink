## Description

<details><summary>Please include a summary of the changes. </summary>

  - What is this change?
  - Is this a bug fix or a feature and does it break any existing functionality?
  - How has it been tested?

</details>
 
**This pr introduces....**

## Type of change

- [ ] Bug fix.
- [ ] New feature.
- [ ] Breaking change - **backwards incompatible change**, changes expected behaviour.
- [ ] Non-user facing change - structural change, dev functionality, docs, etc.

## Checklist:

I have:

- [ ] Performed a self-review of my own code.
- [ ] Commented my code appropriately, focusing on explaining my design decisions (explain why, not how).
- [ ] Made corresponding changes to the documentation (comments, docstring, etc.).
- [ ] Added tests that prove my fix is effective or that my feature works.
- [ ] Checked that new and existing unit tests pass locally with my changes.
- [ ] Updated the change log.

##  Peer review

You should review all of the following:

- **Documentation**:
  - Are docstrings present?
  - Are comments only present where necessary and have they been added/updated?
- **Style guidelines**:
  - Do the new files conforms to the project's contribution guidelines?
- **Functionality**:
  - Does the code works as expected?
  - Does the code handle expected edge cases and exceptions appropriately?
- **Complexity**:
  - Is the code is not overly complex?
  - Has the logic been split into appropriately sized functions?
- **Test coverage**:
  - Are there unit tests for all essential functions for a reasonable range of inputs and conditions?
  - Do added and existing tests locally on the reviewer's machine?

### Review comments

Suggestions should be tailored to the code that you are reviewing. Provide context.
Be critical and clear, but not mean. Ask questions and set actions.

<details><summary>These might include:</summary>

- Bugs that need fixing:
  - Does the new code work as expected?
  - Does the new code work with other code that it is likely to interact with?
  
- Alternative methods:
  - Could the new code be written more efficiently?
  
- Documentation improvements:
  - Does the documentation reflect how the code actually works?
  
- Additional tests that should be implemented:
  - Do the tests effectively assure that the code works correctly?
  - Are there additional edge cases/negative tests to be considered?
  
- Code style improvements:
  - Could the code be written more clearly?
  - Does the code meet the project style guide?

</details>

**Further reading: [code review best practices](https://best-practice-and-impact.github.io/qa-of-code-guidance/peer_review.html).**
