## Description

<!-- Provide a clear and concise description of your changes -->

## Type of Change

<!-- Mark the relevant option(s) with an 'x' -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring (no functional changes)
- [ ] Test improvement
- [ ] CI/CD improvement
- [ ] Dependencies update

## Related Issues

<!-- Link related issues using #issue_number or full URL -->

Closes #
Related to #

## Motivation and Context

<!-- Why is this change required? What problem does it solve? -->

## Changes Made

<!-- List the specific changes made in this PR -->

-
-
-

## Testing

### Testing Performed

<!-- Describe the testing you performed -->

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed
- [ ] Tested on live Slurm cluster

### Test Environment

<!-- Provide details about your test environment -->

- **Provider Version**:
- **Airflow Version**:
- **Slurm Version**:
- **Slurmrestd API Version**:
- **Python Version**:
- **OS**:

### Test Cases

<!-- Describe specific test cases or scenarios tested -->

```python
# Example test case or usage
```

## HPC/Slurm Considerations

<!-- Address any HPC or Slurm-specific impacts -->

- [ ] Tested with different Slurm partitions
- [ ] Verified resource allocation behavior
- [ ] Tested job submission and monitoring
- [ ] Verified log retrieval functionality
- [ ] Tested with containerized workloads (if applicable)
- [ ] Considered impact on high-throughput workloads
- [ ] Not applicable to HPC/Slurm functionality

## Performance Impact

<!-- Describe any performance implications -->

- [ ] No performance impact expected
- [ ] Performance improvement (describe below)
- [ ] Potential performance impact (describe below)

**Details**:


## Breaking Changes

<!-- If this is a breaking change, describe the impact and migration path -->

- [ ] No breaking changes
- [ ] Configuration changes required (describe below)
- [ ] API changes (describe below)
- [ ] Behavior changes (describe below)

**Migration Guide** (if applicable):


## Documentation

<!-- Mark the relevant option(s) -->

- [ ] Documentation updated (docs/, README.md, docstrings)
- [ ] No documentation changes needed
- [ ] Documentation will be added in a follow-up PR

## Checklist

<!-- Mark completed items with an 'x' -->

### Code Quality

- [ ] My code follows the project's style guidelines
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings or errors
- [ ] I have run linting tools (black, isort, flake8)

### Testing

- [ ] I have added tests that prove my fix is effective or my feature works
- [ ] New and existing unit tests pass locally with my changes
- [ ] I have tested on a live Slurm cluster (if applicable)
- [ ] I have verified backward compatibility (if applicable)

### Security

- [ ] I have reviewed my changes for security vulnerabilities
- [ ] I have not introduced hardcoded credentials or secrets
- [ ] I have considered the security implications of my changes
- [ ] I have reviewed the [Security Policy](../SECURITY.md)

### Dependencies

- [ ] I have updated dependencies if needed
- [ ] I have checked for dependency vulnerabilities
- [ ] I have documented any new dependencies

### Other

- [ ] I have updated the CHANGELOG.md (if applicable)
- [ ] I have verified this change works with supported Python versions (3.9-3.11)
- [ ] I have verified this change works with supported Airflow versions (2.5.0+)
- [ ] I have verified this change works with supported Slurm versions (23.11-25.11, API v0.0.40-v0.0.44)

## Screenshots/Logs

<!-- If applicable, add screenshots or logs to help explain your changes -->

## Additional Notes

<!-- Add any additional notes, concerns, or questions for reviewers -->

## Reviewer Checklist

<!-- For reviewers - do not fill this out as the PR author -->

- [ ] Code quality and style are acceptable
- [ ] Tests are comprehensive and pass
- [ ] Documentation is clear and complete
- [ ] Security implications have been considered
- [ ] Performance impact is acceptable
- [ ] Breaking changes are properly documented
- [ ] CI/CD checks pass
