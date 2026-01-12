# Contributing to Airflow Provider Slurm

Thank you for your interest in contributing to the Airflow Provider Slurm project! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Python 3.8+
- Git
- Access to a Slurm cluster (for integration testing)

### Development Setup

```bash
# Fork and clone the repository
git clone https://github.com/JonTK/airflow-provider-slurm
cd airflow-provider-slurm

# Create a virtual environment
python -m venv venv
source venv/bin/activate

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

## How to Contribute

### Reporting Bugs

1. Check existing [issues](https://github.com/JonTK/airflow-provider-slurm/issues) to avoid duplicates
2. Use the bug report template when creating a new issue
3. Include:
   - Python version
   - Airflow version
   - Slurm version
   - Steps to reproduce
   - Expected vs actual behavior
   - Relevant logs or error messages

### Suggesting Features

1. Check existing issues and discussions for similar suggestions
2. Open a new issue using the feature request template
3. Describe the use case and proposed solution
4. Be open to discussion and alternative approaches

### Submitting Code

1. **Fork** the repository
2. **Create a branch** for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following our coding standards
4. **Write tests** for new functionality
5. **Run the test suite**:
   ```bash
   pytest
   ```
6. **Commit your changes** using conventional commits:
   ```bash
   git commit -m "feat: add new feature description"
   ```
7. **Push** to your fork and **submit a pull request**

## Coding Standards

### Code Style

- Follow [PEP 8](https://pep8.org/) guidelines
- Use [Black](https://black.readthedocs.io/) for code formatting
- Use [isort](https://pycqa.github.io/isort/) for import sorting
- Maximum line length: 88 characters

Run formatters before committing:
```bash
black .
isort .
```

### Type Hints

- Use type hints for all function signatures
- Run mypy for type checking:
  ```bash
  mypy airflow_slurm_executor
  ```

### Documentation

- Add docstrings to all public functions and classes
- Update README.md for user-facing changes
- Update CHANGELOG.md for all notable changes

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `test:` - Test additions or modifications
- `refactor:` - Code refactoring
- `chore:` - Maintenance tasks

Examples:
```
feat: add support for GPU resource allocation
fix: handle timeout errors in job submission
docs: update installation instructions
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=airflow_slurm_executor

# Run specific test file
pytest tests/test_executor.py

# Run with verbose output
pytest -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Name test functions `test_*`
- Use pytest fixtures for setup/teardown
- Mock external dependencies (Slurm API, filesystem)

## Pull Request Process

1. Ensure all tests pass
2. Update documentation as needed
3. Add entry to CHANGELOG.md under `[Unreleased]`
4. Request review from maintainers
5. Address review feedback

### PR Checklist

- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Code formatted with Black/isort
- [ ] Type hints added
- [ ] All CI checks pass

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Questions?

- Open a [Discussion](https://github.com/JonTK/airflow-provider-slurm/discussions)
- Check existing issues and documentation

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
