# Contributing to Airflow Slurm Executor

Thank you for your interest in contributing to the Airflow Slurm Executor! This document provides guidelines and instructions for contributing.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct: be respectful, inclusive, and constructive in all interactions.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/airflow-slurm-executor.git
   cd airflow-slurm-executor
   ```
3. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. Install the package in development mode:
   ```bash
   make install-dev
   ```

## Development Workflow

### 1. Create a Branch

Create a feature branch from `main`:
```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Write clean, documented code
- Follow the existing code style
- Add or update tests as needed
- Update documentation if required

### 3. Code Quality

Before committing, ensure your code meets quality standards:

```bash
# Format code
make format

# Run linting
make lint

# Run type checking
make type-check

# Run tests
make test-cov
```

### 4. Commit Your Changes

We use conventional commits for clear history:

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting, etc.)
- `refactor:` Code refactoring
- `test:` Test additions or changes
- `chore:` Maintenance tasks

Example:
```bash
git commit -m "feat: add support for GPU resource allocation"
```

### 5. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub with:
- Clear title and description
- Reference to any related issues
- Summary of changes made

## Testing

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test file
pytest tests/unit/test_token_manager.py

# Run tests across Python versions
tox
```

### Writing Tests

- Place unit tests in `tests/unit/`
- Place integration tests in `tests/integration/`
- Use pytest fixtures for common setup
- Mock external dependencies (Slurm API, etc.)
- Aim for >85% code coverage

Example test:
```python
def test_token_generation(mock_subprocess):
    """Test that token manager generates tokens correctly."""
    mock_subprocess.run.return_value.stdout = "SLURM_JWT=test_token"
    
    manager = SlurmTokenManager()
    token = manager.get_token()
    
    assert token == "test_token"
    mock_subprocess.run.assert_called_once()
```

## Documentation

- Add docstrings to all public functions and classes
- Use Google-style docstrings
- Update README.md for user-facing changes
- Update configuration docs for new options

## Release Process

1. Update version in `airflow_slurm_executor/version.py`
2. Update CHANGELOG.md
3. Create release PR
4. After merge, tag release: `git tag v0.1.0`
5. Push tag: `git push origin v0.1.0`
6. CI/CD will handle PyPI deployment

## Questions?

Feel free to:
- Open an issue for bugs or feature requests
- Start a discussion for general questions
- Reach out to maintainers

Thank you for contributing!