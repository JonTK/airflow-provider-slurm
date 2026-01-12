.PHONY: help install install-dev clean test test-cov lint format type-check build upload upload-test docs

help:
	@echo "Available commands:"
	@echo "  install        Install the package"
	@echo "  install-dev    Install the package with development dependencies"
	@echo "  clean          Remove build artifacts and cache files"
	@echo "  test           Run tests"
	@echo "  test-cov       Run tests with coverage report"
	@echo "  lint           Run linting checks"
	@echo "  format         Format code with black and isort"
	@echo "  type-check     Run type checking with mypy"
	@echo "  build          Build distribution packages"
	@echo "  upload         Upload to PyPI"
	@echo "  upload-test    Upload to Test PyPI"
	@echo "  docs           Build documentation"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type f -name '*.coverage' -delete
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .tox

test:
	pytest

test-cov:
	pytest --cov=airflow_provider_slurm --cov-report=html --cov-report=term

lint:
	flake8 airflow_provider_slurm tests
	black --check airflow_provider_slurm tests
	isort --check-only airflow_provider_slurm tests

format:
	black airflow_provider_slurm tests
	isort airflow_provider_slurm tests

type-check:
	mypy airflow_provider_slurm

build: clean
	python -m build

upload-test: build
	python -m twine upload --repository testpypi dist/*

upload: build
	python -m twine upload dist/*

docs:
	cd docs && make html
