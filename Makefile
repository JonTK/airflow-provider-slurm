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
	pytest --cov=airflow_slurm_executor --cov-report=html --cov-report=term

lint:
	flake8 airflow_slurm_executor tests
	black --check airflow_slurm_executor tests
	isort --check-only airflow_slurm_executor tests

format:
	black airflow_slurm_executor tests
	isort airflow_slurm_executor tests

type-check:
	mypy airflow_slurm_executor

build: clean
	python -m build

upload-test: build
	python -m twine upload --repository testpypi dist/*

upload: build
	python -m twine upload dist/*

docs:
	cd docs && make html
