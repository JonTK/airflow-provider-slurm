# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-12-25

Initial alpha release of `airflow-provider-slurm`.

### Added
- **SlurmExecutor**: Main executor class inheriting from Airflow's BaseExecutor
  - Job submission via `execute_async()` method
  - Status synchronization with `sync()` method
  - Task adoption for scheduler restart recovery
  - Graceful and emergency shutdown support
- **SlurmAPIClient**: REST API communication layer
  - Job submission, querying, and cancellation
  - Retry logic with exponential backoff
  - Authentication header management
  - Support for Slurm REST API v0.0.40-v0.0.44
- **SlurmTokenManager**: JWT token management
  - Token generation via `scontrol` command
  - Token caching and automatic refresh
- **Live Cluster Validation**: Tested against Slurm 25.11.1 cluster
- **Airflow 3.x Compatibility**: Fixed imports for Apache Airflow 3.x support
- **Advanced DAG Examples**: 5 workflow examples
  - Basic Slurm executor demonstration
  - Parallel processing workflows
  - Machine learning pipeline orchestration
  - Bioinformatics workflow examples
  - Distributed computing patterns

### Technical Details
- **API Compatibility**: Slurm REST API v0.0.40-v0.0.44
- **Python Support**: 3.8, 3.9, 3.10, 3.11
- **Airflow Support**: 2.5.0+ and 3.x
- **Live Testing**: Jobs successfully completed on Slurm 25.11.1

---

## Historical Development (pre-release)

### Added

#### Core Features
- **SlurmExecutor**: Main executor class inheriting from Airflow's BaseExecutor
  - Job submission via `execute_async()` method
  - Status synchronization with `sync()` method  
  - Task adoption for scheduler restart recovery
  - Graceful and emergency shutdown support
- **SlurmAPIClient**: REST API communication layer
  - Job submission, querying, and cancellation
  - Retry logic with exponential backoff
  - Authentication header management
  - API version discovery
- **SlurmTokenManager**: JWT token management
  - Token generation via `scontrol` command
  - Token caching and automatic refresh
  - Configurable token lifespan

#### Configuration
- Comprehensive configuration via `airflow.cfg` `[slurm]` section
- Support for environment variables
- Resource allocation defaults (CPU, memory, time limits)
- Container and virtual environment execution modes
- Configurable sync intervals and shutdown behavior

#### Task Features
- Per-task resource specification via `executor_config`
- Support for Slurm partitions, accounts, and QoS
- Container-based execution (requires Slurm container support)
- Virtual environment activation
- Proper log path management for Airflow UI integration

#### Development & Testing
- Comprehensive unit test suite (>90% coverage)
- Integration test framework with mocked Slurm
- Pre-commit hooks for code quality
- CI/CD pipeline with GitHub Actions
- Multi-Python version testing (3.8-3.11)
- Multi-Airflow version testing (2.5.0+)

#### Documentation & Examples
- Detailed README with quick start guide
- Comprehensive API documentation
- Example DAGs demonstrating usage patterns
- Installation and configuration guides
- Troubleshooting documentation
- Contributing guidelines

#### Package & Distribution
- Modern Python packaging with `pyproject.toml`
- PyPI distribution support
- Semantic versioning
- Apache 2.0 license
- GitHub releases automation

### Technical Details

#### Job Naming Convention
- Format: `airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}`
- Enables task adoption after scheduler restarts
- Handles long DAG/task IDs with truncation

#### State Management
- Maps Slurm job states to Airflow task states
- Handles missing jobs with timeout-based failure detection
- Supports job history queries for completed jobs

#### Error Handling
- Comprehensive exception hierarchy
- Graceful degradation on API failures
- Retry logic for transient failures
- Detailed error logging and debugging support

#### Requirements
- Python 3.8+
- Apache Airflow 2.5.0+
- Slurm cluster with REST API (slurmrestd) v0.0.42+
- `scontrol` binary in PATH
- Shared filesystem between Airflow and compute nodes

### Dependencies
- `apache-airflow>=2.5.0`
- `requests>=2.28.0`

### Development Dependencies
- Testing: `pytest`, `pytest-cov`, `pytest-mock`, `responses`
- Code quality: `black`, `isort`, `flake8`, `mypy`
- Documentation: `sphinx`, `sphinx-rtd-theme`
- Build: `build`, `twine`