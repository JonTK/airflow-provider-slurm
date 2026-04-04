"""Shared pytest fixtures for airflow-provider-slurm tests."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from airflow.models.taskinstance import TaskInstanceKey

from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager


@pytest.fixture
def mock_airflow_conf():
    """Mock Airflow configuration with default values.

    This fixture provides a standard Airflow configuration mock that can be
    imported and patched in tests. It includes common Slurm executor settings
    with sensible defaults.

    Returns:
        MagicMock: Mocked Airflow conf object with get/getint/getfloat methods
    """
    mock_conf = MagicMock()

    # Standard configuration values
    config_values = {
        ("slurm", "api_url"): "https://slurm.example.com:6820",
        ("slurm", "username"): None,
        ("slurm", "default_partition"): "compute",
        ("slurm", "default_cpus"): "1",
        ("slurm", "default_mem"): "4G",
        ("slurm", "default_time_limit"): "01:00:00",
        ("slurm", "default_account"): None,
        ("slurm", "airflow_venv"): None,
        ("slurm", "default_container"): None,
        ("slurm", "shutdown_mode"): "cancel",
        ("logging", "base_log_folder"): "/tmp/airflow/logs",
        ("core", "dags_folder"): "/tmp/airflow/dags",
    }

    config_int_values = {
        ("slurm", "token_lifespan"): 3600,
        ("slurm", "default_cpus"): 1,
        ("slurm", "shutdown_wait_timeout"): 300,
        ("slurm", "api_timeout"): 30,
        ("slurm", "api_max_retries"): 3,
    }

    config_float_values = {
        ("slurm", "sync_interval"): 10.0,
    }

    # Setup side effects for different get methods
    mock_conf.get.side_effect = lambda section, key, fallback=None: config_values.get(
        (section, key), fallback
    )

    mock_conf.getint.side_effect = (
        lambda section, key, fallback=None: config_int_values.get(
            (section, key), fallback
        )
    )

    mock_conf.getfloat.side_effect = (
        lambda section, key, fallback=None: config_float_values.get(
            (section, key), fallback
        )
    )

    return mock_conf


@pytest.fixture
def mock_token_manager():
    """Create a mock SlurmTokenManager.

    Returns:
        MagicMock: Mocked token manager with standard test token
    """
    manager = MagicMock(spec=SlurmTokenManager)
    manager.get_token.return_value = "test_token_12345"
    manager.invalidate.return_value = None
    return manager


@pytest.fixture
def slurm_api_client(mock_token_manager):
    """Create a SlurmAPIClient instance with mocked token manager.

    Args:
        mock_token_manager: Fixture providing mocked token manager

    Returns:
        SlurmAPIClient: Configured API client instance for testing
    """
    return SlurmAPIClient(
        base_url="https://slurm.example.com:6820",
        token_manager=mock_token_manager,
    )


@pytest.fixture
def task_instance_key():
    """Create a sample TaskInstanceKey for testing.

    Returns:
        TaskInstanceKey: Standard task instance key with known values
    """
    return TaskInstanceKey(
        dag_id="test_dag",
        task_id="test_task",
        run_id="2024-01-01T00:00:00+00:00",
        try_number=1,
    )


@pytest.fixture
def task_instance_keys():
    """Create multiple TaskInstanceKeys for testing parallel operations.

    Returns:
        list[TaskInstanceKey]: List of task instance keys
    """
    return [
        TaskInstanceKey(
            dag_id="test_dag",
            task_id=f"test_task_{i}",
            run_id="2024-01-01T00:00:00+00:00",
            try_number=1,
        )
        for i in range(3)
    ]


@pytest.fixture
def mock_task_instance():
    """Create a mocked Airflow TaskInstance.

    Returns:
        MagicMock: Mocked task instance with standard attributes
    """
    ti = MagicMock()
    ti.key = TaskInstanceKey(
        dag_id="test_dag",
        task_id="test_task",
        run_id="2024-01-01T00:00:00+00:00",
        try_number=1,
    )
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.run_id = "2024-01-01T00:00:00+00:00"
    ti.try_number = 1
    return ti


@pytest.fixture
def executor_config_basic():
    """Return basic executor configuration for Slurm jobs.

    Returns:
        dict: Basic resource configuration
    """
    return {
        "partition": "compute",
        "cpus_per_task": 1,
        "mem": "4G",
        "time_limit": "00:30:00",
    }


@pytest.fixture
def executor_config_gpu():
    """GPU-enabled executor configuration.

    Returns:
        dict: Configuration with GPU resources
    """
    return {
        "partition": "gpu",
        "cpus_per_task": 4,
        "mem": "16G",
        "time_limit": "02:00:00",
        "gres": "gpu:1",
    }


@pytest.fixture
def executor_config_high_memory():
    """High-memory executor configuration.

    Returns:
        dict: Configuration for memory-intensive tasks
    """
    return {
        "partition": "highmem",
        "cpus_per_task": 2,
        "mem": "64G",
        "time_limit": "04:00:00",
    }


@pytest.fixture
def sample_job_spec():
    """Sample Slurm job specification for testing.

    Returns:
        dict: Complete job specification
    """
    return {
        "script": "#!/bin/bash\nset -euo pipefail\necho 'Hello from Slurm'",
        "job": {
            "name": "test_job",
            "partition": "compute",
            "cpus_per_task": 1,
            "memory_per_node": "4G",
            "time_limit": "00:30:00",
            "standard_output": "/tmp/airflow/logs/test.log",
            "standard_error": "/tmp/airflow/logs/test.log",
        },
    }


@pytest.fixture
def sample_slurm_job_running():
    """Sample Slurm API response for a running job.

    Returns:
        dict: Job status response for running job
    """
    return {
        "job_id": 12345,
        "name": "airflow-test_dag-test_task-12345-1",
        "job_state": "RUNNING",
        "partition": "compute",
        "cpus": 1,
        "memory": 4096,
        "start_time": int(datetime.now().timestamp()),
    }


@pytest.fixture
def sample_slurm_job_completed():
    """Sample Slurm API response for a completed job.

    Returns:
        dict: Job status response for completed job
    """
    return {
        "job_id": 12345,
        "name": "airflow-test_dag-test_task-12345-1",
        "job_state": "COMPLETED",
        "partition": "compute",
        "cpus": 1,
        "memory": 4096,
        "exit_code": 0,
    }


@pytest.fixture
def sample_slurm_job_failed():
    """Sample Slurm API response for a failed job.

    Returns:
        dict: Job status response for failed job
    """
    return {
        "job_id": 12345,
        "name": "airflow-test_dag-test_task-12345-1",
        "job_state": "FAILED",
        "partition": "compute",
        "cpus": 1,
        "memory": 4096,
        "exit_code": 1,
        "state_reason": "NonZeroExitCode",
    }


@pytest.fixture
def sample_slurm_job_pending():
    """Sample Slurm API response for a pending job.

    Returns:
        dict: Job status response for pending job
    """
    return {
        "job_id": 12345,
        "name": "airflow-test_dag-test_task-12345-1",
        "job_state": "PENDING",
        "partition": "compute",
        "cpus": 1,
        "memory": 4096,
        "state_reason": "Resources",
    }


@pytest.fixture
def mock_slurm_api_success(mock_token_manager):
    """Mock SlurmAPIClient with successful operations.

    Returns:
        MagicMock: Configured mock API client for success scenarios
    """
    mock_client = MagicMock()
    mock_client.ping.return_value = True
    mock_client.submit_job.return_value = {"job_id": 12345}
    mock_client.get_jobs.return_value = {"jobs": []}
    mock_client.get_job.return_value = None
    mock_client.cancel_job.return_value = {"status": "cancelled"}
    mock_client.get_api_version.return_value = "v0.0.42"
    return mock_client


@pytest.fixture
def airflow_command():
    """Return standard Airflow task execution command.

    Returns:
        list[str]: Command to run an Airflow task
    """
    return ["airflow", "tasks", "run", "test_dag", "test_task", "2024-01-01", "--local"]
