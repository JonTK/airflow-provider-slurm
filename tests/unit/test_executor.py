"""Unit tests for SlurmExecutor."""

import os
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.state import TaskInstanceState

from airflow_provider_slurm.exceptions import (
    SlurmAPIError,
    SlurmConfigurationError,
    SlurmJobSubmissionError,
)
from airflow_provider_slurm.slurm_executor import SlurmExecutor


class TestSlurmExecutor:
    """Test cases for SlurmExecutor."""

    @pytest.fixture
    def mock_conf(self):
        """Mock Airflow configuration."""
        with patch("airflow_provider_slurm.slurm_executor.conf") as mock:
            # Set default configuration values
            mock.get.side_effect = lambda section, key, fallback=None: {
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
            }.get((section, key), fallback)

            mock.getint.side_effect = lambda section, key, fallback=None: {
                ("slurm", "token_lifespan"): 3600,
                ("slurm", "default_cpus"): 1,
                ("slurm", "shutdown_wait_timeout"): 300,
                ("slurm", "api_timeout"): 30,
                ("slurm", "api_max_retries"): 3,
            }.get((section, key), fallback)

            mock.getfloat.side_effect = lambda section, key, fallback=None: {
                ("slurm", "sync_interval"): 10.0,
            }.get((section, key), fallback)

            yield mock

    @pytest.fixture
    def executor(self, mock_conf):
        """Create executor instance with mocked config."""
        return SlurmExecutor()

    @pytest.fixture
    def task_key(self):
        """Create a sample task instance key."""
        return TaskInstanceKey(
            dag_id="test_dag",
            task_id="test_task",
            run_id="2024-01-01T00:00:00+00:00",
            try_number=1,
        )

    def test_init(self, executor):
        """Test executor initialization."""
        assert executor.token_manager is None
        assert executor.slurm_client is None
        assert executor.running == {}
        assert executor.last_sync_time == 0.0

    @patch("airflow_provider_slurm.slurm_executor.SlurmTokenManager")
    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_start_success(
        self, mock_api_client, mock_token_manager, executor, mock_conf
    ):
        """Test successful executor start."""
        # Mock API client ping
        mock_client_instance = MagicMock()
        mock_client_instance.ping.return_value = True
        mock_api_client.return_value = mock_client_instance

        # Mock filesystem check
        with patch("pathlib.Path.touch"), patch("os.remove"):
            executor.start()

        # Verify components were initialized
        assert executor.token_manager is not None
        assert executor.slurm_client is not None
        mock_client_instance.ping.assert_called_once()

    def test_start_no_api_url(self, executor, mock_conf):
        """Test start fails without API URL."""
        mock_conf.get.side_effect = lambda section, key, fallback=None: ""

        with pytest.raises(SlurmConfigurationError) as exc_info:
            executor.start()

        assert "API URL not configured" in str(exc_info.value)

    @patch("airflow_provider_slurm.slurm_executor.SlurmTokenManager")
    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_start_api_not_reachable(
        self, mock_api_client, mock_token_manager, executor
    ):
        """Test start fails when API is not reachable."""
        # Mock API client ping failure
        mock_client_instance = MagicMock()
        mock_client_instance.ping.return_value = False
        mock_api_client.return_value = mock_client_instance

        with pytest.raises(SlurmConfigurationError) as exc_info:
            executor.start()

        assert "Cannot connect to Slurm API" in str(exc_info.value)

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_execute_async_success(self, mock_api_client, executor, task_key):
        """Test successful task submission."""
        # Setup mock client
        mock_client = MagicMock()
        mock_client.submit_job.return_value = {"job_id": 12345}
        executor.slurm_client = mock_client

        # Execute task
        command = ["airflow", "tasks", "run", "test_dag", "test_task"]
        executor.execute_async(task_key, command)

        # Verify job was submitted and tracked
        assert task_key in executor.running
        assert executor.running[task_key]["slurm_job_id"] == 12345
        assert executor.running[task_key]["command"] == command

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_execute_async_submission_failure(
        self, mock_api_client, executor, task_key
    ):
        """Test handling of job submission failure."""
        # Setup mock client to raise exception
        mock_client = MagicMock()
        mock_client.submit_job.side_effect = SlurmAPIError("Submission failed")
        executor.slurm_client = mock_client

        # Mock fail method
        executor.fail = MagicMock()

        # Execute task
        command = ["airflow", "tasks", "run", "test_dag", "test_task"]
        executor.execute_async(task_key, command)

        # Verify task was marked as failed
        executor.fail.assert_called_once_with(task_key)
        assert task_key not in executor.running

    def test_build_job_name(self, executor):
        """Test job name generation."""
        # Create task key with specific run_id
        task_key = TaskInstanceKey(
            dag_id="test_dag",
            task_id="test_task",
            run_id="manual__2024-01-01",
            try_number=1,
        )

        job_name = executor._build_job_name(task_key)

        assert job_name.startswith("airflow-test_dag-test_task-")
        assert job_name.endswith("-1")  # try_number
        assert len(job_name) <= 256

    def test_build_job_name_long_ids(self, executor):
        """Test job name truncation for long IDs."""
        # Create key with very long IDs
        long_key = TaskInstanceKey(
            dag_id="a" * 200,
            task_id="b" * 200,
            run_id="2024-01-01T00:00:00+00:00",
            try_number=1,
        )

        job_name = executor._build_job_name(long_key)
        assert len(job_name) <= 256

    def test_build_script_basic(self, executor):
        """Test basic script generation."""
        command = ["airflow", "tasks", "run", "dag_id", "task_id"]
        script = executor._build_script(command)

        assert "#!/bin/bash" in script
        assert "set -euo pipefail" in script
        assert "airflow tasks run dag_id task_id" in script

    def test_build_script_with_venv(self, executor):
        """Test script generation with virtual environment."""
        executor.airflow_venv = "/path/to/venv"
        command = ["airflow", "tasks", "run"]

        script = executor._build_script(command)

        assert "source /path/to/venv/bin/activate" in script

    def test_build_script_with_container(self, executor):
        """Test script generation with container (no venv activation)."""
        executor.airflow_venv = "/path/to/venv"
        executor.default_container = "docker://airflow:latest"
        command = ["airflow", "tasks", "run"]

        script = executor._build_script(command)

        # Should not activate venv when using container
        assert "source /path/to/venv/bin/activate" not in script

    @patch("os.makedirs")
    def test_get_log_path(self, mock_makedirs, executor, task_key):
        """Test log path generation."""
        log_path = executor._get_log_path(task_key)

        expected_path = (
            "/tmp/airflow/logs/dags/test_dag/test_task/2024-01-01T00:00:00+00:00/1.log"
        )
        assert log_path == expected_path
        mock_makedirs.assert_called_once()

    def test_build_job_spec(self, executor, task_key):
        """Test job specification building."""
        command = ["airflow", "tasks", "run"]
        executor_config = {
            "cpus_per_task": 4,
            "mem": "16G",
            "partition": "gpu",
        }

        job_spec = executor._build_job_spec(task_key, command, None, executor_config)

        assert "script" in job_spec
        assert "job" in job_spec
        assert job_spec["job"]["cpus_per_task"] == 4
        assert job_spec["job"]["memory_per_node"] == "16G"
        assert job_spec["job"]["partition"] == "gpu"

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_sync_completed_job(self, mock_api_client, executor, task_key):
        """Test sync with completed job."""
        # Setup running job
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
            "submit_time": datetime.now(),
        }

        # Mock API response
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = {
            "jobs": [
                {
                    "job_id": 12345,
                    "job_state": "COMPLETED",
                    "exit_code": 0,
                }
            ]
        }
        executor.slurm_client = mock_client

        # Mock success method
        executor.success = MagicMock()

        # Sync
        executor.last_sync_time = 0  # Force sync
        executor.sync()

        # Verify task was marked as success
        executor.success.assert_called_once_with(task_key)
        assert task_key not in executor.running

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_sync_failed_job(self, mock_api_client, executor, task_key):
        """Test sync with failed job."""
        # Setup running job
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
            "submit_time": datetime.now(),
        }

        # Mock API response
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = {
            "jobs": [
                {
                    "job_id": 12345,
                    "job_state": "FAILED",
                    "state_reason": "NonZeroExitCode",
                }
            ]
        }
        executor.slurm_client = mock_client

        # Mock fail method
        executor.fail = MagicMock()

        # Sync
        executor.last_sync_time = 0
        executor.sync()

        # Verify task was marked as failed
        executor.fail.assert_called_once_with(task_key)
        assert task_key not in executor.running

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_sync_missing_job_timeout(self, mock_api_client, executor, task_key):
        """Test sync with missing job that times out."""
        # Setup running job that's been missing for a while
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
            "submit_time": datetime.now(),
            "missing_since": datetime.now() - timedelta(minutes=10),
        }

        # Mock API response (job not found)
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = {"jobs": []}
        mock_client.get_job_history.return_value = None
        executor.slurm_client = mock_client

        # Mock fail method
        executor.fail = MagicMock()

        # Sync
        executor.last_sync_time = 0
        executor.sync()

        # Verify task was marked as failed
        executor.fail.assert_called_once_with(task_key)
        assert task_key not in executor.running

    def test_sync_throttling(self, executor):
        """Test sync throttling."""
        executor.sync_interval = 10.0
        executor.last_sync_time = time.time()

        # Mock client to verify it's not called
        mock_client = MagicMock()
        executor.slurm_client = mock_client
        executor.running = {"dummy": {}}  # Add dummy job

        # Sync should be skipped due to throttling
        executor.sync()

        mock_client.get_jobs.assert_not_called()

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_end_cancel_mode(self, mock_api_client, executor, task_key):
        """Test graceful shutdown in cancel mode."""
        # Setup running jobs
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
        }
        executor.shutdown_mode = "cancel"

        # Mock client
        mock_client = MagicMock()
        executor.slurm_client = mock_client

        # Mock fail method
        executor.fail = MagicMock()

        # End executor
        executor.end()

        # Verify jobs were cancelled
        mock_client.cancel_job.assert_called_once_with(12345)
        executor.fail.assert_called_once_with(task_key)
        assert len(executor.running) == 0

    @patch("time.sleep")
    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_end_wait_mode(self, mock_api_client, mock_sleep, executor, task_key):
        """Test graceful shutdown in wait mode."""
        # Setup running job
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
        }
        executor.shutdown_mode = "wait"
        executor.shutdown_wait_timeout = 1  # Short timeout for test

        # Mock sync to simulate job completion
        def clear_jobs():
            executor.running.clear()

        executor.sync = MagicMock(side_effect=clear_jobs)

        # End executor
        executor.end()

        # Verify sync was called
        executor.sync.assert_called()

    def test_terminate(self, executor, task_key):
        """Test emergency termination."""
        # Setup running jobs
        executor.running[task_key] = {
            "slurm_job_id": 12345,
            "command": ["cmd"],
        }

        # Mock client
        mock_client = MagicMock()
        executor.slurm_client = mock_client

        # Terminate
        executor.terminate()

        # Verify best-effort cancellation
        mock_client.cancel_job.assert_called_once_with(12345)
        assert len(executor.running) == 0

    @patch("airflow_provider_slurm.slurm_executor.SlurmAPIClient")
    def test_try_adopt_task_instances(self, mock_api_client, executor):
        """Test task adoption after scheduler restart."""
        # Create task instances to adopt
        ti1 = MagicMock()
        ti1.key = TaskInstanceKey("dag1", "task1", "2024-01-01T00:00:00+00:00", 1)

        ti2 = MagicMock()
        ti2.key = TaskInstanceKey("dag2", "task2", "2024-01-01T00:00:00+00:00", 1)

        # Mock Slurm jobs
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = {
            "jobs": [
                {
                    "job_id": 100,
                    "name": executor._build_job_name(ti1.key),
                    "job_state": "RUNNING",
                },
                {
                    "job_id": 200,
                    "name": executor._build_job_name(ti2.key),
                    "job_state": "COMPLETED",  # Should not adopt
                },
            ]
        }
        executor.slurm_client = mock_client

        # Attempt adoption
        adopted = executor.try_adopt_task_instances([ti1, ti2])

        # Verify only running job was adopted
        assert len(adopted) == 1
        assert adopted[0] == ti1
        assert ti1.key in executor.running
        assert executor.running[ti1.key]["slurm_job_id"] == 100
