#!/usr/bin/env python3
"""Test using the actual SlurmExecutor code."""

import sys
import os
import tempfile
from unittest.mock import MagicMock, patch

# Add the package to Python path
sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow.models.taskinstance import TaskInstanceKey
from airflow_slurm_executor.slurm_executor import SlurmExecutor
from airflow_slurm_executor.slurm_token_manager import SlurmTokenManager

BASE_URL = "http://rocky9.ar.jontk.com:6820"
TEST_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjY2Nzg5NjEsImlhdCI6MTc2NjY3NzE2MSwic3VuIjoicm9vdCJ9.FRcLY-j8uao80Obc51d7LgZd3Ql_Oan3H8anIVCjuAg"


def main():
    print("🧪 Testing Real SlurmExecutor")
    print("-" * 35)

    # Mock Airflow config
    with patch("airflow_slurm_executor.slurm_executor.conf") as mock_conf:
        mock_conf.get.side_effect = lambda section, key, fallback=None: {
            ("slurm", "api_url"): BASE_URL,
            ("slurm", "username"): "root",
            ("slurm", "default_partition"): "normal",
            ("slurm", "default_cpus"): "1",
            ("slurm", "default_mem"): "100M",
            ("slurm", "default_time_limit"): "01:00:00",
            ("slurm", "default_account"): None,
            ("slurm", "airflow_venv"): None,
            ("slurm", "default_container"): None,
            ("slurm", "shutdown_mode"): "cancel",
            ("logging", "base_log_folder"): "/tmp/airflow/logs",
            ("core", "dags_folder"): "/tmp/airflow/dags",
        }.get((section, key), fallback)

        mock_conf.getint.side_effect = lambda section, key, fallback=None: {
            ("slurm", "token_lifespan"): 3600,
            ("slurm", "default_cpus"): 1,
            ("slurm", "shutdown_wait_timeout"): 300,
            ("slurm", "api_timeout"): 30,
            ("slurm", "api_max_retries"): 3,
        }.get((section, key), fallback)

        mock_conf.getfloat.side_effect = lambda section, key, fallback=None: {
            ("slurm", "sync_interval"): 10.0,
        }.get((section, key), fallback)

        # Create executor
        executor = SlurmExecutor()

        # Mock the token manager to use our test token
        def mock_fetch_new_token():
            return TEST_TOKEN

        # Override token generation method
        with patch.object(SlurmTokenManager, "_fetch_new_token", mock_fetch_new_token):
            # Mock filesystem check
            with patch("pathlib.Path.touch"), patch("os.remove"), patch("os.makedirs"):
                # Start executor
                print("Starting executor...")
                executor.start()
                print("✅ Executor started successfully")

                # Create a test task
                task_key = TaskInstanceKey(
                    dag_id="test_dag",
                    task_id="test_task",
                    run_id="manual__2025-12-25",
                    try_number=1,
                )

                # Simple test command
                command = [
                    "echo",
                    "Hello from real executor!",
                    "&&",
                    "date",
                    "&&",
                    "hostname",
                ]

                print(f"Submitting task: {task_key}")
                print(f"Command: {' '.join(command)}")

                # Submit the task
                executor.execute_async(task_key, command)

                # Check if it was submitted
                if task_key in executor.running:
                    job_info = executor.running[task_key]
                    job_id = job_info["slurm_job_id"]
                    print(f"✅ Task submitted as Slurm job {job_id}")
                    print(f"Monitor: scontrol show job {job_id}")

                    # The script that was generated
                    script = executor._build_script(command)
                    print(f"\\nGenerated script:")
                    print("-" * 20)
                    print(script)
                    print("-" * 20)

                else:
                    print("❌ Task submission failed")


if __name__ == "__main__":
    main()
