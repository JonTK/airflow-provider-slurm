"""Slurm executor for Apache Airflow."""

import hashlib
import logging
import os
import shlex
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.executors import workloads
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey

from airflow_provider_slurm.exceptions import (
    SlurmAPIError,
    SlurmConfigurationError,
    SlurmJobSubmissionError,
)
from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager

logger = logging.getLogger(__name__)


class SlurmExecutor(BaseExecutor):
    """Execute Airflow tasks on a Slurm cluster via REST API.

    This executor submits tasks as Slurm jobs and monitors their status
    to update Airflow task states accordingly.
    """

    def __init__(self) -> None:
        """Initialize the Slurm executor."""
        super().__init__()

        # Component instances
        self.token_manager: Optional[SlurmTokenManager] = None
        self.slurm_client: Optional[SlurmAPIClient] = None

        # Configuration
        self.api_url: str = ""
        self.username: Optional[str] = None
        self.token_lifespan: int = 3600
        self.default_partition: str = "compute"
        self.default_cpus: int = 1
        self.default_mem: str = "4G"
        self.default_time_limit: str = "01:00:00"
        self.default_account: Optional[str] = None
        self.default_container: Optional[str] = None
        self.airflow_home: str = os.environ.get("AIRFLOW_HOME", "/tmp/airflow")
        self.airflow_venv: Optional[str] = None
        self.sync_interval: float = 10.0
        self.shutdown_mode: str = "cancel"
        self.shutdown_wait_timeout: int = 300

        # State tracking
        self.last_sync_time: float = 0.0
        # Override base class set with dict for job metadata
        self.running: Dict[TaskInstanceKey, Dict[str, Any]] = {}  # type: ignore[assignment]  # noqa: E501

        logger.info("Initialized SlurmExecutor")

    def change_state(self, key, state, info=None, remove_running=True) -> None:
        """Override to handle dict-based self.running instead of set."""
        if remove_running:
            try:
                del self.running[key]
            except KeyError:
                logger.debug(f"Could not find key: {key}")
        self.event_buffer[key] = state, info

    def start(self) -> None:
        """Start the executor and validate configuration."""
        logger.info("Starting SlurmExecutor")

        # Load configuration
        self._load_config()

        # Validate configuration
        if not self.api_url:
            raise SlurmConfigurationError(
                "Slurm API URL not configured. "
                "Set 'api_url' in [slurm] section of airflow.cfg"
            )

        # Initialize components
        self.token_manager = SlurmTokenManager(
            username=self.username,
            lifespan=self.token_lifespan,
        )

        self.slurm_client = SlurmAPIClient(
            base_url=self.api_url,
            token_manager=self.token_manager,
            timeout=conf.getint("slurm", "api_timeout", fallback=30),
            max_retries=conf.getint("slurm", "api_max_retries", fallback=3),
        )

        # Test connection
        if not self.slurm_client.ping():
            raise SlurmConfigurationError(
                f"Cannot connect to Slurm API at {self.api_url}. "
                "Please check the URL and network connectivity."
            )

        # Validate shared filesystem for logs
        self._validate_shared_filesystem()

        logger.info(f"SlurmExecutor started successfully. API: {self.api_url}")

    def _load_config(self) -> None:
        """Load configuration from airflow.cfg."""
        # API configuration
        self.api_url = conf.get("slurm", "api_url", fallback="")
        self.username = conf.get("slurm", "username", fallback=None)
        self.token_lifespan = conf.getint("slurm", "token_lifespan", fallback=3600)

        # Resource defaults
        self.default_partition = conf.get(
            "slurm", "default_partition", fallback="compute"
        )
        self.default_cpus = conf.getint("slurm", "default_cpus", fallback=1)
        self.default_mem = conf.get("slurm", "default_mem", fallback="4G")
        self.default_time_limit = conf.get(
            "slurm", "default_time_limit", fallback="01:00:00"
        )
        self.default_account = conf.get("slurm", "default_account", fallback=None)

        # Environment setup
        self.airflow_venv = conf.get("slurm", "airflow_venv", fallback=None)
        self.default_container = conf.get("slurm", "default_container", fallback=None)

        # Executor behavior
        self.sync_interval = conf.getfloat("slurm", "sync_interval", fallback=10.0)
        self.shutdown_mode = conf.get("slurm", "shutdown_mode", fallback="cancel")
        self.shutdown_wait_timeout = conf.getint(
            "slurm", "shutdown_wait_timeout", fallback=300
        )

        logger.debug(
            f"Loaded configuration: api_url={self.api_url}, "
            f"partition={self.default_partition}"
        )

    def _convert_time_to_minutes(self, time_value: Union[str, int]) -> int:
        """Convert time string or integer to minutes for Slurm REST API.

        The Slurm REST API interprets the time_limit integer as minutes.

        Args:
            time_value: Time as string (HH:MM:SS, MM:SS) or integer (minutes)

        Returns:
            Time in minutes as integer
        """
        if isinstance(time_value, int):
            return time_value

        if isinstance(time_value, str):
            # Handle formats like "01:00:00", "60:00", "3600"
            if ":" in time_value:
                parts = time_value.split(":")
                if len(parts) == 3:  # HH:MM:SS
                    hours, minutes, seconds = map(int, parts)
                    total_minutes = hours * 60 + minutes
                    if seconds > 0:
                        total_minutes += 1  # Round up partial minutes
                    return total_minutes
                elif len(parts) == 2:  # MM:SS
                    minutes, seconds = map(int, parts)
                    if seconds > 0:
                        minutes += 1
                    return minutes
            else:
                # Assume it's already in minutes as string
                return int(time_value)

        # Fallback
        return 60  # 1 hour default

    def _validate_shared_filesystem(self) -> None:
        """Verify log directory is accessible and writable."""
        log_folder = conf.get("logging", "base_log_folder")
        test_file = os.path.join(log_folder, ".slurm_executor_test")

        try:
            Path(test_file).touch()
            os.remove(test_file)
            logger.info(f"Verified shared filesystem access at {log_folder}")
        except Exception as e:
            logger.error(
                f"Cannot write to log folder {log_folder}: {e}. "
                "Ensure this path is on shared storage accessible from compute nodes."
            )
            raise SlurmConfigurationError(
                f"Log folder not writable: {log_folder}"
            ) from e

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: Sequence[str],
        queue: Optional[str] = None,
        executor_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Submit a task to Slurm as a job.

        Args:
            key: Unique task instance identifier
            command: Command to execute
            queue: Airflow queue (can map to Slurm partition)
            executor_config: Task-specific Slurm configuration
                - array: Array specification (e.g., "0-99", "1-100:2")
                - dependency: Dependency specification (e.g., "afterok:12345")
                - All other standard Slurm parameters
        """
        try:
            config = executor_config or {}
            array_spec = config.get("array")
            dependency_spec = config.get("dependency")

            # Build log message
            log_parts = [f"Submitting task {key} to Slurm"]
            if array_spec:
                log_parts.append(f"(array: {array_spec})")
            if dependency_spec:
                log_parts.append(f"(dependency: {dependency_spec})")
            logger.info(" ".join(log_parts))

            # Build job specification
            job_spec, array, dependency = self._build_job_spec(
                key, command, queue, executor_config
            )

            # Submit to Slurm
            assert self.slurm_client is not None, "Executor not started"
            result = self.slurm_client.submit_job(
                job_spec, array=array, dependency=dependency
            )
            job_id = result.get("job_id")

            if job_id:
                # Track the job with array and dependency metadata
                job_metadata: Dict[str, Any] = {
                    "slurm_job_id": job_id,
                    "command": command,
                    "submit_time": datetime.now(),
                }

                # Store array information if this is an array job
                if array:
                    job_metadata["array_spec"] = array
                    job_metadata["array_task_count"] = result.get("array_task_count", 0)
                    job_metadata["is_array"] = True
                else:
                    job_metadata["is_array"] = False

                # Store dependency information if specified
                if dependency:
                    job_metadata["dependency"] = dependency

                # Build success log message
                success_parts = [f"Task {key} submitted as Slurm"]
                if array:
                    success_parts.append(
                        f"array job {job_id} ({job_metadata['array_task_count']} tasks)"
                    )
                else:
                    success_parts.append(f"job {job_id}")
                if dependency:
                    success_parts.append(f"(dependency: {dependency})")

                logger.info(" ".join(success_parts))

                self.running[key] = job_metadata
            else:
                raise SlurmJobSubmissionError(f"No job_id returned for task {key}")

        except Exception as e:
            logger.error(f"Failed to submit task {key}: {e}")
            self.fail(key)

    def _process_workloads(self, wls: "Sequence[workloads.All]") -> None:
        """Process workloads by submitting them to Slurm.

        This is the Airflow 3.x executor API. Each workload contains a
        TaskInstance with all the metadata needed to run the task on a
        compute node via the Airflow supervisor.
        """
        for workload in wls:
            if not isinstance(workload, workloads.ExecuteTask):
                logger.warning(
                    f"Ignoring unknown workload type: {type(workload).__name__}"
                )
                continue

            ti = workload.ti
            key = ti.key
            config = ti.executor_config or {}

            try:
                logger.info(f"Processing workload for task {key}")

                # Build the supervisor command that will run on the compute node
                script = self._build_supervisor_script(workload)

                # Build job parameters
                job_spec, array_spec, dependency_spec = (
                    self._build_job_spec_from_workload(workload, config)
                )
                job_spec["script"] = script

                # Submit to Slurm
                assert self.slurm_client is not None, "Executor not started"
                result = self.slurm_client.submit_job(
                    job_spec, array=array_spec, dependency=dependency_spec
                )
                job_id = result.get("job_id")

                if job_id:
                    job_metadata: Dict[str, Any] = {
                        "slurm_job_id": job_id,
                        "submit_time": datetime.now(),
                        "is_array": bool(array_spec),
                        "workload": workload,
                    }
                    if array_spec:
                        job_metadata["array_spec"] = array_spec
                        job_metadata["array_task_count"] = result.get(
                            "array_task_count", 0
                        )
                    if dependency_spec:
                        job_metadata["dependency"] = dependency_spec

                    self.running[key] = job_metadata
                    del self.queued_tasks[key]
                    logger.info(f"Task {key} submitted as Slurm job {job_id}")
                else:
                    raise SlurmJobSubmissionError(f"No job_id returned for task {key}")

            except Exception as e:
                logger.error(f"Failed to submit task {key}: {e}")
                self.fail(key)

    def _build_supervisor_script(self, workload: "workloads.ExecuteTask") -> str:
        """Build a bash script that launches the Airflow task supervisor on a compute node."""
        ti = workload.ti

        # Determine the execution API server URL
        base_url = conf.get("api", "base_url", fallback="/")
        if base_url.startswith("/"):
            import socket

            hostname = socket.getfqdn()
            base_url = f"http://{hostname}:8080{base_url}"
        execution_api_url = f"{base_url.rstrip('/')}/execution/"
        execution_api_url = conf.get(
            "core", "execution_api_server_url", fallback=execution_api_url
        )

        # Write workload to a file on shared filesystem so compute node can read it
        workload_dir = os.path.join(self.airflow_home, "workloads")
        os.makedirs(workload_dir, exist_ok=True)
        workload_file = os.path.join(workload_dir, f"{ti.id}.json")
        with open(workload_file, "w") as f:
            f.write(workload.model_dump_json())

        lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
        ]

        # Activate virtual environment if configured
        if self.airflow_venv and not self.default_container:
            lines.extend(
                [
                    "# Activate virtual environment",
                    f"source {self.airflow_venv}/bin/activate",
                    "",
                ]
            )

        # Run the task using the Airflow 3.x supervisor
        lines.extend(
            [
                "# Execute Airflow task via supervisor",
                "python3 << 'PYEOF'",
                "import sys",
                "from airflow.sdk.execution_time.supervisor import supervise",
                "from airflow.executors.workloads import ExecuteTask",
                "",
                f'workload = ExecuteTask.model_validate_json(open("{workload_file}").read())',
                "exit_code = supervise(",
                "    ti=workload.ti,",
                "    dag_rel_path=workload.dag_rel_path,",
                "    bundle_info=workload.bundle_info,",
                "    token=workload.token,",
                f'    server="{execution_api_url}",',
                "    log_path=workload.log_path,",
                ")",
                "sys.exit(exit_code)",
                "PYEOF",
            ]
        )

        return "\n".join(lines)

    def _build_job_spec_from_workload(
        self,
        workload: "workloads.ExecuteTask",
        config: Dict[str, Any],
    ) -> tuple[Dict[str, Any], Optional[str], Optional[str]]:
        """Build Slurm job specification from an Airflow 3.x workload."""
        ti = workload.ti
        array_spec = config.get("array")
        dependency_spec = config.get("dependency")

        # Build job name
        run_id_hash = hashlib.sha256(ti.run_id.encode()).hexdigest()[:8]
        dag_id = ti.dag_id.replace("/", "_").replace(".", "_")
        task_id = ti.task_id.replace("/", "_").replace(".", "_")
        job_name = f"airflow-{dag_id}-{task_id}-{run_id_hash}-{ti.try_number}"
        if len(job_name) > 256:
            max_id_length = (256 - 20) // 2
            job_name = f"airflow-{dag_id[:max_id_length]}-{task_id[:max_id_length]}-{run_id_hash}-{ti.try_number}"

        # Determine log path
        base_log_folder = conf.get("logging", "base_log_folder")
        log_path = os.path.join(
            base_log_folder,
            "dags",
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            f"{ti.try_number}.log",
        )
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # Build job parameters
        job_params: Dict[str, Any] = {
            "name": job_name,
            "partition": config.get("partition", self.default_partition),
            "tasks": 1,
            "cpus_per_task": config.get("cpus_per_task", self.default_cpus),
            "memory_per_node": config.get("mem", self.default_mem),
            "time_limit": self._convert_time_to_minutes(
                config.get("time_limit", self.default_time_limit)
            ),
            "current_working_directory": config.get("working_dir", self.airflow_home),
            "environment": self._build_workload_environment(workload),
            "standard_output": log_path,
            "standard_error": log_path,
        }

        # Optional parameters
        if self.default_account or config.get("account"):
            job_params["account"] = config.get("account", self.default_account)
        if config.get("qos"):
            job_params["qos"] = config["qos"]
        if config.get("gres"):
            gres_value = config["gres"]
            if not gres_value.startswith("gres/"):
                gres_value = f"gres/{gres_value}"
            job_params["tres_per_node"] = gres_value
        if config.get("constraint"):
            job_params["constraints"] = config["constraint"]
        if config.get("nodes") is not None:
            job_params["minimum_nodes"] = config["nodes"]
        if config.get("ntasks_per_node") is not None:
            job_params["tasks_per_node"] = config["ntasks_per_node"]
        if config.get("exclusive"):
            job_params["shared"] = ["none"]
        if config.get("nodelist"):
            job_params["required_nodes"] = [config["nodelist"]]

        job_spec = {"job": job_params}
        return job_spec, array_spec, dependency_spec

    def _build_workload_environment(
        self, workload: "workloads.ExecuteTask"
    ) -> List[str]:
        """Build environment variables for a workload execution.

        Returns a list of KEY=value strings as required by the Slurm REST API.
        """
        env = os.environ.copy()

        if "PATH" not in env:
            env["PATH"] = "/usr/local/bin:/usr/bin:/bin"
        if "USER" not in env:
            env["USER"] = os.environ.get("USER", "airflow")
        if "HOME" not in env:
            env["HOME"] = os.path.expanduser("~")

        env.update(
            {
                "AIRFLOW_HOME": self.airflow_home,
                "AIRFLOW__CORE__DAGS_FOLDER": conf.get("core", "dags_folder"),
                "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            }
        )

        # Slurm REST API requires environment as array of "KEY=value" strings
        return [f"{k}={v}" for k, v in env.items()]

    def _build_job_spec(
        self,
        key: TaskInstanceKey,
        command: Sequence[str],
        queue: Optional[str],
        executor_config: Optional[Dict[str, Any]],
    ) -> tuple[Dict[str, Any], Optional[str], Optional[str]]:
        """Build Slurm job specification for a task.

        Args:
            key: Task instance key
            command: Command to execute
            queue: Airflow queue name
            executor_config: Task-specific configuration

        Returns:
            Tuple of (job_spec_dict, array_spec_or_none, dependency_spec_or_none)
        """
        config = executor_config or {}

        # Extract array and dependency parameters (don't include in job params)
        array_spec = config.get("array")
        dependency_spec = config.get("dependency")

        # Build job name
        job_name = self._build_job_name(key)

        # Determine log path
        log_path = self._get_log_path(key)

        # Build script
        script = self._build_script(command)

        # Build job parameters
        job_params = {
            "name": job_name,
            "partition": config.get("partition", queue or self.default_partition),
            "tasks": 1,  # Single task per job
            "cpus_per_task": config.get("cpus_per_task", self.default_cpus),
            "memory_per_node": config.get("mem", self.default_mem),
            "time_limit": self._convert_time_to_minutes(
                config.get("time_limit", self.default_time_limit)
            ),
            "current_working_directory": config.get("working_dir", self.airflow_home),
            "environment": self._build_environment(key),
            "standard_output": log_path,
            "standard_error": log_path,
        }

        # Add optional parameters
        if self.default_account or config.get("account"):
            job_params["account"] = config.get("account", self.default_account)

        if config.get("qos"):
            job_params["qos"] = config["qos"]

        # Container support
        container = config.get("container", self.default_container)
        if container:
            job_params["container"] = container

        # GRES (Generic RESource) support - for GPU, MIC, etc.
        if config.get("gres"):
            # Slurm REST API uses tres_per_node for GRES (e.g., "gpu:1" -> "gres/gpu:1")
            gres_value = config["gres"]
            if not gres_value.startswith("gres/"):
                gres_value = f"gres/{gres_value}"
            job_params["tres_per_node"] = gres_value

        # Node constraints for heterogeneous clusters
        if config.get("constraint"):
            job_params["constraints"] = config["constraint"]

        # Node allocation
        if config.get("nodes") is not None:
            job_params["minimum_nodes"] = config["nodes"]

        # Tasks per node
        if config.get("ntasks_per_node") is not None:
            job_params["tasks_per_node"] = config["ntasks_per_node"]

        # Exclusive allocation
        if config.get("exclusive"):
            # Use "shared" field (works across API versions; "exclusive" deprecated in v0.0.42+)
            job_params["shared"] = ["none"]

        # Node list specification
        if config.get("nodelist"):
            # Slurm REST API uses "required_nodes" field (list of node names)
            job_params["required_nodes"] = [config["nodelist"]]

        job_spec = {
            "script": script,
            "job": job_params,
        }

        return job_spec, array_spec, dependency_spec

    def _build_job_name(self, key: TaskInstanceKey) -> str:
        """Build Slurm job name that encodes task identity.

        Format: airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}
        """
        # Get run_id (standard in Airflow 3.x, fallback for older versions)
        run_id = getattr(key, "run_id", None)
        if run_id is None:
            # Fallback for older Airflow versions
            execution_date = getattr(key, "execution_date", None)
            run_id = str(execution_date) if execution_date else "unknown"

        # Hash run_id for compactness
        run_id_hash = hashlib.sha256(run_id.encode()).hexdigest()[:8]

        # Sanitize IDs
        dag_id = key.dag_id.replace("/", "_").replace(".", "_")
        task_id = key.task_id.replace("/", "_").replace(".", "_")

        job_name = f"airflow-{dag_id}-{task_id}-{run_id_hash}-{key.try_number}"

        # Slurm job name limit is typically 256 chars
        if len(job_name) > 256:
            # Truncate dag_id and task_id to fit
            max_id_length = (256 - 20) // 2
            dag_id = dag_id[:max_id_length]
            task_id = task_id[:max_id_length]
            job_name = f"airflow-{dag_id}-{task_id}-{run_id_hash}-{key.try_number}"

        return job_name

    def _build_script(self, command: Sequence[str]) -> str:
        """Build bash script for job execution."""
        lines = [
            "#!/bin/bash",
            "set -euo pipefail",
            "",
        ]

        # Add virtual environment activation if configured
        if self.airflow_venv and not self.default_container:
            lines.extend(
                [
                    "# Activate virtual environment",
                    f"source {self.airflow_venv}/bin/activate",
                    "",
                ]
            )

        # Add the actual command
        lines.extend(
            [
                "# Execute Airflow task",
                " ".join(shlex.quote(arg) for arg in command),
            ]
        )

        return "\n".join(lines)

    def _build_environment(self, key: TaskInstanceKey) -> Dict[str, str]:
        """Build environment variables for job."""
        env = os.environ.copy()

        # Ensure critical system variables are set
        if "PATH" not in env:
            env["PATH"] = "/usr/local/bin:/usr/bin:/bin"
        if "USER" not in env:
            env["USER"] = os.environ.get("USER", "airflow")
        if "HOME" not in env:
            env["HOME"] = os.path.expanduser("~")

        # Ensure critical Airflow variables are set
        env.update(
            {
                "AIRFLOW_HOME": self.airflow_home,
                "AIRFLOW__CORE__DAGS_FOLDER": conf.get("core", "dags_folder"),
                "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",  # Tasks run in local mode
            }
        )

        return env

    def _get_log_path(self, key: TaskInstanceKey) -> str:
        """Determine log file path for a task instance."""
        # Get base log folder
        base_log_folder = conf.get("logging", "base_log_folder")

        # Build path components
        dag_id = key.dag_id
        task_id = key.task_id
        try_number = key.try_number

        # Handle run_id (Airflow 3.x) or execution_date (older versions)
        if hasattr(key, "run_id"):
            # Use run_id for Airflow 3.x
            execution_date_str = key.run_id
        else:
            # Fallback for older versions
            execution_date = getattr(key, "execution_date", None)
            execution_date_str = (
                execution_date.strftime("%Y-%m-%dT%H:%M:%S%z")
                if execution_date
                else "unknown"
            )

        # Construct path
        log_path = os.path.join(
            base_log_folder,
            "dags",
            dag_id,
            task_id,
            execution_date_str,
            f"{try_number}.log",
        )

        # Ensure directory exists
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        return log_path

    def sync(self) -> None:
        """Poll Slurm for job status and update task states."""
        # Throttle sync calls
        now = time.time()
        if now - self.last_sync_time < self.sync_interval:
            return
        self.last_sync_time = now

        if not self.running:
            return

        logger.debug(f"Syncing status for {len(self.running)} running tasks")

        try:
            assert self.slurm_client is not None, "Executor not started"

            # Update state for each tracked task
            for key, job_info in list(self.running.items()):
                slurm_job_id = job_info["slurm_job_id"]
                is_array = job_info.get("is_array", False)

                try:
                    if is_array:
                        # For array jobs, get aggregated status
                        array_status = self.slurm_client.get_array_status(slurm_job_id)
                        self._handle_array_job_state(key, array_status)
                    else:
                        # For single jobs, get individual status
                        job_data = self.slurm_client.get_job(slurm_job_id)
                        if job_data:
                            self._handle_job_state(key, job_data)
                        else:
                            # Job not found - check history or mark missing
                            self._handle_missing_job(key, job_info)

                except SlurmAPIError as e:
                    logger.debug(f"Could not query status for job {slurm_job_id}: {e}")
                    self._handle_missing_job(key, job_info)

        except Exception as e:
            logger.error(f"Error during sync: {e}")

    def _handle_job_state(self, key: TaskInstanceKey, job_data: Dict[str, Any]) -> None:
        """Process Slurm job state and update Airflow task state."""
        state = job_data.get("job_state", "UNKNOWN")
        # Slurm REST API v0.0.41+ returns job_state as a list
        if isinstance(state, list):
            state = state[0] if state else "UNKNOWN"

        logger.debug(f"Task {key} job {job_data['job_id']} in state {state}")

        # States that don't require action
        if state in ["PENDING", "CONFIGURING", "RUNNING"]:
            return

        # Job completed
        if state == "COMPLETED":
            raw_exit_code = job_data.get("exit_code", 0)
            # Slurm REST API v0.0.41+ returns exit_code as a dict
            if isinstance(raw_exit_code, dict):
                rc = raw_exit_code.get("return_code", {})
                exit_code = rc.get("number", 0) if isinstance(rc, dict) else rc
            else:
                exit_code = raw_exit_code
            if exit_code == 0:
                self.success(key)
                logger.info(f"Task {key} succeeded")
            else:
                self.fail(key)
                logger.error(f"Task {key} failed with exit code {exit_code}")
            del self.running[key]
            return

        # Job failed
        if state in [
            "FAILED",
            "TIMEOUT",
            "CANCELLED",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "DEPENDENCY_NEVER_SATISFIED",
        ]:
            reason = job_data.get("state_reason", "unknown")
            self.fail(key)
            if state == "DEPENDENCY_NEVER_SATISFIED":
                logger.error(
                    f"Task {key} failed: dependency not satisfied "
                    f"(dependent job may have failed) - {reason}"
                )
            else:
                logger.error(f"Task {key} failed: {state} - {reason}")
            del self.running[key]
            return

        # Unknown state
        logger.warning(f"Unknown Slurm state '{state}' for task {key}")

    def _handle_array_job_state(
        self, key: TaskInstanceKey, array_status: Dict[str, Any]
    ) -> None:
        """Process aggregated array job state and update Airflow task state.

        Args:
            key: Task instance key
            array_status: Aggregated array status from get_array_status()
        """
        state = array_status.get("state", "UNKNOWN")
        job_id = array_status.get("job_id")
        total_tasks = array_status.get("total_tasks", 0)
        completed = array_status.get("completed", 0)
        failed = array_status.get("failed", 0)
        running = array_status.get("running", 0)

        logger.debug(
            f"Task {key} array job {job_id} in state {state} "
            f"({completed}/{total_tasks} completed, {failed} failed, {running} running)"
        )

        # States that don't require action (still processing)
        if state in ["PENDING", "RUNNING"]:
            return

        # All tasks completed successfully
        if state == "COMPLETED":
            self.success(key)
            logger.info(
                f"Task {key} array job {job_id} completed successfully "
                f"({total_tasks} tasks)"
            )
            del self.running[key]
            return

        # Some tasks failed, some succeeded
        if state == "PARTIALLY_COMPLETED":
            # Mark task as failed since not all array tasks succeeded
            self.fail(key)
            logger.error(
                f"Task {key} array job {job_id} partially completed: "
                f"{completed} succeeded, {failed} failed"
            )
            del self.running[key]
            return

        # All tasks failed or job cancelled
        if state in ["FAILED", "CANCELLED"]:
            self.fail(key)
            logger.error(
                f"Task {key} array job {job_id} failed: {state} "
                f"({failed}/{total_tasks} tasks failed)"
            )
            del self.running[key]
            return

        # Unknown state
        logger.warning(f"Unknown array job state '{state}' for task {key}")

    def _handle_missing_job(
        self, key: TaskInstanceKey, job_info: Dict[str, Any]
    ) -> None:
        """Handle job that's not in active queue."""
        slurm_job_id = job_info["slurm_job_id"]

        # Try to get job from history
        try:
            assert self.slurm_client is not None, "Executor not started"
            job_history = self.slurm_client.get_job_history(slurm_job_id)

            if job_history:
                # Found in history - process final state
                self._handle_job_state(key, job_history)
                return

        except SlurmAPIError as e:
            logger.debug(f"Could not query history for job {slurm_job_id}: {e}")

        # Track how long it's been missing
        if "missing_since" not in job_info:
            job_info["missing_since"] = datetime.now()
            logger.debug(f"Job {slurm_job_id} not found in active queue, tracking")
            return

        # Check timeout
        missing_duration = datetime.now() - job_info["missing_since"]
        if missing_duration > timedelta(minutes=5):
            self.fail(key)
            logger.error(
                f"Task {key} job {slurm_job_id} missing from Slurm "
                f"for {missing_duration}, marking as failed"
            )
            del self.running[key]

    def end(self) -> None:
        """Gracefully shutdown the executor."""
        if not self.running:
            logger.info("SlurmExecutor shutdown: no running jobs")
            return

        logger.info(f"SlurmExecutor shutdown: {len(self.running)} jobs running")

        if self.shutdown_mode == "cancel":
            self._cancel_all_jobs()
        elif self.shutdown_mode == "wait":
            self._wait_for_jobs(timeout=self.shutdown_wait_timeout)
        else:
            logger.warning(
                f"Unknown shutdown_mode: {self.shutdown_mode}, cancelling jobs"
            )
            self._cancel_all_jobs()

    def _cancel_all_jobs(self) -> None:
        """Cancel all tracked Slurm jobs."""
        assert self.slurm_client is not None, "Executor not started"
        for key, job_info in list(self.running.items()):
            slurm_job_id = job_info["slurm_job_id"]
            try:
                self.slurm_client.cancel_job(slurm_job_id)
                logger.info(f"Cancelled job {slurm_job_id} for task {key}")
            except SlurmAPIError as e:
                logger.warning(f"Failed to cancel job {slurm_job_id}: {e}")

            self.fail(key)

        self.running.clear()

    def _wait_for_jobs(self, timeout: int) -> None:
        """Wait for jobs to complete, then cancel remaining."""
        start_time = time.time()

        while self.running and (time.time() - start_time) < timeout:
            self.sync()
            time.sleep(5)

        if self.running:
            logger.warning(
                f"Timeout waiting for jobs after {timeout}s, "
                f"cancelling {len(self.running)} remaining"
            )
            self._cancel_all_jobs()

    def terminate(self) -> None:
        """Emergency shutdown - kill everything immediately."""
        logger.warning("SlurmExecutor emergency terminate: killing all jobs")

        if self.slurm_client is None:
            return

        # Best-effort cancellation, ignore errors
        for _key, job_info in self.running.items():
            try:
                self.slurm_client.cancel_job(job_info["slurm_job_id"])
            except Exception:
                pass  # Ignore all errors in emergency shutdown

        self.running.clear()

    def try_adopt_task_instances(
        self, tis: Sequence[TaskInstance]
    ) -> List[TaskInstance]:
        """Adopt running tasks after scheduler restart.

        Args:
            tis: List of task instances to potentially adopt

        Returns:
            List of successfully adopted task instances
        """
        if not tis:
            return []

        logger.info(f"Attempting to adopt {len(tis)} task instances")

        adopted = []

        try:
            # Query Slurm for all jobs
            assert self.slurm_client is not None, "Executor not started"
            response = self.slurm_client.get_jobs()
            jobs = response.get("jobs", [])

            # Build lookup by job name
            slurm_jobs = {}
            for job in jobs:
                job_name = job.get("name", "")
                if job_name.startswith("airflow-"):
                    slurm_jobs[job_name] = {
                        "job_id": job["job_id"],
                        "state": job.get("job_state", ""),
                    }

            logger.info(f"Found {len(slurm_jobs)} Airflow jobs in Slurm queue")

            # Try to match each TI to a Slurm job
            for ti in tis:
                key = ti.key
                expected_job_name = self._build_job_name(key)

                if expected_job_name in slurm_jobs:
                    job_info = slurm_jobs[expected_job_name]
                    job_id = job_info["job_id"]
                    state = job_info["state"]

                    # Only adopt if job is still active
                    if state in ["PENDING", "CONFIGURING", "RUNNING"]:
                        # Reconstruct tracking state
                        self.running[key] = {
                            "slurm_job_id": job_id,
                            "command": [],  # Unknown, but not needed
                            "submit_time": datetime.now(),  # Approximate
                        }
                        adopted.append(ti)
                        logger.info(
                            f"Adopted task {key.dag_id}.{key.task_id} "
                            f"as Slurm job {job_id} in state {state}"
                        )
                    else:
                        logger.info(
                            f"Task {key.dag_id}.{key.task_id} job {job_id} "
                            f"already in terminal state {state}, not adopting"
                        )

            logger.info(f"Successfully adopted {len(adopted)} of {len(tis)} tasks")
            return adopted

        except SlurmAPIError as e:
            logger.error(f"Failed to query Slurm for task adoption: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during task adoption: {e}", exc_info=True)
            return []
