"""Slurm Hook for Apache Airflow.

This hook provides a reusable interface to interact with Slurm clusters
via the REST API. It can be used by operators, sensors, and other components.
"""

import logging
import warnings
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

# Airflow 2.x/3.x compatibility for BaseHook import
# Suppress deprecation warnings during import to support both versions
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    try:
        # Airflow 3.x
        from airflow.hooks.base import BaseHook  # type: ignore[import-untyped]
    except ImportError:
        try:
            # Airflow 2.x
            from airflow.hooks.base_hook import BaseHook  # type: ignore[import-untyped, no-redef]
        except ImportError:
            # Fallback for older versions
            from airflow.hooks.base import BaseHook  # type: ignore[import-untyped, no-redef]

from airflow_provider_slurm.exceptions import SlurmAPIError, SlurmConfigurationError
from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager

logger = logging.getLogger(__name__)


class SlurmHook(BaseHook):
    """Hook to interact with Slurm via REST API.

    This hook provides methods for common Slurm operations including job
    submission, status checking, cancellation, and history queries.

    Args:
        slurm_conn_id: Airflow connection ID for Slurm credentials
        api_url: Slurm REST API URL (overrides connection)
        username: Slurm username (overrides connection)
        token_lifespan: JWT token lifespan in seconds
        api_timeout: API request timeout in seconds
        api_max_retries: Maximum number of retry attempts
    """

    conn_name_attr = "slurm_conn_id"
    default_conn_name = "slurm_default"
    conn_type = "slurm"
    hook_name = "Slurm"

    def __init__(
        self,
        slurm_conn_id: str = default_conn_name,
        api_url: Optional[str] = None,
        username: Optional[str] = None,
        token_lifespan: int = 3600,
        api_timeout: int = 30,
        api_max_retries: int = 3,
    ) -> None:
        """Initialize the Slurm hook."""
        super().__init__()
        self.slurm_conn_id = slurm_conn_id
        self._api_url = api_url
        self._username = username
        self.token_lifespan = token_lifespan
        self.api_timeout = api_timeout
        self.api_max_retries = api_max_retries

        self._token_manager: Optional[SlurmTokenManager] = None
        self._client: Optional[SlurmAPIClient] = None

    def get_conn(self) -> SlurmAPIClient:
        """Get or create Slurm API client.

        Returns:
            SlurmAPIClient: Configured Slurm API client

        Raises:
            SlurmConfigurationError: If connection is not properly configured
        """
        if self._client is not None:
            return self._client

        # Get connection details
        api_url, username = self._get_connection_details()

        # Initialize token manager
        self._token_manager = SlurmTokenManager(
            username=username,
            lifespan=self.token_lifespan,
        )

        # Initialize API client
        self._client = SlurmAPIClient(
            base_url=api_url,
            token_manager=self._token_manager,
            timeout=self.api_timeout,
            max_retries=self.api_max_retries,
        )

        logger.info(f"Initialized Slurm connection to {api_url}")
        return self._client

    def _get_connection_details(self) -> Tuple[str, Optional[str]]:
        """Get connection details from Airflow connection or parameters.

        Returns:
            Tuple of (api_url, username)

        Raises:
            SlurmConfigurationError: If API URL is not configured
        """
        # Use explicit parameters if provided
        if self._api_url:
            return self._api_url, self._username

        # Try to get from Airflow connection
        try:
            conn = self.get_connection(self.slurm_conn_id)

            # Validate host is present
            if not conn.host:
                raise SlurmConfigurationError(
                    f"Host not configured in connection {self.slurm_conn_id}"
                )

            # Build URL with proper scheme
            scheme = conn.schema if conn.schema else "https"
            host = conn.host

            # Construct base URL
            if conn.port:
                api_url = f"{scheme}://{host}:{conn.port}"
            else:
                api_url = f"{scheme}://{host}"

            username = conn.login or None

            return api_url, username

        except Exception as e:
            raise SlurmConfigurationError(
                f"Failed to get Slurm connection details: {e}"
            ) from e

    def test_connection(self) -> Tuple[bool, str]:
        """Test the Slurm API connection.

        Returns:
            Tuple of (success, message)
        """
        try:
            client = self.get_conn()
            if client.ping():
                return True, "Connection successful"
            else:
                return False, "Failed to ping Slurm API"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def submit_job(
        self,
        script: str,
        job_name: str,
        partition: Optional[str] = None,
        cpus_per_task: int = 1,
        mem: str = "4G",
        time_limit: str = "01:00:00",
        working_dir: Optional[str] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        environment: Optional[Dict[str, str]] = None,
        gres: Optional[str] = None,
        constraint: Optional[str] = None,
        account: Optional[str] = None,
        qos: Optional[str] = None,
        nodes: Optional[int] = None,
        ntasks_per_node: Optional[int] = None,
        exclusive: bool = False,
        nodelist: Optional[str] = None,
        array: Optional[str] = None,
        dependency: Optional[str] = None,
        **kwargs: Any,
    ) -> int:
        """Submit a job to Slurm.

        Args:
            script: Bash script content to execute
            job_name: Name for the Slurm job
            partition: Slurm partition to use
            cpus_per_task: Number of CPUs per task
            mem: Memory limit (e.g., "4G", "512M")
            time_limit: Time limit in HH:MM:SS format
            working_dir: Working directory for the job
            stdout: Path for standard output
            stderr: Path for standard error
            environment: Environment variables
            gres: Generic resources (e.g., "gpu:2")
            constraint: Node constraints
            account: Slurm account
            qos: Quality of Service
            nodes: Number of nodes to allocate (Slurm -N flag)
            ntasks_per_node: Number of tasks per node (Slurm --ntasks-per-node flag)
            exclusive: Allocate nodes exclusively (Slurm --exclusive flag)
            nodelist: Specific nodes to target (Slurm --nodelist flag, e.g., "node[01-04]", "gpu001,gpu002")
            array: Array specification (e.g., "0-99", "1-100:2", "0-99%10")
            dependency: Dependency specification (e.g., "afterok:12345")
            **kwargs: Additional job parameters

        Returns:
            Job ID of submitted job (parent job ID for arrays)

        Raises:
            SlurmAPIError: If job submission fails

        Examples:
            >>> # Submit single job
            >>> hook.submit_job(script="#!/bin/bash\\necho 'test'", job_name="test_job")
            12345

            >>> # Submit array job with 100 tasks
            >>> hook.submit_job(
            ...     script="#!/bin/bash\\necho $SLURM_ARRAY_TASK_ID",
            ...     job_name="array_job",
            ...     array="0-99"
            ... )
            12346  # Parent job ID

            >>> # Submit job with dependency
            >>> hook.submit_job(
            ...     script="#!/bin/bash\\necho 'dependent job'",
            ...     job_name="dependent_job",
            ...     dependency="afterok:12345"
            ... )
            12347

            >>> # Submit job with multiple dependencies (AND logic)
            >>> hook.submit_job(
            ...     script="#!/bin/bash\\necho 'merge job'",
            ...     job_name="merge_job",
            ...     dependency="afterok:12345:12346"
            ... )
            12348

            >>> # Submit array job with dependency
            >>> hook.submit_job(
            ...     script="#!/bin/bash\\necho $SLURM_ARRAY_TASK_ID",
            ...     job_name="dependent_array",
            ...     array="0-99",
            ...     dependency="afterok:12347"
            ... )
            12349
        """
        client = self.get_conn()

        # Build job specification
        job_params: Dict[str, Any] = {
            "name": job_name,
            "tasks": 1,
            "cpus_per_task": cpus_per_task,
            "memory_per_node": mem,
            "time_limit": self._convert_time_to_minutes(time_limit),
        }

        # Add optional parameters
        if partition:
            job_params["partition"] = partition
        job_params["current_working_directory"] = working_dir or "/tmp"
        if stdout:
            job_params["standard_output"] = stdout
        if stderr:
            job_params["standard_error"] = stderr
        if environment:
            job_params["environment"] = environment
        else:
            # Slurm REST API requires environment to be set
            job_params["environment"] = ["PATH=/usr/bin:/usr/local/bin:/bin"]
        if gres:
            # Slurm REST API uses tres_per_node for GRES (e.g., "gpu:1" -> "gres/gpu:1")
            gres_value = gres if gres.startswith("gres/") else f"gres/{gres}"
            job_params["tres_per_node"] = gres_value
        if constraint:
            job_params["constraints"] = constraint
        if account:
            job_params["account"] = account
        if qos:
            job_params["qos"] = qos
        if nodes is not None:
            job_params["minimum_nodes"] = nodes
        if ntasks_per_node is not None:
            job_params["tasks_per_node"] = ntasks_per_node
        if exclusive:
            # Use "shared" field (works across API versions; "exclusive" deprecated in v0.0.42+)
            job_params["shared"] = ["none"]
        if nodelist:
            # Slurm REST API uses "required_nodes" field (list of node names)
            job_params["required_nodes"] = [nodelist]

        # Add any additional parameters
        job_params.update(kwargs)

        job_spec = {
            "script": script,
            "job": job_params,
        }

        # Build log message
        log_parts = [f"Submitting job {job_name} to Slurm"]
        if array:
            log_parts.append(f"(array: {array})")
        if dependency:
            log_parts.append(f"(dependency: {dependency})")
        logger.info(" ".join(log_parts))

        result = client.submit_job(job_spec, array=array, dependency=dependency)

        job_id = result.get("job_id")
        if not job_id:
            raise SlurmAPIError(f"No job_id returned for job {job_name}")

        # Build success log message
        success_parts = []
        if array:
            task_count = result.get("array_task_count", "unknown")
            success_parts.append(
                f"Array job {job_name} submitted with ID {job_id} ({task_count} tasks)"
            )
        else:
            success_parts.append(f"Job {job_name} submitted with ID {job_id}")

        if dependency:
            success_parts.append(f"(dependency: {dependency})")

        logger.info(" ".join(success_parts))

        return int(job_id)

    def get_job_status(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get status of a specific job.

        Args:
            job_id: Slurm job ID

        Returns:
            Job status information or None if not found
        """
        client = self.get_conn()
        job_info = client.get_job(job_id)
        return job_info

    def get_jobs(
        self, job_ids: Optional[Sequence[Union[int, str]]] = None
    ) -> List[Dict[str, Any]]:
        """Get status of multiple jobs.

        Args:
            job_ids: List of job IDs to query, or None for all jobs

        Returns:
            List of job status dictionaries
        """
        client = self.get_conn()
        # Convert Sequence to List for API client
        job_ids_list = list(job_ids) if job_ids is not None else None
        result = client.get_jobs(job_ids=job_ids_list)
        return result.get("jobs", [])

    def cancel_job(self, job_id: int) -> bool:
        """Cancel a running job.

        Args:
            job_id: Slurm job ID to cancel

        Returns:
            True if cancellation was successful
        """
        client = self.get_conn()
        result = client.cancel_job(job_id)
        return result is not None

    def wait_for_job(
        self,
        job_id: int,
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> str:
        """Wait for a job to complete.

        Args:
            job_id: Slurm job ID to wait for
            timeout: Maximum wait time in seconds
            poll_interval: Interval between status checks in seconds

        Returns:
            Final job state

        Raises:
            SlurmAPIError: If job fails or times out
        """
        import time

        start_time = time.time()
        while time.time() - start_time < timeout:
            job_info = self.get_job_status(job_id)

            if job_info is None:
                raise SlurmAPIError(f"Job {job_id} not found")

            state = job_info.get("job_state", "UNKNOWN")
            # Slurm REST API v0.0.41+ returns job_state as a list
            if isinstance(state, list):
                state = state[0] if state else "UNKNOWN"

            # Terminal states
            if state == "COMPLETED":
                logger.info(f"Job {job_id} completed successfully")
                return state
            elif state in ["FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"]:
                raise SlurmAPIError(f"Job {job_id} failed with state {state}")

            # Still running
            logger.debug(f"Job {job_id} is in state {state}, waiting...")
            time.sleep(poll_interval)

        raise SlurmAPIError(f"Job {job_id} did not complete within {timeout} seconds")

    def get_job_history(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get historical information for a completed job.

        Args:
            job_id: Slurm job ID

        Returns:
            Historical job information or None if not found
        """
        client = self.get_conn()
        return client.get_job_history(job_id)

    def get_array_status(self, job_id: int) -> Dict[str, Any]:
        """Get aggregated status for an array job.

        Args:
            job_id: Array job ID (parent job ID)

        Returns:
            Dictionary with array job status including:
                - job_id: Array job ID
                - total_tasks: Total number of tasks
                - completed: Number of completed tasks
                - running: Number of running tasks
                - pending: Number of pending tasks
                - failed: Number of failed tasks
                - state: Aggregated state
                - tasks: List of individual task details

        Raises:
            SlurmAPIError: If query fails

        Example:
            >>> hook.get_array_status(12345)
            {
                'job_id': 12345,
                'total_tasks': 100,
                'completed': 95,
                'running': 3,
                'pending': 0,
                'failed': 2,
                'state': 'PARTIALLY_COMPLETED',
                ...
            }
        """
        client = self.get_conn()
        return client.get_array_status(job_id)

    def wait_for_array(
        self,
        job_id: int,
        timeout: int = 3600,
        poll_interval: int = 10,
        fail_on_error: bool = True,
    ) -> Dict[str, Any]:
        """Wait for an array job to complete.

        Args:
            job_id: Array job ID to wait for
            timeout: Maximum wait time in seconds
            poll_interval: Interval between status checks in seconds
            fail_on_error: If True, raise exception on any task failure

        Returns:
            Final array status dictionary

        Raises:
            SlurmAPIError: If array job fails or times out

        Example:
            >>> # Wait for all tasks to complete
            >>> status = hook.wait_for_array(12345)
            >>> print(f"Completed {status['completed']}/{status['total_tasks']} tasks")
        """
        import time

        start_time = time.time()
        while time.time() - start_time < timeout:
            array_status = self.get_array_status(job_id)
            state = array_status.get("state", "UNKNOWN")

            logger.debug(
                f"Array job {job_id}: {state} "
                f"({array_status['completed']}/{array_status['total_tasks']} completed)"
            )

            # Check terminal states
            if state == "COMPLETED":
                logger.info(
                    f"Array job {job_id} completed successfully "
                    f"({array_status['total_tasks']} tasks)"
                )
                return array_status

            elif state == "FAILED":
                if fail_on_error:
                    raise SlurmAPIError(
                        f"Array job {job_id} failed: "
                        f"{array_status['failed']}/{array_status['total_tasks']} tasks failed"
                    )
                else:
                    logger.warning(f"Array job {job_id} failed, but continuing")
                    return array_status

            elif state == "PARTIALLY_COMPLETED":
                if fail_on_error:
                    raise SlurmAPIError(
                        f"Array job {job_id} partially completed: "
                        f"{array_status['failed']} tasks failed"
                    )
                else:
                    logger.warning(
                        f"Array job {job_id} partially completed "
                        f"({array_status['completed']} succeeded, {array_status['failed']} failed)"
                    )
                    return array_status

            # Still running
            time.sleep(poll_interval)

        raise SlurmAPIError(
            f"Array job {job_id} did not complete within {timeout} seconds"
        )

    def cancel_array_task(
        self, job_id: int, array_task_id: Optional[int] = None
    ) -> bool:
        """Cancel an array job or specific array task.

        Args:
            job_id: Array job ID
            array_task_id: Specific task ID to cancel (None = cancel all)

        Returns:
            True if cancellation was successful

        Examples:
            >>> # Cancel entire array
            >>> hook.cancel_array_task(12345)

            >>> # Cancel specific task
            >>> hook.cancel_array_task(12345, array_task_id=5)
        """
        client = self.get_conn()
        result = client.cancel_array_task(job_id, array_task_id)
        return result is not None

    @staticmethod
    def _convert_time_to_minutes(time_str: str) -> int:
        """Convert time string HH:MM:SS to minutes for Slurm REST API.

        The Slurm REST API interprets the time_limit integer as minutes.

        Args:
            time_str: Time string in HH:MM:SS format

        Returns:
            Time in minutes
        """
        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            total_minutes = hours * 60 + minutes
            if seconds > 0:
                total_minutes += 1  # Round up partial minutes
            return total_minutes
        elif len(parts) == 2:
            minutes, seconds = map(int, parts)
            if seconds > 0:
                minutes += 1
            return minutes
        else:
            return int(parts[0])

    def close(self) -> None:
        """Close the connection and clean up resources."""
        self._client = None
        self._token_manager = None
