"""Slurm Hook for Apache Airflow.

This hook provides a reusable interface to interact with Slurm clusters
via the REST API. It can be used by operators, sensors, and other components.
"""

import logging
from typing import Any, Dict, List, Optional

from airflow.hooks.base import BaseHook

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

    def _get_connection_details(self) -> tuple[str, Optional[str]]:
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
            api_url = conn.host
            if conn.port:
                api_url = f"{api_url}:{conn.port}"
            if conn.schema:
                api_url = f"{conn.schema}://{api_url}"

            username = conn.login or None

            if not api_url:
                raise SlurmConfigurationError(
                    f"Slurm API URL not configured in connection {self.slurm_conn_id}"
                )

            return api_url, username

        except Exception as e:
            raise SlurmConfigurationError(
                f"Failed to get Slurm connection details: {e}"
            ) from e

    def test_connection(self) -> tuple[bool, str]:
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
            **kwargs: Additional job parameters

        Returns:
            Job ID of submitted job

        Raises:
            SlurmAPIError: If job submission fails
        """
        client = self.get_conn()

        # Build job specification
        job_params: Dict[str, Any] = {
            "name": job_name,
            "tasks": 1,
            "cpus_per_task": cpus_per_task,
            "memory_per_node": mem,
            "time_limit": self._convert_time_to_seconds(time_limit),
        }

        # Add optional parameters
        if partition:
            job_params["partition"] = partition
        if working_dir:
            job_params["current_working_directory"] = working_dir
        if stdout:
            job_params["standard_output"] = stdout
        if stderr:
            job_params["standard_error"] = stderr
        if environment:
            job_params["environment"] = environment
        if gres:
            job_params["gres"] = gres
        if constraint:
            job_params["constraints"] = constraint
        if account:
            job_params["account"] = account
        if qos:
            job_params["qos"] = qos

        # Add any additional parameters
        job_params.update(kwargs)

        job_spec = {
            "script": script,
            "job": job_params,
        }

        logger.info(f"Submitting job {job_name} to Slurm")
        result = client.submit_job(job_spec)

        job_id = result.get("job_id")
        if not job_id:
            raise SlurmAPIError(f"No job_id returned for job {job_name}")

        logger.info(f"Job {job_name} submitted with ID {job_id}")
        return job_id

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
        self, job_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        """Get status of multiple jobs.

        Args:
            job_ids: List of job IDs to query, or None for all jobs

        Returns:
            List of job status dictionaries
        """
        client = self.get_conn()
        result = client.get_jobs(job_ids=job_ids)
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

            # Terminal states
            if state == "COMPLETED":
                logger.info(f"Job {job_id} completed successfully")
                return state
            elif state in ["FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"]:
                raise SlurmAPIError(f"Job {job_id} failed with state {state}")

            # Still running
            logger.debug(f"Job {job_id} is in state {state}, waiting...")
            time.sleep(poll_interval)

        raise SlurmAPIError(
            f"Job {job_id} did not complete within {timeout} seconds"
        )

    def get_job_history(
        self, job_id: int
    ) -> Optional[Dict[str, Any]]:
        """Get historical information for a completed job.

        Args:
            job_id: Slurm job ID

        Returns:
            Historical job information or None if not found
        """
        client = self.get_conn()
        return client.get_job_history(job_id)

    @staticmethod
    def _convert_time_to_seconds(time_str: str) -> int:
        """Convert time string HH:MM:SS to seconds.

        Args:
            time_str: Time string in HH:MM:SS format

        Returns:
            Time in seconds
        """
        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
        else:
            return int(parts[0])

    def close(self) -> None:
        """Close the connection and clean up resources."""
        self._client = None
        self._token_manager = None
