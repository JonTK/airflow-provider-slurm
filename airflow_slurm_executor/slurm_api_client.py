"""Slurm REST API client for job management."""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import requests

from airflow_slurm_executor.exceptions import SlurmAPIError
from airflow_slurm_executor.slurm_token_manager import SlurmTokenManager

logger = logging.getLogger(__name__)


class SlurmAPIClient:
    """Client for interacting with Slurm REST API.

    This class provides methods to:
    - Submit jobs to Slurm
    - Query job status
    - Cancel jobs
    - Access job history from accounting database
    """

    def __init__(
        self,
        base_url: str,
        token_manager: SlurmTokenManager,
        api_version: str = "v0.0.42",
        timeout: int = 30,
        max_retries: int = 3,
    ) -> None:
        """Initialize the API client.

        Args:
            base_url: Base URL for Slurm REST API (e.g., https://slurm-host:6820)
            token_manager: Token manager instance for authentication
            api_version: Slurm REST API version. Default v0.0.42
            timeout: Request timeout in seconds. Default 30
            max_retries: Maximum retry attempts for failed requests. Default 3
        """
        self.base_url = base_url.rstrip("/")
        self.token_manager = token_manager
        self.api_version = api_version
        self.timeout = timeout
        self.max_retries = max_retries

        # Create session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        logger.info(
            f"Initialized SlurmAPIClient for {base_url} "
            f"with API version {api_version}"
        )

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers with fresh token.

        Returns:
            Dictionary with authentication headers
        """
        token = self.token_manager.get_token()
        return {"X-SLURM-USER-TOKEN": token}

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint path
            json_data: JSON data for request body
            **kwargs: Additional arguments for requests

        Returns:
            Response object

        Raises:
            SlurmAPIError: If request fails after all retries
        """
        url = urljoin(self.base_url, endpoint)

        # Add authentication headers
        headers = kwargs.get("headers", {})
        headers.update(self._get_auth_headers())
        kwargs["headers"] = headers

        # Set timeout
        kwargs.setdefault("timeout", self.timeout)

        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}: {method} {url}")

                response = self.session.request(method, url, json=json_data, **kwargs)

                # Success
                if response.status_code < 400:
                    return response

                # Authentication failure - refresh token and retry
                if response.status_code == 401:
                    logger.warning("Authentication failed, refreshing token")
                    self.token_manager.invalidate()
                    headers.update(self._get_auth_headers())
                    if attempt < self.max_retries - 1:
                        continue

                # Other HTTP errors
                error_msg = f"{method} {url} failed with status {response.status_code}"
                logger.error(f"{error_msg}: {response.text}")

                raise SlurmAPIError(
                    error_msg,
                    status_code=response.status_code,
                    response_text=response.text,
                )

            except requests.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")

                if attempt == self.max_retries - 1:
                    raise SlurmAPIError(
                        f"Request failed after {self.max_retries} attempts: {e}"
                    ) from e

                # Exponential backoff: 2^attempt seconds
                sleep_time = 2**attempt
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)

        raise SlurmAPIError(f"Request failed after {self.max_retries} attempts")

    def get_api_version(self) -> str:
        """Discover available API versions.

        Returns:
            API version string

        Raises:
            SlurmAPIError: If version discovery fails
        """
        response = self._request("GET", "/openapi/v3")
        data = response.json()

        # Extract version from OpenAPI spec
        version = data.get("info", {}).get("version", self.api_version)
        logger.info(f"Discovered Slurm API version: {version}")

        return version

    def submit_job(self, job_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a job to Slurm.

        Args:
            job_spec: Job specification dictionary with 'script' and 'job' keys

        Returns:
            Response dictionary containing job_id

        Raises:
            SlurmAPIError: If job submission fails
        """
        endpoint = f"/slurm/{self.api_version}/job/submit"

        logger.info(f"Submitting job: {job_spec.get('job', {}).get('name', 'unknown')}")
        logger.debug(f"Job specification: {json.dumps(job_spec, indent=2)}")

        response = self._request("POST", endpoint, json_data=job_spec)
        result = response.json()

        # Check for errors in response
        if "errors" in result and result["errors"]:
            error_msgs = [err.get("error", str(err)) for err in result["errors"]]
            raise SlurmAPIError(f"Job submission failed: {'; '.join(error_msgs)}")

        job_id = result.get("job_id")
        if not job_id:
            raise SlurmAPIError(f"No job_id in submission response: {result}")

        logger.info(f"Successfully submitted job {job_id}")
        return result

    def get_jobs(
        self, job_ids: Optional[List[Union[int, str]]] = None
    ) -> Dict[str, Any]:
        """Query active jobs.

        Args:
            job_ids: Optional list of specific job IDs to query.
                    If None, returns all jobs for the user.

        Returns:
            Dictionary with 'jobs' key containing list of job info

        Raises:
            SlurmAPIError: If query fails
        """
        endpoint = f"/slurm/{self.api_version}/jobs"

        # Build query parameters
        params = {}
        if job_ids:
            # Convert job IDs to comma-separated string
            job_ids_str = ",".join(str(jid) for jid in job_ids)
            params["job_id"] = job_ids_str

        logger.debug(f"Querying jobs: {params}")

        response = self._request("GET", endpoint, params=params)
        result = response.json()

        # Check for errors
        if "errors" in result and result["errors"]:
            # Log errors but don't fail - jobs might not exist
            for err in result["errors"]:
                logger.warning(f"Job query warning: {err}")

        jobs = result.get("jobs", [])
        logger.info(f"Retrieved {len(jobs)} jobs from Slurm")

        return result

    def get_job(self, job_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Get details for a specific job.

        Args:
            job_id: Slurm job ID

        Returns:
            Job details dictionary or None if not found

        Raises:
            SlurmAPIError: If query fails
        """
        endpoint = f"/slurm/{self.api_version}/job/{job_id}"

        try:
            response = self._request("GET", endpoint)
            result = response.json()

            # Extract job info from response
            jobs = result.get("jobs", [])
            if jobs:
                return jobs[0]

            return None

        except SlurmAPIError as e:
            if e.status_code == 404:
                logger.debug(f"Job {job_id} not found")
                return None
            raise

    def cancel_job(self, job_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Cancel a Slurm job.

        Args:
            job_id: Slurm job ID to cancel

        Returns:
            Response dictionary or None if job doesn't exist

        Raises:
            SlurmAPIError: If cancellation fails
        """
        endpoint = f"/slurm/{self.api_version}/job/{job_id}"

        logger.info(f"Cancelling job {job_id}")

        try:
            response = self._request("DELETE", endpoint)
            result = response.json()

            logger.info(f"Successfully cancelled job {job_id}")
            return result

        except SlurmAPIError as e:
            # 404 is acceptable - job already finished
            if e.status_code == 404 or "not found" in str(e).lower():
                logger.debug(f"Job {job_id} not found (already completed?)")
                return None
            raise

    def get_job_history(self, job_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Query job history from accounting database.

        This method attempts to find completed jobs that are no longer
        in the active queue but are recorded in the accounting database.

        Args:
            job_id: Slurm job ID

        Returns:
            Job info from accounting or None if not found

        Raises:
            SlurmAPIError: If query fails
        """
        # Try the regular job endpoint first - it includes accounting data
        job_info = self.get_job(job_id)
        if job_info:
            return job_info

        # If not found, the job might be too old or purged
        logger.debug(f"Job {job_id} not found in job history")
        return None

    def ping(self) -> bool:
        """Test API connectivity.

        Returns:
            True if API is reachable and responding
        """
        try:
            self.get_api_version()
            return True
        except Exception as e:
            logger.error(f"Ping failed: {e}")
            return False
