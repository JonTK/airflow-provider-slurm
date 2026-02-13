"""Slurm REST API client for job management."""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import requests

from airflow_provider_slurm.exceptions import SlurmAPIError
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager

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

        return version  # type: ignore[no-any-return]

    @staticmethod
    def validate_array_spec(array_spec: str) -> Tuple[bool, Optional[str]]:
        """Validate Slurm array job specification.

        Args:
            array_spec: Array specification string

        Returns:
            Tuple of (is_valid, error_message)

        Valid formats:
            - Range: "0-99", "1-100"
            - Range with step: "0-99:5", "1-100:2"
            - Explicit list: "1,5,10,15,20"
            - Max concurrent tasks: "0-99%10" (range with limit)
        """
        if not array_spec or not isinstance(array_spec, str):
            return False, "Array spec must be a non-empty string"

        # Remove whitespace
        spec = array_spec.strip()

        # Pattern for valid array specifications
        # Matches: N-M, N-M:S, N-M%L, N-M:S%L, or N,N,N...
        patterns = [
            r"^\d+$",  # Single task (technically valid but not an array)
            r"^\d+-\d+$",  # Range: 0-99
            r"^\d+-\d+:\d+$",  # Step: 0-99:5
            r"^\d+-\d+%\d+$",  # Range with limit: 0-99%10
            r"^\d+-\d+:\d+%\d+$",  # Step with limit: 0-99:5%10
            r"^\d+(,\d+)+$",  # List: 1,5,10 (at least 2 items)
        ]

        for pattern in patterns:
            if re.match(pattern, spec):
                # Additional validation for ranges
                if "-" in spec:
                    # Extract start and end
                    range_part = spec.split("%")[0].split(":")[0]
                    start, end = map(int, range_part.split("-"))
                    if start > end:
                        return False, f"Invalid range: start ({start}) > end ({end})"
                    if start < 0:
                        return False, f"Array indices must be non-negative"
                return True, None

        return (
            False,
            f"Invalid array specification '{spec}'. "
            "Valid formats: '0-99', '0-99:5', '0-99%10', '1,5,10,15'",
        )

    @staticmethod
    def parse_array_spec(array_spec: str) -> int:
        """Calculate total number of tasks in an array specification.

        Args:
            array_spec: Array specification string

        Returns:
            Total number of tasks

        Examples:
            "0-99" -> 100
            "0-99:5" -> 20
            "1,5,10,15" -> 4
        """
        spec = array_spec.strip().split("%")[0]  # Remove limit if present

        # Range with step
        if ":" in spec:
            range_part, step_str = spec.split(":")
            start, end = map(int, range_part.split("-"))
            step = int(step_str)
            return len(range(start, end + 1, step))

        # Simple range
        elif "-" in spec:
            start, end = map(int, spec.split("-"))
            return end - start + 1

        # Explicit list
        elif "," in spec:
            return len(spec.split(","))

        # Single value
        else:
            return 1

    @staticmethod
    def validate_dependency(dependency: str) -> Tuple[bool, Optional[str]]:
        """Validate Slurm job dependency specification.

        Args:
            dependency: Dependency specification string

        Returns:
            Tuple of (is_valid, error_message)

        Valid formats:
            - after:job_id[+time]
            - afterok:job_id[:job_id...]
            - afternotok:job_id[:job_id...]
            - afterany:job_id[:job_id...]
            - aftercorr:job_id
            - singleton
            - afterburstbuffer:job_id
            - Combinations with , (AND) or ? (OR)

        Examples:
            >>> validate_dependency("afterok:12345")
            (True, None)

            >>> validate_dependency("afterok:12345:12346")
            (True, None)

            >>> validate_dependency("afterok:12345,afterany:12346")
            (True, None)

            >>> validate_dependency("afterok:12345?afternotok:12346")
            (True, None)

            >>> validate_dependency("invalid:abc")
            (False, "Invalid dependency type 'invalid'")
        """
        if not dependency or not isinstance(dependency, str):
            return False, "Dependency must be a non-empty string"

        # Remove whitespace
        dep = dependency.strip()

        if not dep:
            return False, "Empty dependency specification"

        # Define valid dependency type patterns
        patterns = {
            "after": r"after:\d+(?:\+\d+)?",  # after:job_id or after:job_id+time
            "afterok": r"afterok:\d+(?::\d+)*",  # afterok:job_id[:job_id...]
            "afternotok": r"afternotok:\d+(?::\d+)*",
            "afterany": r"afterany:\d+(?::\d+)*",
            "aftercorr": r"aftercorr:\d+",  # aftercorr:job_id (single job)
            "singleton": r"singleton",
            "afterburstbuffer": r"afterburstbuffer:\d+",
        }

        # Build combined pattern for a single dependency clause
        single_patterns = "|".join(f"(?:{p})" for p in patterns.values())
        single_clause_pattern = f"^({single_patterns})$"

        # Pattern for full dependency string with combinators
        # Allows: dep1,dep2 (AND) or dep1?dep2 (OR)
        full_pattern = f"^({single_patterns})(?:[,?]({single_patterns}))*$"

        # Check if it matches the full pattern
        if re.match(full_pattern, dep):
            # Extract individual clauses to validate each
            clauses = re.split(r"[,?]", dep)
            for clause in clauses:
                if not re.match(single_clause_pattern, clause):
                    # Try to identify the dependency type
                    dep_type = clause.split(":")[0] if ":" in clause else clause
                    if dep_type not in patterns:
                        return (
                            False,
                            f"Invalid dependency type '{dep_type}'. "
                            f"Valid types: {', '.join(patterns.keys())}",
                        )
                    return False, f"Invalid format for dependency clause '{clause}'"

            return True, None

        # If we get here, the format is invalid
        return (
            False,
            f"Invalid dependency specification '{dep}'. "
            "Valid formats: 'afterok:job_id', 'afterok:job1:job2', "
            "'afterok:job1,afterany:job2', 'singleton'",
        )

    def submit_job(
        self,
        job_spec: Dict[str, Any],
        array: Optional[str] = None,
        dependency: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit a job to Slurm.

        Args:
            job_spec: Job specification dictionary with 'script' and 'job' keys
            array: Optional array specification (e.g., "0-99", "1-100:2", "1,5,10")
            dependency: Optional dependency specification (e.g., "afterok:12345")

        Returns:
            Response dictionary containing:
                - job_id: Parent job ID
                - array: Array specification (if array job)
                - array_task_count: Number of array tasks (if array job)
                - dependency: Dependency specification (if dependency set)

        Raises:
            SlurmAPIError: If job submission fails, array spec is invalid,
                          or dependency spec is invalid

        Examples:
            >>> # Submit single job
            >>> client.submit_job(job_spec)
            {'job_id': 12345}

            >>> # Submit array job
            >>> client.submit_job(job_spec, array="0-99")
            {'job_id': 12345, 'array': '0-99', 'array_task_count': 100}

            >>> # Submit job with dependency
            >>> client.submit_job(job_spec, dependency="afterok:12345")
            {'job_id': 12346, 'dependency': 'afterok:12345'}

            >>> # Submit array job with dependency
            >>> client.submit_job(job_spec, array="0-99", dependency="afterok:12345")
            {'job_id': 12346, 'array': '0-99', 'array_task_count': 100, 'dependency': 'afterok:12345'}
        """
        # Validate array specification if provided
        if array:
            is_valid, error_msg = self.validate_array_spec(array)
            if not is_valid:
                raise SlurmAPIError(f"Invalid array specification: {error_msg}")

            # Add array to job spec
            if "job" not in job_spec:
                job_spec["job"] = {}
            job_spec["job"]["array"] = array
            logger.info(f"Submitting array job with spec: {array}")

        # Validate dependency specification if provided
        if dependency:
            is_valid, error_msg = self.validate_dependency(dependency)
            if not is_valid:
                raise SlurmAPIError(f"Invalid dependency specification: {error_msg}")

            # Add dependency to job spec
            if "job" not in job_spec:
                job_spec["job"] = {}
            job_spec["job"]["dependency"] = dependency
            logger.info(f"Submitting job with dependency: {dependency}")

        endpoint = f"/slurm/{self.api_version}/job/submit"

        job_name = job_spec.get("job", {}).get("name", "unknown")
        logger.info(
            f"Submitting job: {job_name}" + (f" (array: {array})" if array else "")
        )
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

        # Enrich response with array information
        if array:
            result["array"] = array
            result["array_task_count"] = self.parse_array_spec(array)

        # Enrich response with dependency information
        if dependency:
            result["dependency"] = dependency

        # Build success log message
        log_parts = [f"Successfully submitted"]
        if array:
            log_parts.append(
                f"array job {job_id} with {result['array_task_count']} tasks"
            )
        else:
            log_parts.append(f"job {job_id}")
        if dependency:
            log_parts.append(f"(dependency: {dependency})")

        logger.info(" ".join(log_parts))

        return result  # type: ignore[no-any-return]

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

        return result  # type: ignore[no-any-return]

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
                return jobs[0]  # type: ignore[no-any-return]

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
            return result  # type: ignore[no-any-return]

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

    def get_array_status(self, job_id: int) -> Dict[str, Any]:
        """Get aggregated status for an array job.

        Queries all tasks in an array job and aggregates their states.

        Args:
            job_id: Array job ID (parent job ID)

        Returns:
            Dictionary with:
                - job_id: Array job ID
                - array_spec: Array specification if available
                - total_tasks: Total number of array tasks
                - completed: Number of completed tasks
                - running: Number of running tasks
                - pending: Number of pending tasks
                - failed: Number of failed tasks
                - state: Aggregated state (PENDING/RUNNING/COMPLETED/PARTIALLY_COMPLETED/FAILED)
                - tasks: Optional list of individual task details

        Raises:
            SlurmAPIError: If query fails
        """
        logger.debug(f"Getting array status for job {job_id}")

        # Query all jobs - Slurm returns array tasks as separate entries
        result = self.get_jobs([job_id])
        jobs = result.get("jobs", [])

        if not jobs:
            # Try history
            job_info = self.get_job_history(job_id)
            if job_info:
                jobs = [job_info]

        if not jobs:
            raise SlurmAPIError(f"Array job {job_id} not found")

        # Aggregate states
        state_counts = {
            "PENDING": 0,
            "RUNNING": 0,
            "COMPLETED": 0,
            "FAILED": 0,
            "CANCELLED": 0,
            "TIMEOUT": 0,
        }

        array_spec = None
        task_details = []

        for job in jobs:
            state = job.get("job_state", "UNKNOWN")
            if isinstance(state, list):
                state = state[0] if state else "UNKNOWN"

            # Count states
            if state in state_counts:
                state_counts[state] += 1
            elif state in ["COMPLETING", "CONFIGURING"]:
                state_counts["RUNNING"] += 1
            elif "FAIL" in state.upper():
                state_counts["FAILED"] += 1

            # Extract array info
            if not array_spec:
                # Try to get array specification from job info
                array_job_id = job.get("array_job_id")
                array_task_id = job.get("array_task_id")
                if array_job_id and array_task_id is not None:
                    # This is an array task
                    pass  # array_spec would need to be reconstructed or stored

            task_details.append(
                {
                    "task_id": job.get("array_task_id", 0),
                    "state": state,
                    "exit_code": job.get("exit_code"),
                }
            )

        total = len(jobs)
        completed = state_counts["COMPLETED"]
        running = state_counts["RUNNING"]
        pending = state_counts["PENDING"]
        failed = (
            state_counts["FAILED"] + state_counts["CANCELLED"] + state_counts["TIMEOUT"]
        )

        # Determine aggregated state
        if failed == total:
            agg_state = "FAILED"
        elif completed == total:
            agg_state = "COMPLETED"
        elif failed > 0 and (completed + failed) == total:
            agg_state = "PARTIALLY_COMPLETED"
        elif running > 0:
            agg_state = "RUNNING"
        else:
            agg_state = "PENDING"

        return {
            "job_id": job_id,
            "array_spec": array_spec,
            "total_tasks": total,
            "completed": completed,
            "running": running,
            "pending": pending,
            "failed": failed,
            "state": agg_state,
            "tasks": task_details,
        }

    def cancel_array_task(
        self, job_id: int, array_task_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Cancel an entire array job or specific array task.

        Args:
            job_id: Array job ID
            array_task_id: Specific task ID to cancel. If None, cancels entire array.

        Returns:
            Response dictionary or None if job doesn't exist

        Raises:
            SlurmAPIError: If cancellation fails

        Examples:
            >>> # Cancel entire array
            >>> client.cancel_array_task(12345)

            >>> # Cancel specific task
            >>> client.cancel_array_task(12345, array_task_id=5)
        """
        if array_task_id is not None:
            # Cancel specific array task
            # Slurm uses job_id_task_id format
            full_job_id = f"{job_id}_{array_task_id}"
            logger.info(f"Cancelling array task {full_job_id}")
            return self.cancel_job(full_job_id)
        else:
            # Cancel entire array
            logger.info(f"Cancelling entire array job {job_id}")
            return self.cancel_job(job_id)

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
