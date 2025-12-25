"""Unit tests for SlurmAPIClient."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses

from airflow_slurm_executor.exceptions import SlurmAPIError
from airflow_slurm_executor.slurm_api_client import SlurmAPIClient
from airflow_slurm_executor.slurm_token_manager import SlurmTokenManager


class TestSlurmAPIClient:
    """Test cases for SlurmAPIClient."""

    @pytest.fixture
    def mock_token_manager(self):
        """Create a mock token manager."""
        manager = MagicMock(spec=SlurmTokenManager)
        manager.get_token.return_value = "test_token_12345"
        return manager

    @pytest.fixture
    def api_client(self, mock_token_manager):
        """Create an API client with mock token manager."""
        return SlurmAPIClient(
            base_url="https://slurm.example.com:6820",
            token_manager=mock_token_manager,
        )

    def test_init(self, mock_token_manager):
        """Test client initialization."""
        client = SlurmAPIClient(
            base_url="https://slurm.example.com:6820/",
            token_manager=mock_token_manager,
            api_version="v0.0.40",
            timeout=60,
            max_retries=5,
        )
        
        assert client.base_url == "https://slurm.example.com:6820"  # Trailing slash removed
        assert client.api_version == "v0.0.40"
        assert client.timeout == 60
        assert client.max_retries == 5

    @responses.activate
    def test_get_api_version(self, api_client):
        """Test API version discovery."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/openapi/v3",
            json={"info": {"version": "v0.0.42"}},
            status=200,
        )
        
        version = api_client.get_api_version()
        assert version == "v0.0.42"

    @responses.activate
    def test_submit_job_success(self, api_client):
        """Test successful job submission."""
        job_spec = {
            "script": "#!/bin/bash\necho 'Hello'",
            "job": {
                "name": "test_job",
                "partition": "compute",
            }
        }
        
        responses.add(
            responses.POST,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/submit",
            json={"job_id": 12345},
            status=200,
        )
        
        result = api_client.submit_job(job_spec)
        assert result["job_id"] == 12345
        
        # Verify request
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["X-SLURM-USER-TOKEN"] == "test_token_12345"

    @responses.activate
    def test_submit_job_with_errors(self, api_client):
        """Test job submission with errors in response."""
        job_spec = {"script": "#!/bin/bash", "job": {}}
        
        responses.add(
            responses.POST,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/submit",
            json={
                "errors": [
                    {"error": "Invalid partition"},
                    {"error": "Insufficient resources"},
                ]
            },
            status=200,
        )
        
        with pytest.raises(SlurmAPIError) as exc_info:
            api_client.submit_job(job_spec)
        
        assert "Invalid partition" in str(exc_info.value)
        assert "Insufficient resources" in str(exc_info.value)

    @responses.activate
    def test_submit_job_no_job_id(self, api_client):
        """Test job submission response without job_id."""
        job_spec = {"script": "#!/bin/bash", "job": {}}
        
        responses.add(
            responses.POST,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/submit",
            json={"status": "ok"},  # Missing job_id
            status=200,
        )
        
        with pytest.raises(SlurmAPIError) as exc_info:
            api_client.submit_job(job_spec)
        
        assert "No job_id in submission response" in str(exc_info.value)

    @responses.activate
    def test_get_jobs_all(self, api_client):
        """Test querying all jobs."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            json={
                "jobs": [
                    {"job_id": 123, "job_state": "RUNNING"},
                    {"job_id": 124, "job_state": "PENDING"},
                ]
            },
            status=200,
        )
        
        result = api_client.get_jobs()
        assert len(result["jobs"]) == 2
        assert result["jobs"][0]["job_id"] == 123

    @responses.activate
    def test_get_jobs_specific(self, api_client):
        """Test querying specific jobs."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs?job_id=123,124",
            json={
                "jobs": [
                    {"job_id": 123, "job_state": "RUNNING"},
                ]
            },
            status=200,
        )
        
        result = api_client.get_jobs(job_ids=[123, 124])
        assert len(result["jobs"]) == 1
        
        # Verify query parameter (URL encoded)
        assert "job_id=123%2C124" in responses.calls[0].request.url

    @responses.activate
    def test_get_job_found(self, api_client):
        """Test getting a specific job that exists."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/12345",
            json={
                "jobs": [
                    {"job_id": 12345, "job_state": "COMPLETED", "exit_code": 0}
                ]
            },
            status=200,
        )
        
        job_info = api_client.get_job(12345)
        assert job_info is not None
        assert job_info["job_id"] == 12345
        assert job_info["job_state"] == "COMPLETED"

    @responses.activate
    def test_get_job_not_found(self, api_client):
        """Test getting a job that doesn't exist."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/99999",
            json={"error": "Job not found"},
            status=404,
        )
        
        job_info = api_client.get_job(99999)
        assert job_info is None

    @responses.activate
    def test_cancel_job_success(self, api_client):
        """Test successful job cancellation."""
        responses.add(
            responses.DELETE,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/12345",
            json={"status": "Job cancelled"},
            status=200,
        )
        
        result = api_client.cancel_job(12345)
        assert result is not None
        assert result["status"] == "Job cancelled"

    @responses.activate
    def test_cancel_job_not_found(self, api_client):
        """Test cancelling a job that doesn't exist."""
        responses.add(
            responses.DELETE,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/99999",
            json={"error": "Job not found"},
            status=404,
        )
        
        result = api_client.cancel_job(99999)
        assert result is None  # Should return None for 404

    @responses.activate
    def test_auth_retry(self, api_client, mock_token_manager):
        """Test token refresh on authentication failure."""
        # First request fails with 401
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            json={"error": "Invalid token"},
            status=401,
        )
        
        # Second request succeeds after token refresh
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            json={"jobs": []},
            status=200,
        )
        
        result = api_client.get_jobs()
        assert result["jobs"] == []
        
        # Verify token was invalidated and refreshed
        mock_token_manager.invalidate.assert_called_once()
        assert mock_token_manager.get_token.call_count >= 2

    @responses.activate
    def test_request_retry_on_error(self, api_client):
        """Test retry logic for network errors."""
        # First two attempts fail
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            body=requests.RequestException("Connection error"),
        )
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            body=requests.RequestException("Connection error"),
        )
        
        # Third attempt succeeds
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
            json={"jobs": []},
            status=200,
        )
        
        with patch("time.sleep"):  # Skip actual sleep in tests
            result = api_client.get_jobs()
        
        assert result["jobs"] == []
        assert len(responses.calls) == 3

    @responses.activate
    def test_request_max_retries_exceeded(self, api_client):
        """Test that requests fail after max retries."""
        # All attempts fail
        for _ in range(3):  # max_retries default is 3
            responses.add(
                responses.GET,
                "https://slurm.example.com:6820/slurm/v0.0.42/jobs",
                body=requests.RequestException("Connection error"),
            )
        
        with patch("time.sleep"):  # Skip actual sleep in tests
            with pytest.raises(SlurmAPIError) as exc_info:
                api_client.get_jobs()
        
        assert "after 3 attempts" in str(exc_info.value)

    @responses.activate
    def test_ping_success(self, api_client):
        """Test successful ping."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/openapi/v3",
            json={"info": {"version": "v0.0.42"}},
            status=200,
        )
        
        assert api_client.ping() is True

    @responses.activate
    def test_ping_failure(self, api_client):
        """Test failed ping."""
        responses.add(
            responses.GET,
            "https://slurm.example.com:6820/openapi/v3",
            status=500,
        )
        
        assert api_client.ping() is False

    def test_auth_headers(self, api_client, mock_token_manager):
        """Test authentication header generation."""
        headers = api_client._get_auth_headers()
        assert headers["X-SLURM-USER-TOKEN"] == "test_token_12345"
        mock_token_manager.get_token.assert_called_once()

    @responses.activate
    def test_http_error_details(self, api_client):
        """Test that HTTP error details are preserved."""
        responses.add(
            responses.POST,
            "https://slurm.example.com:6820/slurm/v0.0.42/job/submit",
            json={"error": "Bad request", "details": "Invalid script"},
            status=400,
        )
        
        with pytest.raises(SlurmAPIError) as exc_info:
            api_client.submit_job({"script": "", "job": {}})
        
        error = exc_info.value
        assert error.status_code == 400
        assert "Bad request" in error.response_text