"""Unit tests for SlurmHook."""

from unittest.mock import MagicMock, patch

import pytest

from airflow_provider_slurm.exceptions import SlurmAPIError, SlurmConfigurationError
from airflow_provider_slurm.hooks.slurm_hook import SlurmHook


class TestSlurmHook:
    """Test cases for SlurmHook."""

    def test_init_default(self):
        """Test hook initialization with defaults."""
        hook = SlurmHook()
        assert hook.slurm_conn_id == "slurm_default"
        assert hook._api_url is None
        assert hook._username is None
        assert hook.token_lifespan == 3600
        assert hook.api_timeout == 30
        assert hook.api_max_retries == 3

    def test_init_with_params(self):
        """Test hook initialization with custom parameters."""
        hook = SlurmHook(
            slurm_conn_id="my_slurm",
            api_url="https://slurm.example.com:6820",
            username="testuser",
            token_lifespan=7200,
            api_timeout=60,
            api_max_retries=5,
        )
        assert hook.slurm_conn_id == "my_slurm"
        assert hook._api_url == "https://slurm.example.com:6820"
        assert hook._username == "testuser"
        assert hook.token_lifespan == 7200
        assert hook.api_timeout == 60
        assert hook.api_max_retries == 5

    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager")
    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmAPIClient")
    def test_get_conn_with_explicit_url(self, mock_client_class, mock_token_class):
        """Test get_conn with explicit API URL."""
        hook = SlurmHook(api_url="https://slurm.example.com:6820", username="testuser")

        client = hook.get_conn()

        # Verify token manager was created
        mock_token_class.assert_called_once_with(username="testuser", lifespan=3600)

        # Verify API client was created
        mock_client_class.assert_called_once()
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["base_url"] == "https://slurm.example.com:6820"
        assert call_kwargs["timeout"] == 30
        assert call_kwargs["max_retries"] == 3

        # Verify client is cached
        client2 = hook.get_conn()
        assert client is client2

    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager")
    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmAPIClient")
    def test_get_conn_from_connection(self, mock_client_class, mock_token_class):
        """Test get_conn using Airflow connection."""
        mock_conn = MagicMock()
        mock_conn.host = "slurm.example.com"
        mock_conn.port = 6820
        mock_conn.schema = "https"
        mock_conn.login = "testuser"

        hook = SlurmHook(slurm_conn_id="test_conn")

        with patch.object(hook, "get_connection", return_value=mock_conn):
            hook.get_conn()

        # Verify token manager was created
        mock_token_class.assert_called_once_with(username="testuser", lifespan=3600)

        # Verify API client was created with correct URL
        mock_client_class.assert_called_once()
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["base_url"] == "https://slurm.example.com:6820"

    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager")
    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmAPIClient")
    def test_get_conn_default_https_scheme(self, mock_client_class, mock_token_class):
        """Test get_conn defaults to https when no scheme provided."""
        mock_conn = MagicMock()
        mock_conn.host = "slurm.example.com"
        mock_conn.port = 6820
        mock_conn.schema = None  # No scheme provided
        mock_conn.login = None

        hook = SlurmHook()

        with patch.object(hook, "get_connection", return_value=mock_conn):
            hook.get_conn()

        # Verify URL uses https by default
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["base_url"] == "https://slurm.example.com:6820"

    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager")
    @patch("airflow_provider_slurm.hooks.slurm_hook.SlurmAPIClient")
    def test_get_conn_no_port(self, mock_client_class, mock_token_class):
        """Test get_conn without port."""
        mock_conn = MagicMock()
        mock_conn.host = "slurm.example.com"
        mock_conn.port = None
        mock_conn.schema = "https"
        mock_conn.login = None

        hook = SlurmHook()

        with patch.object(hook, "get_connection", return_value=mock_conn):
            hook.get_conn()

        # Verify URL without port
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["base_url"] == "https://slurm.example.com"

    def test_get_conn_no_host_raises_error(self):
        """Test get_conn raises error when host is missing."""
        mock_conn = MagicMock()
        mock_conn.host = None
        mock_conn.port = 6820
        mock_conn.schema = "https"
        mock_conn.login = None

        hook = SlurmHook()

        with patch.object(hook, "get_connection", return_value=mock_conn):
            with pytest.raises(SlurmConfigurationError) as exc_info:
                hook.get_conn()

            assert "Host not configured" in str(exc_info.value)

    def test_test_connection_success(self):
        """Test test_connection with successful ping."""
        mock_client = MagicMock()
        mock_client.ping.return_value = True

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        success, message = hook.test_connection()

        assert success is True
        assert message == "Connection successful"
        mock_client.ping.assert_called_once()

    def test_test_connection_failure(self):
        """Test test_connection with failed ping."""
        mock_client = MagicMock()
        mock_client.ping.return_value = False

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        success, message = hook.test_connection()

        assert success is False
        assert "Failed to ping" in message

    def test_test_connection_exception(self):
        """Test test_connection handles exceptions."""
        hook = SlurmHook(api_url="https://slurm.example.com:6820")

        with patch.object(hook, "get_conn", side_effect=Exception("Connection error")):
            success, message = hook.test_connection()

            assert success is False
            assert "Connection failed: Connection error" in message

    def test_submit_job_basic(self):
        """Test basic job submission."""
        mock_client = MagicMock()
        mock_client.submit_job.return_value = {"job_id": 12345}

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        job_id = hook.submit_job(
            script="#!/bin/bash\necho 'test'",
            job_name="test_job",
            partition="compute",
        )

        assert job_id == 12345
        mock_client.submit_job.assert_called_once()

        # Verify job spec structure
        job_spec = mock_client.submit_job.call_args[0][0]
        assert job_spec["script"] == "#!/bin/bash\necho 'test'"
        assert job_spec["job"]["name"] == "test_job"
        assert job_spec["job"]["partition"] == "compute"

    def test_submit_job_with_gres(self):
        """Test job submission with GRES (GPU)."""
        mock_client = MagicMock()
        mock_client.submit_job.return_value = {"job_id": 12345}

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        job_id = hook.submit_job(
            script="#!/bin/bash\npython train.py",
            job_name="gpu_job",
            gres="gpu:2",
            constraint="volta",
        )

        assert job_id == 12345

        # Verify GRES and constraint in job spec
        job_spec = mock_client.submit_job.call_args[0][0]
        assert job_spec["job"]["gres"] == "gpu:2"
        assert job_spec["job"]["constraints"] == "volta"

    def test_submit_job_no_job_id_raises_error(self):
        """Test submit_job raises error when no job_id returned."""
        mock_client = MagicMock()
        mock_client.submit_job.return_value = {"status": "ok"}  # No job_id

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        with pytest.raises(SlurmAPIError) as exc_info:
            hook.submit_job(script="#!/bin/bash\necho 'test'", job_name="test_job")

        assert "No job_id returned" in str(exc_info.value)

    def test_get_job_status(self):
        """Test getting job status."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "job_id": 12345,
            "job_state": "RUNNING",
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        job_info = hook.get_job_status(12345)

        assert job_info["job_id"] == 12345
        assert job_info["job_state"] == "RUNNING"
        mock_client.get_job.assert_called_once_with(12345)

    def test_get_jobs(self):
        """Test getting multiple job statuses."""
        mock_client = MagicMock()
        mock_client.get_jobs.return_value = {
            "jobs": [
                {"job_id": 123, "job_state": "RUNNING"},
                {"job_id": 124, "job_state": "PENDING"},
            ]
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        jobs = hook.get_jobs([123, 124])

        assert len(jobs) == 2
        assert jobs[0]["job_id"] == 123
        mock_client.get_jobs.assert_called_once()

    def test_cancel_job_success(self):
        """Test successful job cancellation."""
        mock_client = MagicMock()
        mock_client.cancel_job.return_value = {"status": "cancelled"}

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        result = hook.cancel_job(12345)

        assert result is True
        mock_client.cancel_job.assert_called_once_with(12345)

    def test_cancel_job_not_found(self):
        """Test job cancellation when job not found."""
        mock_client = MagicMock()
        mock_client.cancel_job.return_value = None

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        result = hook.cancel_job(99999)

        assert result is False

    @patch("time.sleep")
    def test_wait_for_job_completed(self, mock_sleep):
        """Test wait_for_job with successful completion."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "job_id": 12345,
            "job_state": "COMPLETED",
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        state = hook.wait_for_job(12345, timeout=60, poll_interval=1)

        assert state == "COMPLETED"
        mock_sleep.assert_not_called()  # Should complete immediately

    @patch("time.sleep")
    def test_wait_for_job_failed(self, mock_sleep):
        """Test wait_for_job with job failure."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "job_id": 12345,
            "job_state": "FAILED",
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        with pytest.raises(SlurmAPIError) as exc_info:
            hook.wait_for_job(12345, timeout=60)

        assert "failed with state FAILED" in str(exc_info.value)

    @patch("time.sleep")
    @patch("time.time")
    def test_wait_for_job_timeout(self, mock_time, mock_sleep):
        """Test wait_for_job timeout."""
        # Simulate time progression
        mock_time.side_effect = [0, 10, 20, 30, 40, 50, 60, 70]

        mock_client = MagicMock()
        mock_client.get_job.return_value = {
            "job_id": 12345,
            "job_state": "RUNNING",
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        with pytest.raises(SlurmAPIError) as exc_info:
            hook.wait_for_job(12345, timeout=60, poll_interval=10)

        assert "did not complete within 60 seconds" in str(exc_info.value)

    @patch("time.sleep")
    def test_wait_for_job_not_found(self, mock_sleep):
        """Test wait_for_job when job not found."""
        mock_client = MagicMock()
        mock_client.get_job.return_value = None

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        with pytest.raises(SlurmAPIError) as exc_info:
            hook.wait_for_job(12345)

        assert "Job 12345 not found" in str(exc_info.value)

    def test_get_job_history(self):
        """Test getting job history."""
        mock_client = MagicMock()
        mock_client.get_job_history.return_value = {
            "job_id": 12345,
            "exit_code": 0,
        }

        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = mock_client

        history = hook.get_job_history(12345)

        assert history["job_id"] == 12345
        assert history["exit_code"] == 0
        mock_client.get_job_history.assert_called_once_with(12345)

    def test_convert_time_to_seconds_hms(self):
        """Test time conversion HH:MM:SS format."""
        assert SlurmHook._convert_time_to_seconds("01:30:45") == 5445

    def test_convert_time_to_seconds_ms(self):
        """Test time conversion MM:SS format."""
        assert SlurmHook._convert_time_to_seconds("30:45") == 1845

    def test_convert_time_to_seconds_s(self):
        """Test time conversion seconds only."""
        assert SlurmHook._convert_time_to_seconds("120") == 120

    def test_close(self):
        """Test hook cleanup."""
        hook = SlurmHook(api_url="https://slurm.example.com:6820")
        hook._client = MagicMock()
        hook._token_manager = MagicMock()

        hook.close()

        assert hook._client is None
        assert hook._token_manager is None
