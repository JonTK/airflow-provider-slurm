"""Integration tests against real Slurm cluster.

These tests run against an actual Slurm cluster (default: rocky9.ar.jontk.com).
They are automatically skipped if the cluster is unavailable.

Usage:
    # Run all tests including real cluster tests
    pytest tests/integration/test_real_cluster.py

    # Run only real cluster tests
    pytest -m real_cluster

    # Skip real cluster tests
    pytest -m "not real_cluster"
    # or
    SKIP_REAL_CLUSTER_TESTS=1 pytest

Environment Variables:
    SLURM_TEST_CLUSTER_HOST: Cluster hostname (default: rocky9.ar.jontk.com)
    SLURM_TEST_CLUSTER_PORT: API port (default: 6820)
    SLURM_TEST_CLUSTER_USER: SSH user (default: root)
    SLURM_TEST_SSH_KEY: SSH key path (default: ~/.ssh/id_rsa)
    SKIP_REAL_CLUSTER_TESTS: Set to 1/true/yes to skip
"""

import logging
import time
from unittest.mock import MagicMock

import pytest

from airflow_provider_slurm.hooks.slurm_hook import SlurmHook
from airflow_provider_slurm.operators.slurm import SlurmOperator
from airflow_provider_slurm.sensors.slurm import SlurmSensor
from tests.utils.cluster_helpers import (
    fetch_token_via_ssh,
    get_cluster_config,
    is_cluster_available,
)

logger = logging.getLogger(__name__)

# Check cluster availability once at module load
CLUSTER_AVAILABLE = is_cluster_available()
CLUSTER_CONFIG = get_cluster_config()

# Skip reason message
SKIP_REASON = (
    f"Slurm cluster {CLUSTER_CONFIG['host']} not available. "
    "Set SLURM_TEST_CLUSTER_HOST and ensure SSH access is configured."
)


@pytest.fixture(scope="module")
def cluster_token():
    """Fetch and cache cluster token for all tests."""
    if not CLUSTER_AVAILABLE:
        pytest.skip(SKIP_REASON)

    token = fetch_token_via_ssh(
        host=CLUSTER_CONFIG["host"],
        user=CLUSTER_CONFIG["user"],
        ssh_key=CLUSTER_CONFIG["ssh_key"],
    )

    if not token:
        pytest.skip("Failed to fetch token from cluster")

    return token


@pytest.fixture
def slurm_hook(cluster_token):
    """Create SlurmHook configured for real cluster."""
    # Create mock token manager with real token
    from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager

    token_manager = MagicMock(spec=SlurmTokenManager)
    token_manager.get_token.return_value = cluster_token

    # Create hook
    api_url = f"http://{CLUSTER_CONFIG['host']}:{CLUSTER_CONFIG['port']}"
    hook = SlurmHook(api_url=api_url, username=CLUSTER_CONFIG["user"])

    # Replace token manager with our mock
    hook._token_manager = token_manager

    return hook


@pytest.mark.real_cluster
@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestRealClusterIntegration:
    """Integration tests against real Slurm cluster."""

    def test_hook_connection(self, slurm_hook):
        """Test basic connection to real cluster."""
        logger.info("Testing connection to real cluster")

        success, message = slurm_hook.test_connection()

        assert success is True, f"Connection test failed: {message}"
        logger.info(f"✓ Connection test passed: {message}")

    def test_hook_submit_and_monitor_job(self, slurm_hook):
        """Test job submission and monitoring on real cluster."""
        logger.info("Testing job submission and monitoring")

        # Submit a simple job
        script = """#!/bin/bash
echo "Testing from airflow-provider-slurm"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
sleep 5
echo "Job complete"
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="test-airflow-provider",
            partition="debug",  # Use debug partition for quick testing
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        assert job_id is not None
        assert isinstance(job_id, int)
        logger.info(f"✓ Job submitted with ID: {job_id}")

        # Monitor job status
        max_wait = 60  # Wait up to 60 seconds
        start_time = time.time()

        while time.time() - start_time < max_wait:
            job_info = slurm_hook.get_job_status(job_id)

            if job_info is None:
                # Check history
                job_info = slurm_hook.get_job_history(job_id)

            assert job_info is not None, f"Job {job_id} not found"

            state = job_info.get("job_state", "UNKNOWN")
            logger.info(f"Job {job_id} state: {state}")

            if state == "COMPLETED":
                logger.info(f"✓ Job {job_id} completed successfully")
                break
            elif state in ["FAILED", "CANCELLED", "TIMEOUT"]:
                pytest.fail(f"Job {job_id} failed with state {state}")

            time.sleep(2)
        else:
            pytest.fail(f"Job {job_id} did not complete within {max_wait} seconds")

    def test_hook_cancel_job(self, slurm_hook):
        """Test job cancellation on real cluster."""
        logger.info("Testing job cancellation")

        # Submit a long-running job
        script = """#!/bin/bash
echo "Starting long job..."
sleep 300
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="test-cancel",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",
        )

        assert job_id is not None
        logger.info(f"✓ Job submitted with ID: {job_id}")

        # Wait for job to start
        time.sleep(2)

        # Cancel the job
        success = slurm_hook.cancel_job(job_id)
        assert success is True
        logger.info(f"✓ Job {job_id} cancelled successfully")

        # Verify job is cancelled
        time.sleep(1)
        job_info = slurm_hook.get_job_status(job_id)
        if job_info:
            state = job_info.get("job_state")
            assert state in ["CANCELLED", "CANCELLING"], f"Job state is {state}"

    def test_hook_get_multiple_jobs(self, slurm_hook):
        """Test querying multiple jobs."""
        logger.info("Testing multi-job query")

        # Submit two jobs
        script = "#!/bin/bash\necho 'test'\nsleep 5"

        job_id1 = slurm_hook.submit_job(
            script=script,
            job_name="test-multi-1",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        job_id2 = slurm_hook.submit_job(
            script=script,
            job_name="test-multi-2",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        logger.info(f"✓ Submitted jobs: {job_id1}, {job_id2}")

        # Query both jobs
        jobs = slurm_hook.get_jobs([job_id1, job_id2])

        assert len(jobs) >= 1  # At least one should be found
        job_ids = [job.get("job_id") for job in jobs]
        assert job_id1 in job_ids or job_id2 in job_ids

        logger.info(f"✓ Retrieved {len(jobs)} jobs")

        # Cleanup
        slurm_hook.cancel_job(job_id1)
        slurm_hook.cancel_job(job_id2)


@pytest.mark.real_cluster
@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
@pytest.mark.skip(reason="Requires Airflow context - manual testing only")
class TestOperatorSensorIntegration:
    """Integration tests for Operator and Sensor (requires Airflow context).

    These tests are skipped by default as they require full Airflow context.
    They can be run manually with a test DAG.
    """

    def test_operator_submit(self, cluster_token):
        """Test SlurmOperator job submission."""
        # This would require full Airflow context with TaskInstance
        # Placeholder for future implementation with test DAG
        pass

    def test_sensor_wait(self, cluster_token):
        """Test SlurmSensor job monitoring."""
        # This would require full Airflow context with TaskInstance
        # Placeholder for future implementation with test DAG
        pass
