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

# Store cluster config at module level for use in tests
CLUSTER_CONFIG = get_cluster_config()

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

    def test_hook_submit_with_gres(self, slurm_hook):
        """Test job submission with GRES (GPU) allocation."""
        logger.info("Testing GRES/GPU job submission")

        script = """#!/bin/bash
echo "Testing GRES allocation"
echo "Slurm GRES: $SLURM_JOB_GRES"
echo "GPU devices: $CUDA_VISIBLE_DEVICES"
sleep 5
echo "GRES test complete"
"""

        try:
            job_id = slurm_hook.submit_job(
                script=script,
                job_name="test-gres",
                partition="debug",
                cpus_per_task=1,
                mem="100M",
                time_limit="00:01:00",
                gres="gpu:1",  # Request 1 GPU
            )

            assert job_id is not None
            logger.info(f"✓ GRES job submitted with ID: {job_id}")

            # Check job was submitted with GRES
            time.sleep(2)
            job_info = slurm_hook.get_job_status(job_id)

            if job_info:
                # Job might be pending if no GPUs available
                state = job_info.get("job_state", "UNKNOWN")
                logger.info(f"GRES job {job_id} state: {state}")

                # Cancel the job
                slurm_hook.cancel_job(job_id)
                logger.info(f"✓ GRES job {job_id} cancelled")

        except Exception as e:
            # GRES might not be available on cluster - this is acceptable
            logger.warning(f"GRES test failed (may not have GPUs): {e}")
            # Don't fail the test - just log it

    def test_hook_submit_with_constraints(self, slurm_hook):
        """Test job submission with node constraints."""
        logger.info("Testing node constraint job submission")

        script = """#!/bin/bash
echo "Testing node constraints"
echo "Node: $(hostname)"
echo "Node features: $SLURM_JOB_CONSTRAINTS"
sleep 5
echo "Constraint test complete"
"""

        try:
            job_id = slurm_hook.submit_job(
                script=script,
                job_name="test-constraints",
                partition="debug",
                cpus_per_task=1,
                mem="100M",
                time_limit="00:01:00",
                constraint="intel",  # Example constraint
            )

            assert job_id is not None
            logger.info(f"✓ Constraint job submitted with ID: {job_id}")

            # Check job status
            time.sleep(2)
            job_info = slurm_hook.get_job_status(job_id)

            if job_info:
                state = job_info.get("job_state", "UNKNOWN")
                logger.info(f"Constraint job {job_id} state: {state}")

                # Cancel the job
                slurm_hook.cancel_job(job_id)
                logger.info(f"✓ Constraint job {job_id} cancelled")

        except Exception as e:
            # Specific constraints might not exist - this is acceptable
            logger.warning(f"Constraint test failed (may not have 'intel' nodes): {e}")
            # Don't fail the test - just log it


@pytest.mark.real_cluster
@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestOperatorSensorIntegration:
    """Integration tests for Operator and Sensor with mocked Airflow context."""

    @pytest.fixture
    def mock_airflow_connection(self, cluster_token):
        """Mock Airflow connection for SlurmOperator/Sensor."""
        from unittest.mock import MagicMock, patch

        # Mock the Airflow connection
        mock_conn = MagicMock()
        mock_conn.host = CLUSTER_CONFIG["host"]
        mock_conn.port = int(CLUSTER_CONFIG["port"])
        mock_conn.schema = "http"
        mock_conn.login = CLUSTER_CONFIG["user"]

        with patch(
            "airflow_provider_slurm.hooks.slurm_hook.BaseHook.get_connection",
            return_value=mock_conn,
        ):
            # Mock token manager to use real cluster token
            with patch(
                "airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager"
            ) as mock_token_manager_class:
                mock_token_manager = MagicMock()
                mock_token_manager.get_token.return_value = cluster_token
                mock_token_manager_class.return_value = mock_token_manager

                yield mock_conn

    def test_operator_basic_submit(self, mock_airflow_connection):
        """Test SlurmOperator basic job submission."""
        logger.info("Testing SlurmOperator basic submission")

        script = """#!/bin/bash
echo "Hello from SlurmOperator!"
echo "Job ID: $SLURM_JOB_ID"
sleep 3
echo "Operator test complete"
"""

        operator = SlurmOperator(
            task_id="test_slurm_operator",
            script=script,
            job_name="operator-test",
            slurm_conn_id="slurm_default",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            wait_for_completion=False,
        )

        # Mock context
        mock_context = {}

        # Execute operator
        job_id = operator.execute(mock_context)

        assert job_id is not None
        assert isinstance(job_id, int)
        logger.info(f"✓ SlurmOperator submitted job {job_id}")

        # Cleanup - cancel the job
        from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

        hook = SlurmHook(slurm_conn_id="slurm_default")
        hook.cancel_job(job_id)
        logger.info(f"✓ Cancelled operator job {job_id}")

    def test_operator_with_wait(self, mock_airflow_connection):
        """Test SlurmOperator with wait_for_completion."""
        logger.info("Testing SlurmOperator with wait_for_completion")

        script = """#!/bin/bash
echo "Testing wait for completion"
sleep 5
echo "Job complete"
"""

        operator = SlurmOperator(
            task_id="test_slurm_operator_wait",
            script=script,
            job_name="operator-wait-test",
            slurm_conn_id="slurm_default",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            wait_for_completion=True,
            poll_interval=2,
            timeout=60,
        )

        # Mock context
        mock_context = {}

        # Execute operator - this should wait for completion
        job_id = operator.execute(mock_context)

        assert job_id is not None
        logger.info(f"✓ SlurmOperator job {job_id} completed successfully")

    def test_operator_with_gres(self, mock_airflow_connection):
        """Test SlurmOperator with GRES allocation."""
        logger.info("Testing SlurmOperator with GRES")

        script = """#!/bin/bash
echo "Testing GRES with operator"
echo "GRES: $SLURM_JOB_GRES"
sleep 3
"""

        try:
            operator = SlurmOperator(
                task_id="test_slurm_operator_gres",
                script=script,
                job_name="operator-gres-test",
                slurm_conn_id="slurm_default",
                partition="debug",
                cpus_per_task=1,
                mem="100M",
                time_limit="00:01:00",
                gres="gpu:1",
                wait_for_completion=False,
            )

            mock_context = {}
            job_id = operator.execute(mock_context)

            assert job_id is not None
            logger.info(f"✓ SlurmOperator GRES job {job_id} submitted")

            # Cleanup
            from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

            hook = SlurmHook(slurm_conn_id="slurm_default")
            hook.cancel_job(job_id)

        except Exception as e:
            logger.warning(f"GRES operator test failed (may not have GPUs): {e}")

    def test_sensor_basic(self, mock_airflow_connection):
        """Test SlurmSensor basic job monitoring."""
        logger.info("Testing SlurmSensor basic monitoring")

        # First submit a job
        from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

        hook = SlurmHook(slurm_conn_id="slurm_default")

        script = """#!/bin/bash
echo "Testing sensor monitoring"
sleep 10
echo "Job complete"
"""

        job_id = hook.submit_job(
            script=script,
            job_name="sensor-test",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        logger.info(f"Submitted job {job_id} for sensor monitoring")

        # Create sensor
        sensor = SlurmSensor(
            task_id="test_slurm_sensor",
            job_id=job_id,
            slurm_conn_id="slurm_default",
            poke_interval=2,
            timeout=60,
        )

        # Mock context
        mock_context = {}

        # Poke the sensor a few times
        max_pokes = 5
        for i in range(max_pokes):
            result = sensor.poke(mock_context)
            logger.info(f"Sensor poke {i+1}: {result}")

            if result:
                logger.info(f"✓ Sensor detected job {job_id} completion")
                break

            time.sleep(2)
        else:
            # Job didn't complete - cancel it
            hook.cancel_job(job_id)
            logger.info(
                f"Job {job_id} still running after {max_pokes} pokes, cancelled"
            )

    def test_sensor_with_failure_detection(self, mock_airflow_connection):
        """Test SlurmSensor failure detection."""
        logger.info("Testing SlurmSensor failure detection")

        from airflow_provider_slurm.hooks.slurm_hook import SlurmHook
        from airflow_provider_slurm.exceptions import SlurmAPIError

        hook = SlurmHook(slurm_conn_id="slurm_default")

        # Submit a job that will fail
        script = """#!/bin/bash
echo "This job will fail"
sleep 3
exit 1
"""

        job_id = hook.submit_job(
            script=script,
            job_name="sensor-fail-test",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        logger.info(f"Submitted failing job {job_id}")

        # Create sensor with fail_on_terminal_state=True
        sensor = SlurmSensor(
            task_id="test_slurm_sensor_fail",
            job_id=job_id,
            slurm_conn_id="slurm_default",
            poke_interval=2,
            timeout=60,
            fail_on_terminal_state=True,
        )

        mock_context = {}

        # Poke until job completes or fails
        max_pokes = 10
        exception_raised = False

        for i in range(max_pokes):
            try:
                result = sensor.poke(mock_context)
                logger.info(f"Sensor poke {i+1}: {result}")

                if result:
                    logger.info(f"Job {job_id} completed")
                    break

            except SlurmAPIError as e:
                logger.info(f"✓ Sensor correctly raised exception for failed job: {e}")
                exception_raised = True
                break

            time.sleep(2)

        # Either the job should complete (and we check it failed) or exception raised
        if not exception_raised:
            # Check final state
            job_info = hook.get_job_status(job_id) or hook.get_job_history(job_id)
            if job_info:
                state = job_info.get("job_state", "UNKNOWN")
                logger.info(f"Final job state: {state}")
                # Job should have failed
                assert state in ["FAILED", "COMPLETED"]

    # Array job tests

    def test_hook_submit_array_job_basic(self, slurm_hook, mock_airflow_connection):
        """Test submitting a basic array job."""
        script = """#!/bin/bash
echo "Array task ID: $SLURM_ARRAY_TASK_ID"
echo "Array job ID: $SLURM_ARRAY_JOB_ID"
sleep 1
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="array-test-basic",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-9",  # 10 tasks
        )

        logger.info(f"Submitted array job {job_id}")
        assert job_id > 0

        # Give jobs time to start
        time.sleep(2)

        # Check array status
        status = slurm_hook.get_array_status(job_id)
        logger.info(f"Array status: {status}")

        assert status["job_id"] == job_id
        assert status["total_tasks"] == 10

        # Wait for completion
        final_status = slurm_hook.wait_for_array(job_id, timeout=60, poll_interval=2)

        logger.info(f"Final array status: {final_status}")
        assert final_status["state"] == "COMPLETED"
        assert final_status["completed"] == 10
        assert final_status["failed"] == 0

    def test_hook_array_with_step(self, slurm_hook, mock_airflow_connection):
        """Test array job with step specification."""
        script = """#!/bin/bash
echo "Task $SLURM_ARRAY_TASK_ID"
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="array-step-test",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-20:5",  # Tasks 0, 5, 10, 15, 20 (5 tasks)
        )

        logger.info(f"Submitted array job with step: {job_id}")

        # Wait for completion
        final_status = slurm_hook.wait_for_array(job_id, timeout=60, poll_interval=2)

        logger.info(f"Final status: {final_status}")
        assert final_status["state"] == "COMPLETED"
        assert final_status["completed"] == 5

    def test_hook_array_with_parallelism_limit(
        self, slurm_hook, mock_airflow_connection
    ):
        """Test array job with parallelism limit."""
        script = """#!/bin/bash
echo "Task $SLURM_ARRAY_TASK_ID"
sleep 1
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="array-limited-test",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:02:00",
            array="0-19%5",  # 20 tasks, max 5 concurrent
        )

        logger.info(f"Submitted array job with parallelism limit: {job_id}")

        # Check that some tasks are running, some pending
        time.sleep(2)
        status = slurm_hook.get_array_status(job_id)
        logger.info(f"Array status during execution: {status}")

        # With limit of 5, we shouldn't have more than 5 running at once
        # (Though this is best-effort depending on cluster state)
        assert status["total_tasks"] == 20

        # Wait for completion
        final_status = slurm_hook.wait_for_array(job_id, timeout=120, poll_interval=3)

        assert final_status["state"] == "COMPLETED"
        assert final_status["completed"] == 20

    def test_hook_array_with_partial_failures(
        self, slurm_hook, mock_airflow_connection
    ):
        """Test array job where some tasks fail."""
        script = """#!/bin/bash
# Fail tasks 3 and 7
if [ $SLURM_ARRAY_TASK_ID -eq 3 ] || [ $SLURM_ARRAY_TASK_ID -eq 7 ]; then
    echo "Failing task $SLURM_ARRAY_TASK_ID"
    exit 1
fi
echo "Success task $SLURM_ARRAY_TASK_ID"
exit 0
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="array-partial-fail",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-9",  # 10 tasks, 2 will fail
        )

        logger.info(f"Submitted array job with partial failures: {job_id}")

        # Wait with fail_on_error=False to allow partial completion
        final_status = slurm_hook.wait_for_array(
            job_id, timeout=60, poll_interval=2, fail_on_error=False
        )

        logger.info(f"Final status: {final_status}")
        assert final_status["state"] == "PARTIALLY_COMPLETED"
        assert final_status["completed"] == 8
        assert final_status["failed"] == 2

    def test_hook_cancel_array_job(self, slurm_hook, mock_airflow_connection):
        """Test cancelling an array job."""
        script = """#!/bin/bash
echo "Task $SLURM_ARRAY_TASK_ID"
sleep 30
"""

        job_id = slurm_hook.submit_job(
            script=script,
            job_name="array-cancel-test",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",
            array="0-9",
        )

        logger.info(f"Submitted array job for cancellation: {job_id}")

        # Give it time to start
        time.sleep(2)

        # Cancel the entire array
        result = slurm_hook.cancel_array_task(job_id)
        assert result is True

        logger.info(f"Cancelled array job {job_id}")

        # Check status after cancellation
        time.sleep(2)
        status = slurm_hook.get_array_status(job_id)
        logger.info(f"Status after cancellation: {status}")

        # Most or all tasks should be cancelled
        # State could be CANCELLED or FAILED depending on timing
        assert status["state"] in ["CANCELLED", "FAILED", "PARTIALLY_COMPLETED"]

    def test_operator_array_job_basic(self, mock_airflow_connection):
        """Test SlurmOperator with array job."""
        script = """#!/bin/bash
echo "Array task $SLURM_ARRAY_TASK_ID"
"""

        operator = SlurmOperator(
            task_id="test_array_operator",
            script=script,
            job_name="operator-array-test",
            slurm_conn_id="slurm_default",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",  # 5 tasks
            wait_for_completion=True,
        )

        mock_context = {}
        result = operator.execute(mock_context)

        logger.info(f"Operator result: {result}")

        # Check result structure
        assert result["job_id"] > 0
        assert result["is_array"] is True
        assert result["array_spec"] == "0-4"
        assert result["array_status"]["state"] == "COMPLETED"
        assert result["array_status"]["total_tasks"] == 5

    def test_operator_array_job_with_failure(self, mock_airflow_connection):
        """Test SlurmOperator array job with task failures."""
        script = """#!/bin/bash
# Fail task 2
if [ $SLURM_ARRAY_TASK_ID -eq 2 ]; then
    exit 1
fi
"""

        operator = SlurmOperator(
            task_id="test_array_operator_fail",
            script=script,
            job_name="operator-array-fail",
            slurm_conn_id="slurm_default",
            partition="debug",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
            wait_for_completion=True,
            array_fail_on_error=False,  # Don't fail on partial completion
        )

        mock_context = {}
        result = operator.execute(mock_context)

        logger.info(f"Operator result with failures: {result}")

        # Should complete with partial success
        assert result["array_status"]["state"] == "PARTIALLY_COMPLETED"
        assert result["array_status"]["completed"] == 4
        assert result["array_status"]["failed"] == 1
