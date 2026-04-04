"""Integration tests against local Slurm cluster.

These tests run against a Slurm cluster on localhost with slurmrestd on port 6820.
They exercise the full stack: SlurmAPIClient, SlurmHook, SlurmOperator, and SlurmSensor.

Tests are parametrized to run against both v0.0.41 and v0.0.44 API versions to ensure
compatibility across the supported Slurm REST API range.

Usage:
    pytest tests/integration/test_local_cluster.py -v -s

Cluster layout (expected):
    - Partitions: normal (node1), gpu (node2, gres=gpu:1), all (node1-2)
    - API: http://localhost:6820 (slurmrestd)
    - Auth: JWT via `scontrol token`
"""

import logging
import re
import subprocess
import time
from unittest.mock import MagicMock, patch

import pytest

from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager

logger = logging.getLogger(__name__)

API_URL = "http://localhost:6820"
API_VERSIONS = ["v0.0.41", "v0.0.44"]


def _get_local_token(lifespan: int = 3600) -> str:
    """Get a JWT token from the local scontrol."""
    result = subprocess.run(
        ["scontrol", "token", f"lifespan={lifespan}"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        pytest.skip(f"Cannot get local token: {result.stderr}")
    match = re.search(r"SLURM_JWT=(\S+)", result.stdout)
    if not match:
        pytest.skip(f"Cannot parse token from: {result.stdout}")
    return match.group(1)


def _local_cluster_available() -> bool:
    """Check if local slurmrestd is running."""
    try:
        import requests

        token = _get_local_token()
        resp = requests.get(
            f"{API_URL}/slurm/{API_VERSIONS[0]}/ping/",
            headers={"X-SLURM-USER-TOKEN": token},
            timeout=5,
        )
        return resp.status_code == 200
    except Exception:
        return False


def _get_job_state(job_info):
    """Extract job state string from job info dict."""
    if job_info is None:
        return None
    state = job_info.get("job_state", "UNKNOWN")
    if isinstance(state, list):
        state = state[0]
    return state


CLUSTER_AVAILABLE = _local_cluster_available()
SKIP_REASON = "Local Slurm cluster not available (slurmrestd on localhost:6820)"


@pytest.fixture(scope="module")
def local_token():
    """Get a fresh JWT token for the test module."""
    if not CLUSTER_AVAILABLE:
        pytest.skip(SKIP_REASON)
    return _get_local_token()


@pytest.fixture(params=API_VERSIONS)
def api_version(request):
    """Parametrize tests across API versions."""
    return request.param


@pytest.fixture
def api_client(local_token, api_version):
    """Create a SlurmAPIClient configured for the local cluster."""
    token_manager = MagicMock(spec=SlurmTokenManager)
    token_manager.get_token.return_value = local_token

    return SlurmAPIClient(
        base_url=API_URL,
        token_manager=token_manager,
        api_version=api_version,
        timeout=30,
        max_retries=3,
    )


@pytest.fixture
def slurm_hook(local_token, api_version):
    """Create a SlurmHook configured for the local cluster."""
    from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

    token_manager = MagicMock(spec=SlurmTokenManager)
    token_manager.get_token.return_value = local_token

    hook = SlurmHook(api_url=API_URL, username="jontk")
    hook._token_manager = token_manager
    hook._client = SlurmAPIClient(
        base_url=API_URL,
        token_manager=token_manager,
        api_version=api_version,
    )
    return hook


@pytest.fixture
def mock_airflow_connection(local_token, api_version):
    """Mock Airflow connection for Operator/Sensor tests."""
    mock_conn = MagicMock()
    mock_conn.host = "localhost"
    mock_conn.port = 6820
    mock_conn.schema = "http"
    mock_conn.login = "jontk"

    with patch(
        "airflow_provider_slurm.hooks.slurm_hook.BaseHook.get_connection",
        return_value=mock_conn,
    ):
        with patch(
            "airflow_provider_slurm.hooks.slurm_hook.SlurmTokenManager"
        ) as mock_tm_class:
            mock_tm = MagicMock()
            mock_tm.get_token.return_value = local_token
            mock_tm_class.return_value = mock_tm

            # Patch the default API version so Operator/Sensor use the parametrized version
            with patch.object(
                SlurmAPIClient, "__init__",
                wraps=SlurmAPIClient.__init__,
            ) as mock_init:
                original_init = SlurmAPIClient.__init__

                def patched_init(self, base_url, token_manager, api_version=api_version, **kwargs):
                    original_init(self, base_url, token_manager, api_version=api_version, **kwargs)

                with patch.object(SlurmAPIClient, "__init__", patched_init):
                    yield mock_conn


def _wait_for_job(api_client, job_id, timeout=30):
    """Wait for a job to reach a terminal state."""
    for _ in range(timeout):
        info = api_client.get_job(job_id)
        if info is None:
            return "COMPLETED"  # Job left queue = completed
        state = _get_job_state(info)
        if state in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"):
            return state
        time.sleep(1)
    return state


# ─── API Client Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestAPIClient:
    """Direct API client tests against local cluster."""

    def test_ping(self, api_client):
        assert api_client.ping() is True

    def test_submit_simple_job(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho hello\nhostname\ndate",
            "job": {
                "name": "test-api-simple",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
            },
        }
        result = api_client.submit_job(job_spec)
        job_id = result["job_id"]
        assert job_id > 0

        info = api_client.get_job(job_id)
        assert info is not None
        state = _get_job_state(info)
        logger.info(f"Job {job_id} state: {state}")
        assert state in ("PENDING", "RUNNING", "COMPLETED")

    def test_submit_and_wait_for_completion(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho 'test complete'\nexit 0",
            "job": {
                "name": "test-api-complete",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
            },
        }
        result = api_client.submit_job(job_spec)
        job_id = result["job_id"]

        state = _wait_for_job(api_client, job_id, timeout=30)
        logger.info(f"Job {job_id} final state: {state}")
        assert state == "COMPLETED"

    def test_submit_gpu_job(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho GRES=$SLURM_JOB_GRES\nnvidia-smi -L 2>/dev/null || echo 'no nvidia-smi'",
            "job": {
                "name": "test-api-gpu",
                "partition": "gpu",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "gres": "gpu:1",
                "time_limit": {"minutes": 1},
            },
        }
        result = api_client.submit_job(job_spec)
        job_id = result["job_id"]

        state = _wait_for_job(api_client, job_id, timeout=30)
        logger.info(f"GPU job {job_id} final state: {state}")
        assert state == "COMPLETED"

    def test_submit_array_job(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            "job": {
                "name": "test-api-array",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
            },
        }
        result = api_client.submit_job(job_spec, array="0-4")
        assert result.get("array") == "0-4"
        assert result.get("array_task_count") == 5

        state = _wait_for_job(api_client, result["job_id"], timeout=60)
        logger.info(f"Array job final state: {state}")

    def test_submit_with_dependency(self, api_client):
        prereq_spec = {
            "script": "#!/bin/bash\necho prereq done\nexit 0",
            "job": {
                "name": "test-prereq",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
            },
        }
        prereq_id = api_client.submit_job(prereq_spec)["job_id"]

        dep_spec = {
            "script": "#!/bin/bash\necho dependent done\nexit 0",
            "job": {
                "name": "test-dependent",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
            },
        }
        dep_result = api_client.submit_job(
            dep_spec, dependency=f"afterok:{prereq_id}"
        )
        dep_id = dep_result["job_id"]
        assert dep_result.get("dependency") == f"afterok:{prereq_id}"

        state = _wait_for_job(api_client, dep_id, timeout=60)
        logger.info(f"Dependent job {dep_id} final state: {state}")
        assert state == "COMPLETED"

    def test_cancel_job(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\nsleep 300",
            "job": {
                "name": "test-cancel",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 5},
            },
        }
        result = api_client.submit_job(job_spec)
        job_id = result["job_id"]

        time.sleep(2)
        cancel_result = api_client.cancel_job(job_id)
        assert cancel_result is not None
        logger.info(f"Cancelled job {job_id}")

    def test_submit_exclusive_job(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho Exclusive\nexit 0",
            "job": {
                "name": "test-exclusive",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
                "exclusive": True,
            },
        }
        result = api_client.submit_job(job_spec)
        state = _wait_for_job(api_client, result["job_id"], timeout=30)
        logger.info(f"Exclusive job final state: {state}")
        assert state == "COMPLETED"

    def test_submit_with_nodelist(self, api_client):
        job_spec = {
            "script": "#!/bin/bash\necho on $(hostname)\nexit 0",
            "job": {
                "name": "test-nodelist",
                "partition": "normal",
                "current_working_directory": "/tmp",
                "environment": ["PATH=/usr/bin:/bin"],
                "time_limit": {"minutes": 1},
                "required_nodes": ["node1"],
            },
        }
        result = api_client.submit_job(job_spec)
        state = _wait_for_job(api_client, result["job_id"], timeout=30)
        logger.info(f"Nodelist job final state: {state}")
        assert state == "COMPLETED"


# ─── Hook Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestHook:
    """SlurmHook tests against local cluster."""

    def test_connection(self, slurm_hook):
        success, msg = slurm_hook.test_connection()
        assert success is True

    def test_submit_and_monitor(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho hello from hook\nsleep 2\nexit 0",
            job_name="test-hook-basic",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )
        assert job_id > 0

        state = slurm_hook.wait_for_job(job_id, timeout=30)
        logger.info(f"Job {job_id} final state: {state}")
        assert state == "COMPLETED"

    def test_submit_gpu_job(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho GRES=$SLURM_JOB_GRES\nexit 0",
            job_name="test-hook-gpu",
            partition="gpu",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            gres="gpu:1",
        )
        assert job_id > 0

        state = slurm_hook.wait_for_job(job_id, timeout=30)
        logger.info(f"GPU job {job_id} state: {state}")
        assert state == "COMPLETED"

    def test_submit_array_job(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            job_name="test-hook-array",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
        )
        assert job_id > 0

        status = slurm_hook.wait_for_array(job_id, timeout=60, poll_interval=2)
        logger.info(f"Array status: {status}")
        assert status["state"] == "COMPLETED"
        assert status["total_tasks"] == 5

    def test_submit_with_dependency(self, slurm_hook):
        prereq_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho prereq\nsleep 2\nexit 0",
            job_name="test-hook-prereq",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        dep_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho dependent\nexit 0",
            job_name="test-hook-dep",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            dependency=f"afterok:{prereq_id}",
        )
        logger.info(f"Dependency chain: {prereq_id} -> {dep_id}")

        state = slurm_hook.wait_for_job(prereq_id, timeout=30)
        assert state == "COMPLETED"

        state = slurm_hook.wait_for_job(dep_id, timeout=30)
        assert state == "COMPLETED"

    def test_cancel_job(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 300",
            job_name="test-hook-cancel",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",
        )
        time.sleep(2)
        result = slurm_hook.cancel_job(job_id)
        assert result is True

    def test_submit_exclusive(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho exclusive\nexit 0",
            job_name="test-hook-exclusive",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            exclusive=True,
        )
        assert job_id > 0
        state = slurm_hook.wait_for_job(job_id, timeout=30)
        assert state == "COMPLETED"

    def test_submit_with_nodelist(self, slurm_hook):
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho on $(hostname)\nexit 0",
            job_name="test-hook-nodelist",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            nodelist="node1",
        )
        assert job_id > 0
        state = slurm_hook.wait_for_job(job_id, timeout=30)
        assert state == "COMPLETED"


# ─── Operator Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestOperator:
    """SlurmOperator tests against local cluster."""

    def test_basic_submit(self, mock_airflow_connection):
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_basic",
            script="#!/bin/bash\necho operator test\nexit 0",
            job_name="test-op-basic",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            wait_for_completion=True,
            poll_interval=2,
            timeout=30,
        )
        result = operator.execute({})
        assert result is not None
        logger.info(f"Operator result: {result}")

    def test_gpu_submit(self, mock_airflow_connection):
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_gpu",
            script="#!/bin/bash\necho GRES=$SLURM_JOB_GRES\nexit 0",
            job_name="test-op-gpu",
            slurm_conn_id="slurm_default",
            partition="gpu",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            gres="gpu:1",
            wait_for_completion=True,
            poll_interval=2,
            timeout=30,
        )
        result = operator.execute({})
        assert result is not None
        logger.info(f"GPU operator result: {result}")

    def test_array_submit(self, mock_airflow_connection):
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_array",
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            job_name="test-op-array",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
            wait_for_completion=True,
            poll_interval=2,
            timeout=60,
        )
        result = operator.execute({})
        assert result["is_array"] is True
        assert result["array_status"]["state"] == "COMPLETED"
        assert result["array_status"]["total_tasks"] == 5

    def test_dependency_submit(self, mock_airflow_connection):
        from airflow_provider_slurm.hooks.slurm_hook import SlurmHook
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        hook = SlurmHook(slurm_conn_id="slurm_default")
        prereq_id = hook.submit_job(
            script="#!/bin/bash\necho prereq\nsleep 2\nexit 0",
            job_name="test-op-prereq",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        operator = SlurmOperator(
            task_id="test_op_dep",
            script="#!/bin/bash\necho dependent\nexit 0",
            job_name="test-op-dep",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            dependency=f"afterok:{prereq_id}",
            wait_for_completion=True,
            poll_interval=2,
            timeout=60,
        )
        result = operator.execute({})
        assert result.get("dependency") == f"afterok:{prereq_id}"
        logger.info(f"Dependent operator completed: {result}")


# ─── Sensor Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestSensor:
    """SlurmSensor tests against local cluster."""

    def test_sensor_detects_completion(self, slurm_hook, mock_airflow_connection):
        from airflow_provider_slurm.sensors.slurm import SlurmSensor

        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho sensor test\nsleep 3\nexit 0",
            job_name="test-sensor",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        sensor = SlurmSensor(
            task_id="test_sensor",
            job_id=job_id,
            slurm_conn_id="slurm_default",
            poke_interval=2,
            timeout=30,
        )

        for i in range(15):
            result = sensor.poke({})
            if result:
                logger.info(f"Sensor detected job {job_id} completion")
                return
            time.sleep(2)

        pytest.fail(f"Sensor did not detect completion for job {job_id}")

    def test_sensor_detects_failure(self, slurm_hook, mock_airflow_connection):
        """Test sensor raises on job failure with fail_on_terminal_state=True."""
        from airflow_provider_slurm.exceptions import SlurmAPIError
        from airflow_provider_slurm.sensors.slurm import SlurmSensor

        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nexit 1",
            job_name="test-sensor-fail",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        sensor = SlurmSensor(
            task_id="test_sensor_fail",
            job_id=job_id,
            slurm_conn_id="slurm_default",
            poke_interval=2,
            timeout=30,
            fail_on_terminal_state=True,
        )

        for i in range(15):
            try:
                result = sensor.poke({})
                if result:
                    pytest.fail("Sensor should have raised on failure, not returned True")
                time.sleep(2)
            except SlurmAPIError as e:
                logger.info(f"Sensor correctly raised on failure: {e}")
                return

        pytest.fail(f"Sensor did not detect failure for job {job_id}")

    def test_sensor_no_raise_on_failure(self, slurm_hook, mock_airflow_connection):
        """Test sensor returns True (not raises) with fail_on_terminal_state=False."""
        from airflow_provider_slurm.sensors.slurm import SlurmSensor

        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nexit 1",
            job_name="test-sensor-nofail",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        sensor = SlurmSensor(
            task_id="test_sensor_nofail",
            job_id=job_id,
            slurm_conn_id="slurm_default",
            poke_interval=2,
            timeout=30,
            fail_on_terminal_state=False,
        )

        for i in range(15):
            result = sensor.poke({})
            if result:
                logger.info(f"Sensor returned True for failed job (no raise)")
                return
            time.sleep(2)

        pytest.fail(f"Sensor did not detect terminal state for job {job_id}")


# ─── Advanced Hook Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestHookAdvanced:
    """Advanced hook tests: failure handling, array features, dependencies."""

    # ── Array job edge cases ──

    def test_array_with_step(self, slurm_hook):
        """Test array job with step specification (0-20:5 = tasks 0,5,10,15,20)."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            job_name="test-array-step",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-20:5",
        )

        status = slurm_hook.wait_for_array(job_id, timeout=60, poll_interval=2)
        logger.info(f"Array step status: {status}")
        assert status["state"] == "COMPLETED"
        assert status["total_tasks"] == 5

    def test_array_with_parallelism_limit(self, slurm_hook):
        """Test array job with max concurrent tasks (0-9%3)."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nsleep 1\nexit 0",
            job_name="test-array-limit",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:02:00",
            array="0-9%3",
        )

        status = slurm_hook.wait_for_array(job_id, timeout=120, poll_interval=2)
        logger.info(f"Array parallelism status: {status}")
        assert status["state"] == "COMPLETED"
        assert status["total_tasks"] == 10

    def test_array_with_explicit_list(self, slurm_hook):
        """Test array job with explicit task list (1,5,10)."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            job_name="test-array-list",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="1,5,10",
        )

        status = slurm_hook.wait_for_array(job_id, timeout=60, poll_interval=2)
        logger.info(f"Array list status: {status}")
        assert status["state"] == "COMPLETED"
        assert status["total_tasks"] == 3

    def test_array_partial_failure(self, slurm_hook):
        """Test array with some failing tasks and fail_on_error=False."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nif [ $SLURM_ARRAY_TASK_ID -eq 2 ]; then exit 1; fi\nexit 0",
            job_name="test-array-partial",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
        )

        status = slurm_hook.wait_for_array(
            job_id, timeout=60, poll_interval=2, fail_on_error=False
        )
        logger.info(f"Partial failure status: {status}")
        assert status["state"] == "PARTIALLY_COMPLETED"
        assert status["completed"] == 4
        assert status["failed"] == 1

    def test_array_partial_failure_raises(self, slurm_hook):
        """Test array with failing tasks and fail_on_error=True raises."""
        from airflow_provider_slurm.exceptions import SlurmAPIError

        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nif [ $SLURM_ARRAY_TASK_ID -eq 1 ]; then exit 1; fi\nexit 0",
            job_name="test-array-fail-raise",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
        )

        with pytest.raises(SlurmAPIError, match="partially completed"):
            slurm_hook.wait_for_array(
                job_id, timeout=60, poll_interval=2, fail_on_error=True
            )

    def test_cancel_array_task_all(self, slurm_hook):
        """Test cancelling an entire array job."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 300",
            job_name="test-cancel-array",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",
            array="0-4",
        )
        time.sleep(2)

        result = slurm_hook.cancel_array_task(job_id)
        assert result is True
        logger.info(f"Cancelled entire array job {job_id}")

    def test_cancel_array_task_specific(self, slurm_hook):
        """Test cancelling a specific array task."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 300",
            job_name="test-cancel-task",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",
            array="0-4",
        )
        time.sleep(2)

        # Cancel just task 2
        result = slurm_hook.cancel_array_task(job_id, array_task_id=2)
        assert result is True
        logger.info(f"Cancelled task 2 of array job {job_id}")

        # Cancel the rest for cleanup
        slurm_hook.cancel_array_task(job_id)

    # ── Dependency types ──

    def test_dependency_afterany(self, slurm_hook):
        """Test afterany dependency (runs after job ends, regardless of exit)."""
        # Submit a job that fails
        failing_id = slurm_hook.submit_job(
            script="#!/bin/bash\nexit 1",
            job_name="test-dep-fail",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )
        logger.info(f"Submitted failing job {failing_id}")

        # Submit cleanup job with afterany
        cleanup_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho cleanup\nexit 0",
            job_name="test-dep-cleanup",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            dependency=f"afterany:{failing_id}",
        )
        logger.info(f"Submitted cleanup job {cleanup_id} (afterany:{failing_id})")

        # Wait for failing job (expect error)
        try:
            slurm_hook.wait_for_job(failing_id, timeout=30)
        except Exception:
            pass  # Expected to fail

        # Cleanup job should still complete
        state = slurm_hook.wait_for_job(cleanup_id, timeout=30)
        assert state == "COMPLETED"

    def test_dependency_afternotok(self, slurm_hook):
        """Test afternotok dependency (runs only if predecessor fails)."""
        # Submit a job that fails
        failing_id = slurm_hook.submit_job(
            script="#!/bin/bash\nexit 1",
            job_name="test-dep-notok-fail",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        # Submit recovery job with afternotok
        recovery_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho recovery\nexit 0",
            job_name="test-dep-recovery",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            dependency=f"afternotok:{failing_id}",
        )
        logger.info(f"Recovery {recovery_id} afternotok:{failing_id}")

        try:
            slurm_hook.wait_for_job(failing_id, timeout=30)
        except Exception:
            pass

        state = slurm_hook.wait_for_job(recovery_id, timeout=30)
        assert state == "COMPLETED"

    def test_dependency_multiple_jobs(self, slurm_hook):
        """Test dependency on multiple predecessor jobs."""
        job_ids = []
        for i in range(3):
            jid = slurm_hook.submit_job(
                script=f"#!/bin/bash\necho prereq {i}\nsleep 1\nexit 0",
                job_name=f"test-multi-prereq-{i}",
                partition="normal",
                cpus_per_task=1,
                mem="100M",
                time_limit="00:01:00",
            )
            job_ids.append(jid)

        # Depend on all three
        dep_spec = "afterok:" + ":".join(str(j) for j in job_ids)
        final_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho all done\nexit 0",
            job_name="test-multi-dep",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            dependency=dep_spec,
        )
        logger.info(f"Final job {final_id} depends on {job_ids}")

        for jid in job_ids:
            slurm_hook.wait_for_job(jid, timeout=30)

        state = slurm_hook.wait_for_job(final_id, timeout=30)
        assert state == "COMPLETED"

    def test_dependency_with_array(self, slurm_hook):
        """Test array job with dependency on a regular job."""
        prereq_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho setup\nsleep 2\nexit 0",
            job_name="test-arr-dep-prereq",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        array_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho Task $SLURM_ARRAY_TASK_ID\nexit 0",
            job_name="test-arr-dep-array",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:02:00",
            array="0-4",
            dependency=f"afterok:{prereq_id}",
        )
        logger.info(f"Array {array_id} depends on {prereq_id}")

        slurm_hook.wait_for_job(prereq_id, timeout=30)
        status = slurm_hook.wait_for_array(array_id, timeout=60, poll_interval=2)
        assert status["state"] == "COMPLETED"
        assert status["total_tasks"] == 5

    # ── Job parameters ──

    def test_custom_working_dir(self, slurm_hook):
        """Test job with explicit working directory."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\npwd\nexit 0",
            job_name="test-workdir",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            working_dir="/tmp",
        )

        state = slurm_hook.wait_for_job(job_id, timeout=30)
        assert state == "COMPLETED"

    def test_custom_environment(self, slurm_hook):
        """Test job with custom environment variables."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho MY_VAR=$MY_VAR\nexit 0",
            job_name="test-env",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            environment={"MY_VAR": "test_value", "PATH": "/usr/bin:/bin"},
        )

        state = slurm_hook.wait_for_job(job_id, timeout=30)
        assert state == "COMPLETED"

    def test_stdout_stderr_paths(self, slurm_hook):
        """Test job with explicit stdout/stderr paths."""
        import os
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, "test_%j.out")
            stderr_path = os.path.join(tmpdir, "test_%j.err")

            job_id = slurm_hook.submit_job(
                script="#!/bin/bash\necho stdout_test\necho stderr_test >&2\nexit 0",
                job_name="test-stdio",
                partition="normal",
                cpus_per_task=1,
                mem="100M",
                time_limit="00:01:00",
                stdout=stdout_path,
                stderr=stderr_path,
            )

            state = slurm_hook.wait_for_job(job_id, timeout=30)
            assert state == "COMPLETED"

            # Verify output files were created (substituting %j with job_id)
            actual_stdout = stdout_path.replace("%j", str(job_id))
            actual_stderr = stderr_path.replace("%j", str(job_id))
            # Give filesystem a moment to sync
            time.sleep(1)
            assert os.path.exists(actual_stdout), f"stdout not found: {actual_stdout}"
            assert os.path.exists(actual_stderr), f"stderr not found: {actual_stderr}"
            logger.info(f"Job {job_id} stdout/stderr files created")

    def test_multi_cpu_job(self, slurm_hook):
        """Test job requesting multiple CPUs."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\necho CPUs=$SLURM_CPUS_PER_TASK\nnproc\nexit 0",
            job_name="test-multi-cpu",
            partition="normal",
            cpus_per_task=2,
            mem="200M",
            time_limit="00:01:00",
        )

        state = slurm_hook.wait_for_job(job_id, timeout=30)
        assert state == "COMPLETED"

    # ── Parameter verification tests ──
    # These verify parameters actually took effect, not just that the job completed.

    def _wait_for_running(self, slurm_hook, job_id, timeout=15):
        """Wait for a job to reach RUNNING or COMPLETED state."""
        for _ in range(timeout):
            info = slurm_hook.get_job_status(job_id)
            if info:
                state = info.get("job_state", "UNKNOWN")
                if isinstance(state, list):
                    state = state[0]
                if state in ("RUNNING", "COMPLETING", "COMPLETED"):
                    return info
            time.sleep(1)
        return slurm_hook.get_job_status(job_id)

    def test_gpu_actually_allocated(self, slurm_hook):
        """Verify GPU is actually allocated via tres_per_node, not silently ignored."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-gpu-verify",
            partition="gpu",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            gres="gpu:1",
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        tres_alloc = info.get("tres_alloc_str", "")
        logger.info(f"Job {job_id} TRES allocated: {tres_alloc}")
        assert "gres/gpu=1" in tres_alloc, f"GPU not allocated! tres_alloc_str={tres_alloc}"

        slurm_hook.cancel_job(job_id)

    def test_time_limit_correct(self, slurm_hook):
        """Verify time_limit is set correctly (in minutes, not 60x too large)."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 5\nexit 0",
            job_name="test-time-verify",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:05:00",  # 5 minutes
        )

        info = slurm_hook.get_job_status(job_id)
        assert info is not None
        time_limit = info.get("time_limit", {})
        if isinstance(time_limit, dict):
            limit_minutes = time_limit.get("number", 0)
        else:
            limit_minutes = time_limit
        logger.info(f"Job {job_id} time_limit: {time_limit}")
        assert limit_minutes == 5, f"Expected 5 minutes, got {limit_minutes}"

        slurm_hook.cancel_job(job_id)

    def test_nodelist_actually_targets_node(self, slurm_hook):
        """Verify nodelist actually constrains to specified node."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-node-verify",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            nodelist="node1",
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        nodes = info.get("nodes", info.get("batch_host", ""))
        logger.info(f"Job {job_id} assigned to nodes: {nodes}")
        assert "node1" in str(nodes), f"Job not on node1! nodes={nodes}"

        slurm_hook.cancel_job(job_id)

    def test_memory_actually_set(self, slurm_hook):
        """Verify memory allocation is applied."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-mem-verify",
            partition="normal",
            cpus_per_task=1,
            mem="256M",
            time_limit="00:01:00",
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        tres_alloc = info.get("tres_alloc_str", "")
        logger.info(f"Job {job_id} TRES: {tres_alloc}")
        assert "mem=256M" in tres_alloc, f"Memory not 256M! tres={tres_alloc}"

        slurm_hook.cancel_job(job_id)

    def test_node_count_actually_set(self, slurm_hook):
        """Verify nodes parameter allocates multiple nodes."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-nodecount-verify",
            partition="all",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            nodes=2,
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        node_count = info.get("node_count", {})
        if isinstance(node_count, dict):
            count = node_count.get("number", 0)
        else:
            count = node_count
        logger.info(f"Job {job_id} node_count: {node_count}")
        assert count == 2, f"Expected 2 nodes, got {count}"

        slurm_hook.cancel_job(job_id)

    def test_tasks_per_node_actually_set(self, slurm_hook):
        """Verify ntasks_per_node parameter is applied."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-tpn-verify",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            ntasks_per_node=2,
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        tpn = info.get("tasks_per_node", {})
        if isinstance(tpn, dict):
            tpn_val = tpn.get("number", 0)
        else:
            tpn_val = tpn
        logger.info(f"Job {job_id} tasks_per_node: {tpn}")
        assert tpn_val == 2, f"Expected 2 tasks_per_node, got {tpn_val}"

        slurm_hook.cancel_job(job_id)

    def test_exclusive_actually_set(self, slurm_hook):
        """Verify exclusive flag is applied."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 3\nexit 0",
            job_name="test-excl-verify",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            exclusive=True,
        )

        info = self._wait_for_running(slurm_hook, job_id)
        assert info is not None
        # Exclusive jobs get all CPUs on the node
        tres_alloc = info.get("tres_alloc_str", "")
        logger.info(f"Job {job_id} TRES: {tres_alloc}")
        # node1 has 6 CPUs - exclusive should allocate all of them
        assert "cpu=6" in tres_alloc, f"Expected all 6 CPUs for exclusive, got tres={tres_alloc}"

        slurm_hook.cancel_job(job_id)

    def test_job_failure_detected(self, slurm_hook):
        """Test that wait_for_job raises on failed job."""
        from airflow_provider_slurm.exceptions import SlurmAPIError

        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nexit 42",
            job_name="test-fail-detect",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        with pytest.raises(SlurmAPIError, match="failed with state"):
            slurm_hook.wait_for_job(job_id, timeout=30)

    def test_get_job_status_directly(self, slurm_hook):
        """Test get_job_status returns job info."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 5\nexit 0",
            job_name="test-status",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
        )

        info = slurm_hook.get_job_status(job_id)
        assert info is not None
        assert "job_state" in info
        logger.info(f"Job {job_id} status: {info.get('job_state')}")

        # Cleanup
        slurm_hook.cancel_job(job_id)

    def test_get_array_status_directly(self, slurm_hook):
        """Test get_array_status returns aggregated array info."""
        job_id = slurm_hook.submit_job(
            script="#!/bin/bash\nsleep 5\nexit 0",
            job_name="test-array-status",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-2",
        )
        time.sleep(2)

        status = slurm_hook.get_array_status(job_id)
        assert status["job_id"] == job_id
        assert status["total_tasks"] == 3
        assert "completed" in status
        assert "running" in status
        assert "pending" in status
        assert "failed" in status
        assert "state" in status
        assert "tasks" in status
        logger.info(f"Array status: {status['state']}, tasks: {status['total_tasks']}")

        # Cleanup
        slurm_hook.cancel_array_task(job_id)


# ─── Advanced Operator Tests ───


@pytest.mark.skipif(not CLUSTER_AVAILABLE, reason=SKIP_REASON)
class TestOperatorAdvanced:
    """Advanced operator tests."""

    def test_exclusive_submit(self, mock_airflow_connection):
        """Test operator with exclusive node allocation."""
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_exclusive",
            script="#!/bin/bash\necho exclusive\nexit 0",
            job_name="test-op-exclusive",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            exclusive=True,
            wait_for_completion=True,
            poll_interval=2,
            timeout=30,
        )
        result = operator.execute({})
        assert result is not None
        logger.info(f"Exclusive operator result: {result}")

    def test_nodelist_submit(self, mock_airflow_connection):
        """Test operator with specific node targeting."""
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_nodelist",
            script="#!/bin/bash\necho on $(hostname)\nexit 0",
            job_name="test-op-nodelist",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            nodelist="node1",
            wait_for_completion=True,
            poll_interval=2,
            timeout=30,
        )
        result = operator.execute({})
        assert result is not None
        logger.info(f"Nodelist operator result: {result}")

    def test_array_partial_failure_no_raise(self, mock_airflow_connection):
        """Test operator with array_fail_on_error=False allows partial failure."""
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_array_partial",
            script="#!/bin/bash\nif [ $SLURM_ARRAY_TASK_ID -eq 2 ]; then exit 1; fi\nexit 0",
            job_name="test-op-array-partial",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            array="0-4",
            array_fail_on_error=False,
            wait_for_completion=True,
            poll_interval=2,
            timeout=60,
        )
        result = operator.execute({})
        assert result["is_array"] is True
        assert result["array_status"]["state"] == "PARTIALLY_COMPLETED"
        assert result["array_status"]["completed"] == 4
        assert result["array_status"]["failed"] == 1
        logger.info(f"Partial failure operator result: {result}")

    def test_no_wait_for_completion(self, mock_airflow_connection):
        """Test operator with wait_for_completion=False returns immediately."""
        from airflow_provider_slurm.hooks.slurm_hook import SlurmHook
        from airflow_provider_slurm.operators.slurm import SlurmOperator

        operator = SlurmOperator(
            task_id="test_op_nowait",
            script="#!/bin/bash\nsleep 10\nexit 0",
            job_name="test-op-nowait",
            slurm_conn_id="slurm_default",
            partition="normal",
            cpus_per_task=1,
            mem="100M",
            time_limit="00:01:00",
            wait_for_completion=False,
        )
        result = operator.execute({})
        assert result is not None
        assert result["job_id"] > 0
        # Should not have array_status since we didn't wait
        assert "array_status" not in result
        logger.info(f"No-wait operator result: {result}")

        # Cleanup
        hook = SlurmHook(slurm_conn_id="slurm_default")
        hook.cancel_job(result["job_id"])
