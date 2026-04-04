"""Slurm Sensor for Apache Airflow.

This sensor waits for a Slurm job to reach a terminal state.
"""

import logging
from typing import Any, Optional, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from airflow_provider_slurm.exceptions import SlurmAPIError
from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

logger = logging.getLogger(__name__)


class SlurmSensor(BaseSensorOperator):
    """Wait for a Slurm job to complete.

    This sensor checks the status of a Slurm job and returns True when
    the job reaches a terminal state (COMPLETED, FAILED, CANCELLED, etc.).

    Args:
        job_id: Slurm job ID to monitor (can be templated)
        slurm_conn_id: Airflow connection ID for Slurm
        fail_on_terminal_state: If True, fail when job ends in non-success state
        success_states: States considered successful (default: ["COMPLETED"])
        failure_states: States considered failed (default: ["FAILED", "CANCELLED",
                        "TIMEOUT", "NODE_FAIL", "PREEMPTED"])

    Returns:
        True when job reaches terminal state

    Example:
        Wait for a specific job:

        >>> wait_for_job = SlurmSensor(
        ...     task_id="wait_for_training",
        ...     job_id=12345,
        ...     poke_interval=30,
        ...     timeout=3600,
        ... )

        Wait for job from previous task:

        >>> wait_for_job = SlurmSensor(
        ...     task_id="wait_for_completion",
        ...     job_id="{{ task_instance.xcom_pull(task_ids='submit_job') }}",
        ...     poke_interval=60,
        ... )

        Don't fail on job failure (just wait for completion):

        >>> wait_for_job = SlurmSensor(
        ...     task_id="wait_regardless",
        ...     job_id="{{ ti.xcom_pull(task_ids='submit_job') }}",
        ...     fail_on_terminal_state=False,
        ... )
    """

    template_fields: Sequence[str] = ("job_id",)
    ui_color = "#f0e68c"

    def __init__(
        self,
        *,
        job_id: int,
        slurm_conn_id: str = "slurm_default",
        fail_on_terminal_state: bool = True,
        success_states: Optional[Sequence[str]] = None,
        failure_states: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the SlurmSensor."""
        super().__init__(**kwargs)
        self.job_id = job_id
        self.slurm_conn_id = slurm_conn_id
        self.fail_on_terminal_state = fail_on_terminal_state

        # Define success and failure states
        self.success_states = success_states or ["COMPLETED"]
        self.failure_states = failure_states or [
            "FAILED",
            "CANCELLED",
            "TIMEOUT",
            "NODE_FAIL",
            "PREEMPTED",
            "BOOT_FAIL",
            "DEADLINE",
            "OUT_OF_MEMORY",
        ]

    def poke(self, context: Context) -> bool:
        """Check if the Slurm job has reached a terminal state.

        Args:
            context: Airflow task context

        Returns:
            True if job is in terminal state, False otherwise

        Raises:
            SlurmAPIError: If fail_on_terminal_state is True and job failed
        """
        hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)

        logger.info(f"Checking status of Slurm job {self.job_id}")

        # Get job status
        job_info = hook.get_job_status(self.job_id)

        if job_info is None:
            # Job not found - might have been cleaned up
            # Check history
            logger.warning(
                f"Job {self.job_id} not found in active queue, checking history"
            )
            job_info = hook.get_job_history(self.job_id)

            if job_info is None:
                raise SlurmAPIError(f"Job {self.job_id} not found in queue or history")

        state = job_info.get("job_state", "UNKNOWN")
        # Slurm REST API v0.0.41+ returns job_state as a list
        if isinstance(state, list):
            state = state[0] if state else "UNKNOWN"
        logger.info(f"Job {self.job_id} is in state: {state}")

        # Check if in success state
        if state in self.success_states:
            logger.info(f"Job {self.job_id} completed successfully")
            return True

        # Check if in failure state
        if state in self.failure_states:
            if self.fail_on_terminal_state:
                state_reason = job_info.get("state_reason", "Unknown")
                raise SlurmAPIError(
                    f"Job {self.job_id} failed with state {state}: {state_reason}"
                )
            else:
                logger.warning(
                    f"Job {self.job_id} ended in failure state {state}, "
                    "but fail_on_terminal_state=False"
                )
                return True

        # Still running or pending
        logger.info(f"Job {self.job_id} still in non-terminal state: {state}")
        return False

    def execute(self, context: Context) -> Any:
        """Execute the sensor and push job info to XCom.

        Args:
            context: Airflow task context

        Returns:
            Final job information dictionary
        """
        # Call parent execute which handles the poke loop
        super().execute(context)

        # After poke returns True, get final job info
        hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)
        job_info = hook.get_job_status(self.job_id)

        if job_info is None:
            job_info = hook.get_job_history(self.job_id)

        logger.info(f"Job {self.job_id} monitoring complete")
        return job_info
