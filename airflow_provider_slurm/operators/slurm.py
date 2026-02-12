"""Slurm Operator for Apache Airflow.

This operator submits jobs directly to Slurm and waits for completion.
"""

import logging
from typing import Any, Dict, Optional, Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context

from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

logger = logging.getLogger(__name__)


class SlurmOperator(BaseOperator):
    """Submit a job to Slurm and optionally wait for completion.

    This operator uses SlurmHook to submit a bash script as a Slurm job.
    It can either return immediately after submission or wait for the job
    to complete.

    Args:
        script: Bash script content to execute
        job_name: Name for the Slurm job
        slurm_conn_id: Airflow connection ID for Slurm
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
        wait_for_completion: If True, wait for job to complete
        poll_interval: Polling interval in seconds when waiting
        timeout: Maximum wait time in seconds
        **kwargs: Additional Slurm job parameters

    Returns:
        Job ID of the submitted job (accessible via XCom)

    Example:
        Simple job submission:

        >>> submit_job = SlurmOperator(
        ...     task_id="submit_training",
        ...     script="#!/bin/bash\\npython train.py",
        ...     job_name="ml_training",
        ...     partition="gpu",
        ...     cpus_per_task=4,
        ...     mem="16G",
        ...     gres="gpu:2",
        ... )

        Job with environment variables:

        >>> submit_with_env = SlurmOperator(
        ...     task_id="process_data",
        ...     script="#!/bin/bash\\npython process.py",
        ...     job_name="data_processing",
        ...     environment={
        ...         "INPUT_PATH": "/data/input",
        ...         "OUTPUT_PATH": "/data/output",
        ...     },
        ... )
    """

    template_fields: Sequence[str] = (
        "script",
        "job_name",
        "working_dir",
        "stdout",
        "stderr",
        "environment",
    )
    template_ext: Sequence[str] = (".sh", ".bash")
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        script: str,
        job_name: str,
        slurm_conn_id: str = "slurm_default",
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
        wait_for_completion: bool = False,
        poll_interval: int = 10,
        timeout: int = 3600,
        **kwargs: Any,
    ) -> None:
        """Initialize the SlurmOperator."""
        super().__init__(**kwargs)
        self.script = script
        self.job_name = job_name
        self.slurm_conn_id = slurm_conn_id
        self.partition = partition
        self.cpus_per_task = cpus_per_task
        self.mem = mem
        self.time_limit = time_limit
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr
        self.environment = environment
        self.gres = gres
        self.constraint = constraint
        self.account = account
        self.qos = qos
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.extra_kwargs = kwargs

    def execute(self, context: Context) -> int:
        """Execute the operator.

        Args:
            context: Airflow task context

        Returns:
            Slurm job ID

        Raises:
            SlurmAPIError: If job submission or execution fails
        """
        hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)

        logger.info(f"Submitting Slurm job: {self.job_name}")

        # Submit the job
        job_id = hook.submit_job(
            script=self.script,
            job_name=self.job_name,
            partition=self.partition,
            cpus_per_task=self.cpus_per_task,
            mem=self.mem,
            time_limit=self.time_limit,
            working_dir=self.working_dir,
            stdout=self.stdout,
            stderr=self.stderr,
            environment=self.environment,
            gres=self.gres,
            constraint=self.constraint,
            account=self.account,
            qos=self.qos,
            **self.extra_kwargs,
        )

        logger.info(f"Job submitted successfully with ID: {job_id}")

        # Optionally wait for completion
        if self.wait_for_completion:
            logger.info(f"Waiting for job {job_id} to complete...")
            final_state = hook.wait_for_job(
                job_id=job_id,
                timeout=self.timeout,
                poll_interval=self.poll_interval,
            )
            logger.info(f"Job {job_id} completed with state: {final_state}")

        return job_id

    def on_kill(self) -> None:
        """Cancel the Slurm job if task is killed."""
        if hasattr(self, "_job_id") and self._job_id:
            logger.warning(
                f"Task killed - attempting to cancel Slurm job {self._job_id}"
            )
            try:
                hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)
                hook.cancel_job(self._job_id)
                logger.info(f"Successfully cancelled job {self._job_id}")
            except Exception as e:
                logger.error(f"Failed to cancel job {self._job_id}: {e}")
