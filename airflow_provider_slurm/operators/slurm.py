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
    to complete. Supports both single jobs and job arrays.

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
        nodes: Number of nodes to allocate (Slurm -N flag)
        ntasks_per_node: Number of tasks per node (Slurm --ntasks-per-node flag)
        exclusive: Allocate nodes exclusively (Slurm --exclusive flag)
        array: Array job specification (e.g., "0-99", "1-100:2", "0-99%10")
        array_fail_on_error: For array jobs, fail if any task fails
        dependency: Dependency specification (e.g., "afterok:12345") - templatable
        wait_for_completion: If True, wait for job to complete
        poll_interval: Polling interval in seconds when waiting
        timeout: Maximum wait time in seconds
        **kwargs: Additional Slurm job parameters

    Returns:
        Job ID of the submitted job (parent job ID for arrays, accessible via XCom)

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

        Array job submission:

        >>> array_job = SlurmOperator(
        ...     task_id="array_processing",
        ...     script="#!/bin/bash\\necho Processing task $SLURM_ARRAY_TASK_ID",
        ...     job_name="batch_processing",
        ...     array="0-99",  # 100 tasks
        ...     wait_for_completion=True,
        ... )

        Array job with limited parallelism:

        >>> limited_array = SlurmOperator(
        ...     task_id="controlled_array",
        ...     script="#!/bin/bash\\npython process.py $SLURM_ARRAY_TASK_ID",
        ...     job_name="controlled_processing",
        ...     array="0-999%50",  # 1000 tasks, max 50 concurrent
        ...     wait_for_completion=True,
        ...     array_fail_on_error=False,  # Continue even if some tasks fail
        ... )

        Job with dependency (using XCom):

        >>> @task
        ... def submit_preprocessing():
        ...     op = SlurmOperator(
        ...         task_id="preprocess",
        ...         script="#!/bin/bash\\npython preprocess.py",
        ...         job_name="preprocessing"
        ...     )
        ...     return op.execute({})["job_id"]
        ...
        >>> @task
        ... def submit_analysis(preproc_job_id):
        ...     op = SlurmOperator(
        ...         task_id="analysis",
        ...         script="#!/bin/bash\\npython analyze.py",
        ...         job_name="analysis",
        ...         dependency=f"afterok:{preproc_job_id}"  # Wait for preprocessing
        ...     )
        ...     return op.execute({})
        ...
        >>> preproc = submit_preprocessing()
        >>> analysis = submit_analysis(preproc)

        Job with templatable dependency:

        >>> dependent_job = SlurmOperator(
        ...     task_id="dependent_task",
        ...     script="#!/bin/bash\\necho 'Running after job completes'",
        ...     job_name="dependent_job",
        ...     dependency="afterok:{{ task_instance.xcom_pull('previous_task') }}",
        ... )

        Multi-node parallel job:

        >>> mpi_job = SlurmOperator(
        ...     task_id="mpi_simulation",
        ...     script="#!/bin/bash\\nmpirun -np 64 ./simulate",
        ...     job_name="mpi_parallel",
        ...     nodes=4,  # Allocate 4 nodes
        ...     ntasks_per_node=16,  # 16 tasks per node = 64 total tasks
        ...     cpus_per_task=2,  # 2 CPUs per task
        ...     mem="32G",  # Memory per node
        ...     time_limit="04:00:00",
        ... )

        Exclusive node allocation (no sharing with other jobs):

        >>> exclusive_job = SlurmOperator(
        ...     task_id="exclusive_workload",
        ...     script="#!/bin/bash\\n./intensive_computation",
        ...     job_name="exclusive",
        ...     nodes=2,
        ...     exclusive=True,  # Allocate nodes exclusively
        ...     time_limit="02:00:00",
        ... )
    """

    template_fields: Sequence[str] = (
        "script",
        "job_name",
        "working_dir",
        "stdout",
        "stderr",
        "environment",
        "array",
        "dependency",
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
        nodes: Optional[int] = None,
        ntasks_per_node: Optional[int] = None,
        exclusive: bool = False,
        array: Optional[str] = None,
        array_fail_on_error: bool = True,
        dependency: Optional[str] = None,
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
        self.nodes = nodes
        self.ntasks_per_node = ntasks_per_node
        self.exclusive = exclusive
        self.array = array
        self.array_fail_on_error = array_fail_on_error
        self.dependency = dependency
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.extra_kwargs = kwargs
        self._job_id: Optional[int] = None

    def execute(self, context: Context) -> Dict[str, Any]:
        """Execute the operator.

        Args:
            context: Airflow task context

        Returns:
            Dictionary containing:
                - job_id: Slurm job ID (parent ID for array jobs)
                - is_array: Boolean indicating if this is an array job
                - array_spec: Array specification if array job
                - array_task_count: Number of tasks if array job
                - array_status: Final array status if waited and is array

        Raises:
            SlurmAPIError: If job submission or execution fails
        """
        hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)

        # Build submission log message
        log_parts = [f"Submitting Slurm"]
        if self.array:
            log_parts.append(f"array job: {self.job_name} ({self.array})")
        else:
            log_parts.append(f"job: {self.job_name}")
        if self.dependency:
            log_parts.append(f"with dependency: {self.dependency}")
        logger.info(" ".join(log_parts))

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
            nodes=self.nodes,
            ntasks_per_node=self.ntasks_per_node,
            exclusive=self.exclusive,
            array=self.array,
            dependency=self.dependency,
            **self.extra_kwargs,
        )

        # Store job_id for on_kill
        self._job_id = job_id

        # Build return value
        result: Dict[str, Any] = {
            "job_id": job_id,
            "is_array": bool(self.array),
        }

        if self.array:
            result["array_spec"] = self.array
        if self.dependency:
            result["dependency"] = self.dependency

        # Build success log message
        success_parts = []
        if self.array:
            success_parts.append(
                f"Array job submitted successfully with ID: {job_id} ({self.array})"
            )
        else:
            success_parts.append(f"Job submitted successfully with ID: {job_id}")
        if self.dependency:
            success_parts.append(f"(dependency: {self.dependency})")
        logger.info(" ".join(success_parts))

        # Optionally wait for completion
        if self.wait_for_completion:
            if self.array:
                logger.info(f"Waiting for array job {job_id} to complete...")
                array_status = hook.wait_for_array(
                    job_id=job_id,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                    fail_on_error=self.array_fail_on_error,
                )
                result["array_status"] = array_status
                result["array_task_count"] = array_status.get("total_tasks", 0)
                logger.info(
                    f"Array job {job_id} completed: "
                    f"{array_status['completed']}/{array_status['total_tasks']} tasks succeeded"
                )
            else:
                logger.info(f"Waiting for job {job_id} to complete...")
                final_state = hook.wait_for_job(
                    job_id=job_id,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                )
                logger.info(f"Job {job_id} completed with state: {final_state}")

        return result

    def on_kill(self) -> None:
        """Cancel the Slurm job if task is killed."""
        if self._job_id:
            job_type = "array job" if self.array else "job"
            logger.warning(
                f"Task killed - attempting to cancel Slurm {job_type} {self._job_id}"
            )
            try:
                hook = SlurmHook(slurm_conn_id=self.slurm_conn_id)
                if self.array:
                    # Cancel entire array job
                    hook.cancel_array_task(self._job_id)
                else:
                    hook.cancel_job(self._job_id)
                logger.info(f"Successfully cancelled {job_type} {self._job_id}")
            except Exception as e:
                logger.error(f"Failed to cancel {job_type} {self._job_id}: {e}")
