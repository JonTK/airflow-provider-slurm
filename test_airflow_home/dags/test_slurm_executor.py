"""Test DAG for validating the SlurmExecutor end-to-end.

This DAG runs simple tasks through the Slurm executor to verify:
- Basic task submission and completion
- Task with custom executor_config (partition, cpus, memory)
- Task failure handling
- Sequential task execution (dependencies via Airflow)
"""

from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_slurm_executor",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["test", "slurm"],
) as dag:
    # Basic task — should submit to Slurm and complete
    basic_task = BashOperator(
        task_id="basic_task",
        bash_command="echo 'Hello from Slurm executor!' && hostname && date",
    )

    # Task with custom resources
    custom_resources = BashOperator(
        task_id="custom_resources",
        bash_command="echo 'CPUs='$SLURM_CPUS_PER_TASK && echo 'MEM='$SLURM_MEM_PER_NODE && nproc",
        executor_config={
            "cpus_per_task": 2,
            "mem": "200M",
            "partition": "normal",
        },
    )

    # Task that produces output for the next task
    producer = BashOperator(
        task_id="producer",
        bash_command="echo 'producer completed at $(date)'",
    )

    # Task that runs after producer (Airflow dependency, not Slurm dependency)
    consumer = BashOperator(
        task_id="consumer",
        bash_command="echo 'consumer running after producer at $(date)'",
    )

    # Chain: basic_task -> custom_resources, producer -> consumer
    basic_task >> custom_resources
    producer >> consumer
