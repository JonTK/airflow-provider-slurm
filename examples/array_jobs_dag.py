"""Example DAG demonstrating Slurm job arrays.

This DAG shows various patterns for using job arrays with the Slurm provider:
- Basic array job submission
- Parameter sweeps
- Batch data processing
- Array jobs with failure handling
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from airflow_provider_slurm.hooks.slurm_hook import SlurmHook
from airflow_provider_slurm.operators.slurm import SlurmOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "slurm_array_jobs_examples",
    default_args=default_args,
    description="Examples of Slurm job arrays for high-throughput computing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["slurm", "array", "hpc", "example"],
) as dag:
    # Example 1: Basic Array Job
    basic_array = SlurmOperator(
        task_id="basic_array_job",
        script="""#!/bin/bash
echo "Starting array task ${SLURM_ARRAY_TASK_ID}"
echo "Job ID: ${SLURM_ARRAY_JOB_ID}"

# Simulate some work
sleep $((RANDOM % 10 + 5))

echo "Task ${SLURM_ARRAY_TASK_ID} completed successfully"
""",
        job_name="basic_array_example",
        array="0-9",  # 10 tasks
        partition="debug",
        cpus_per_task=1,
        mem="1G",
        time_limit="00:05:00",
        wait_for_completion=True,
    )

    # Example 2: Array with Parallelism Limit
    limited_array = SlurmOperator(
        task_id="limited_parallel_array",
        script="""#!/bin/bash
echo "Processing task ${SLURM_ARRAY_TASK_ID}"

# Simulate I/O-bound work
sleep 5

echo "Task ${SLURM_ARRAY_TASK_ID} done"
""",
        job_name="limited_array_example",
        array="0-99%10",  # 100 tasks, max 10 concurrent
        partition="debug",
        cpus_per_task=1,
        mem="1G",
        time_limit="00:10:00",
        wait_for_completion=True,
    )

    # Example 3: Parameter Sweep
    param_sweep = SlurmOperator(
        task_id="hyperparameter_sweep",
        script="""#!/bin/bash
# Map task ID to hyperparameters
LEARNING_RATES=(0.001 0.005 0.01 0.05 0.1)
BATCH_SIZES=(16 32 64)

# Calculate indices (5 LRs * 3 BSs = 15 combinations)
LR_INDEX=$((SLURM_ARRAY_TASK_ID / 3))
BS_INDEX=$((SLURM_ARRAY_TASK_ID % 3))

LR=${LEARNING_RATES[$LR_INDEX]}
BS=${BATCH_SIZES[$BS_INDEX]}

echo "Task ${SLURM_ARRAY_TASK_ID}: Training with LR=${LR}, BS=${BS}"

# Simulate training
sleep 10

# Save results
cat > "results_${SLURM_ARRAY_TASK_ID}.json" <<EOF
{
  "task_id": ${SLURM_ARRAY_TASK_ID},
  "learning_rate": ${LR},
  "batch_size": ${BS},
  "accuracy": 0.$((RANDOM % 100))
}
EOF

echo "Saved results to results_${SLURM_ARRAY_TASK_ID}.json"
""",
        job_name="param_sweep_example",
        array="0-14",  # 5 * 3 = 15 combinations
        partition="debug",
        cpus_per_task=2,
        mem="2G",
        time_limit="00:15:00",
        wait_for_completion=True,
    )

    # Example 4: Array with Failure Handling
    fault_tolerant_array = SlurmOperator(
        task_id="fault_tolerant_array",
        script="""#!/bin/bash
echo "Processing task ${SLURM_ARRAY_TASK_ID}"

# Randomly fail ~20% of tasks for demonstration
if [ $((RANDOM % 5)) -eq 0 ]; then
    echo "Simulating failure for task ${SLURM_ARRAY_TASK_ID}"
    exit 1
fi

echo "Task ${SLURM_ARRAY_TASK_ID} succeeded"
""",
        job_name="fault_tolerant_example",
        array="0-19",  # 20 tasks, expect ~4 failures
        partition="debug",
        cpus_per_task=1,
        mem="1G",
        time_limit="00:05:00",
        array_fail_on_error=False,  # Continue even with failures
        wait_for_completion=True,
    )

    # Example 5: Check results and retry failed tasks
    @task
    def check_and_retry_failures(**context):
        """Check array job results and optionally retry failures."""
        # Get results from previous task
        result = context["task_instance"].xcom_pull(task_ids="fault_tolerant_array")

        total = result["array_status"]["total_tasks"]
        completed = result["array_status"]["completed"]
        failed = result["array_status"]["failed"]

        print("Array job results:")
        print(f"  Total tasks: {total}")
        print(f"  Completed: {completed}")
        print(f"  Failed: {failed}")
        print(f"  Success rate: {completed/total*100:.1f}%")

        # If too many failures, could trigger retry logic here
        if failed > total * 0.3:  # More than 30% failed
            print(f"WARNING: High failure rate ({failed}/{total})")
            # Could resubmit failed tasks here
        else:
            print("Acceptable failure rate, continuing...")

        return {"total": total, "completed": completed, "failed": failed}

    # Example 6: Dynamic array size based on data
    @task
    def prepare_data_batches():
        """Prepare data and determine batch count."""
        # In real scenario, would scan input directory or database
        num_files = 50  # Simulated
        print(f"Found {num_files} files to process")
        return num_files

    @task
    def process_batches(num_batches):
        """Process data in batches using array job."""
        hook = SlurmHook(slurm_conn_id="slurm_default")

        script = """#!/bin/bash
echo "Processing batch ${{SLURM_ARRAY_TASK_ID}}"

# In real scenario, would process actual data
sleep 5

echo "Batch ${{SLURM_ARRAY_TASK_ID}} complete"
"""

        job_id = hook.submit_job(
            script=script,
            job_name="dynamic_batch_processing",
            array=f"0-{num_batches-1}",
            partition="debug",
            cpus_per_task=2,
            mem="2G",
            time_limit="00:10:00",
        )

        print(f"Submitted dynamic array job {job_id} with {num_batches} tasks")

        # Wait for completion
        final_status = hook.wait_for_array(job_id, timeout=600, poll_interval=10)

        print(
            f"Dynamic array completed: {final_status['completed']}/{final_status['total_tasks']} tasks"
        )

        return {
            "job_id": job_id,
            "num_batches": num_batches,
            "status": final_status,
        }

    # Define task dependencies
    basic_array >> limited_array
    limited_array >> param_sweep
    param_sweep >> fault_tolerant_array
    fault_tolerant_array >> check_and_retry_failures()

    # Dynamic workflow
    num_batches = prepare_data_batches()
    process_batches(num_batches)
