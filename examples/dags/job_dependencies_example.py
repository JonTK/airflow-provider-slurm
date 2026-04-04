"""Example DAG demonstrating Slurm job dependencies.

This DAG shows various patterns for using Slurm job dependencies:
- Sequential pipeline (afterok)
- Conditional execution (afterok, afternotok, afterany)
- Fan-out/fan-in patterns
- Array job dependencies (aftercorr)
- Dynamic dependencies with XCom

Run this DAG:
    airflow dags test job_dependencies_example 2024-01-01
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from airflow_provider_slurm.operators.slurm import SlurmOperator

# Default arguments for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="job_dependencies_example",
    default_args=default_args,
    description="Demonstrate Slurm job dependency patterns",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["slurm", "dependencies", "example"],
) as dag:

    # ============================================================================
    # Pattern 1: Linear Pipeline (Sequential Processing)
    # ============================================================================

    @task(task_id="linear_pipeline_demo")
    def linear_pipeline():
        """Demonstrate sequential job dependencies (afterok)."""
        print("=== Linear Pipeline Pattern ===")

        # Step 1: Download data
        download = SlurmOperator(
            task_id="download_data",
            script="""#!/bin/bash
echo "Downloading dataset..."
sleep 2
echo "Download complete"
exit 0
""",
            job_name="download",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
        )

        download_result = download.execute({})
        download_id = download_result["job_id"]
        print(f"  ✓ Download job submitted: {download_id}")

        # Step 2: Extract (depends on download)
        extract = SlurmOperator(
            task_id="extract_data",
            script="""#!/bin/bash
echo "Extracting data..."
sleep 2
echo "Extraction complete"
exit 0
""",
            job_name="extract",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
            dependency=f"afterok:{download_id}",  # Wait for successful download
        )

        extract_result = extract.execute({})
        extract_id = extract_result["job_id"]
        print(f"  ✓ Extract job submitted: {extract_id} (depends on {download_id})")

        # Step 3: Process (depends on extract)
        process = SlurmOperator(
            task_id="process_data",
            script="""#!/bin/bash
echo "Processing data..."
sleep 2
echo "Processing complete"
exit 0
""",
            job_name="process",
            partition="debug",
            cpus_per_task=2,
            mem="2G",
            time_limit="00:05:00",
            dependency=f"afterok:{extract_id}",  # Wait for successful extraction
        )

        process_result = process.execute({})
        print(
            f"  ✓ Process job submitted: {process_result['job_id']} (depends on {extract_id})"
        )

        return {
            "download_id": download_id,
            "extract_id": extract_id,
            "process_id": process_result["job_id"],
        }

    # ============================================================================
    # Pattern 2: Conditional Execution (Success/Failure Paths)
    # ============================================================================

    @task(task_id="conditional_execution_demo")
    def conditional_execution():
        """Demonstrate conditional dependencies (afterok, afternotok, afterany)."""
        print("=== Conditional Execution Pattern ===")

        # Main job that might fail
        main_job = SlurmOperator(
            task_id="main_processing",
            script="""#!/bin/bash
echo "Running main processing..."
sleep 2
# Randomly succeed or fail for demo
RANDOM_EXIT=$((RANDOM % 2))
echo "Exit code: $RANDOM_EXIT"
exit $RANDOM_EXIT
""",
            job_name="main_processing",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
        )

        main_result = main_job.execute({})
        main_id = main_result["job_id"]
        print(f"  ✓ Main job submitted: {main_id}")

        # Success path - only runs if main succeeds
        success_job = SlurmOperator(
            task_id="on_success",
            script="""#!/bin/bash
echo "Main job succeeded - running post-processing..."
sleep 1
echo "Post-processing complete"
""",
            job_name="success_handler",
            partition="debug",
            cpus_per_task=1,
            mem="500M",
            time_limit="00:05:00",
            dependency=f"afterok:{main_id}",  # Only after successful completion
        )

        success_result = success_job.execute({})
        print(
            f"  ✓ Success handler submitted: {success_result['job_id']} (afterok:{main_id})"
        )

        # Failure path - only runs if main fails
        failure_job = SlurmOperator(
            task_id="on_failure",
            script="""#!/bin/bash
echo "Main job failed - running recovery..."
sleep 1
echo "Recovery complete"
""",
            job_name="failure_handler",
            partition="debug",
            cpus_per_task=1,
            mem="500M",
            time_limit="00:05:00",
            dependency=f"afternotok:{main_id}",  # Only after failure
        )

        failure_result = failure_job.execute({})
        print(
            f"  ✓ Failure handler submitted: {failure_result['job_id']} (afternotok:{main_id})"
        )

        # Cleanup - runs regardless of success/failure
        cleanup_job = SlurmOperator(
            task_id="cleanup",
            script="""#!/bin/bash
echo "Cleaning up temporary files..."
sleep 1
echo "Cleanup complete"
""",
            job_name="cleanup",
            partition="debug",
            cpus_per_task=1,
            mem="500M",
            time_limit="00:05:00",
            dependency=f"afterany:{main_id}",  # After any completion
        )

        cleanup_result = cleanup_job.execute({})
        print(
            f"  ✓ Cleanup job submitted: {cleanup_result['job_id']} (afterany:{main_id})"
        )

        return {
            "main_id": main_id,
            "success_id": success_result["job_id"],
            "failure_id": failure_result["job_id"],
            "cleanup_id": cleanup_result["job_id"],
        }

    # ============================================================================
    # Pattern 3: Fan-Out / Fan-In (Parallel Processing + Aggregation)
    # ============================================================================

    @task(task_id="fan_out_fan_in_demo")
    def fan_out_fan_in():
        """Demonstrate fan-out/fan-in pattern with dependencies."""
        print("=== Fan-Out / Fan-In Pattern ===")

        # Single preprocessing job
        preprocess = SlurmOperator(
            task_id="preprocess_data",
            script="""#!/bin/bash
echo "Preprocessing data for parallel analysis..."
sleep 2
echo "Data ready for analysis"
""",
            job_name="preprocess",
            partition="debug",
            cpus_per_task=2,
            mem="2G",
            time_limit="00:05:00",
        )

        preprocess_result = preprocess.execute({})
        preprocess_id = preprocess_result["job_id"]
        print(f"  ✓ Preprocess job submitted: {preprocess_id}")

        # Fan-out: Multiple parallel analysis jobs
        analysis_jobs = []
        analysis_types = ["statistical", "ml", "visualization"]

        for analysis_type in analysis_types:
            analysis = SlurmOperator(
                task_id=f"analysis_{analysis_type}",
                script=f"""#!/bin/bash
echo "Running {analysis_type} analysis..."
sleep 3
echo "{analysis_type} analysis complete"
""",
                job_name=f"analysis_{analysis_type}",
                partition="debug",
                cpus_per_task=4,
                mem="4G",
                time_limit="00:05:00",
                dependency=f"afterok:{preprocess_id}",  # All depend on preprocess
            )

            result = analysis.execute({})
            analysis_jobs.append(result["job_id"])
            print(
                f"  ✓ {analysis_type} analysis submitted: {result['job_id']} (depends on {preprocess_id})"
            )

        # Fan-in: Aggregation job depends on ALL analysis jobs
        aggregation_dependency = f"afterok:{':'.join(map(str, analysis_jobs))}"

        aggregation = SlurmOperator(
            task_id="aggregate_results",
            script="""#!/bin/bash
echo "Aggregating all analysis results..."
sleep 2
echo "Aggregation complete"
""",
            job_name="aggregation",
            partition="debug",
            cpus_per_task=2,
            mem="2G",
            time_limit="00:05:00",
            dependency=aggregation_dependency,  # Waits for all analysis jobs
        )

        agg_result = aggregation.execute({})
        print(
            f"  ✓ Aggregation job submitted: {agg_result['job_id']} (depends on {len(analysis_jobs)} jobs)"
        )

        return {
            "preprocess_id": preprocess_id,
            "analysis_ids": analysis_jobs,
            "aggregation_id": agg_result["job_id"],
        }

    # ============================================================================
    # Pattern 4: Array Job Dependencies (aftercorr)
    # ============================================================================

    @task(task_id="array_dependencies_demo")
    def array_dependencies():
        """Demonstrate array job dependencies with aftercorr."""
        print("=== Array Job Dependencies Pattern ===")

        # Stage 1: Preprocessing array (10 files)
        preprocess_array = SlurmOperator(
            task_id="preprocess_array",
            script="""#!/bin/bash
echo "Preprocessing file $SLURM_ARRAY_TASK_ID..."
sleep 2
echo "File $SLURM_ARRAY_TASK_ID preprocessed"
exit 0
""",
            job_name="preprocess_array",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
            array="0-9",  # 10 files
        )

        preprocess_result = preprocess_array.execute({})
        preprocess_array_id = preprocess_result["job_id"]
        print(f"  ✓ Preprocess array submitted: {preprocess_array_id} (10 tasks)")

        # Stage 2: Analysis array (each task waits for corresponding preprocess task)
        analysis_array = SlurmOperator(
            task_id="analysis_array",
            script="""#!/bin/bash
echo "Analyzing file $SLURM_ARRAY_TASK_ID..."
sleep 2
echo "File $SLURM_ARRAY_TASK_ID analyzed"
exit 0
""",
            job_name="analysis_array",
            partition="debug",
            cpus_per_task=2,
            mem="2G",
            time_limit="00:05:00",
            array="0-9",
            dependency=f"aftercorr:{preprocess_array_id}",  # Task N waits for task N
        )

        analysis_result = analysis_array.execute({})
        analysis_array_id = analysis_result["job_id"]
        print(
            f"  ✓ Analysis array submitted: {analysis_array_id} "
            f"(10 tasks, aftercorr:{preprocess_array_id})"
        )

        # Stage 3: Aggregation (waits for entire analysis array)
        aggregation = SlurmOperator(
            task_id="aggregate_array_results",
            script="""#!/bin/bash
echo "Aggregating results from all files..."
sleep 2
echo "All results aggregated"
""",
            job_name="aggregate",
            partition="debug",
            cpus_per_task=1,
            mem="2G",
            time_limit="00:05:00",
            dependency=f"afterok:{analysis_array_id}",  # Waits for all analysis tasks
        )

        agg_result = aggregation.execute({})
        print(
            f"  ✓ Aggregation job submitted: {agg_result['job_id']} (depends on all {analysis_array_id} tasks)"
        )

        return {
            "preprocess_array_id": preprocess_array_id,
            "analysis_array_id": analysis_array_id,
            "aggregation_id": agg_result["job_id"],
        }

    # ============================================================================
    # Pattern 5: Dynamic Dependencies with XCom
    # ============================================================================

    @task(task_id="submit_base_job")
    def submit_base_job():
        """Submit a base job and return job_id via XCom."""
        base = SlurmOperator(
            task_id="base_job",
            script="""#!/bin/bash
echo "Running base job..."
sleep 2
echo "Base job complete"
""",
            job_name="base",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
        )

        result = base.execute({})
        print(f"  ✓ Base job submitted: {result['job_id']}")
        return result["job_id"]  # Return for XCom

    @task(task_id="submit_dependent_job")
    def submit_dependent_job(base_job_id: int):
        """Submit a job that depends on the base job (using XCom)."""
        dependent = SlurmOperator(
            task_id="dependent_job",
            script="""#!/bin/bash
echo "Running dependent job..."
sleep 2
echo "Dependent job complete"
""",
            job_name="dependent",
            partition="debug",
            cpus_per_task=1,
            mem="1G",
            time_limit="00:05:00",
            dependency=f"afterok:{base_job_id}",  # Dynamic dependency from XCom
        )

        result = dependent.execute({})
        print(
            f"  ✓ Dependent job submitted: {result['job_id']} (depends on {base_job_id})"
        )
        return result

    # ============================================================================
    # Execute all patterns
    # ============================================================================

    # Run patterns independently
    pattern1_result = linear_pipeline()
    pattern2_result = conditional_execution()
    pattern3_result = fan_out_fan_in()
    pattern4_result = array_dependencies()

    # Pattern 5: Dynamic dependencies with XCom
    base_id = submit_base_job()
    submit_dependent_job(base_id)
