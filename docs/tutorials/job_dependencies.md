# Job Dependencies Tutorial

This tutorial demonstrates how to use Slurm job dependencies to create sequential and conditional workflows in Apache Airflow.

## Table of Contents

- [Overview](#overview)
- [Basic Concepts](#basic-concepts)
- [Dependency Types](#dependency-types)
- [Usage Patterns](#usage-patterns)
- [Advanced Examples](#advanced-examples)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Job dependencies allow you to control the execution order of Slurm jobs based on the completion status of other jobs. This is useful for:

- **Sequential workflows**: Ensure jobs run in order
- **Conditional execution**: Run jobs based on success/failure of prerequisites
- **Resource coordination**: Prevent resource conflicts
- **Data pipelines**: Ensure data is ready before processing

### When to Use Dependencies

**Use Slurm dependencies when:**
- Jobs need to run on the same Slurm cluster
- You want minimal overhead between steps
- Jobs have data dependencies (shared filesystems)
- You need fine-grained control over job ordering

**Use Airflow task dependencies when:**
- Jobs run on different systems
- You need complex branching logic
- Tasks involve API calls or external systems
- You want Airflow UI visibility of the workflow

## Basic Concepts

### Dependency Specification Format

Dependencies use Slurm's native specification format:

```
dependency_type:job_id[:job_id...]
```

**Examples:**
```python
"afterok:12345"                    # Single job
"afterok:12345:12346:12347"        # Multiple jobs (AND)
"afterok:12345,afterany:12346"     # Multiple types (AND)
"afterok:12345?afterok:12346"      # OR logic
```

### How Dependencies Work

1. Submit prerequisite job(s) and capture job ID(s)
2. Submit dependent job with dependency specification
3. Slurm holds dependent job until dependency is satisfied
4. Job runs automatically when dependency completes

### Dependency States

When a dependency condition is met, Slurm transitions the job from `PENDING` to `RUNNING`. If the dependency can never be satisfied (e.g., prerequisite job failed for an `afterok` dependency), the job enters `DEPENDENCY_NEVER_SATISFIED` state.

## Dependency Types

### afterok - After Successful Completion

Job starts only if the specified job(s) complete successfully (exit code 0).

```python
from airflow_provider_slurm.operators.slurm import SlurmOperator

# Prerequisite job
preprocess = SlurmOperator(
    task_id='preprocess_data',
    script='''#!/bin/bash
echo "Preprocessing data..."
python preprocess.py
exit 0  # Success
''',
    job_name='preprocess',
    partition='compute',
)

# Dependent job - only runs if preprocess succeeds
result = preprocess.execute({})
job_id = result['job_id']

analysis = SlurmOperator(
    task_id='analyze_data',
    script='#!/bin/bash\npython analyze.py',
    job_name='analysis',
    dependency=f'afterok:{job_id}',
    partition='compute',
)
```

**Use case:** Standard sequential processing where each step must succeed.

### afterany - After Any Completion

Job starts when the specified job(s) complete, regardless of exit status.

```python
# Job that might fail
risky_job = SlurmOperator(
    task_id='risky_processing',
    script='''#!/bin/bash
python risky_script.py
# May exit with 0 or non-zero
''',
    job_name='risky_job',
    partition='compute',
)

result = risky_job.execute({})

# Cleanup runs regardless of success/failure
cleanup = SlurmOperator(
    task_id='cleanup',
    script='''#!/bin/bash
echo "Cleaning up temporary files..."
rm -rf /tmp/job_data/*
''',
    job_name='cleanup',
    dependency=f'afterany:{result["job_id"]}',
    partition='compute',
)
```

**Use case:** Cleanup tasks, logging, notification jobs that should run regardless of success.

### afternotok - After Failure

Job starts only if the specified job(s) fail (non-zero exit status).

```python
# Main job that might fail
main_job = SlurmOperator(
    task_id='main_job',
    script='#!/bin/bash\npython main_script.py',
    job_name='main',
    partition='compute',
)

result = main_job.execute({})

# Fallback processing only if main job fails
fallback = SlurmOperator(
    task_id='fallback_processing',
    script='''#!/bin/bash
echo "Main job failed, running fallback..."
python fallback_script.py
''',
    job_name='fallback',
    dependency=f'afternotok:{result["job_id"]}',
    partition='compute',
)
```

**Use case:** Error recovery, alternative processing paths, failure notifications.

### aftercorr - After Corresponding Array Task

For array jobs, start when the corresponding task ID completes successfully.

```python
# Array job: tasks 0-9
array_job = SlurmOperator(
    task_id='array_processing',
    script='''#!/bin/bash
echo "Processing batch $SLURM_ARRAY_TASK_ID"
python process_batch.py --batch $SLURM_ARRAY_TASK_ID
''',
    job_name='batch_process',
    array='0-9',
    partition='compute',
)

result = array_job.execute({})

# Corresponding analysis: task N waits for batch N
analysis_array = SlurmOperator(
    task_id='array_analysis',
    script='''#!/bin/bash
echo "Analyzing batch $SLURM_ARRAY_TASK_ID"
python analyze_batch.py --batch $SLURM_ARRAY_TASK_ID
''',
    job_name='batch_analysis',
    array='0-9',
    dependency=f'aftercorr:{result["job_id"]}',
    partition='compute',
)
```

**Use case:** Array job pipelines where each analysis task depends on its corresponding processing task.

### singleton - Mutual Exclusion

Only one job with the same name can run at a time. New jobs wait for previous instances to complete.

```python
# First instance
instance1 = SlurmOperator(
    task_id='db_update_1',
    script='#!/bin/bash\npython update_db.py --batch 1',
    job_name='database_update',  # Same name
    dependency='singleton',
    partition='compute',
)

# Second instance - waits for first
instance2 = SlurmOperator(
    task_id='db_update_2',
    script='#!/bin/bash\npython update_db.py --batch 2',
    job_name='database_update',  # Same name
    dependency='singleton',
    partition='compute',
)
```

**Use case:** Database updates, file locking scenarios, exclusive resource access.

### after - After Job Starts

Job starts after the specified job begins execution (doesn't wait for completion).

```python
# Long-running job
long_job = SlurmOperator(
    task_id='long_training',
    script='#!/bin/bash\npython train_model.py --epochs 1000',
    job_name='model_training',
    time_limit='10:00:00',
    partition='gpu',
)

result = long_job.execute({})

# Monitoring starts once training starts
monitor = SlurmOperator(
    task_id='monitor_progress',
    script='''#!/bin/bash
while true; do
    python check_training_metrics.py
    sleep 60
done
''',
    job_name='training_monitor',
    dependency=f'after:{result["job_id"]}',
    time_limit='10:00:00',
    partition='compute',
)
```

**Use case:** Monitoring jobs, parallel logging, resource tracking.

### afterburstbuffer - After Burst Buffer Stage Out

Job starts after burst buffer data is staged out from the specified job.

```python
# Job with burst buffer
bb_job = SlurmOperator(
    task_id='write_results',
    script='''#!/bin/bash
# Write large results to burst buffer
python generate_results.py --output $DW_JOB_STRIPED/results.dat
''',
    job_name='bb_write',
    partition='compute',
)

result = bb_job.execute({})

# Process after data is staged out
process = SlurmOperator(
    task_id='process_results',
    script='#!/bin/bash\npython process_results.py',
    job_name='process',
    dependency=f'afterburstbuffer:{result["job_id"]}',
    partition='compute',
)
```

**Use case:** Large-scale I/O workflows on systems with burst buffers (e.g., Cray DataWarp).

## Usage Patterns

### Pattern 1: Linear Pipeline

```python
from airflow import DAG
from airflow_provider_slurm.operators.slurm import SlurmOperator
from datetime import datetime

with DAG('linear_pipeline', start_date=datetime(2024, 1, 1), schedule=None) as dag:

    # Step 1: Download data
    download = SlurmOperator(
        task_id='download',
        script='#!/bin/bash\nwget https://example.com/data.tar.gz',
        job_name='download',
    )

    download_result = download.execute({})

    # Step 2: Extract (depends on download)
    extract = SlurmOperator(
        task_id='extract',
        script='#!/bin/bash\ntar -xzf data.tar.gz',
        job_name='extract',
        dependency=f'afterok:{download_result["job_id"]}',
    )

    extract_result = extract.execute({})

    # Step 3: Process (depends on extract)
    process = SlurmOperator(
        task_id='process',
        script='#!/bin/bash\npython process.py',
        job_name='process',
        dependency=f'afterok:{extract_result["job_id"]}',
    )
```

### Pattern 2: Fan-Out / Fan-In

```python
# Single preprocessing job
preprocess = SlurmOperator(
    task_id='preprocess',
    script='#!/bin/bash\npython preprocess.py',
    job_name='preprocess',
)

result = preprocess.execute({})
preprocess_id = result['job_id']

# Multiple analysis jobs (fan-out) - all depend on preprocess
analysis_jobs = []
for analysis_type in ['statistical', 'ml', 'visualization']:
    job = SlurmOperator(
        task_id=f'analysis_{analysis_type}',
        script=f'#!/bin/bash\npython analyze_{analysis_type}.py',
        job_name=f'analysis_{analysis_type}',
        dependency=f'afterok:{preprocess_id}',
        partition='compute',
    )
    result = job.execute({})
    analysis_jobs.append(result['job_id'])

# Aggregation job (fan-in) - depends on all analysis jobs
aggregation = SlurmOperator(
    task_id='aggregate',
    script='#!/bin/bash\npython aggregate_results.py',
    job_name='aggregate',
    dependency=f'afterok:{":".join(map(str, analysis_jobs))}',
    partition='compute',
)
```

### Pattern 3: Conditional Execution

```python
# Main processing
main_job = SlurmOperator(
    task_id='main_processing',
    script='#!/bin/bash\npython process.py',
    job_name='main',
)

result = main_job.execute({})
job_id = result['job_id']

# Success path
success_job = SlurmOperator(
    task_id='on_success',
    script='#!/bin/bash\necho "Processing succeeded" | mail user@example.com',
    job_name='success_notification',
    dependency=f'afterok:{job_id}',
)

# Failure path
failure_job = SlurmOperator(
    task_id='on_failure',
    script='#!/bin/bash\necho "Processing failed" | mail user@example.com',
    job_name='failure_notification',
    dependency=f'afternotok:{job_id}',
)

# Cleanup (runs regardless)
cleanup_job = SlurmOperator(
    task_id='cleanup',
    script='#!/bin/bash\nrm -rf /tmp/processing_*',
    job_name='cleanup',
    dependency=f'afterany:{job_id}',
)
```

### Pattern 4: Array Pipeline

```python
# Stage 1: Array preprocessing
preprocess_array = SlurmOperator(
    task_id='preprocess_array',
    script='''#!/bin/bash
echo "Preprocessing chunk $SLURM_ARRAY_TASK_ID"
python preprocess.py --chunk $SLURM_ARRAY_TASK_ID
''',
    job_name='preprocess',
    array='0-99',  # 100 chunks
    partition='compute',
)

result1 = preprocess_array.execute({})

# Stage 2: Corresponding analysis (each task waits for its preprocessing task)
analysis_array = SlurmOperator(
    task_id='analysis_array',
    script='''#!/bin/bash
echo "Analyzing chunk $SLURM_ARRAY_TASK_ID"
python analyze.py --chunk $SLURM_ARRAY_TASK_ID
''',
    job_name='analysis',
    array='0-99',
    dependency=f'aftercorr:{result1["job_id"]}',
    partition='compute',
)

result2 = analysis_array.execute({})

# Stage 3: Aggregation (waits for all analysis tasks)
aggregate = SlurmOperator(
    task_id='aggregate',
    script='#!/bin/bash\npython aggregate_all.py',
    job_name='aggregate',
    dependency=f'afterok:{result2["job_id"]}',
    partition='compute',
)
```

## Advanced Examples

### Example 1: Complex Dependency Logic

Combining multiple dependency types with AND/OR logic:

```python
# Three prerequisite jobs
job1 = SlurmOperator(task_id='job1', script='#!/bin/bash\necho job1', job_name='job1')
job2 = SlurmOperator(task_id='job2', script='#!/bin/bash\necho job2', job_name='job2')
job3 = SlurmOperator(task_id='job3', script='#!/bin/bash\necho job3', job_name='job3')

id1 = job1.execute({})['job_id']
id2 = job2.execute({})['job_id']
id3 = job3.execute({})['job_id']

# Complex: (job1 AND job2) OR job3 must complete successfully
# This is expressed as: afterok:id1,afterok:id2?afterok:id3
dependent = SlurmOperator(
    task_id='dependent',
    script='#!/bin/bash\necho "Running after dependencies"',
    job_name='dependent',
    dependency=f'afterok:{id1},afterok:{id2}?afterok:{id3}',
)
```

### Example 2: Dynamic Dependencies with Templating

Use Airflow's templating to dynamically construct dependencies:

```python
from airflow import DAG
from airflow.decorators import task
from airflow_provider_slurm.operators.slurm import SlurmOperator
from datetime import datetime

with DAG('dynamic_dependencies', start_date=datetime(2024, 1, 1), schedule=None) as dag:

    @task
    def submit_preprocessing():
        """Submit preprocessing job and return job_id."""
        op = SlurmOperator(
            task_id='preprocess',
            script='#!/bin/bash\npython preprocess.py',
            job_name='preprocess',
        )
        return op.execute({})['job_id']

    @task
    def submit_analysis(preprocess_job_id: int):
        """Submit analysis job with dependency on preprocessing."""
        op = SlurmOperator(
            task_id='analysis',
            script='#!/bin/bash\npython analyze.py',
            job_name='analysis',
            dependency=f'afterok:{preprocess_job_id}',  # Dynamic dependency
        )
        return op.execute({})

    preprocess_id = submit_preprocessing()
    analysis_result = submit_analysis(preprocess_id)

# Alternative: Using templatable fields
analysis_templated = SlurmOperator(
    task_id='analysis_templated',
    script='#!/bin/bash\npython analyze.py',
    job_name='analysis',
    dependency="afterok:{{ task_instance.xcom_pull('submit_preprocessing') }}",
)
```

### Example 3: Retry with Dependency

Submit a retry job that only runs if the original fails:

```python
from airflow.decorators import task

@task
def submit_with_retry():
    """Submit job with automatic retry on failure."""
    # Main job
    main = SlurmOperator(
        task_id='main_job',
        script='#!/bin/bash\npython risky_process.py',
        job_name='main',
    )

    main_result = main.execute({})
    main_id = main_result['job_id']

    # Retry job (only runs if main fails)
    retry = SlurmOperator(
        task_id='retry_job',
        script='#!/bin/bash\npython risky_process.py --retry',
        job_name='retry',
        dependency=f'afternotok:{main_id}',
    )

    retry_result = retry.execute({})
    retry_id = retry_result['job_id']

    # Success notification (runs if main OR retry succeeds)
    notify = SlurmOperator(
        task_id='notify_success',
        script='#!/bin/bash\necho "Processing succeeded"',
        job_name='notify',
        dependency=f'afterok:{main_id}?afterok:{retry_id}',
    )

    return notify.execute({})
```

### Example 4: Time-Delayed Dependency

Start a job 30 minutes after another job starts:

```python
# Long-running training job
training = SlurmOperator(
    task_id='training',
    script='#!/bin/bash\npython train_model.py',
    job_name='training',
    time_limit='05:00:00',
)

result = training.execute({})

# Checkpoint collection starts 30 minutes after training starts
checkpoint = SlurmOperator(
    task_id='collect_checkpoints',
    script='''#!/bin/bash
# Collect checkpoints every 10 minutes
while true; do
    python save_checkpoint.py
    sleep 600
done
''',
    job_name='checkpoint',
    dependency=f'after:{result["job_id"]}+00:30:00',  # Start 30 min after training starts
    time_limit='05:00:00',
)
```

## Best Practices

### 1. Use XCom for Job IDs

Store job IDs in XCom for reusability:

```python
@task
def submit_job():
    op = SlurmOperator(task_id='my_job', script='...', job_name='job')
    result = op.execute({})
    return result['job_id']  # Store in XCom

job_id = submit_job()

@task
def submit_dependent(job_id: int):
    op = SlurmOperator(
        task_id='dependent',
        script='...',
        dependency=f'afterok:{job_id}',
    )
    return op.execute({})

submit_dependent(job_id)
```

### 2. Handle Dependency Failures

Monitor for `DEPENDENCY_NEVER_SATISFIED` state:

```python
from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

hook = SlurmHook()
job_status = hook.get_job_status(job_id)

if job_status.get('job_state') == 'DEPENDENCY_NEVER_SATISFIED':
    # Dependency failed - handle appropriately
    print(f"Job {job_id} dependency was never satisfied")
    # Consider cancelling the job or triggering fallback
    hook.cancel_job(job_id)
```

### 3. Document Dependency Chains

Add comments explaining complex dependencies:

```python
# Complex dependency:
# - Job starts if EITHER:
#   - (preprocessing AND validation) both succeed, OR
#   - fallback succeeds
dependency = (
    f'afterok:{preprocess_id},afterok:{validate_id}'  # preprocessing AND validation
    f'?afterok:{fallback_id}'  # OR fallback
)
```

### 4. Use Meaningful Job Names

Job names help track dependencies in Slurm queue:

```python
job_name = f'dag_{dag_id}_task_{task_id}_{execution_date}'
```

### 5. Combine with Array Jobs

Use dependencies with array jobs for complex pipelines:

```python
# First array: preprocessing 100 files
preprocess = SlurmOperator(
    task_id='preprocess',
    array='0-99',
    script='#!/bin/bash\npython preprocess.py --file $SLURM_ARRAY_TASK_ID',
    job_name='preprocess',
)

# Second array: each analysis waits for corresponding preprocess
analysis = SlurmOperator(
    task_id='analysis',
    array='0-99',
    script='#!/bin/bash\npython analyze.py --file $SLURM_ARRAY_TASK_ID',
    job_name='analysis',
    dependency=f'aftercorr:{preprocess.execute({})["job_id"]}',
)
```

### 6. Use afterany for Cleanup

Always run cleanup jobs with `afterany`:

```python
cleanup = SlurmOperator(
    task_id='cleanup',
    script='#!/bin/bash\nrm -rf /tmp/work/*',
    dependency=f'afterany:{job_id}',  # Runs regardless of success/failure
)
```

### 7. Test Dependency Chains

Test on small datasets first:

```python
# Development/testing
if environment == 'dev':
    array_spec = '0-9'  # 10 tasks
else:
    array_spec = '0-999'  # 1000 tasks
```

## Troubleshooting

### Issue: Job Stuck in PENDING

**Symptom:** Job shows `PENDING` state indefinitely.

**Diagnosis:**
```bash
squeue -j <job_id> -o "%i %T %r"
# Check "REASON" column
```

Common reasons:
- `Dependency` - Waiting for dependency to complete
- `DependencyNeverSatisfied` - Dependency will never be met (prerequisite failed)

**Solution:**
- Check prerequisite job status: `scontrol show job <prerequisite_job_id>`
- If prerequisite failed, cancel dependent job: `scancel <job_id>`
- Fix prerequisite and resubmit

### Issue: Dependency Never Satisfied

**Symptom:** Job enters `DEPENDENCY_NEVER_SATISFIED` state.

**Cause:** Prerequisite job failed, but dependency requires success (`afterok`).

**Solution:**
- Use `afterany` if job should run regardless of prerequisite status
- Check prerequisite job logs to fix the failure
- Resubmit both jobs

### Issue: Circular Dependencies

**Symptom:** All jobs stuck in `PENDING`.

**Example:**
```python
# WRONG: Job A depends on Job B, Job B depends on Job A
# This creates a circular dependency
```

**Solution:**
- Review dependency chain to ensure it's acyclic
- Use directed acyclic graph (DAG) principles

### Issue: Job Starts Before Dependency

**Symptom:** Job runs even though prerequisite hasn't completed.

**Cause:** Using `after` instead of `afterok`/`afterany`.

**Solution:**
- Use `afterok` to wait for successful completion
- Use `afterany` to wait for any completion
- `after` only waits for job to *start*, not complete

### Issue: Array Job Dependency Confusion

**Symptom:** Array job waits for wrong tasks.

**Clarification:**
- `afterok:<array_job_id>` - Wait for ALL array tasks to complete
- `aftercorr:<array_job_id>` - Each task waits for its corresponding task

**Example:**
```python
# WRONG: Second array waits for entire first array
dependency=f'afterok:{array1_id}'  # Waits for all 100 tasks

# CORRECT: Each task waits for corresponding task
dependency=f'aftercorr:{array1_id}'  # Task 5 waits for task 5
```

## Performance Considerations

### Dependency Overhead

Dependencies add minimal overhead:
- Slurm checks dependencies every scheduling cycle
- No additional network/API calls
- Job stays in queue (no resource consumption)

### Scaling with Dependencies

For large workflows:
- Dependencies scale linearly with job count
- Use array dependencies (`aftercorr`) for parallel scaling
- Avoid deeply nested sequential chains (use arrays instead)

### Monitoring

Monitor dependency chains:
```bash
# Show all jobs with dependencies
squeue -o "%.18i %.20j %.8u %.8T %.10M %.10l %.6D %R %E"

# Column %E shows dependencies
```

## Summary

Job dependencies provide powerful workflow control:

| Dependency Type | Use Case | Waits For |
|-----------------|----------|-----------|
| `afterok` | Sequential pipeline | Successful completion |
| `afterany` | Cleanup, logging | Any completion |
| `afternotok` | Error recovery | Failure |
| `aftercorr` | Array pipelines | Corresponding array task |
| `singleton` | Resource exclusion | Previous instance |
| `after` | Monitoring | Job start |
| `afterburstbuffer` | I/O workflows | Burst buffer stage-out |

**Key Takeaways:**
- Use native Slurm dependencies for same-cluster workflows
- Combine with Airflow task dependencies for complex logic
- Test dependency chains on small datasets
- Monitor for `DEPENDENCY_NEVER_SATISFIED` state
- Document complex dependency logic

For more information, see:
- [Slurm Documentation: Job Dependencies](https://slurm.schedmd.com/sbatch.html#OPT_dependency)
- [Job Arrays Tutorial](job_arrays.md)
- [SlurmOperator API Reference](../api/operators.md)
