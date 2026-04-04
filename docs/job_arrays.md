# Job Arrays Tutorial

Job arrays allow you to submit hundreds or thousands of similar jobs with a single Slurm command. This is essential for parameter sweeps, batch processing, and high-throughput computing (HTC) workloads.

## Table of Contents

- [What are Job Arrays?](#what-are-job-arrays)
- [Array Specification Formats](#array-specification-formats)
- [Using Arrays with SlurmOperator](#using-arrays-with-slurmoperator)
- [Using Arrays with SlurmHook](#using-arrays-with-slurmhook)
- [Using Arrays with Executor Config](#using-arrays-with-executor-config)
- [Best Practices](#best-practices)
- [Monitoring and Debugging](#monitoring-and-debugging)
- [Common Patterns](#common-patterns)

## What are Job Arrays?

A job array is a collection of similar jobs that differ only by an index variable (`$SLURM_ARRAY_TASK_ID`). Instead of submitting 1000 individual jobs, you submit one array job with 1000 tasks.

**Benefits:**
- **Efficiency**: Single submission for thousands of tasks
- **Scheduler-friendly**: Reduced scheduler overhead
- **Simple scaling**: Easy to scale from 10 to 10,000 tasks
- **Resource control**: Limit concurrent tasks to prevent cluster overload

## Array Specification Formats

### Range Format

```python
array='0-99'  # Tasks 0, 1, 2, ..., 99 (100 tasks)
```

### Range with Step

```python
array='0-99:5'  # Tasks 0, 5, 10, 15, ..., 95 (20 tasks)
array='1-100:2'  # Tasks 1, 3, 5, 7, ..., 99 (50 tasks)
```

### Explicit List

```python
array='1,5,10,15,20'  # Exactly these task IDs (5 tasks)
```

### Parallelism Limit

Control maximum concurrent tasks using `%`:

```python
array='0-999%50'  # 1000 tasks, max 50 concurrent
array='0-99%10'   # 100 tasks, max 10 concurrent
```

**Why limit parallelism?**
- Prevent cluster overload
- Reduce scheduler pressure
- Better for I/O-bound workloads
- Fair sharing with other users

## Using Arrays with SlurmOperator

### Basic Array Job

```python
from airflow_provider_slurm.operators.slurm import SlurmOperator

process_data = SlurmOperator(
    task_id='process_all_files',
    script='''#!/bin/bash
# Process file based on task ID
INPUT_FILE="data_${SLURM_ARRAY_TASK_ID}.csv"
OUTPUT_FILE="results_${SLURM_ARRAY_TASK_ID}.json"

echo "Processing $INPUT_FILE..."
python analyze.py --input $INPUT_FILE --output $OUTPUT_FILE
''',
    job_name='data_processing',
    array='0-99',  # 100 files to process
    partition='compute',
    cpus_per_task=2,
    mem='4G',
    time_limit='00:30:00',
    wait_for_completion=True,
)
```

### Array with Failure Handling

```python
# Continue even if some tasks fail
tolerant_array = SlurmOperator(
    task_id='batch_with_failures',
    script='''#!/bin/bash
# Some files might be corrupted or missing
python process.py --task-id $SLURM_ARRAY_TASK_ID || true
''',
    array='0-999',
    array_fail_on_error=False,  # Don't fail if some tasks fail
    wait_for_completion=True,
)
```

The operator will return XCom data with failure information:

```python
def check_results(**context):
    result = context['task_instance'].xcom_pull(task_ids='batch_with_failures')

    print(f"Total tasks: {result['array_status']['total_tasks']}")
    print(f"Completed: {result['array_status']['completed']}")
    print(f"Failed: {result['array_status']['failed']}")

    if result['array_status']['state'] == 'PARTIALLY_COMPLETED':
        # Handle partial completion
        print("Some tasks failed, but continuing...")
```

### Parameter Sweeps

```python
# ML hyperparameter tuning
param_sweep = SlurmOperator(
    task_id='hyperparameter_sweep',
    script='''#!/bin/bash
# Map task ID to hyperparameters
LEARNING_RATES=(0.001 0.005 0.01 0.05 0.1)
BATCH_SIZES=(16 32 64 128 256)

LR_INDEX=$((SLURM_ARRAY_TASK_ID / 5))
BS_INDEX=$((SLURM_ARRAY_TASK_ID % 5))

LR=${LEARNING_RATES[$LR_INDEX]}
BS=${BATCH_SIZES[$BS_INDEX]}

echo "Training with LR=$LR, BS=$BS"
python train.py --lr $LR --batch-size $BS --output results_${SLURM_ARRAY_TASK_ID}.json
''',
    array='0-24',  # 5 x 5 = 25 combinations
    partition='gpu',
    gres='gpu:1',
    cpus_per_task=4,
    mem='16G',
    time_limit='02:00:00',
    wait_for_completion=True,
)
```

## Using Arrays with SlurmHook

For more control, use the hook directly:

```python
from airflow_provider_slurm.hooks.slurm_hook import SlurmHook

hook = SlurmHook(slurm_conn_id='slurm_default')

# Submit array job
job_id = hook.submit_job(
    script='''#!/bin/bash
echo "Processing task $SLURM_ARRAY_TASK_ID"
python process.py $SLURM_ARRAY_TASK_ID
''',
    job_name='my_array',
    array='0-99%20',  # 100 tasks, max 20 concurrent
    partition='compute',
    cpus_per_task=2,
    mem='4G',
)

print(f"Submitted array job {job_id}")

# Monitor progress
while True:
    status = hook.get_array_status(job_id)

    print(f"Progress: {status['completed']}/{status['total_tasks']} completed")
    print(f"Running: {status['running']}, Failed: {status['failed']}")

    if status['state'] in ['COMPLETED', 'FAILED', 'PARTIALLY_COMPLETED']:
        break

    time.sleep(10)

# Wait for completion
final_status = hook.wait_for_array(
    job_id,
    timeout=3600,
    poll_interval=10,
    fail_on_error=False,  # Don't raise exception on partial failure
)

print(f"Final state: {final_status['state']}")
print(f"Success rate: {final_status['completed']}/{final_status['total_tasks']}")
```

## Using Arrays with Executor Config

You can also use arrays with the Slurm executor:

```python
from airflow.decorators import task

@task(executor_config={
    'array': '0-99',
    'partition': 'compute',
    'cpus_per_task': 2,
    'mem': '4G',
})
def process_batch():
    import os
    task_id = os.environ.get('SLURM_ARRAY_TASK_ID', '0')
    print(f"Processing task {task_id}")
    # Your processing logic
```

**Note**: When using executor_config, each Airflow task maps to one Slurm array job (all tasks). You won't get individual Airflow tasks per array task.

## Best Practices

### Array Size Guidelines

| Array Size | Recommendation | Notes |
|------------|---------------|-------|
| 1-100 | Fast queries | Minimal overhead |
| 100-1,000 | Good performance | Standard use case |
| 1,000-10,000 | Use parallelism limit | Add `%n` to limit concurrent tasks |
| 10,000+ | Consider chunking | Split into multiple smaller arrays |

### Choosing Parallelism Limits

```python
# CPU-bound tasks: Can run many concurrently
array='0-999%100'  # 1000 tasks, 100 concurrent

# I/O-bound tasks: Limit to prevent storage overload
array='0-999%20'   # 1000 tasks, 20 concurrent

# Memory-intensive tasks: Fewer concurrent to avoid OOM
array='0-499%10'   # 500 tasks, 10 concurrent
```

### Error Handling Strategies

**Strategy 1: Fail Fast (default)**
```python
SlurmOperator(
    array='0-99',
    array_fail_on_error=True,  # Default
    # Fails immediately if any task fails
)
```

**Strategy 2: Best Effort**
```python
SlurmOperator(
    array='0-99',
    array_fail_on_error=False,
    # Completes even with failures, check XCom for details
)
```

**Strategy 3: Retry Failed Tasks**
```python
# In a subsequent task, retry only failed tasks
def retry_failed_tasks(**context):
    result = context['task_instance'].xcom_pull(task_ids='initial_batch')

    if result['array_status']['failed'] > 0:
        # Extract failed task IDs and resubmit
        failed_tasks = [
            task['task_id']
            for task in result['array_status'].get('tasks', [])
            if task['state'] == 'FAILED'
        ]

        # Build array spec for only failed tasks
        array_spec = ','.join(map(str, failed_tasks))

        # Resubmit
        hook = SlurmHook()
        hook.submit_job(
            script=script,
            array=array_spec,
            # ... other params
        )
```

### Output File Management

Use `$SLURM_ARRAY_TASK_ID` in filenames:

```bash
#!/bin/bash
# Good: One file per task
OUTPUT="results_${SLURM_ARRAY_TASK_ID}.txt"
python process.py > $OUTPUT

# Bad: All tasks write to same file (race condition!)
OUTPUT="results.txt"
python process.py >> $OUTPUT  # Don't do this!
```

Configure Slurm output/error files:

```python
SlurmOperator(
    stdout='/shared/logs/job_%A_%a.out',  # %A = job ID, %a = task ID
    stderr='/shared/logs/job_%A_%a.err',
    # ...
)
```

### Resource Allocation

**Per-task resources** (not total):
```python
SlurmOperator(
    array='0-99',  # 100 tasks
    cpus_per_task=4,  # 4 CPUs *per task*
    mem='8G',  # 8GB *per task*
    # Total: Up to 100 * 4 = 400 CPUs, 800GB memory (if all run concurrently)
)
```

**Limit total resources:**
```python
SlurmOperator(
    array='0-999%25',  # Max 25 concurrent
    cpus_per_task=4,   # 4 per task
    # Max concurrent: 25 * 4 = 100 CPUs
)
```

## Monitoring and Debugging

### Check Array Status

```python
hook = SlurmHook()
status = hook.get_array_status(job_id)

print(f"Job ID: {status['job_id']}")
print(f"Total: {status['total_tasks']}")
print(f"Completed: {status['completed']}")
print(f"Running: {status['running']}")
print(f"Pending: {status['pending']}")
print(f"Failed: {status['failed']}")
print(f"State: {status['state']}")
```

### Cancel Array Jobs

```python
# Cancel entire array
hook.cancel_array_task(job_id)

# Cancel specific task
hook.cancel_array_task(job_id, array_task_id=5)

# Cancel in on_kill handler
operator = SlurmOperator(...)
# Automatically cancels array on Airflow task kill
```

### Debugging Failed Tasks

```bash
# Check logs for specific task
cat /shared/logs/job_12345_47.err  # Task 47

# List all failed tasks
sacct -j 12345 --format=JobID,State,ExitCode | grep FAILED
```

## Common Patterns

### Pattern 1: File Processing

```python
# Process all files in a directory
files = glob.glob('/data/input/*.csv')

process_files = SlurmOperator(
    task_id='process_all_files',
    script=f'''#!/bin/bash
FILES=({' '.join(files)})
FILE=${{FILES[$SLURM_ARRAY_TASK_ID]}}
python process.py --input "$FILE" --output "/data/output/result_$SLURM_ARRAY_TASK_ID.json"
''',
    array=f'0-{len(files)-1}',
)
```

### Pattern 2: Database Batch Processing

```python
# Process database records in batches
total_records = 1000000
batch_size = 10000
num_batches = total_records // batch_size

batch_process = SlurmOperator(
    task_id='batch_db_processing',
    script=f'''#!/bin/bash
BATCH_SIZE={batch_size}
OFFSET=$((SLURM_ARRAY_TASK_ID * BATCH_SIZE))

python process_db.py --offset $OFFSET --limit $BATCH_SIZE
''',
    array=f'0-{num_batches-1}',
)
```

### Pattern 3: Simulation Ensemble

```python
# Run 100 simulations with different random seeds
ensemble = SlurmOperator(
    task_id='run_ensemble',
    script='''#!/bin/bash
SEED=$((SLURM_ARRAY_TASK_ID + 42))  # Unique seed per task
python simulate.py --seed $SEED --output "results_${SLURM_ARRAY_TASK_ID}.nc"
''',
    array='0-99',
    partition='compute',
    cpus_per_task=8,
)
```

### Pattern 4: Dynamic Array from Previous Task

```python
@task
def prepare_workload():
    # Determine what work needs to be done
    files_to_process = scan_input_directory()
    return len(files_to_process)

@task
def process_workload(num_tasks):
    operator = SlurmOperator(
        task_id='dynamic_array',
        script='''#!/bin/bash
        python process.py $SLURM_ARRAY_TASK_ID
        ''',
        array=f'0-{num_tasks-1}',  # Dynamic array size
    )
    return operator.execute({})

num_tasks = prepare_workload()
results = process_workload(num_tasks)
```

## Performance Tips

1. **Use parallelism limits** for large arrays to prevent scheduler overload
2. **Batch small jobs**: If individual tasks are < 1 minute, combine multiple into one task
3. **Use fast storage**: Put input/output on parallel filesystem, not NFS
4. **Monitor cluster load**: Adjust parallelism based on cluster utilization
5. **Cleanup**: Remove intermediate files to avoid inode exhaustion

## Troubleshooting

**Problem**: Array job stuck in pending
- **Solution**: Check partition limits, QOS settings, or reduce parallelism

**Problem**: High failure rate
- **Solution**: Add `set -x` to script for debugging, check resource limits

**Problem**: "Invalid array specification" error
- **Solution**: Verify array format matches `\d+-\d+(:\d+)?(%\d+)?` or `\d+(,\d+)*`

**Problem**: Partial completion state
- **Solution**: Check failed task logs, consider using `array_fail_on_error=False`

## See Also

- [Slurm Array Documentation](https://slurm.schedmd.com/job_array.html)
- [Configuration Guide](configuration.md)
- [Troubleshooting](troubleshooting.md)
