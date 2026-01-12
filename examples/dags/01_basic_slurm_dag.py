"""
Basic Slurm Executor DAG Example.

This example demonstrates the simplest usage of the Slurm executor with basic tasks
that run shell commands on the Slurm cluster.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# DAG configuration
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Slurm-specific configurations
    "queue": "normal",  # Slurm partition
    "executor_config": {
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "1G",
            "time_limit": "00:10:00",
            "account": "research",
        }
    },
}

dag = DAG(
    "basic_slurm_example",
    default_args=default_args,
    description="Basic example of using Slurm executor",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["slurm", "basic", "example"],
)

# Start marker
start = DummyOperator(task_id="start", dag=dag)

# Basic system information task
system_info = BashOperator(
    task_id="get_system_info",
    bash_command="""
    echo "=== System Information ==="
    echo "Node: $(hostname)"
    echo "Date: $(date)"
    echo "User: $(whoami)"
    echo "Working directory: $(pwd)"
    echo "CPU cores: $(nproc)"
    echo "Memory info:"
    free -h
    echo "Disk space:"
    df -h /tmp
    """,
    dag=dag,
)

# Simple data processing task
process_data = BashOperator(
    task_id="process_sample_data",
    bash_command="""
    echo "=== Processing Sample Data ==="

    # Create sample dataset
    DATA_FILE="/tmp/sample_data_{{ ds_nodash }}.csv"
    echo "timestamp,value,category" > $DATA_FILE

    # Generate 1000 sample records
    for i in $(seq 1 1000); do
        timestamp=$(date -d "$i minutes ago" '+%Y-%m-%d %H:%M:%S')
        value=$((RANDOM % 100))
        category=$((RANDOM % 5))
        echo "$timestamp,$value,cat_$category" >> $DATA_FILE
    done

    echo "Generated $(wc -l < $DATA_FILE) lines of data"
    echo "Sample data:"
    head -5 $DATA_FILE

    # Basic statistics
    echo "=== Basic Statistics ==="
    echo "Total records: $(tail -n +2 $DATA_FILE | wc -l)"
    echo "Categories:"
    tail -n +2 $DATA_FILE | cut -d',' -f3 | sort | uniq -c

    # Save summary
    SUMMARY_FILE="/tmp/data_summary_{{ ds_nodash }}.txt"
    echo "Data processing completed at $(date)" > $SUMMARY_FILE
    echo "Input file: $DATA_FILE" >> $SUMMARY_FILE
    echo "Records processed: $(tail -n +2 $DATA_FILE | wc -l)" >> $SUMMARY_FILE

    echo "Summary saved to: $SUMMARY_FILE"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "512M",
            "time_limit": "00:05:00",
        }
    },
    dag=dag,
)

# Cleanup task
cleanup = BashOperator(
    task_id="cleanup_files",
    bash_command="""
    echo "=== Cleanup ==="
    echo "Removing temporary files for {{ ds }}"

    # List files before cleanup
    echo "Files to clean:"
    ls -la /tmp/*{{ ds_nodash }}* 2>/dev/null || echo "No files to clean"

    # Remove files
    rm -f /tmp/sample_data_{{ ds_nodash }}.csv
    rm -f /tmp/data_summary_{{ ds_nodash }}.txt

    echo "Cleanup completed"
    """,
    dag=dag,
)

# End marker
end = DummyOperator(task_id="end", dag=dag)

# Define task dependencies
start >> system_info >> process_data >> cleanup >> end
