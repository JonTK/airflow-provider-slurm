"""
Slurm Executor Test DAG.

A simple test DAG to verify your Slurm executor configuration is working correctly.
Run this first before using the more complex examples.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "slurm_executor_test",
    default_args=default_args,
    description="Test DAG for Slurm executor functionality",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["test", "slurm", "verification"],
)


def test_python_environment():
    """Test that Python environment is working correctly."""
    import os
    import platform
    import sys

    print("=== Python Environment Test ===")
    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print(f"Platform: {platform.platform()}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"User: {os.getenv('USER', 'unknown')}")
    print(f"Home: {os.getenv('HOME', 'unknown')}")
    print(f"PATH: {os.getenv('PATH', 'unknown')[:200]}...")

    # Test basic computation
    result = sum(i * i for i in range(1000))
    print(f"Computation test result: {result}")

    print("✓ Python environment test completed successfully!")


# Start marker
start = DummyOperator(task_id="start", dag=dag)

# Test 1: Basic shell command
test_shell = BashOperator(
    task_id="test_shell_command",
    bash_command="""
    echo "=== Shell Command Test ==="
    echo "Hostname: $(hostname)"
    echo "Date: $(date)"
    echo "Current user: $(whoami)"
    echo "Working directory: $(pwd)"
    echo "Available memory:"
    free -h || echo "free command not available"
    echo "CPU info:"
    nproc || echo "nproc command not available"
    echo "✓ Shell command test completed successfully!"
    """,
    executor_config={
        "slurm": {
            "partition": "debug",  # Use debug partition for quick testing
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:02:00",
        }
    },
    dag=dag,
)

# Test 2: Python environment
test_python = PythonOperator(
    task_id="test_python_environment",
    python_callable=test_python_environment,
    executor_config={
        "slurm": {
            "partition": "debug",
            "cpus_per_task": 1,
            "mem": "512M",
            "time_limit": "00:02:00",
        }
    },
    dag=dag,
)

# Test 3: File system operations
test_filesystem = BashOperator(
    task_id="test_filesystem_access",
    bash_command="""
    echo "=== Filesystem Test ==="

    # Test temp directory access
    TEMP_DIR="/tmp/slurm_test_$(date +%s)"
    mkdir -p "$TEMP_DIR"
    echo "Created temp directory: $TEMP_DIR"

    # Test file operations
    TEST_FILE="$TEMP_DIR/test_file.txt"
    echo "Test data from Slurm executor $(date)" > "$TEST_FILE"
    echo "File created: $TEST_FILE"
    echo "File contents:"
    cat "$TEST_FILE"

    # Test file permissions
    ls -la "$TEST_FILE"

    # Cleanup
    rm -rf "$TEMP_DIR"
    echo "Cleanup completed"

    echo "✓ Filesystem test completed successfully!"
    """,
    executor_config={
        "slurm": {
            "partition": "debug",
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:02:00",
        }
    },
    dag=dag,
)

# Test 4: Resource allocation
test_resources = BashOperator(
    task_id="test_resource_allocation",
    bash_command="""
    echo "=== Resource Allocation Test ==="

    echo "Slurm job environment variables:"
    env | grep SLURM | sort

    echo "CPU information:"
    echo "Allocated CPUs: ${SLURM_CPUS_PER_TASK:-unknown}"
    echo "Available cores: $(nproc 2>/dev/null || echo 'unknown')"

    echo "Memory information:"
    echo "Allocated memory: ${SLURM_MEM_PER_NODE:-unknown}"
    if command -v free > /dev/null; then
        echo "System memory:"
        free -h
    fi

    echo "Job information:"
    echo "Job ID: ${SLURM_JOB_ID:-unknown}"
    echo "Job name: ${SLURM_JOB_NAME:-unknown}"
    echo "Partition: ${SLURM_JOB_PARTITION:-unknown}"
    echo "Node list: ${SLURM_JOB_NODELIST:-unknown}"

    echo "✓ Resource allocation test completed successfully!"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",  # Use normal partition to test resource allocation
            "cpus_per_task": 2,
            "mem": "1G",
            "time_limit": "00:03:00",
        }
    },
    dag=dag,
)

# Test 5: Network connectivity (if applicable)
test_network = BashOperator(
    task_id="test_network_connectivity",
    bash_command="""
    echo "=== Network Connectivity Test ==="

    echo "Testing basic network connectivity..."

    # Test DNS resolution
    if command -v nslookup > /dev/null; then
        echo "DNS test (google.com):"
        nslookup google.com || echo "DNS lookup failed"
    fi

    # Test HTTP connectivity
    if command -v curl > /dev/null; then
        echo "HTTP connectivity test:"
        curl -s --connect-timeout 5 http://httpbin.org/ip || echo "HTTP test failed"
    elif command -v wget > /dev/null; then
        echo "HTTP connectivity test (wget):"
        wget -q --timeout=5 -O - http://httpbin.org/ip || echo "HTTP test failed"
    else
        echo "No HTTP client available (curl/wget)"
    fi

    echo "✓ Network connectivity test completed!"
    """,
    executor_config={
        "slurm": {
            "partition": "debug",
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:03:00",
        }
    },
    dag=dag,
)

# Success marker
success = BashOperator(
    task_id="test_success",
    bash_command="""
    echo "========================================="
    echo "🎉 All Slurm Executor Tests PASSED! 🎉"
    echo "========================================="
    echo ""
    echo "Your Slurm executor is configured correctly and ready for production use."
    echo ""
    echo "Next steps:"
    echo "1. Try running the basic example DAGs"
    echo "2. Monitor job performance with 'squeue' and 'scontrol'"
    echo "3. Check Airflow logs for any issues"
    echo "4. Scale up to more complex workflows"
    echo ""
    echo "Timestamp: $(date)"
    echo "Node: $(hostname)"
    """,
    trigger_rule="all_success",
    executor_config={
        "slurm": {
            "partition": "debug",
            "cpus_per_task": 1,
            "mem": "128M",
            "time_limit": "00:01:00",
        }
    },
    dag=dag,
)

# End marker
end = DummyOperator(task_id="end", dag=dag)

# Define dependencies
(
    start
    >> [test_shell, test_python, test_filesystem, test_resources, test_network]
    >> success
    >> end
)
