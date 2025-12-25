"""
Simple example DAG using the Slurm executor.

This demonstrates basic task execution on a Slurm cluster.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "simple_slurm_example",
    default_args=default_args,
    description="A simple example DAG using Slurm executor",
    schedule=timedelta(days=1),
    catchup=False,
    tags=["example", "slurm"],
) as dag:

    @task
    def hello_slurm():
        """Simple hello world task."""
        import time
        import socket
        
        print(f"Hello from Slurm! Running on node: {socket.gethostname()}")
        print("This task is executing on a Slurm compute node")
        
        # Simulate some work
        time.sleep(5)
        
        return "Task completed successfully!"

    @task(executor_config={
        "cpus_per_task": 2,
        "mem": "4G",
        "time_limit": "00:10:00",
        "partition": "compute",
    })
    def cpu_intensive_task():
        """Task with custom resource requirements."""
        import time
        import os
        
        print(f"Running CPU intensive task with {os.cpu_count()} CPUs")
        print("Allocated 2 CPUs and 4GB memory via executor_config")
        
        # Simulate CPU work
        result = sum(i * i for i in range(100000))
        time.sleep(10)
        
        return f"Computation result: {result}"

    @task
    def process_results(hello_result: str, cpu_result: str):
        """Process results from previous tasks."""
        print(f"Hello task result: {hello_result}")
        print(f"CPU task result: {cpu_result}")
        
        return "All tasks completed successfully!"

    # Define task dependencies
    hello_result = hello_slurm()
    cpu_result = cpu_intensive_task()
    
    process_results(hello_result, cpu_result)