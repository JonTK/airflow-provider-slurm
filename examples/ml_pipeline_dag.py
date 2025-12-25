"""
Machine Learning pipeline example using Slurm executor.

This demonstrates a typical ML workflow with different resource requirements
for each stage of the pipeline.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "ml_pipeline_slurm",
    default_args=default_args,
    description="ML pipeline using Slurm for compute-intensive tasks",
    schedule="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["ml", "slurm", "pipeline"],
) as dag:

    @task(executor_config={
        "cpus_per_task": 1,
        "mem": "8G",
        "time_limit": "00:30:00",
        "partition": "compute",
    })
    def extract_data():
        """Extract data from various sources."""
        import time
        import pandas as pd
        
        print("Extracting data from sources...")
        
        # Simulate data extraction
        data_size = 1000000
        data = {
            "feature_1": range(data_size),
            "feature_2": [x * 2 for x in range(data_size)],
            "target": [x % 2 for x in range(data_size)]
        }
        
        print(f"Extracted {data_size} records")
        time.sleep(30)  # Simulate extraction time
        
        return "/tmp/extracted_data.parquet"

    @task(executor_config={
        "cpus_per_task": 4,
        "mem": "16G", 
        "time_limit": "01:00:00",
        "partition": "compute",
    })
    def preprocess_data(data_path: str):
        """Preprocess and feature engineering."""
        import time
        
        print(f"Preprocessing data from {data_path}")
        print("Running feature engineering with 4 CPUs")
        
        # Simulate preprocessing
        time.sleep(60)  # Simulate preprocessing time
        
        return "/tmp/preprocessed_data.parquet"

    @task(executor_config={
        "cpus_per_task": 8,
        "mem": "32G",
        "time_limit": "04:00:00", 
        "partition": "gpu",  # GPU partition for training
        "container": "docker://tensorflow/tensorflow:latest-gpu",
    })
    def train_model(preprocessed_data_path: str):
        """Train ML model on GPU."""
        import time
        
        print(f"Training model with data from {preprocessed_data_path}")
        print("Using GPU partition with TensorFlow container")
        print("Allocated 8 CPUs and 32GB memory for training")
        
        # Simulate model training
        epochs = 100
        for epoch in range(epochs):
            if epoch % 20 == 0:
                print(f"Epoch {epoch}/{epochs}")
            time.sleep(2)  # Simulate training time
        
        return "/tmp/trained_model.pkl"

    @task(executor_config={
        "cpus_per_task": 2,
        "mem": "8G",
        "time_limit": "00:30:00",
        "partition": "compute",
    })
    def evaluate_model(model_path: str):
        """Evaluate model performance."""
        import time
        import random
        
        print(f"Evaluating model from {model_path}")
        
        # Simulate evaluation
        time.sleep(30)
        
        # Mock evaluation metrics
        accuracy = round(random.uniform(0.85, 0.95), 4)
        f1_score = round(random.uniform(0.80, 0.90), 4)
        
        metrics = {
            "accuracy": accuracy,
            "f1_score": f1_score,
            "model_path": model_path
        }
        
        print(f"Model evaluation complete: {metrics}")
        return metrics

    @task(executor_config={
        "cpus_per_task": 1,
        "mem": "4G",
        "time_limit": "00:15:00",
        "partition": "compute",
    })
    def deploy_model(model_metrics: dict):
        """Deploy model if performance is acceptable."""
        accuracy_threshold = 0.80
        
        if model_metrics["accuracy"] >= accuracy_threshold:
            print(f"Model meets threshold ({accuracy_threshold}). Deploying...")
            print(f"Deploying model: {model_metrics['model_path']}")
            print("Model deployed to production!")
            return "deployed"
        else:
            print(f"Model accuracy {model_metrics['accuracy']} below threshold {accuracy_threshold}")
            print("Model deployment skipped")
            return "skipped"

    # Define the ML pipeline
    data_path = extract_data()
    preprocessed_path = preprocess_data(data_path)
    model_path = train_model(preprocessed_path)
    metrics = evaluate_model(model_path)
    deploy_result = deploy_model(metrics)