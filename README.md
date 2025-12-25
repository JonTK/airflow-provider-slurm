# Airflow Slurm Executor

Execute Apache Airflow tasks on HPC clusters using Slurm REST API.

## Features

- 🚀 Submit Airflow tasks as Slurm jobs via REST API
- 📊 Monitor job status and update task states in real-time
- 📝 Stream logs from Slurm to Airflow UI
- 🔧 Configure resources (CPU, memory, time limits) per task
- 🐳 Support for both containerized and virtual environment execution
- 🔄 Automatic job recovery after scheduler restarts
- ⚡ Efficient batch job submission for high-throughput workloads

## Requirements

- Python 3.8+
- Apache Airflow 2.5.0+
- Slurm cluster with REST API (slurmrestd) v0.0.42+
- `scontrol` binary available in PATH
- Shared filesystem between Airflow and Slurm compute nodes

## Installation

```bash
pip install airflow-slurm-executor
```

## Quick Start

1. Configure Airflow to use the Slurm executor:

```ini
# airflow.cfg
[core]
executor = airflow_slurm_executor.SlurmExecutor

[slurm]
api_url = https://your-slurm-cluster:6820
default_partition = compute
```

2. Create a DAG that leverages Slurm resources:

```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    'slurm_example',
    start_date=datetime(2024, 1, 1),
    schedule=None,
) as dag:
    
    @task(executor_config={
        'partition': 'gpu',
        'cpus_per_task': 4,
        'mem': '16G',
        'time_limit': '02:00:00',
    })
    def gpu_task():
        import torch
        # Your GPU workload here
        return "Task completed on Slurm!"
    
    gpu_task()
```

## Configuration

See [Configuration Guide](docs/configuration.md) for detailed options.

## Development

```bash
# Clone the repository
git clone https://github.com/jontk/airflow-slurm-executor
cd airflow-slurm-executor

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
black . && isort . && flake8
```

## Documentation

- [Installation Guide](docs/installation.md)
- [Configuration Reference](docs/configuration.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Contributing](CONTRIBUTING.md)

## License

Apache License 2.0 - see [LICENSE](LICENSE) file for details.

## Acknowledgments

This executor was developed to bridge the gap between modern data orchestration and traditional HPC infrastructure, enabling organizations to leverage their existing Slurm clusters for Airflow workflows.