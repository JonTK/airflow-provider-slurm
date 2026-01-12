# Airflow Provider Slurm

[![PyPI version](https://badge.fury.io/py/airflow-provider-slurm.svg)](https://badge.fury.io/py/airflow-provider-slurm)
[![Python Support](https://img.shields.io/pypi/pyversions/airflow-provider-slurm.svg)](https://pypi.org/project/airflow-provider-slurm/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Alpha](https://img.shields.io/badge/Status-Alpha-orange.svg)]()

🚀 **Apache Airflow Executor for Slurm HPC Clusters**

Execute Apache Airflow tasks on High-Performance Computing (HPC) clusters using the Slurm REST API. Validated against live Slurm 25.11.1 infrastructure.

## Features

- 🚀 Submit Airflow tasks as Slurm jobs via REST API
- 📊 Monitor job status and update task states in real-time
- 📝 Stream logs from Slurm to Airflow UI
- 🔧 Configure resources (CPU, memory, time limits) per task
- 🐳 Support for both containerized and virtual environment execution
- 🔄 Automatic job recovery after scheduler restarts
- ⚡ Efficient batch job submission for high-throughput workloads

## ✨ Key Highlights

- 🎯 **Live Tested**: Validated against live Slurm 25.11.1 clusters
- 🔧 **HPC Ready**: Supports HPC, ML, bioinformatics, and distributed computing workloads
- ⚡ **High Performance**: Optimized for large-scale workflow orchestration
- 🛡️ **Reliable**: Comprehensive error handling and job recovery mechanisms
- 📈 **Scalable**: Dynamic resource allocation and multi-partition support

## Requirements

- **Python**: 3.8, 3.9, 3.10, 3.11
- **Apache Airflow**: 2.5.0+ (including 3.x support)
- **Slurm**: 20.02+ with REST API (slurmrestd) v0.0.40-v0.0.44  
- **System**: `scontrol` binary available in PATH
- **Storage**: Shared filesystem between Airflow and Slurm compute nodes

## Installation

```bash
pip install airflow-provider-slurm
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
git clone https://github.com/JonTK/airflow-provider-slurm
cd airflow-provider-slurm

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