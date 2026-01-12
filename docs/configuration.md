# Configuration

This document describes how to configure the Airflow Provider Slurm executor.

## Airflow Configuration

Add the following to your `airflow.cfg`:

```ini
[core]
executor = airflow_provider_slurm.SlurmExecutor

[slurm]
# Slurm REST API URL
api_url = https://your-slurm-cluster:6820

# Default partition for job submission
default_partition = compute

# Default QoS (optional)
# default_qos = normal

# Job polling interval in seconds (default: 10)
# poll_interval = 10

# Maximum concurrent jobs (optional, uses Slurm limits by default)
# max_concurrent_jobs = 100

# Batch size for status queries (default: 50)
# batch_size = 50

# Default resource limits (optional)
# default_cpus = 1
# default_memory = 4G
# default_time_limit = 01:00:00
```

## Environment Variables

You can also configure the executor using environment variables:

- `SLURM_API_URL`: Slurm REST API URL
- `SLURM_DEFAULT_PARTITION`: Default partition
- `AIRFLOW_SLURM_TOKEN`: Pre-generated JWT token (optional)

## Task-Level Configuration

Configure resources per task using `executor_config`:

```python
from airflow.decorators import task

@task(executor_config={
    'partition': 'gpu',
    'cpus_per_task': 4,
    'mem': '16G',
    'time_limit': '02:00:00',
    'gres': 'gpu:1',
    'constraint': 'volta',
})
def gpu_task():
    # Your GPU workload
    pass
```

## Available Executor Config Options

| Option | Description | Example |
|--------|-------------|---------|
| `partition` | Slurm partition | `'compute'` |
| `qos` | Quality of Service | `'high'` |
| `cpus_per_task` | CPUs per task | `4` |
| `mem` | Memory limit | `'16G'` |
| `time_limit` | Time limit (HH:MM:SS) | `'02:00:00'` |
| `gres` | Generic resources | `'gpu:2'` |
| `constraint` | Node constraints | `'haswell'` |
| `account` | Slurm account | `'my-project'` |
| `environment` | Environment variables | `{'PATH': '/usr/bin'}` |

## Authentication

The executor uses JWT tokens for authentication with the Slurm REST API. Tokens are automatically generated using the `scontrol token` command.

### Token Refresh

Tokens are cached and automatically refreshed when they expire. The default token lifetime is 1800 seconds (30 minutes).

## Advanced Configuration

### Custom Token Manager

You can provide a custom token manager:

```python
from airflow_provider_slurm import SlurmExecutor, SlurmTokenManager

class CustomTokenManager(SlurmTokenManager):
    def get_token(self):
        # Custom token logic
        return "your-token"
```

### Retry Configuration

Configure API retry behavior:

```ini
[slurm]
# Maximum retry attempts (default: 3)
max_retries = 3

# Retry backoff factor (default: 2)
retry_backoff_factor = 2

# Retry delay in seconds (default: 1)
retry_delay = 1
```

## Troubleshooting

See [Troubleshooting Guide](troubleshooting.md) for common issues and solutions.
