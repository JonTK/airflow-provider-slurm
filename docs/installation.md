# Installation

This guide covers installation and setup of the Airflow Provider Slurm.

## Prerequisites

Before installing, ensure you have:

- **Python**: 3.9, 3.10, or 3.11
- **Apache Airflow**: 2.5.0 or later (including 3.x)
- **Slurm**: 23.11+ with REST API enabled
- **scontrol**: Available in PATH for token generation
- **Shared filesystem**: Between Airflow scheduler and Slurm compute nodes

## Installation Methods

### From PyPI (Recommended)

```bash
pip install airflow-provider-slurm
```

### From Source

```bash
git clone https://github.com/JonTK/airflow-provider-slurm
cd airflow-provider-slurm
pip install -e .
```

### For Development

```bash
git clone https://github.com/JonTK/airflow-provider-slurm
cd airflow-provider-slurm
pip install -e ".[dev]"
```

## Slurm REST API Setup

### Enable slurmrestd

The provider requires the Slurm REST API daemon (slurmrestd) to be running.

1. Install slurmrestd (usually included with Slurm):
   ```bash
   # On RHEL/CentOS/Rocky
   yum install slurm-slurmrestd

   # On Ubuntu/Debian
   apt-get install slurmrestd
   ```

2. Start slurmrestd:
   ```bash
   slurmrestd -a rest_auth/jwt 0.0.0.0:6820
   ```

3. Verify it's running:
   ```bash
   curl -H "X-SLURM-USER-TOKEN:$TOKEN" http://localhost:6820/slurm/v0.0.40/ping
   ```

### Generate JWT Token

The provider automatically generates tokens using scontrol:

```bash
scontrol token lifespan=3600
```

Ensure the Airflow user has permission to run this command.

## Airflow Configuration

1. Update `airflow.cfg`:
   ```ini
   [core]
   executor = airflow_provider_slurm.SlurmExecutor

   [slurm]
   api_url = https://your-slurm-cluster:6820
   default_partition = compute
   ```

2. Restart Airflow scheduler:
   ```bash
   airflow scheduler
   ```

## Verify Installation

1. Check the executor is loaded:
   ```bash
   airflow config get-value core executor
   ```

2. Test with a simple DAG:
   ```python
   from airflow import DAG
   from airflow.decorators import task
   from datetime import datetime

   with DAG('test_slurm', start_date=datetime(2024, 1, 1), schedule=None):
       @task
       def hello():
           print("Hello from Slurm!")
           return "success"

       hello()
   ```

3. Trigger the DAG:
   ```bash
   airflow dags trigger test_slurm
   ```

## Filesystem Setup

Ensure the shared filesystem is mounted on both the Airflow scheduler and all Slurm compute nodes:

```bash
# Example: Using NFS
# On scheduler:
df -h | grep /shared

# On compute nodes:
df -h | grep /shared
```

The Airflow `AIRFLOW_HOME` directory should be accessible from compute nodes.

## Network Requirements

- Airflow scheduler must reach slurmrestd API (default port 6820)
- Compute nodes must access the shared filesystem
- Firewall rules should allow:
  - Scheduler -> slurmrestd (TCP 6820)
  - Scheduler <-> Compute nodes (filesystem protocols: NFS, Lustre, etc.)

## Next Steps

- [Configuration Guide](configuration.md) - Configure the executor
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Examples](../examples/) - Example DAGs

## Common Installation Issues

### "scontrol: command not found"

Ensure Slurm tools are in PATH:
```bash
export PATH=/usr/bin:$PATH
which scontrol
```

### "Connection refused to slurmrestd"

Check slurmrestd is running:
```bash
systemctl status slurmrestd
# or
ps aux | grep slurmrestd
```

### "Permission denied generating token"

Ensure the Airflow user can run scontrol:
```bash
# As Airflow user:
scontrol token lifespan=1800
```

See [Troubleshooting Guide](troubleshooting.md) for more help.
