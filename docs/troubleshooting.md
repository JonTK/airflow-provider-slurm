# Troubleshooting

Common issues and solutions for Airflow Provider Slurm.

## Connection Issues

### Cannot connect to Slurm REST API

**Symptoms:**
```
ConnectionError: Failed to connect to https://slurm-cluster:6820
```

**Solutions:**

1. Verify slurmrestd is running:
   ```bash
   curl http://localhost:6820/slurm/v0.0.40/ping
   ```

2. Check firewall rules:
   ```bash
   telnet slurm-cluster 6820
   ```

3. Verify API URL in configuration:
   ```bash
   airflow config get-value slurm api_url
   ```

### SSL Certificate Errors

**Symptoms:**
```
SSLError: certificate verify failed
```

**Solutions:**

1. Use valid SSL certificates for slurmrestd

2. For testing only, disable SSL verification (not recommended for production):
   ```ini
   [slurm]
   verify_ssl = false
   ```

## Authentication Issues

### Token Generation Fails

**Symptoms:**
```
RuntimeError: Failed to generate Slurm token
```

**Solutions:**

1. Verify scontrol is available:
   ```bash
   which scontrol
   scontrol token lifespan=1800
   ```

2. Check user permissions:
   ```bash
   # As Airflow user:
   id
   scontrol token lifespan=1800
   ```

3. Ensure Slurm authentication is configured:
   ```bash
   cat /etc/slurm/slurm.conf | grep AuthType
   ```

### Token Expired

**Symptoms:**
```
AuthenticationError: Token expired
```

**Solutions:**

Tokens are automatically refreshed. If issues persist:

1. Check token lifetime configuration
2. Verify system time synchronization (NTP)
3. Manually refresh token cache

## Job Submission Issues

### Jobs Stuck in Queued State

**Symptoms:**
- Tasks show "queued" in Airflow UI
- No Slurm jobs appearing in `squeue`

**Solutions:**

1. Check Slurm job submission logs:
   ```bash
   grep "job submit" /var/log/slurm/slurmrestd.log
   ```

2. Verify partition exists and is available:
   ```bash
   sinfo -p compute
   ```

3. Check Slurm limits:
   ```bash
   sacctmgr show qos
   sacctmgr show user $USER
   ```

### Jobs Fail Immediately

**Symptoms:**
```
TaskInstanceState.FAILED: Job failed with exit code 1
```

**Solutions:**

1. Check Slurm job logs:
   ```bash
   # Find the Slurm job ID from Airflow task logs
   sacct -j <job_id> --format=JobID,State,ExitCode,Reason
   ```

2. Review job output files:
   ```bash
   cat $AIRFLOW_HOME/logs/slurm_job_*.out
   ```

3. Verify resources are available:
   ```bash
   sinfo -N -l
   ```

## Filesystem Issues

### Task Cannot Find Files

**Symptoms:**
```
FileNotFoundError: [Errno 2] No such file or directory
```

**Solutions:**

1. Verify shared filesystem is mounted:
   ```bash
   # On scheduler:
   df -h | grep /shared

   # On compute node:
   ssh compute-node "df -h | grep /shared"
   ```

2. Check file permissions:
   ```bash
   ls -la $AIRFLOW_HOME
   ```

3. Ensure consistent paths between scheduler and compute nodes

### Log Files Not Appearing

**Symptoms:**
- Task shows "success" but no logs in Airflow UI

**Solutions:**

1. Check log directory permissions:
   ```bash
   ls -la $AIRFLOW_HOME/logs
   ```

2. Verify log paths are on shared filesystem

3. Check Slurm job output:
   ```bash
   ls -la $AIRFLOW_HOME/logs/slurm_job_*.out
   ```

## Performance Issues

### High Job Submission Latency

**Symptoms:**
- Slow task starts
- Long delays between task queuing and execution

**Solutions:**

1. Reduce polling interval:
   ```ini
   [slurm]
   poll_interval = 5  # Default is 10 seconds
   ```

2. Increase batch size for status queries:
   ```ini
   [slurm]
   batch_size = 100  # Default is 50
   ```

3. Check network latency to slurmrestd

### Scheduler Memory Usage

**Symptoms:**
- High memory consumption
- OOM errors

**Solutions:**

1. Limit concurrent jobs:
   ```ini
   [slurm]
   max_concurrent_jobs = 100
   ```

2. Increase polling interval:
   ```ini
   [slurm]
   poll_interval = 15
   ```

## API Version Issues

### Unsupported API Version

**Symptoms:**
```
ValueError: Unsupported Slurm REST API version
```

**Solutions:**

1. Check slurmrestd version:
   ```bash
   curl http://localhost:6820/openapi/v3 | jq .info.version
   ```

2. Ensure you're running Slurm 23.11-25.11

3. Verify API version in code matches slurmrestd

## Debugging Tips

### Enable Debug Logging

Add to `airflow.cfg`:
```ini
[logging]
logging_level = DEBUG
```

Or set environment variable:
```bash
export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

### Check Slurm Logs

```bash
# Slurm daemon logs
tail -f /var/log/slurm/slurmctld.log

# REST API logs
tail -f /var/log/slurm/slurmrestd.log

# Slurm accounting
sacct -a --format=JobID,JobName,State,ExitCode,Start,End
```

### Inspect Task Logs

```bash
# Airflow task logs
cat $AIRFLOW_HOME/logs/dag_id/task_id/date/1.log

# Slurm job output
cat $AIRFLOW_HOME/logs/slurm_job_*.out
cat $AIRFLOW_HOME/logs/slurm_job_*.err
```

### Test API Connectivity

```bash
# Generate token
TOKEN=$(scontrol token lifespan=3600 | grep SLURM_JWT | cut -d= -f2)

# Test ping
curl -H "X-SLURM-USER-TOKEN:$TOKEN" \
  http://localhost:6820/slurm/v0.0.40/ping

# List jobs
curl -H "X-SLURM-USER-TOKEN:$TOKEN" \
  http://localhost:6820/slurm/v0.0.40/jobs
```

## Getting Help

If you're still experiencing issues:

1. Check [GitHub Issues](https://github.com/JonTK/airflow-provider-slurm/issues)
2. Review [Configuration Guide](configuration.md)
3. Open a new issue with:
   - Airflow version
   - Slurm version
   - Provider version
   - Error logs
   - Steps to reproduce

## Known Issues

### Airflow 3.x Compatibility

Some features may behave differently in Airflow 3.x. Check the CHANGELOG for known issues and workarounds.

### Slurm API Limitations

- Job array support is limited in REST API v0.0.40-v0.0.44
- Some advanced Slurm features may not be exposed via REST API
- Token lifetimes are limited by Slurm configuration

See [SECURITY.md](../SECURITY.md) for security considerations and limitations.
