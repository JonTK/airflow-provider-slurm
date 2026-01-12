# Slurm Executor for Apache Airflow - Technical Specification

**Version:** 1.0  
**Date:** December 10, 2024  
**Author:** Jon  

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Detailed Design](#3-detailed-design)
4. [Configuration](#4-configuration)
5. [Error Handling](#5-error-handling)
6. [Testing Strategy](#6-testing-strategy)
7. [Implementation Roadmap](#7-implementation-roadmap)
8. [Package Structure](#8-package-structure)
9. [Dependencies](#9-dependencies)
10. [API Reference](#10-api-reference)
11. [Troubleshooting Guide](#11-troubleshooting-guide)
12. [Success Criteria](#12-success-criteria)
13. [Appendices](#appendices)

---

## 1. Overview

### 1.1 Purpose

Implement a custom Airflow executor that enables task execution on HPC clusters running Slurm, using the Slurm REST API for job submission and monitoring.

### 1.2 Goals

- Enable Airflow workflows to leverage existing Slurm/HPC infrastructure
- Provide seamless integration using modern Slurm REST API (v0.0.42+)
- Support both containerized and virtual environment execution modes
- Maintain compatibility with Airflow's executor interface and logging system

### 1.3 Non-Goals (for MVP)

- Support for Slurm API versions older than v0.0.42
- MPI or multi-node job support
- Slurm job arrays for mapped tasks (deferred to Phase 4)
- Advanced Slurm features (reservations, licenses, GRES beyond basic resources)

### 1.4 Quick Start for Developers

**Prerequisites:**
- Python 3.8+
- Access to Slurm cluster with REST API v0.0.42+
- `scontrol` binary in PATH

**Basic development workflow:**

1. **Clone and setup:**
```bash
git clone <repo>
cd airflow-slurm-executor
pip install -e .
pip install -r requirements-dev.txt
```

2. **Configure Airflow:**
```ini
# airflow.cfg
[core]
executor = airflow_provider_slurm.SlurmExecutor

[slurm]
api_url = https://your-slurm-cluster:6820
default_partition = compute
```

3. **Run tests:**
```bash
pytest tests/unit/
pytest tests/integration/  # Requires Slurm access
```

4. **Test with example DAG:**
```bash
cp examples/simple_dag.py ~/airflow/dags/
airflow dags test simple_slurm_dag
```

**Key files to start with:**
- `slurm_executor.py` - Main executor logic
- `slurm_api_client.py` - REST API wrapper
- `tests/unit/test_executor.py` - Test examples

---

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────┐
│                   Airflow Scheduler                      │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │           SlurmExecutor                         │    │
│  │                                                 │    │
│  │  • execute_async() - Submit tasks              │    │
│  │  • sync() - Poll job status                    │    │
│  │  • end() - Graceful shutdown                   │    │
│  │  • terminate() - Emergency shutdown            │    │
│  └─────────────────┬──────────────────────────────┘    │
│                    │                                     │
│  ┌─────────────────▼──────────────────────────────┐    │
│  │         SlurmAPIClient                          │    │
│  │                                                 │    │
│  │  • submit_job()                                │    │
│  │  • get_jobs()                                  │    │
│  │  • get_job_history()                           │    │
│  │  • cancel_job()                                │    │
│  └─────────────────┬──────────────────────────────┘    │
│                    │                                     │
│  ┌─────────────────▼──────────────────────────────┐    │
│  │       SlurmTokenManager                         │    │
│  │                                                 │    │
│  │  • get_token() - Get/refresh JWT               │    │
│  │  • Uses: scontrol token                        │    │
│  └─────────────────────────────────────────────────┘    │
└──────────────────────┬───────────────────────────────────┘
                       │
                       │ HTTPS + JWT
                       ▼
         ┌──────────────────────────────┐
         │    Slurm REST API             │
         │    (slurmrestd)               │
         └──────────────┬────────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │    Slurm Controller           │
         │    (slurmctld)                │
         └──────────────┬────────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │    Compute Nodes              │
         │    (slurmd)                   │
         │                               │
         │  • Execute Airflow tasks      │
         │  • Write logs to shared FS    │
         └───────────────────────────────┘
```

### 2.2 Key Classes

#### 2.2.1 SlurmExecutor

**Inheritance:** `airflow.executors.base_executor.BaseExecutor`

**Responsibilities:**
- Translate Airflow task instances to Slurm jobs
- Submit jobs via SlurmAPIClient
- Poll job status and update Airflow state
- Handle graceful and emergency shutdown
- Manage task-to-job-ID mapping

**State:**
```python
{
    'running': {
        TaskInstanceKey: {
            'slurm_job_id': int,
            'command': List[str],
            'submit_time': datetime,
            'missing_since': Optional[datetime]  # For tracking disappeared jobs
        }
    },
    'last_sync_time': float,
    'slurm_client': SlurmAPIClient,
    'token_manager': SlurmTokenManager,
    # Configuration fields...
}
```

#### 2.2.2 SlurmAPIClient

**Responsibilities:**
- HTTP communication with Slurm REST API
- Request retry logic with exponential backoff
- Authentication header management
- API version compatibility

**Methods:**
- `submit_job(job_spec: dict) -> dict` - Submit job, return job_id
- `get_jobs(job_ids: Optional[List[int]]) -> dict` - Query active jobs
- `get_job_history(job_id: int) -> Optional[dict]` - Query accounting DB
- `cancel_job(job_id: int) -> dict` - Cancel a job
- `get_api_version() -> str` - Discover API version

#### 2.2.3 SlurmTokenManager

**Responsibilities:**
- Generate JWT tokens via `scontrol token`
- Cache tokens until near expiration
- Automatic token refresh

**Methods:**
- `get_token() -> str` - Get valid token (refresh if needed)
- `_fetch_new_token() -> str` - Call scontrol to generate token
- `_token_is_valid() -> bool` - Check if cached token is still valid

---

## 3. Detailed Design

### 3.1 Task Submission Flow

#### 3.1.1 execute_async() Implementation

```python
def execute_async(
    self,
    key: TaskInstanceKey,
    command: List[str],
    queue: Optional[str] = None,
    executor_config: Optional[dict] = None
) -> None:
    """
    Submit task to Slurm
    
    Args:
        key: Unique task instance identifier
        command: Command to execute (e.g., ['airflow', 'tasks', 'run', ...])
        queue: Airflow queue (can map to Slurm partition)
        executor_config: Dict of Slurm-specific overrides
    """
    # 1. Build job specification
    job_spec = self._build_job_spec(key, command, queue, executor_config)
    
    # 2. Submit to Slurm
    job_id = self._submit_job(job_spec)
    
    # 3. Track the job
    if job_id:
        self.running[key] = {
            'slurm_job_id': job_id,
            'command': command,
            'submit_time': datetime.now(),
        }
        self.log.info(f"Submitted task {key} as Slurm job {job_id}")
    else:
        # Submission failed - mark as failed for retry
        self.fail(key)
```

#### 3.1.2 Job Specification Building

**Function:** `_build_job_spec(key, command, queue, executor_config) -> dict`

**Logic:**
1. Extract task instance metadata (dag_id, task_id, execution_date, try_number)
2. Determine log file path using Airflow's log handler
3. Build job name: `airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}`
4. Construct bash script with proper escaping
5. Apply resource requirements from executor_config or defaults
6. Set environment variables
7. Configure working directory

**Job Spec Structure (Slurm REST API v0.0.42):**
```json
{
  "script": "#!/bin/bash\nset -euo pipefail\n...",
  "job": {
    "name": "airflow-my_dag-my_task-a3f5b2c1-1",
    "partition": "compute",
    "tasks": 1,
    "cpus_per_task": 4,
    "memory_per_node": "16G",
    "time_limit": "02:00:00",
    "current_working_directory": "/shared/airflow",
    "environment": {
      "AIRFLOW_HOME": "/shared/airflow",
      "AIRFLOW__CORE__DAGS_FOLDER": "/shared/airflow/dags",
      "PYTHONPATH": "...",
      "PATH": "..."
    },
    "standard_output": "/shared/airflow/logs/dags/my_dag/my_task/2024-01-01T00:00:00+00:00/1.log",
    "standard_error": "/shared/airflow/logs/dags/my_dag/my_task/2024-01-01T00:00:00+00:00/1.log",
    "account": "airflow_account",
    "qos": "normal",
    "container": "docker://registry/airflow:2.8.0"
  }
}
```

#### 3.1.3 Script Generation

**Two modes based on configuration:**

**Mode 1: Container-based (if container specified)**
```bash
#!/bin/bash
set -euo pipefail

# Airflow command (container has environment)
airflow tasks run my_dag my_task 2024-01-01T00:00:00+00:00 --local --pool default --subdir /shared/airflow/dags/my_dag.py
```

**Mode 2: Virtual environment (if venv specified, no container)**
```bash
#!/bin/bash
set -euo pipefail

# Activate virtual environment
source /shared/airflow/venv/bin/activate

# Execute Airflow task
airflow tasks run my_dag my_task 2024-01-01T00:00:00+00:00 --local --pool default --subdir /shared/airflow/dags/my_dag.py
```

**Command escaping:** Use `shlex.quote()` for all command arguments

#### 3.1.4 Resource Mapping

**Executor config schema:**
```python
executor_config = {
    'partition': str,           # Slurm partition
    'cpus_per_task': int,       # Number of CPUs
    'mem': str,                 # Memory (e.g., '16G', '32768M')
    'time_limit': str,          # Wall time (HH:MM:SS)
    'account': str,             # Slurm account
    'qos': str,                 # Quality of Service
    'container': str,           # Container image
    'working_dir': str,         # Override working directory
}
```

**Defaults (from config):**
- partition: `default_partition` (config: 'compute')
- cpus_per_task: `default_cpus` (config: 1)
- mem: `default_mem` (config: '4G')
- time_limit: `default_time_limit` (config: '01:00:00')

**Precedence:** executor_config > defaults > Slurm cluster defaults

### 3.2 Status Synchronization

#### 3.2.1 sync() Implementation

**Called periodically by Airflow scheduler (every few seconds)**

```python
def sync(self) -> None:
    """Poll Slurm for job status and update Airflow task states"""
    
    # Throttle: only sync every min_sync_interval seconds
    now = time.time()
    if now - self.last_sync_time < self.min_sync_interval:
        return
    self.last_sync_time = now
    
    if not self.running:
        return  # Nothing to check
    
    # 1. Fetch status of all tracked jobs from Slurm
    job_statuses = self._fetch_job_statuses()
    
    # 2. Update state for each tracked task
    for key, job_info in list(self.running.items()):
        slurm_job_id = job_info['slurm_job_id']
        
        if slurm_job_id in job_statuses:
            # Job found in active queue
            self._handle_job_state(key, job_statuses[slurm_job_id])
        else:
            # Job not in active queue - check accounting or fail
            self._handle_missing_job(key, job_info)
```

#### 3.2.2 Fetching Job Statuses

**Function:** `_fetch_job_statuses() -> Dict[int, dict]`

```python
def _fetch_job_statuses(self) -> Dict[int, dict]:
    """
    Query Slurm for all tracked jobs
    Returns: {job_id: {'state': str, 'exit_code': int, 'reason': str}}
    """
    try:
        # Get all jobs (Slurm filters by user automatically via JWT)
        response = self.slurm_client.get_jobs()
        
        # Extract our tracked job IDs
        our_job_ids = {info['slurm_job_id'] for info in self.running.values()}
        
        # Build lookup dict
        statuses = {}
        for job in response.get('jobs', []):
            job_id = job['job_id']
            if job_id in our_job_ids:
                statuses[job_id] = {
                    'state': job['job_state'],
                    'exit_code': job.get('exit_code', 0),
                    'reason': job.get('state_reason', ''),
                }
        
        return statuses
        
    except SlurmAPIError as e:
        self.log.warning(f"Failed to fetch job statuses: {e}")
        return {}  # Return empty, will retry next sync
```

#### 3.2.3 State Mapping

**Slurm job states → Airflow task states:**

| Slurm State | Airflow Action | Notes |
|------------|---------------|-------|
| PENDING | No action | Job queued, keep as running in Airflow |
| CONFIGURING | No action | Job being configured |
| RUNNING | No action | Job executing |
| COMPLETED (exit_code=0) | success(key) | Task succeeded |
| COMPLETED (exit_code≠0) | fail(key) | Task failed with error |
| FAILED | fail(key) | Job failed |
| TIMEOUT | fail(key) | Job exceeded time limit |
| CANCELLED | fail(key) | Job was cancelled |
| NODE_FAIL | fail(key) | Node failure |
| OUT_OF_MEMORY | fail(key) | OOM kill |
| PREEMPTED | fail(key) | Job preempted (Airflow will retry) |

**Implementation:**

```python
def _handle_job_state(self, key: TaskInstanceKey, slurm_state: dict) -> None:
    """Process Slurm job state and update Airflow"""
    state = slurm_state['state']
    
    # States that don't require action
    if state in ['PENDING', 'CONFIGURING', 'RUNNING']:
        return
    
    # Job completed
    if state == 'COMPLETED':
        exit_code = slurm_state.get('exit_code', 0)
        if exit_code == 0:
            self.success(key)
            self.log.info(f"Task {key} succeeded")
        else:
            self.fail(key)
            self.log.error(f"Task {key} failed with exit code {exit_code}")
        del self.running[key]
        return
    
    # Job failed
    if state in ['FAILED', 'TIMEOUT', 'CANCELLED', 'NODE_FAIL', 'OUT_OF_MEMORY', 'PREEMPTED']:
        self.fail(key)
        self.log.error(f"Task {key} failed: {state} - {slurm_state.get('reason', 'unknown')}")
        del self.running[key]
        return
    
    # Unknown state
    self.log.warning(f"Unknown Slurm state '{state}' for task {key}")
```

#### 3.2.4 Missing Job Handling

**Function:** `_handle_missing_job(key, job_info) -> None`

**Logic:**
1. Job not in active queue - might be completed and purged
2. Query accounting database for historical record
3. If found in accounting: process final state
4. If not found: track how long it's been missing
5. If missing > 5 minutes: mark as failed

```python
def _handle_missing_job(self, key: TaskInstanceKey, job_info: dict) -> None:
    """Handle job that's not in active queue"""
    slurm_job_id = job_info['slurm_job_id']
    
    # Try accounting database
    try:
        job_history = self.slurm_client.get_job_history(slurm_job_id)
        
        if job_history:
            # Found in accounting - process final state
            state = job_history['state']
            exit_code = job_history.get('exit_code', 0)
            
            if state == 'COMPLETED' and exit_code == 0:
                self.success(key)
                self.log.info(f"Task {key} found in accounting: COMPLETED")
            else:
                self.fail(key)
                self.log.error(f"Task {key} found in accounting: {state}")
            
            del self.running[key]
            return
    
    except SlurmAPIError as e:
        self.log.debug(f"Could not query accounting for job {slurm_job_id}: {e}")
    
    # Track how long it's been missing
    if 'missing_since' not in job_info:
        job_info['missing_since'] = datetime.now()
        self.log.debug(f"Job {slurm_job_id} not found in active queue, tracking")
        return
    
    # Check timeout
    missing_duration = datetime.now() - job_info['missing_since']
    if missing_duration > timedelta(minutes=5):
        self.fail(key)
        self.log.error(
            f"Task {key} job {slurm_job_id} missing from Slurm for {missing_duration}, "
            "marking as failed"
        )
        del self.running[key]
```

### 3.3 Logging Integration

#### 3.3.1 Log Path Determination

**Airflow's log path pattern:**
```
{base_log_folder}/dags/{dag_id}/{task_id}/{execution_date}/{try_number}.log
```

**Implementation:**

```python
def _get_log_path(self, key: TaskInstanceKey) -> str:
    """
    Determine log file path for a task instance
    Uses Airflow's log handler configuration
    """
    # Airflow's log handler knows the pattern
    # This is simplified - actual implementation would use log handler
    from airflow.utils.log.file_task_handler import FileTaskHandler
    
    handler = FileTaskHandler(base_log_folder=self.log_folder)
    log_path = handler._render_filename(
        ti=key,  # TaskInstanceKey has dag_id, task_id, etc.
        try_number=key.try_number
    )
    
    return log_path
```

**Example paths:**
```
/shared/airflow/logs/dags/example_dag/example_task/2024-12-09T10:00:00+00:00/1.log
/shared/airflow/logs/dags/ml_pipeline/train_model/2024-12-09T14:30:00+00:00/2.log
```

#### 3.3.2 Shared Filesystem Requirements

**Prerequisites:**
- Shared filesystem accessible from:
  - Airflow scheduler (writes job specs, reads config)
  - Slurm compute nodes (write logs)
  - Airflow web server (reads logs for UI)

**Common setups:**
- NFS mounted at same path on all nodes
- Lustre parallel filesystem
- GPFS or other cluster filesystems

**Validation on startup:**
```python
def _validate_shared_filesystem(self) -> None:
    """Verify log directory is accessible and writable"""
    test_file = os.path.join(self.log_folder, '.slurm_executor_test')
    
    try:
        # Try to create a test file
        Path(test_file).touch()
        os.remove(test_file)
        self.log.info(f"Verified shared filesystem access at {self.log_folder}")
    except Exception as e:
        self.log.error(
            f"Cannot write to log folder {self.log_folder}: {e}. "
            "Ensure this path is on shared storage accessible from compute nodes."
        )
        raise
```

#### 3.3.3 Log Streaming Behavior

**How it works:**
1. Slurm writes stdout/stderr to log file on shared FS
2. Airflow's FileTaskHandler tails this file when user views logs in UI
3. Live streaming works because file is actively being written to

**Buffering consideration:**
- Slurm buffers stdout/stderr (typically line-buffered)
- Minor delay (seconds) between task output and log visibility
- Acceptable for most use cases

### 3.4 Authentication

#### 3.4.1 Token Generation

**Method:** Use `scontrol token` command

**Command format:**
```bash
scontrol token lifespan=3600 username=airflow_user
```

**Output format:**
```
SLURM_JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### 3.4.2 SlurmTokenManager Implementation

```python
class SlurmTokenManager:
    def __init__(self, username: Optional[str] = None, lifespan: int = 3600):
        self.username = username or getpass.getuser()
        self.lifespan = lifespan
        self.token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
    
    def get_token(self) -> str:
        """Get valid token, refresh if expired"""
        if self._token_is_valid():
            return self.token
        return self._fetch_new_token()
    
    def _token_is_valid(self) -> bool:
        """Check if cached token is still valid"""
        if not self.token or not self.token_expiry:
            return False
        
        # Refresh 5 minutes before expiry
        buffer = timedelta(minutes=5)
        return datetime.now() < (self.token_expiry - buffer)
    
    def _fetch_new_token(self) -> str:
        """Generate new token via scontrol"""
        cmd = ['scontrol', 'token', f'lifespan={self.lifespan}']
        if self.username:
            cmd.append(f'username={self.username}')
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=10
            )
            
            # Parse: "SLURM_JWT=token_value"
            output = result.stdout.strip()
            if not output.startswith('SLURM_JWT='):
                raise SlurmTokenError(f"Unexpected output: {output}")
            
            self.token = output.split('=', 1)[1]
            self.token_expiry = datetime.now() + timedelta(seconds=self.lifespan)
            
            return self.token
            
        except subprocess.CalledProcessError as e:
            raise SlurmTokenError(f"Token generation failed: {e.stderr}")
        except subprocess.TimeoutExpired:
            raise SlurmTokenError("Token generation timed out")

class SlurmTokenError(Exception):
    pass
```

#### 3.4.3 API Authentication Headers

**Header format:**
```python
headers = {
    'X-SLURM-USER-TOKEN': token,
    'Content-Type': 'application/json',
}
```

**Implementation in SlurmAPIClient:**
```python
def _auth_headers(self) -> dict:
    """Get headers with fresh authentication token"""
    token = self.token_manager.get_token()
    return {
        'X-SLURM-USER-TOKEN': token,
        'Content-Type': 'application/json',
    }
```

### 3.5 Cleanup and Shutdown

#### 3.5.1 Graceful Shutdown: end()

```python
def end(self) -> None:
    """
    Called when Airflow scheduler is shutting down gracefully
    Behavior depends on shutdown_mode configuration
    """
    if not self.running:
        self.log.info("SlurmExecutor shutdown: no running jobs")
        return
    
    self.log.info(f"SlurmExecutor shutdown: {len(self.running)} jobs running")
    
    if self.shutdown_mode == 'cancel':
        self._cancel_all_jobs()
    elif self.shutdown_mode == 'wait':
        self._wait_for_jobs(timeout=self.shutdown_wait_timeout)
    else:
        self.log.warning(f"Unknown shutdown_mode: {self.shutdown_mode}, cancelling jobs")
        self._cancel_all_jobs()
```

**Cancel all jobs:**
```python
def _cancel_all_jobs(self) -> None:
    """Cancel all tracked Slurm jobs"""
    for key, job_info in list(self.running.items()):
        slurm_job_id = job_info['slurm_job_id']
        try:
            self.slurm_client.cancel_job(slurm_job_id)
            self.log.info(f"Cancelled job {slurm_job_id} for task {key}")
        except SlurmAPIError as e:
            self.log.warning(f"Failed to cancel job {slurm_job_id}: {e}")
        
        self.fail(key)
    
    self.running.clear()
```

**Wait for jobs:**
```python
def _wait_for_jobs(self, timeout: int) -> None:
    """Wait for jobs to complete, then cancel remaining"""
    start_time = time.time()
    
    while self.running and (time.time() - start_time) < timeout:
        self.sync()
        time.sleep(5)
    
    if self.running:
        self.log.warning(
            f"Timeout waiting for jobs after {timeout}s, "
            f"cancelling {len(self.running)} remaining"
        )
        self._cancel_all_jobs()
```

#### 3.5.2 Emergency Shutdown: terminate()

```python
def terminate(self) -> None:
    """
    Emergency shutdown - kill everything immediately
    Called on SIGTERM or fatal errors
    """
    self.log.warning("SlurmExecutor emergency terminate: killing all jobs")
    
    # Best-effort cancellation, ignore errors
    for key, job_info in self.running.items():
        try:
            self.slurm_client.cancel_job(job_info['slurm_job_id'])
        except Exception:
            pass  # Ignore all errors in emergency shutdown
    
    self.running.clear()
```

#### 3.5.3 Job Cancellation API

**SlurmAPIClient method:**
```python
def cancel_job(self, job_id: int) -> Optional[dict]:
    """
    Cancel a Slurm job
    
    API: DELETE /slurm/v0.0.42/job/{job_id}
    
    Returns: Response dict or None if job doesn't exist
    Raises: SlurmAPIError on failure
    """
    url = f"{self.base_url}/slurm/{self.api_version}/job/{job_id}"
    
    try:
        response = self._request('DELETE', url)
        return response.json()
    except SlurmAPIError as e:
        # 404 is acceptable - job already finished
        if '404' in str(e) or 'not found' in str(e).lower():
            return None
        raise
```

### 3.6 Task Adoption (Scheduler Restart Recovery)

#### 3.6.1 Purpose

When the Airflow scheduler restarts (due to crash, deployment, or maintenance), tasks that were running continue to execute on Slurm. Task adoption allows the restarted scheduler to reconnect to these in-flight jobs and continue monitoring them, rather than losing track and potentially re-submitting them.

#### 3.6.2 Design Approach

**Strategy:** Query Slurm for jobs matching our naming convention and reconstruct tracking state.

**Job Name Encoding:** Job names must contain sufficient information to uniquely identify the task instance. Our format:
```
airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}
```

Where:
- `dag_id`: DAG identifier
- `task_id`: Task identifier  
- `run_id_hash`: First 8 chars of SHA256 hash of run_id (handles execution_date/logical_date uniqueness)
- `try_number`: Attempt number

**Example:** `airflow-ml_pipeline-train_model-a3f5b2c1-1`

**Why hash run_id?** Slurm job names have length limits and run_ids can be long (especially with manual run_ids). The hash ensures uniqueness while keeping names manageable.

#### 3.6.3 Implementation

**Update job name generation in _build_job_spec():**

```python
import hashlib

def _build_job_name(self, key: TaskInstanceKey) -> str:
    """
    Build Slurm job name that encodes task identity
    
    Format: airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}
    
    Args:
        key: TaskInstanceKey containing task metadata
        
    Returns:
        Job name string (max 256 chars for Slurm)
    """
    # Get run_id (logical_date or dag_run.run_id)
    run_id = key.run_id  # Available in Airflow 2.2+
    
    # Hash run_id for compactness
    run_id_hash = hashlib.sha256(run_id.encode()).hexdigest()[:8]
    
    # Build name with sanitization
    dag_id = key.dag_id.replace('/', '_').replace('.', '_')
    task_id = key.task_id.replace('/', '_').replace('.', '_')
    
    job_name = f"airflow-{dag_id}-{task_id}-{run_id_hash}-{key.try_number}"
    
    # Slurm job name limit is typically 256 chars
    if len(job_name) > 256:
        # Truncate dag_id and task_id to fit
        max_id_length = (256 - 20) // 2  # Reserve space for prefix, hash, try_number
        dag_id = dag_id[:max_id_length]
        task_id = task_id[:max_id_length]
        job_name = f"airflow-{dag_id}-{task_id}-{run_id_hash}-{key.try_number}"
    
    return job_name
```

**Implement try_adopt_task_instances():**

```python
def try_adopt_task_instances(self, tis: List[TaskInstance]) -> List[TaskInstance]:
    """
    Adopt tasks that are already running after scheduler restart
    
    Called by Airflow when scheduler starts. We query Slurm for jobs
    matching our naming pattern and reconnect to any that correspond
    to task instances Airflow thinks should be running.
    
    Args:
        tis: List of task instances to potentially adopt
        
    Returns:
        List of successfully adopted task instances
    """
    if not tis:
        return []
    
    self.log.info(f"Attempting to adopt {len(tis)} task instances")
    
    adopted = []
    
    try:
        # Query Slurm for all jobs belonging to our user
        response = self.slurm_client.get_jobs()
        jobs = response.get('jobs', [])
        
        # Build lookup: job_name -> job_info
        slurm_jobs = {}
        for job in jobs:
            job_name = job.get('name', '')
            if job_name.startswith('airflow-'):
                slurm_jobs[job_name] = {
                    'job_id': job['job_id'],
                    'state': job.get('job_state', ''),
                }
        
        self.log.info(f"Found {len(slurm_jobs)} Airflow jobs in Slurm queue")
        
        # Try to match each TI to a Slurm job
        for ti in tis:
            key = ti.key
            expected_job_name = self._build_job_name(key)
            
            if expected_job_name in slurm_jobs:
                job_info = slurm_jobs[expected_job_name]
                job_id = job_info['job_id']
                state = job_info['state']
                
                # Only adopt if job is still active
                if state in ['PENDING', 'CONFIGURING', 'RUNNING']:
                    # Reconstruct tracking state
                    self.running[key] = {
                        'slurm_job_id': job_id,
                        'command': [],  # Unknown, but not needed for monitoring
                        'submit_time': datetime.now(),  # Approximate
                    }
                    adopted.append(ti)
                    self.log.info(
                        f"Adopted task {key.dag_id}.{key.task_id} "
                        f"(run_id={key.run_id}, try={key.try_number}) "
                        f"as Slurm job {job_id} in state {state}"
                    )
                else:
                    # Job already finished - don't adopt
                    self.log.info(
                        f"Task {key.dag_id}.{key.task_id} job {job_id} "
                        f"already in terminal state {state}, not adopting"
                    )
        
        self.log.info(f"Successfully adopted {len(adopted)} of {len(tis)} tasks")
        return adopted
        
    except SlurmAPIError as e:
        self.log.error(f"Failed to query Slurm for task adoption: {e}")
        return []
    except Exception as e:
        self.log.error(f"Unexpected error during task adoption: {e}", exc_info=True)
        return []
```

**Update execute_async() to use new job name:**

```python
def _build_job_spec(self, key, command, queue, executor_config):
    # ... other code ...
    
    job_name = self._build_job_name(key)
    
    job_spec = {
        'name': job_name,  # Use structured name instead of simple concatenation
        # ... rest of spec
    }
    
    return job_spec
```

#### 3.6.4 Edge Cases and Limitations

**Name collisions:**
- Hash collisions are extremely unlikely (SHA256 with 8 chars = 2^32 possibilities)
- Within same DAG, run_ids are unique by design
- If collision somehow occurs, try_number provides additional differentiation

**Job name length limits:**
- Slurm typically allows 256 character job names
- Our implementation truncates long dag_id/task_id if needed
- Hash remains intact to preserve uniqueness

**Unknown command:**
- Adopted jobs have empty `command` in tracking dict
- Not needed for monitoring (only job_id matters)
- Prevents re-execution but doesn't affect success/failure detection

**Timing windows:**
- Small window between scheduler crash and restart where job might complete
- If job finishes before adoption scan, it won't be adopted (acceptable - sync will handle)
- If job finishes during adoption scan, handled gracefully (terminal state check)

**Multiple schedulers:**
- In multi-scheduler setups (HA), both schedulers see same Slurm jobs
- Airflow's DB coordination prevents double-adoption
- Job name uniqueness ensures correct scheduler adopts correct tasks

#### 3.6.5 Testing Task Adoption

**Unit test:**
```python
def test_adopt_task_instances(self):
    # Setup: Mock Slurm response with jobs
    self.executor.slurm_client.get_jobs.return_value = {
        'jobs': [
            {
                'job_id': 12345,
                'name': 'airflow-test_dag-test_task-a3f5b2c1-1',
                'job_state': 'RUNNING'
            }
        ]
    }
    
    # Create matching TI
    ti = TaskInstance(...)
    ti.dag_id = 'test_dag'
    ti.task_id = 'test_task'
    ti.run_id = 'manual__2024-01-01'  # Will hash to a3f5b2c1
    ti.try_number = 1
    
    # Attempt adoption
    adopted = self.executor.try_adopt_task_instances([ti])
    
    # Verify
    assert len(adopted) == 1
    assert ti.key in self.executor.running
    assert self.executor.running[ti.key]['slurm_job_id'] == 12345
```

**Integration test:**
```python
@pytest.mark.integration
def test_scheduler_restart_with_running_jobs(self):
    # Submit job
    executor1 = SlurmExecutor()
    executor1.start()
    executor1.execute_async(key, command)
    
    # Wait for job to start
    time.sleep(5)
    executor1.sync()
    assert key in executor1.running
    job_id = executor1.running[key]['slurm_job_id']
    
    # Simulate scheduler restart
    executor1.end()
    
    # New executor instance (scheduler restart)
    executor2 = SlurmExecutor()
    executor2.start()
    
    # Attempt adoption
    ti = create_ti_from_key(key)
    adopted = executor2.try_adopt_task_instances([ti])
    
    # Verify adoption
    assert len(adopted) == 1
    assert key in executor2.running
    assert executor2.running[key]['slurm_job_id'] == job_id
    
    # Verify monitoring continues
    executor2.sync()
    # Eventually job completes and executor detects it
```

### 3.7 Job Arrays for Short Task Optimization (Phase 4 Enhancement)

#### 3.7.1 Problem Statement

**Issue:** Slurm has per-job overhead:
- Job submission: ~100-500ms
- Scheduler evaluation: ~50-200ms  
- Job startup on compute node: ~500ms-2s

For tasks that run in seconds, this overhead becomes significant. A DAG with 100 tasks that each run 5 seconds could spend:
- Job overhead: 100 × 1s = 100s
- Actual work: 100 × 5s = 500s
- Efficiency: 500/(500+100) = 83%

**Solution:** Use Slurm job arrays to batch multiple tasks into a single job submission.

#### 3.7.2 Slurm Job Arrays Overview

**What they are:**
A single job submission that creates multiple job array elements, each running the same script with different parameters.

**Example:**
```bash
sbatch --array=0-99 task_script.sh
# Creates jobs: jobid_0, jobid_1, ... jobid_99
# Each gets environment variable: SLURM_ARRAY_TASK_ID
```

**Benefits:**
- Single API call submits N tasks
- Scheduler processes array as unit (faster)
- Shared startup overhead
- Better cluster utilization

#### 3.7.3 Design Approach

**Strategy:** Detect when multiple tasks can be batched and submit as job array.

**Batching criteria:**
- Tasks ready to submit at same time
- Similar resource requirements (CPU, memory, partition)
- Same container/environment
- Short estimated duration (configurable threshold)

**Implementation options:**

**Option A: Implicit batching (aggressive)**
- Executor automatically batches compatible tasks
- Transparent to users
- Risk: Unexpected behavior

**Option B: Explicit batching (conservative)**
- Users opt-in via executor_config flag
- More predictable
- Requires user awareness

**Recommendation:** Start with **Option B** (explicit) for safety.

#### 3.7.4 Implementation Design

**Configuration:**
```ini
[slurm]
# Enable job array support
enable_job_arrays = True

# Maximum tasks per array (Slurm typically limits to 1000-10000)
max_array_size = 1000

# Batch window: wait this long to accumulate tasks before submitting array
batch_window_seconds = 5

# Only batch tasks estimated shorter than this
batch_task_duration_threshold = 300  # 5 minutes
```

**Executor_config for opt-in:**
```python
@task(executor_config={
    'cpus_per_task': 2,
    'mem': '4G',
    'enable_batching': True,  # Opt into job arrays
    'estimated_duration': 60,  # Help executor decide if worth batching
})
def short_task(index: int):
    # Task that runs ~60 seconds
    pass
```

**Modified execute_async() with batching:**

```python
class SlurmExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        # ... existing init ...
        
        # Batching state
        self.pending_batch = {}  # {batch_key: [tasks]}
        self.batch_timers = {}   # {batch_key: timer_start}
        self.enable_job_arrays = False
        self.batch_window = 5
        self.max_array_size = 1000
    
    def execute_async(self, key, command, queue=None, executor_config=None):
        """Submit task, with optional batching"""
        
        # Check if batching enabled and task opts in
        config = executor_config or {}
        if (self.enable_job_arrays and 
            config.get('enable_batching', False) and
            self._should_batch(config)):
            
            # Add to batch instead of immediate submission
            self._add_to_batch(key, command, queue, config)
        else:
            # Immediate submission (existing behavior)
            self._submit_single_task(key, command, queue, config)
    
    def _should_batch(self, config):
        """Determine if task should be batched"""
        # Only batch if estimated short
        duration = config.get('estimated_duration', float('inf'))
        return duration <= self.batch_task_duration_threshold
    
    def _add_to_batch(self, key, command, queue, config):
        """Add task to pending batch"""
        # Create batch key from compatible parameters
        batch_key = self._make_batch_key(queue, config)
        
        if batch_key not in self.pending_batch:
            self.pending_batch[batch_key] = []
            self.batch_timers[batch_key] = time.time()
        
        self.pending_batch[batch_key].append({
            'key': key,
            'command': command,
            'config': config,
        })
        
        # Check if we should flush this batch
        if (len(self.pending_batch[batch_key]) >= self.max_array_size or
            time.time() - self.batch_timers[batch_key] >= self.batch_window):
            self._flush_batch(batch_key)
    
    def _make_batch_key(self, queue, config):
        """Create key for grouping compatible tasks"""
        # Tasks must match on these parameters to batch together
        return (
            queue,
            config.get('partition', self.default_partition),
            config.get('cpus_per_task', self.default_cpus),
            config.get('mem', self.default_mem),
            config.get('container', self.default_container),
            # Don't include time_limit - we'll use max of batch
        )
    
    def _flush_batch(self, batch_key):
        """Submit accumulated batch as job array"""
        tasks = self.pending_batch.pop(batch_key)
        self.batch_timers.pop(batch_key)
        
        if not tasks:
            return
        
        self.log.info(f"Submitting batch of {len(tasks)} tasks as job array")
        
        # Submit job array
        array_size = len(tasks)
        job_spec = self._build_array_job_spec(tasks, batch_key)
        
        try:
            response = self.slurm_client.submit_job_array(job_spec, array_size)
            base_job_id = response['job_id']
            
            # Track each array element
            for idx, task in enumerate(tasks):
                array_job_id = f"{base_job_id}_{idx}"
                self.running[task['key']] = {
                    'slurm_job_id': array_job_id,
                    'command': task['command'],
                    'submit_time': datetime.now(),
                    'is_array_job': True,
                    'array_base_id': base_job_id,
                    'array_index': idx,
                }
            
            self.log.info(f"Submitted job array {base_job_id} with {array_size} elements")
            
        except Exception as e:
            self.log.error(f"Failed to submit job array: {e}")
            # Fallback: submit individually
            for task in tasks:
                self._submit_single_task(task['key'], task['command'], None, task['config'])
    
    def sync(self):
        """Enhanced sync to handle batch timers"""
        # Check for batches that have waited long enough
        current_time = time.time()
        batches_to_flush = []
        
        for batch_key, start_time in list(self.batch_timers.items()):
            if current_time - start_time >= self.batch_window:
                batches_to_flush.append(batch_key)
        
        for batch_key in batches_to_flush:
            self._flush_batch(batch_key)
        
        # Regular sync
        super().sync()  # Calls parent's sync logic
```

**Build array job spec:**

```python
def _build_array_job_spec(self, tasks, batch_key):
    """
    Build job spec for array job
    
    Each array element runs a wrapper script that:
    1. Reads SLURM_ARRAY_TASK_ID
    2. Looks up corresponding task from manifest
    3. Executes that task's command
    """
    queue, partition, cpus, mem, container = batch_key
    
    # Create task manifest (maps array index -> command)
    manifest = {}
    for idx, task in enumerate(tasks):
        manifest[idx] = {
            'command': task['command'],
            'key': str(task['key']),  # For logging
        }
    
    # Write manifest to shared filesystem
    manifest_path = self._write_manifest(manifest)
    
    # Build wrapper script
    script = self._build_array_wrapper_script(manifest_path)
    
    # Determine max time limit from all tasks
    max_time_limit = self.default_time_limit
    for task in tasks:
        task_limit = task['config'].get('time_limit', self.default_time_limit)
        if self._time_limit_to_seconds(task_limit) > self._time_limit_to_seconds(max_time_limit):
            max_time_limit = task_limit
    
    job_spec = {
        'script': script,
        'job': {
            'name': f'airflow-array-{int(time.time())}',
            'partition': partition,
            'cpus_per_task': cpus,
            'memory_per_node': mem,
            'time_limit': max_time_limit,
            'current_working_directory': self.airflow_home,
            'environment': self._build_environment(None),  # Common env
            'container': container if container else None,
        }
    }
    
    return job_spec

def _write_manifest(self, manifest):
    """Write manifest file for array job"""
    manifest_dir = os.path.join(self.airflow_home, 'slurm_manifests')
    os.makedirs(manifest_dir, exist_ok=True)
    
    manifest_path = os.path.join(manifest_dir, f'manifest_{int(time.time())}_{os.getpid()}.json')
    
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f)
    
    return manifest_path

def _build_array_wrapper_script(self, manifest_path):
    """
    Build script that reads manifest and executes appropriate command
    """
    script = f'''#!/bin/bash
set -euo pipefail

# Get array task ID
TASK_ID=$SLURM_ARRAY_TASK_ID

# Read manifest
MANIFEST_PATH="{manifest_path}"

# Extract command for this task ID
COMMAND=$(python3 -c "
import json
import sys
with open('$MANIFEST_PATH') as f:
    manifest = json.load(f)
    task = manifest.get(str($TASK_ID))
    if task:
        print(' '.join(task['command']))
    else:
        sys.exit(1)
")

# Determine log path from task key
TASK_KEY=$(python3 -c "
import json
with open('$MANIFEST_PATH') as f:
    manifest = json.load(f)
    print(manifest[str($TASK_ID)]['key'])
")

# Execute command (output goes to individual log per array element)
echo "Executing task $TASK_KEY as array element $TASK_ID"
eval $COMMAND
'''
    
    if self.airflow_venv:
        script = f'''#!/bin/bash
set -euo pipefail

# Activate venv
source {self.airflow_venv}/bin/activate

{script}
'''
    
    return script
```

**API client support for arrays:**

```python
class SlurmAPIClient:
    def submit_job_array(self, job_spec: dict, array_size: int) -> dict:
        """
        Submit job array
        
        Args:
            job_spec: Job specification
            array_size: Number of array elements
            
        Returns:
            {'job_id': base_job_id}
        """
        # Add array specification to job
        job_spec['job']['array'] = f'0-{array_size-1}'
        
        # Submit via normal endpoint
        return self.submit_job(job_spec)
```

**Enhanced status querying:**

```python
def _fetch_job_statuses(self):
    """Enhanced to handle array jobs"""
    try:
        response = self.slurm_client.get_jobs()
        
        statuses = {}
        for job in response.get('jobs', []):
            job_id = job['job_id']
            
            # Handle array jobs: job_id might be "12345_3" (array element)
            if '_' in str(job_id):
                # Array job element
                array_job_id = str(job_id)
            else:
                # Regular job
                array_job_id = job_id
            
            # Check if this is one of our tracked jobs
            for key, info in self.running.items():
                if info['slurm_job_id'] == array_job_id:
                    statuses[array_job_id] = {
                        'state': job['job_state'],
                        'exit_code': job.get('exit_code', 0),
                        'reason': job.get('state_reason', ''),
                    }
                    break
        
        return statuses
        
    except SlurmAPIError as e:
        self.log.warning(f"Failed to fetch job statuses: {e}")
        return {}
```

#### 3.7.5 Logging for Array Jobs

**Challenge:** Each array element needs its own log file.

**Solution:** Use Slurm's array-aware output path patterns or redirect in wrapper script.

**In wrapper script:**
```python
# In wrapper script
LOG_PATH=$(python3 -c "
import json
with open('$MANIFEST_PATH') as f:
    manifest = json.load(f)
    task = manifest[str($TASK_ID)]
    # Compute log path from task key
    print(compute_log_path(task['key']))
")

# Redirect output
exec > "$LOG_PATH" 2>&1

# Execute command
eval $COMMAND
```

#### 3.7.6 Benefits and Trade-offs

**Benefits:**
- **Reduced overhead:** Single API call for N tasks
- **Faster scheduling:** Slurm scheduler processes array as unit
- **Better throughput:** For workloads with many small tasks, 2-5x speedup possible

**Trade-offs:**
- **Complexity:** More complex code, harder to debug
- **Batching delay:** Tasks wait up to `batch_window` seconds before submission
- **All-or-nothing:** If array job fails to submit, all tasks in batch affected
- **Resource uniformity:** All tasks in array must have same resources

**When to use:**
- DAGs with 50+ similar short tasks (< 5 min each)
- High-throughput data processing pipelines
- Parameter sweeps, batch inference

**When not to use:**
- Long-running tasks (overhead already small %)
- Tasks with varied resource needs
- Critical path tasks (latency-sensitive)

#### 3.7.7 Configuration Example

**Example DAG using batching:**

```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    'batch_processing',
    start_date=datetime(2024, 1, 1),
    schedule=None,
) as dag:
    
    # Generate 100 similar short tasks
    @task(executor_config={
        'cpus_per_task': 1,
        'mem': '2G',
        'enable_batching': True,
        'estimated_duration': 30,  # 30 seconds each
    })
    def process_chunk(chunk_id: int):
        # Process one chunk of data
        import time
        print(f"Processing chunk {chunk_id}")
        time.sleep(30)
        return f"chunk_{chunk_id}_done"
    
    # This will create 100 tasks that get batched into job arrays
    chunks = [process_chunk(i) for i in range(100)]
```

**Expected behavior:**
- Without batching: 100 separate Slurm jobs, ~100s overhead + 3000s work = 3100s total
- With batching: ~10 job arrays of 10 tasks each (limited by batch window), ~10s overhead + 3000s work = 3010s total
- Actual batching depends on task submission timing and batch_window

#### 3.7.8 Alternative: Dynamic Task Mapping Integration

**Airflow 2.3+** introduced dynamic task mapping. This could integrate naturally:

```python
@task
def generate_chunks():
    return list(range(100))

@task(executor_config={'enable_batching': True})
def process_chunk(chunk_id: int):
    # Process chunk
    pass

# Mapped tasks are perfect for batching
chunks = generate_chunks()
process_chunk.expand(chunk_id=chunks)
```

**Executor could detect mapped tasks and automatically batch them** since:
- They're all from same task definition (same resources)
- They're submitted at once
- They're independent

This could be even more transparent than explicit opt-in.

---

## 4. Configuration

### 4.1 Configuration File (airflow.cfg)

```ini
[slurm]
# ========================================
# Slurm REST API Configuration
# ========================================

# Slurm REST API endpoint (slurmrestd)
# Required. Must include protocol (http:// or https://)
api_url = https://slurm-head.example.com:6820

# Slurm username for job submission
# Optional. Defaults to current system user.
# Must have permission to submit jobs and access REST API
username = 

# JWT token lifespan in seconds
# Default: 3600 (1 hour)
token_lifespan = 3600

# ========================================
# Default Resource Allocations
# ========================================

# Default Slurm partition for job submission
# Default: compute
default_partition = compute

# Default CPU cores per task
# Default: 1
default_cpus = 1

# Default memory allocation
# Format: <number><unit> where unit is K, M, G, T
# Default: 4G
default_mem = 4G

# Default wall time limit
# Format: HH:MM:SS
# Default: 01:00:00 (1 hour)
default_time_limit = 01:00:00

# Default Slurm account
# Optional. If not set, uses user's default account
default_account = 

# ========================================
# Execution Environment
# ========================================

# Path to Python virtual environment
# Optional. Used when not running in containers.
# If set, jobs will activate this venv before running tasks
airflow_venv = 

# Default container image for job execution
# Optional. Format: docker://registry/image:tag
# If set, jobs will run inside this container (requires Slurm container support)
# Takes precedence over airflow_venv
default_container = 

# ========================================
# Executor Behavior
# ========================================

# Minimum interval between Slurm status polls (seconds)
# Default: 10
# Higher values reduce API load but increase task state update latency
sync_interval = 10

# Shutdown behavior when executor is stopping
# Options: cancel, wait
# - cancel: Immediately cancel all running jobs
# - wait: Wait for jobs to finish (with timeout)
# Default: cancel
shutdown_mode = cancel

# Timeout for waiting during shutdown (seconds)
# Only used if shutdown_mode = wait
# After timeout, remaining jobs are cancelled
# Default: 300 (5 minutes)
shutdown_wait_timeout = 300

# ========================================
# API Client Configuration
# ========================================

# Timeout for API requests (seconds)
# Default: 30
api_timeout = 30

# Maximum retry attempts for failed API requests
# Default: 3
api_max_retries = 3
```

### 4.2 Executor Configuration in Tasks

**Per-task overrides using executor_config:**

```python
from airflow.decorators import task

@task(executor_config={
    'partition': 'gpu',
    'cpus_per_task': 8,
    'mem': '32G',
    'time_limit': '04:00:00',
    'qos': 'high',
    'container': 'docker://myregistry/ml-env:latest'
})
def train_model():
    # This task will run on GPU partition with 8 CPUs and 32GB RAM
    pass

@task(executor_config={
    'partition': 'compute',
    'cpus_per_task': 2,
    'mem': '8G',
})
def preprocess_data():
    # This task uses different resources
    pass
```

**Supported executor_config keys:**
- `partition` (str): Slurm partition name
- `cpus_per_task` (int): Number of CPU cores
- `mem` (str): Memory allocation (e.g., '16G', '32768M')
- `time_limit` (str): Wall time limit (HH:MM:SS format)
- `account` (str): Slurm account
- `qos` (str): Quality of Service
- `container` (str): Container image (docker://...)
- `working_dir` (str): Working directory override

### 4.3 Environment Variables

**Alternative to airflow.cfg (useful for containerized deployments):**

```bash
# Required
AIRFLOW__SLURM__API_URL=https://slurm-head.example.com:6820

# Optional
AIRFLOW__SLURM__USERNAME=airflow_user
AIRFLOW__SLURM__TOKEN_LIFESPAN=3600
AIRFLOW__SLURM__DEFAULT_PARTITION=compute
AIRFLOW__SLURM__DEFAULT_CPUS=1
AIRFLOW__SLURM__DEFAULT_MEM=4G
AIRFLOW__SLURM__DEFAULT_TIME_LIMIT=01:00:00
AIRFLOW__SLURM__AIRFLOW_VENV=/shared/airflow/venv
AIRFLOW__SLURM__DEFAULT_CONTAINER=docker://registry/airflow:2.8.0
AIRFLOW__SLURM__SYNC_INTERVAL=10
AIRFLOW__SLURM__SHUTDOWN_MODE=cancel
```

---

## 5. Error Handling

### 5.1 API Errors

**Retry strategy:**
```python
def _request(self, method: str, url: str, **kwargs) -> requests.Response:
    """Make HTTP request with exponential backoff retry"""
    for attempt in range(self.max_retries):
        try:
            response = self.session.request(method, url, **kwargs)
            
            # Success
            if response.status_code < 400:
                return response
            
            # Authentication failure - refresh token and retry
            if response.status_code == 401:
                self.token_manager.token = None
                if attempt < self.max_retries - 1:
                    continue
            
            # Other errors - don't retry
            raise SlurmAPIError(f"{method} {url} failed: {response.text}")
            
        except requests.RequestException as e:
            if attempt == self.max_retries - 1:
                raise SlurmAPIError(f"Request failed after {self.max_retries} attempts: {e}")
            
            # Exponential backoff: 2^attempt seconds
            time.sleep(2 ** attempt)
```

### 5.2 Token Generation Failures

**Handling:**
1. Log error with details
2. Retry with exponential backoff
3. After max retries, raise exception (executor will fail)

**Common causes:**
- `scontrol` binary not in PATH
- User doesn't have permission to generate tokens
- Slurm controller unreachable

### 5.3 Job Submission Failures

**Possible failures:**
- API unreachable
- Invalid job specification
- User over quota
- Partition doesn't exist
- Insufficient resources

**Handling:**
```python
def execute_async(self, key, command, queue=None, executor_config=None):
    try:
        job_spec = self._build_job_spec(key, command, queue, executor_config)
        job_id = self._submit_job(job_spec)
        
        if job_id:
            self.running[key] = {...}
        else:
            # Submission returned None - treat as failure
            self.fail(key)
            
    except Exception as e:
        self.log.error(f"Failed to submit task {key}: {e}")
        self.fail(key)  # Airflow will retry based on task retry settings
```

### 5.4 State Synchronization Failures

**If sync() fails (API down):**
- Log warning
- Return without updating state
- Tasks remain in current state
- Will retry on next sync() call
- Tasks eventually timeout if Slurm stays down

**If individual job disappears unexpectedly:**
- Check accounting database
- Track how long it's been missing
- After 5 minutes, mark as failed

### 5.5 Shutdown Failures

**If jobs can't be cancelled during shutdown:**
- Log errors but continue
- Don't block shutdown
- Jobs will continue running on Slurm
- Orphaned jobs can be cleaned up manually with `scancel`

---

## 6. Testing Strategy

### 6.1 Unit Tests

**Mock components:**
- Mock `SlurmAPIClient` responses
- Mock `subprocess.run` for token generation
- Test state transitions
- Test configuration loading
- Test error handling paths

**Example test structure:**
```python
class TestSlurmExecutor(unittest.TestCase):
    def setUp(self):
        self.executor = SlurmExecutor()
        self.executor.slurm_client = MagicMock()
    
    def test_submit_job_success(self):
        # Mock API response
        self.executor.slurm_client.submit_job.return_value = {'job_id': 12345}
        
        # Execute task
        key = TaskInstanceKey(...)
        self.executor.execute_async(key, ['airflow', 'tasks', 'run', ...])
        
        # Verify job tracked
        self.assertIn(key, self.executor.running)
        self.assertEqual(self.executor.running[key]['slurm_job_id'], 12345)
    
    def test_sync_completed_job(self):
        # Setup: job is running
        key = TaskInstanceKey(...)
        self.executor.running[key] = {'slurm_job_id': 12345, ...}
        
        # Mock API: job completed successfully
        self.executor.slurm_client.get_jobs.return_value = {
            'jobs': [{'job_id': 12345, 'job_state': 'COMPLETED', 'exit_code': 0}]
        }
        
        # Sync
        self.executor.sync()
        
        # Verify task marked as success
        self.assertNotIn(key, self.executor.running)
        # Check success() was called
```

### 6.2 Integration Tests

**Requirements:**
- Access to Slurm cluster with REST API
- Or Docker container running Slurm (e.g., slurm-docker-cluster)

**Test scenarios:**
1. Submit simple job, wait for completion
2. Submit job that fails (exit code 1)
3. Submit job that times out
4. Cancel running job
5. Executor shutdown with running jobs
6. Token refresh during long-running test
7. API unavailable during sync

**Example integration test:**
```python
@pytest.mark.integration
class TestSlurmExecutorIntegration:
    @pytest.fixture
    def executor(self):
        executor = SlurmExecutor()
        executor.start()
        yield executor
        executor.end()
    
    def test_simple_job_execution(self, executor):
        # Submit job
        key = TaskInstanceKey(...)
        executor.execute_async(key, ['echo', 'hello'])
        
        # Wait for completion
        timeout = 60
        start = time.time()
        while key in executor.running and time.time() - start < timeout:
            executor.sync()
            time.sleep(2)
        
        # Verify success
        assert key not in executor.running
        # Check logs contain 'hello'
```

### 6.3 Manual Testing Checklist

- [ ] Install executor in Airflow environment
- [ ] Configure airflow.cfg with test cluster
- [ ] Create simple DAG with 1 task
- [ ] Run DAG, verify task executes on Slurm
- [ ] Check logs appear in Airflow UI
- [ ] Create DAG with parallel tasks
- [ ] Verify tasks run concurrently on Slurm
- [ ] Test task failure (exit code 1)
- [ ] Test task retry after failure
- [ ] Test executor_config overrides
- [ ] Test container mode
- [ ] Test venv mode
- [ ] Test scheduler restart with running jobs
- [ ] Test graceful shutdown (cancel mode)
- [ ] Test graceful shutdown (wait mode)

---

## 7. Implementation Roadmap

### 7.1 Phase 1: Core Implementation (MVP)

**Deliverables:**
- [ ] Project structure and packaging setup
- [ ] `SlurmTokenManager` class
- [ ] `SlurmAPIClient` class (submit, query, cancel)
- [ ] `SlurmExecutor` class (execute_async, sync, end, terminate)
- [ ] Task adoption support (try_adopt_task_instances)
- [ ] Configuration loading
- [ ] Basic error handling
- [ ] Unit tests

**Estimated effort:** 2-3 weeks

### 7.2 Phase 2: Testing and Documentation

**Deliverables:**
- [ ] Integration tests
- [ ] README with installation instructions
- [ ] Configuration guide
- [ ] Usage examples (example DAGs)
- [ ] Troubleshooting guide
- [ ] API documentation (docstrings)

**Estimated effort:** 1-2 weeks

### 7.3 Phase 3: Refinement and Release

**Deliverables:**
- [ ] Handle edge cases from testing
- [ ] Performance optimization (batching, caching)
- [ ] Logging improvements
- [ ] PyPI package publishing
- [ ] GitHub repository setup
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Contribution guidelines

**Estimated effort:** 1-2 weeks

### 7.4 Phase 4: Advanced Features (Post-MVP)

**Optional enhancements:**
- [ ] Job arrays for short task optimization
- [ ] Support for older API versions (v0.0.40)
- [ ] Advanced Slurm features (GRES, licenses)
- [ ] Metrics and monitoring
- [ ] Alternative auth methods (beyond scontrol token)
- [ ] S3 log backend (for non-shared-filesystem setups)

---

## 8. Package Structure

```
airflow-slurm-executor/
├── README.md
├── LICENSE
├── setup.py
├── requirements.txt
├── requirements-dev.txt
├── .gitignore
├── .github/
│   └── workflows/
│       ├── tests.yml
│       └── publish.yml
├── airflow_provider_slurm/
│   ├── __init__.py
│   ├── slurm_executor.py          # Main SlurmExecutor class
│   ├── slurm_api_client.py        # SlurmAPIClient class
│   ├── slurm_token_manager.py     # SlurmTokenManager class
│   ├── exceptions.py              # Custom exceptions
│   └── version.py                 # Version info
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── test_executor.py
│   │   ├── test_api_client.py
│   │   └── test_token_manager.py
│   ├── integration/
│   │   └── test_executor_integration.py
│   └── fixtures/
│       └── mock_responses.py
├── examples/
│   ├── simple_dag.py
│   ├── parallel_tasks_dag.py
│   └── ml_pipeline_dag.py
└── docs/
    ├── installation.md
    ├── configuration.md
    ├── troubleshooting.md
    └── development.md
```

---

## 9. Dependencies

### 9.1 Runtime Dependencies

```
apache-airflow>=2.5.0
requests>=2.28.0
```

### 9.2 Development Dependencies

```
pytest>=7.0.0
pytest-mock>=3.10.0
black>=22.0.0
isort>=5.10.0
mypy>=0.990
flake8>=5.0.0
```

### 9.3 System Dependencies

- Python 3.8+
- Access to Slurm cluster with REST API (slurmrestd) v0.0.42+
- `scontrol` binary in PATH (for token generation)
- Shared filesystem between scheduler and compute nodes (for logs)

---

## 10. API Reference

### 10.1 SlurmExecutor

**Public Methods:**
```python
class SlurmExecutor(BaseExecutor):
    def start() -> None:
        """Initialize executor and validate configuration"""
    
    def execute_async(
        key: TaskInstanceKey,
        command: List[str],
        queue: Optional[str] = None,
        executor_config: Optional[dict] = None
    ) -> None:
        """Submit task to Slurm"""
    
    def sync() -> None:
        """Poll Slurm for job status updates"""
    
    def end() -> None:
        """Gracefully shutdown executor"""
    
    def terminate() -> None:
        """Emergency shutdown"""
    
    def try_adopt_task_instances(
        tis: List[TaskInstance]
    ) -> List[TaskInstance]:
        """Adopt running tasks after scheduler restart"""
```

### 10.2 SlurmAPIClient

```python
class SlurmAPIClient:
    def __init__(
        base_url: str,
        token_manager: SlurmTokenManager,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """Initialize API client"""
    
    def submit_job(job_spec: dict) -> dict:
        """Submit job to Slurm
        
        Returns: {'job_id': int, ...}
        Raises: SlurmAPIError
        """
    
    def get_jobs(job_ids: Optional[List[int]] = None) -> dict:
        """Query active jobs
        
        Returns: {'jobs': [{'job_id': int, 'job_state': str, ...}]}
        Raises: SlurmAPIError
        """
    
    def get_job_history(job_id: int) -> Optional[dict]:
        """Query accounting database for completed job
        
        Returns: Job info dict or None if not found
        Raises: SlurmAPIError
        """
    
    def cancel_job(job_id: int) -> Optional[dict]:
        """Cancel a job
        
        Returns: Response dict or None if job doesn't exist
        Raises: SlurmAPIError
        """
    
    def get_api_version() -> str:
        """Get Slurm REST API version"""
```

### 10.3 SlurmTokenManager

```python
class SlurmTokenManager:
    def __init__(
        username: Optional[str] = None,
        lifespan: int = 3600
    ):
        """Initialize token manager"""
    
    def get_token() -> str:
        """Get valid JWT token (refresh if needed)
        
        Returns: JWT token string
        Raises: SlurmTokenError
        """
```

---

## 11. Troubleshooting Guide

### 11.1 Common Issues

**Issue: "Failed to connect to Slurm API"**
- Verify `api_url` is correct
- Check slurmrestd is running: `systemctl status slurmrestd`
- Test connectivity: `curl -k https://slurm-head:6820/openapi/v3`
- Check firewall rules

**Issue: "Token generation failed"**
- Verify `scontrol` is in PATH: `which scontrol`
- Check user has permission: `scontrol token lifespan=60`
- Verify Slurm is running: `scontrol ping`

**Issue: "Jobs not appearing in Airflow logs"**
- Verify log path is on shared filesystem
- Check file permissions on log directory
- Verify compute nodes can write to log path
- Check Slurm job output: `cat slurm-12345.out`

**Issue: "Tasks stuck in running state"**
- Check if jobs are actually running: `squeue -u $USER`
- Check scheduler logs for sync errors
- Verify API is responding: `curl -k https://slurm-head:6820/slurm/v0.0.42/jobs`
- Increase `sync_interval` if API is slow

**Issue: "Container not found"**
- Verify Slurm has container support: `scontrol show config | grep PluginDir`
- Check container image is accessible from compute nodes
- Test manually: `srun --container=docker://image:tag /bin/bash`

### 11.2 Debugging Tips

**Enable debug logging:**
```python
import logging
logging.getLogger('airflow_provider_slurm').setLevel(logging.DEBUG)
```

**Check Slurm job details:**
```bash
scontrol show job 12345
sacct -j 12345 -o JobID,JobName,State,ExitCode,Reason
```

**Inspect job script:**
```bash
scontrol write batch_script 12345 /tmp/job_script.sh
cat /tmp/job_script.sh
```

**Test job submission manually:**
```bash
# Create test script
cat > test_job.sh << 'EOF'
#!/bin/bash
echo "Hello from Slurm"
sleep 10
echo "Done"
EOF

# Submit
sbatch --partition=compute --output=/tmp/test.log test_job.sh

# Monitor
squeue
tail -f /tmp/test.log
```

---

## 12. Success Criteria

### 12.1 Functional Requirements

- [x] Submit Airflow tasks as Slurm jobs via REST API ✓
- [x] Monitor job status and update Airflow task states ✓
- [x] Stream logs to Airflow UI ✓
- [x] Support resource customization per task ✓
- [x] Handle failures and retries ✓
- [x] Graceful shutdown ✓
- [x] Task adoption after scheduler restart ✓

### 12.2 Non-Functional Requirements

- **Performance:** Sync interval of 10-30 seconds acceptable
- **Reliability:** Handle API failures without losing task state
- **Scalability:** Support hundreds of concurrent jobs
- **Usability:** Simple configuration, clear error messages
- **Maintainability:** Clean code, well documented, tested

### 12.3 Documentation Requirements

- Installation guide
- Configuration reference
- Usage examples
- API documentation
- Troubleshooting guide
- Development/contribution guide

---

## Appendices

### A. Slurm REST API Endpoints Used

**Base URL:** `https://slurm-head:6820/slurm/v0.0.42`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/job/submit` | POST | Submit new job |
| `/jobs` | GET | List active jobs |
| `/job/{job_id}` | GET | Get job details (including accounting) |
| `/job/{job_id}` | DELETE | Cancel job |
| `/openapi/v3` | GET | Get API version info |

### B. Slurm Job State Reference

| State | Description | Terminal? |
|-------|-------------|-----------|
| PENDING | Job waiting in queue | No |
| CONFIGURING | Job being configured | No |
| RUNNING | Job executing | No |
| COMPLETED | Job finished | Yes |
| FAILED | Job failed | Yes |
| TIMEOUT | Job exceeded time limit | Yes |
| CANCELLED | Job was cancelled | Yes |
| NODE_FAIL | Node failure | Yes |
| PREEMPTED | Job preempted by higher priority | Yes |
| OUT_OF_MEMORY | Job killed for OOM | Yes |

### C. Example DAGs

**Simple DAG:**
```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    'simple_slurm_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    @task
    def hello_slurm():
        print("Hello from Slurm!")
        return "success"
    
    hello_slurm()
```

**Resource-intensive DAG:**
```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    'ml_training_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    @task(executor_config={
        'partition': 'gpu',
        'cpus_per_task': 8,
        'mem': '64G',
        'time_limit': '08:00:00',
        'container': 'docker://registry/ml-training:latest'
    })
    def train_model():
        import torch
        # Training code here
        return "model_trained"
    
    @task(executor_config={
        'partition': 'compute',
        'cpus_per_task': 4,
        'mem': '16G',
    })
    def evaluate_model():
        # Evaluation code
        return "evaluation_complete"
    
    train_model() >> evaluate_model()
```

### D. Known Limitations

**Shared Filesystem Requirement:**
- Logs require shared filesystem between scheduler, compute nodes, and web server
- Alternative S3 backend deferred to Phase 4

**Task Granularity:**
- Optimized for tasks running minutes or longer
- High per-job overhead (~1-2s) makes it less efficient for second-long tasks
- Job arrays (Phase 4) will address this for batch workloads

**No Task-Level Parallelism:**
- Each Airflow task maps to one Slurm job
- Multiple parallel executions of same task not supported
- DAG-level parallelism (different tasks) works normally

**API Version Support:**
- Only supports Slurm REST API v0.0.42+
- Earlier versions have different API contracts
- Requires Slurm 24.05 or newer

### E. Comparison with Other Executors

| Feature | Slurm | Kubernetes | Celery | Dask |
|---------|-------|------------|--------|------|
| Infrastructure | HPC cluster | K8s cluster | Message queue | Dask cluster |
| Resource guarantees | ✓ (native) | ✓ (requests/limits) | ✗ | ✗ |
| Shared FS required | ✓ (for logs) | ✗ | ✗ | ✗ |
| Container support | ✓ (optional) | ✓ (native) | Depends | Depends |
| Task adoption | ✓ | ✓ | ✓ | Limited |
| Queue management | ✓ (partitions) | ✓ (namespaces) | ✓ (queues) | ✗ |
| Best for | HPC workloads | Cloud-native | Distributed apps | Data science |

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-12-10 | Initial specification |

---

**End of Specification**
