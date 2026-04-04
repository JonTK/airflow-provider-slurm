# Architecture

This document describes the architecture and design of the Airflow Slurm Provider.

## Overview

The Airflow Slurm Provider enables Apache Airflow to execute tasks on Slurm HPC clusters via the Slurm REST API. It consists of an executor, hooks, and supporting components that integrate seamlessly with Airflow's task execution framework.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Airflow Scheduler                         │
│                                                                   │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                │
│  │    DAG     │  │    DAG     │  │    DAG     │                │
│  │  Parser    │  │  Executor  │  │  Executor  │                │
│  └────────────┘  └────────────┘  └────────────┘                │
│         │               │               │                        │
│         └───────────────┼───────────────┘                        │
│                         │                                        │
│                         ▼                                        │
│              ┌─────────────────────┐                            │
│              │   SlurmExecutor     │                            │
│              │                     │                            │
│              │ • execute_async()   │                            │
│              │ • sync()            │                            │
│              │ • try_adopt()       │                            │
│              └─────────────────────┘                            │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       │ JWT Token
                       │ Job Submission
                       │ Status Queries
                       ▼
        ┌──────────────────────────────┐
        │   Slurm REST API (slurmrestd) │
        │                                │
        │  • Job Submission              │
        │  • Job Status                  │
        │  • Job Cancellation            │
        │  • Job History                 │
        └───────────────┬────────────────┘
                        │
                        │ sbatch commands
                        │ squeue queries
                        ▼
        ┌──────────────────────────────┐
        │      Slurm Controller         │
        │         (slurmctld)            │
        └───────────────┬────────────────┘
                        │
                        │ Job scheduling
                        │ Resource allocation
                        ▼
        ┌──────────────────────────────┐
        │      Compute Nodes            │
        │                                │
        │  ┌────────┐  ┌────────┐      │
        │  │ Task 1 │  │ Task 2 │  ... │
        │  └────────┘  └────────┘      │
        │       │           │           │
        │       └───────────┘           │
        │              │                │
        │              ▼                │
        │     ┌─────────────────┐      │
        │     │ Shared Filesystem│      │
        │     │   (NFS/Lustre)   │      │
        │     │                  │      │
        │     │ • Task Logs      │      │
        │     │ • Airflow State  │      │
        │     └─────────────────┘      │
        └──────────────────────────────┘
```

## Core Components

### 1. SlurmExecutor

The main executor class that implements Airflow's `BaseExecutor` interface.

**Responsibilities:**
- Task submission to Slurm as batch jobs
- Periodic job status synchronization
- Task state management (success, failure, retry)
- Job adoption after scheduler restart
- Graceful shutdown and cleanup

**Key Methods:**
- `start()`: Initialize executor and validate configuration
- `execute_async()`: Submit a task to Slurm
- `sync()`: Poll job status and update Airflow task states
- `end()`: Graceful shutdown with configurable behavior
- `try_adopt_task_instances()`: Recover running jobs after restart

### 2. SlurmAPIClient

HTTP client for interacting with the Slurm REST API.

**Features:**
- Automatic JWT token management via SlurmTokenManager
- Retry logic with exponential backoff
- Error handling and response validation
- Support for multiple API versions (v0.0.40 - v0.0.44)

**Key Methods:**
- `submit_job()`: Submit job to Slurm
- `get_jobs()`: Query job status (single or batch)
- `get_job()`: Get specific job details
- `cancel_job()`: Cancel running job
- `get_job_history()`: Query completed job history
- `ping()`: Health check

### 3. SlurmTokenManager

Manages JWT authentication tokens for Slurm REST API.

**Features:**
- Automatic token generation using `scontrol token`
- Token caching and refresh
- Configurable token lifespan
- Thread-safe token operations

**Key Methods:**
- `get_token()`: Get valid token (cached or new)
- `invalidate()`: Force token refresh
- `_generate_token()`: Generate new token via scontrol

### 4. SlurmHook

Airflow hook for reusable Slurm operations in DAGs.

**Features:**
- Airflow connection support
- High-level job submission interface
- Job status monitoring
- Blocking wait operations

**Key Methods:**
- `submit_job()`: Submit job with high-level parameters
- `get_job_status()`: Get job state
- `cancel_job()`: Cancel job
- `wait_for_job()`: Block until job completes

### 5. Exception Classes

Custom exceptions for clear error handling:
- `SlurmExecutorException`: Base exception
- `SlurmAPIError`: API communication errors
- `SlurmConfigurationError`: Configuration issues
- `SlurmJobSubmissionError`: Job submission failures
- `SlurmTokenError`: Authentication issues
- `SlurmJobNotFoundError`: Job not found

## Task Execution Flow

### Normal Task Execution

```
┌─────────────┐
│   Airflow   │
│  Scheduler  │
└──────┬──────┘
       │
       │ 1. Task ready to run
       ▼
┌─────────────────┐
│ SlurmExecutor   │
│ execute_async() │
└──────┬──────────┘
       │
       │ 2. Build job specification
       │    • Script generation
       │    • Resource configuration
       │    • Log path setup
       ▼
┌─────────────────┐
│ _build_job_spec()│
└──────┬──────────┘
       │
       │ 3. Submit to Slurm REST API
       ▼
┌─────────────────┐
│ SlurmAPIClient  │
│  submit_job()   │
└──────┬──────────┘
       │
       │ 4. JWT Token + Job Spec
       ▼
┌─────────────────┐
│  Slurm REST API │
│  (slurmrestd)   │
└──────┬──────────┘
       │
       │ 5. Job ID returned
       ▼
┌─────────────────┐
│ SlurmExecutor   │
│ Track in        │
│ self.running    │
└──────┬──────────┘
       │
       │ 6. Periodic sync (every 10s)
       ▼
┌─────────────────┐
│ SlurmExecutor   │
│     sync()      │
└──────┬──────────┘
       │
       │ 7. Query job status
       ▼
┌─────────────────┐
│ SlurmAPIClient  │
│   get_jobs()    │
└──────┬──────────┘
       │
       │ 8. Job states returned
       ▼
┌─────────────────┐
│ SlurmExecutor   │
│ Update task     │
│ states          │
└──────┬──────────┘
       │
       │ 9. Notify Airflow
       │    • success()
       │    • fail()
       ▼
┌─────────────────┐
│    Airflow      │
│  Task Instance  │
│  State Updated  │
└─────────────────┘
```

### Job Adoption After Restart

When the Airflow scheduler restarts, it needs to recover running jobs:

```
┌─────────────────┐
│   Scheduler     │
│   Restart       │
└────────┬────────┘
         │
         │ 1. Find orphaned tasks
         ▼
┌─────────────────────────────┐
│ SlurmExecutor               │
│ try_adopt_task_instances()  │
└────────┬────────────────────┘
         │
         │ 2. Query all running jobs
         ▼
┌─────────────────┐
│ SlurmAPIClient  │
│   get_jobs()    │
└────────┬────────┘
         │
         │ 3. Match job names to task instances
         ▼
┌─────────────────┐
│ Job name format:│
│ airflow-        │
│ {dag}-{task}-   │
│ {hash}-{try}    │
└────────┬────────┘
         │
         │ 4. Adopt matched jobs
         ▼
┌─────────────────┐
│ Add to          │
│ self.running    │
│ Continue        │
│ monitoring      │
└─────────────────┘
```

## Configuration Flow

```
┌─────────────────────────────────────────────┐
│          Configuration Sources              │
│                                             │
│  1. airflow.cfg [slurm] section           │
│  2. Environment variables (AIRFLOW__SLURM__)│
│  3. Task executor_config                    │
│  4. Defaults in SlurmExecutor              │
└─────────────┬───────────────────────────────┘
              │
              │ Priority: Task > Env > Config > Defaults
              ▼
┌─────────────────────────────────────────────┐
│         Configuration Parameters            │
│                                             │
│  Global (Executor):                        │
│  • api_url                                 │
│  • default_partition                       │
│  • default_cpus                            │
│  • default_mem                             │
│  • default_time_limit                      │
│  • sync_interval                           │
│  • shutdown_mode                           │
│                                             │
│  Per-Task (executor_config):               │
│  • partition                               │
│  • cpus_per_task                           │
│  • mem                                     │
│  • time_limit                              │
│  • gres (GPU resources)                    │
│  • constraint (node features)              │
│  • qos                                     │
│  • account                                 │
└─────────────────────────────────────────────┘
```

## Resource Configuration

Tasks can specify resources via `executor_config`:

```python
@task(executor_config={
    'partition': 'gpu',
    'cpus_per_task': 8,
    'mem': '32G',
    'time_limit': '04:00:00',
    'gres': 'gpu:tesla_v100:4',
    'constraint': 'nvlink',
    'qos': 'high',
    'account': 'research-project',
})
def gpu_intensive_task():
    # Task implementation
    pass
```

These map to Slurm job parameters:

```bash
sbatch \
  --partition=gpu \
  --cpus-per-task=8 \
  --mem=32G \
  --time=04:00:00 \
  --gres=gpu:tesla_v100:4 \
  --constraint=nvlink \
  --qos=high \
  --account=research-project \
  script.sh
```

## State Management

### Job States

The executor tracks jobs through various states:

```
Task Created
     │
     ▼
┌──────────┐
│ PENDING  │ ← Submitted to Slurm, waiting for resources
└────┬─────┘
     │
     ▼
┌──────────┐
│ RUNNING  │ ← Executing on compute node
└────┬─────┘
     │
     ├─── Success ──→ ┌──────────┐
     │                │COMPLETED │ → success()
     │                └──────────┘
     │
     ├─── Failure ──→ ┌──────────┐
     │                │ FAILED   │ → fail()
     │                └──────────┘
     │
     ├─── Cancelled → ┌──────────┐
     │                │CANCELLED │ → fail()
     │                └──────────┘
     │
     └─── Timeout ──→ ┌──────────┐
                      │ TIMEOUT  │ → fail()
                      └──────────┘
```

### Missing Job Handling

If a job disappears from the queue (e.g., completed and cleaned up):

```
Job Missing in Queue
        │
        ▼
┌───────────────┐
│ Mark as       │
│ "missing"     │
│ with timestamp│
└───────┬───────┘
        │
        │ Wait 5 minutes
        ▼
┌───────────────┐
│ Check job     │
│ history API   │
└───────┬───────┘
        │
        ├─── Found ────→ Update state based on exit code
        │
        └─── Not Found → Mark as failed (job lost)
```

## Shared Filesystem Requirements

The executor requires a shared filesystem for:

1. **Task Logs**: Compute nodes write logs that scheduler reads
2. **Airflow Code**: Tasks need access to DAGs and plugins
3. **State Coordination**: Multiple scheduler processes

```
┌─────────────────────────────────────────────┐
│         Shared Filesystem Layout            │
│                                             │
│  $AIRFLOW_HOME/                            │
│  ├── dags/          (DAG definitions)      │
│  ├── logs/          (Task execution logs)  │
│  ├── plugins/       (Custom plugins)       │
│  └── airflow.cfg    (Configuration)        │
│                                             │
│  Accessible from:                          │
│  • Airflow scheduler                       │
│  • All Slurm compute nodes                 │
│                                             │
│  Common implementations:                    │
│  • NFS (Network File System)               │
│  • Lustre (Parallel filesystem)            │
│  • BeeGFS                                   │
│  • GPFS                                     │
└─────────────────────────────────────────────┘
```

## Security Architecture

### Authentication

```
┌─────────────────┐
│  SlurmExecutor  │
└────────┬────────┘
         │
         │ 1. Request token
         ▼
┌─────────────────┐
│ TokenManager    │
│                 │
│ Cache check     │
└────────┬────────┘
         │
         │ 2. Generate if needed
         ▼
┌─────────────────┐
│ Execute:        │
│ scontrol token  │
│ lifespan=3600   │
└────────┬────────┘
         │
         │ 3. JWT token
         ▼
┌─────────────────┐
│ Cache token     │
│ Set expiry      │
└────────┬────────┘
         │
         │ 4. Return token
         ▼
┌─────────────────┐
│ API Request     │
│ Header:         │
│ X-SLURM-USER-   │
│ TOKEN: <jwt>    │
└─────────────────┘
```

### Authorization

- Token generation requires user permissions in Slurm
- Job submission uses user's Slurm account and limits
- Resource requests subject to QoS policies
- Partition access controlled by Slurm ACLs

## Scalability Considerations

### Batch Status Queries

The executor batches job status queries to reduce API calls:

```
Instead of:
  get_job(123)
  get_job(124)
  get_job(125)
  ...

Executor does:
  get_jobs([123, 124, 125, ...])  # Single API call
```

### Sync Interval

Configurable polling interval (default 10s) balances:
- **Lower interval**: Faster task state updates, more API load
- **Higher interval**: Less API load, slower state updates

### Concurrent Job Limits

Respects Slurm's:
- Per-user job limits
- Partition limits
- QoS limits
- Account limits

## Error Handling

### Retry Strategy

```
API Request
    │
    ├─── Success ────────→ Return result
    │
    ├─── 401/403 ────────→ Refresh token, retry once
    │
    ├─── 5xx/Timeout ────→ Exponential backoff
    │    (Retries: 1, 2, 4, 8 seconds)
    │    └─ Max 3 attempts
    │
    └─── 4xx (other) ────→ Fail immediately (bad request)
```

### Job Submission Failures

```
submit_job() fails
    │
    ├─── Invalid partition ──→ Log error, mark task failed
    ├─── Insufficient resources → Log error, mark task failed
    ├─── Network error ─────────→ Retry with backoff
    └─── API unavailable ───────→ Retry with backoff
```

## Performance Characteristics

### Latency

- **Job Submission**: 100-500ms (depends on API)
- **Status Sync**: 50-200ms per batch
- **Token Generation**: 100-300ms (cached)

### Throughput

- **Jobs per second**: 10-50 (depends on API capacity)
- **Status queries**: 1000+ jobs per query
- **Concurrent jobs**: Limited by Slurm configuration

### Resource Usage

- **Memory**: ~50-100MB per executor instance
- **CPU**: Minimal (<1% during sync)
- **Network**: ~1KB per job submission, ~100 bytes per status check

## Design Decisions

### Why REST API vs. Direct CLI?

✅ **REST API Advantages:**
- No SSH required
- Better error handling
- Structured responses
- API versioning
- Scalable architecture

❌ **CLI Disadvantages:**
- SSH overhead
- Parsing text output
- Breaking changes in output format
- Error handling complexity

### Why Shared Filesystem?

Required for:
- Task log collection
- Airflow code access on compute nodes
- Standard HPC cluster architecture
- Compatibility with existing workflows

### Why JWT Tokens?

- Standard Slurm authentication method
- Temporary credentials (security)
- No password storage needed
- Automatic generation and refresh

## Future Enhancements

Potential improvements:

1. **Job Arrays**: Submit multiple tasks as Slurm job array
2. **Dependencies**: Native Slurm job dependencies
3. **Checkpointing**: Save/restore job state
4. **GPU Topology**: Awareness of GPU interconnects
5. **Metrics**: Prometheus/StatsD integration
6. **Batch Optimization**: Intelligent job batching
7. **Priority Management**: Dynamic priority adjustment
8. **Cost Tracking**: Resource usage and billing integration

## References

- [Slurm REST API Documentation](https://slurm.schedmd.com/rest_api.html)
- [Airflow Executor Interface](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
- [Slurm Job Submission](https://slurm.schedmd.com/sbatch.html)
