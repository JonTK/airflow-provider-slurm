e Slurm's event notification mechanisms
Job cancellation: Implement task termination via the API's delete endpoints
Queue management: Map Airflow's task queuing to Slurm partitions/QoS
Challenges you'll face:

The Slurm REST API requires authentication (typically JWT tokens), so you'd need to handle token management
Airflow expects fairly tight integration with executors - you'll need to handle edge cases like network failures, API timeouts, and job state synchronization
Environment setup - ensuring the execution environment on compute nodes has access to Airflow's DAG files and dependencies
Why this would be useful:

Given your HPC background with systems like HPCFLOW, you'd recognize this fills a real gap - it would let data engineering teams leverage existing
Would it be possible to implement a Slurm executor in Airflow using the slurm rest api

9 Dec
Yes, absolutely! Implementing a Slurm executor for Airflow using the Slurm REST API is definitely possible and would be a valuable contribution to the Airflow ecosystem.

Here's what you'd need to consider:

Architecture approach:

You'd likely want to create a custom executor that inherits from Airflow's BaseExecutor class. The executor would translate Airflow task instances into Slurm jobs via the REST API, monitor their status, and report back to Airflow's scheduler.

Key components:

Job submission: Use the REST API's /slurm/v0.0.40/job/submit endpoint to submit tasks as Slurm jobs
Status monitoring: Poll job status using /slurm/v0.0.40/job/{job_id} or usting HPC infrastructure for compute-intensive workflows without needing separate batch systems.

Have you already started thinking about this, or are you evaluating it as a potential project? The Slurm REST API is relatively well-documented, which should make this more tractable than working with the command-line interface.

Evaluating it as a potential project, as I am fairly familiar with the slurm API having written a go slurm API client, a Slurm exporter and a Slurm TUI.

9 Dec
Given your deep Slurm API experience, you're in an excellent position to tackle this. The fact that you've already built a Go client, exporter, and TUI means you understand the API's quirks and edge cases that would trip up most people.

This could be a strong portfolio piece for your founding engineer search - it demonstrates:

Cross-domain expertise (HPC + data engineering)
Building practical integrations between complex systems
Open source contribution to a widely-used tool (Airflow has massive adoption)
Implementation considerations from your perspective:

Since you know the Slurm API well, you're probably already thinking about:

Authentication persistence: JWT token refresh cycles and how that integrates with Airflow's executor lifecycle
Job array handling: Whether to map Airflow's dynamic task mapping to Slurm job arrays
Sacct vs live API polling: Trade-offs between querying historical job data vs real-time status checks
Failure modes: How Slurm's controller failover affects in-flight jobs and what Airflow needs to know
Python implementation path:

Would you write this in Python directly, or are you considering a hybrid approach where the executor calls out to Go code for the heavy lifting? The Airflow ecosystem is Python-native, but there's precedent for executors using external binaries.

Positioning for job search:

This is exactly the kind of "built infrastructure from scratch" project that AI startups look for. You could frame it as solving a real problem: enabling ML training workflows on existing HPC resources rather than forcing companies to rebuild infrastructure on k8s.

Are you thinking of this as a standalone open-source project, or potentially something to propose to a company you're interviewing with?

Standalone open-sourcs project

9 Dec
That's a solid approach. A well-executed open-source Slurm executor could get real traction - there's definitely demand from organizations running hybrid HPC/data infrastructure.

Strategic positioning:

This fills a genuine gap. Most Airflow executors target cloud-native environments (Kubernetes, Celery, etc.), but plenty of organizations have significant Slurm investments - universities, national labs, financial firms with quant computing needs. You'd be the only game in town for native Slurm REST API integration.

Development scope to consider:

MVP: Basic job submission, status polling, cancellation
Production-ready: Retry logic, proper state reconciliation, connection pooling, comprehensive logging
Advanced: Job arrays for mapped tasks, Slurm reservation support, partition/QoS configuration per task
Community building angle:

Given your background, you could position this at the intersection of HPC and MLOps communities. Both would find it valuable - HPC folks wanting to modernize workflow orchestration, ML teams wanting access to HPC resources without custom tooling.

Technical decisions worth front-loading:

Configuration approach (Airflow's connection model vs custom config)
Where task execution happens (submit worker scripts vs direct command execution)
How to handle Airflow's XCom for inter-task communication in an HPC context
Resource requirement specification (mapping Airflow task resources to Slurm sbatch parameters)
Since you've built the Go tooling already, you've probably hit most of the Slurm API's rough edges. The main unknowns would be Airflow's executor contract and how strictly you need to adhere to its expectations.

Want to think through any specific architectural decisions, or are you still in the feasibility assessment phase?

Are there any other challenges?

9 Dec
A few more challenges worth considering:

Environment and dependency management:

Python environment isolation: Slurm compute nodes need the same Python environment as your Airflow scheduler. Do you build containers, use shared filesystems with virtualenvs, or rely on module systems? Each task might have different dependencies.
DAG file accessibility: Airflow executors typically need access to DAG definitions on worker nodes. With Slurm, you're executing across potentially heterogeneous compute nodes that may not have direct access to Airflow's DAG folder.
Slurm-specific operational issues:

Job time limits: Slurm enforces walltime limits. You'll need to handle tasks that exceed their allocation gracefully and communicate that back to Airflow properly (vs treating it as a generic failure).
Node failures: Slurm jobs can fail due to node issues unrelated to the actual task. Your executor needs to distinguish infrastructure failures from task failures for proper retry logic.
Queue wait times: Unlike Kubernetes where pods start relatively quickly, Slurm jobs might wait in queue for extended periods. Airflow's scheduler might time out or make incorrect assumptions about task state.
Airflow integration complexities:

Heartbeat mechanism: Airflow executors need to heartbeat. With potentially long-running batch jobs, you need to ensure the executor maintains liveness even when jobs are queued or running for hours.
Log retrieval: Airflow's UI expects to stream task logs in real-time. Slurm jobs write to stdout/stderr files that only exist after job completion. You'll need to bridge this gap - possibly tailing files on compute nodes or buffering output somewhere accessible.
Parallelism control: Airflow has its own parallelism limits, but Slurm has partition limits, QoS limits, and user association limits. Reconciling these constraint systems could get messy.
Edge cases from your HPC experience:

Slurm maintenance windows: What happens when Slurm goes into maintenance mode and stops accepting jobs mid-DAG execution?
Multi-cluster setups: Some sites run federated Slurm. Do you support this, or limit to single-cluster?
Pre-emption: If using pre-emptible partitions, jobs can be cancelled by higher-priority work. How does this map to Airflow's task state model?
Performance and scaling:

API rate limiting: The Slurm REST API might have rate limits or performance characteristics that differ from Slurm's command-line tools. Polling hundreds of job statuses could become expensive.
Batch job submission overhead: Slurm has per-job overhead. For DAGs with many short tasks, this could be prohibitive compared to other executors.
The authentication token lifecycle is probably your biggest ongoing operational headache - it's not just refresh cycles, but also handling scenarios where the auth service is temporarily unavailable while jobs are running.

Does this surface any dealbreakers, or mostly expected complexity?

It's fairly easy to generate Auth tokens in slurm a service for Auth could easily be implemented.

9 Dec
Good point - if you control the auth service implementation, that removes a major operational headache. You could build something lightweight that issues and refreshes tokens on demand, sidestepping the typical JWT lifecycle management issues.

Given your experience, the real challenges are probably:

The log streaming problem - This feels like the trickiest UX issue. Airflow users expect live logs in the UI, but Slurm jobs write to files on compute nodes. You'd need some mechanism to make these accessible - maybe a log forwarding sidecar, or mounting a shared filesystem that Airflow's web server can read from.
State reconciliation at scale - When you have hundreds of tasks in flight and the Slurm REST API goes down briefly, or the Airflow scheduler restarts, getting everything back in sync correctly is non-trivial. This is where bugs creep in.
The "Airflow expects fast" problem - Airflow's design assumptions lean toward relatively quick task execution (seconds to minutes). Slurm jobs that sit in queue for hours or run for days might expose edge cases in Airflow's state machine or timeouts that other executors don't hit.
The log streaming thing specifically - how are you thinking about solving that? It's probably the most visible user-facing issue. Options I can think of:

Shared filesystem (NFS/Lustre) that both compute nodes and Airflow web server can access
Agent on compute nodes that streams logs to a central service
Accept that logs only appear post-execution (breaks user expectations)
Job wrapper script that tees output to both local file and remote destination
Your Go background might make an agent-based approach appealing, but it adds deployment complexity.

I feel a sidecar that forwards logs to be reasonable, let's consider that options

9 Dec
A log forwarding sidecar is a clean solution. It keeps the concerns separated and gives you flexibility in how logs are transported.

Architecture options:

Option 1: Sidecar as part of the Slurm job Submit each Airflow task as a Slurm job that actually runs two processes - the task itself and a log forwarder. The forwarder tails the task's output and streams it somewhere Airflow can access.

Option 2: Persistent sidecar daemons on compute nodes Run a daemon on each Slurm compute node that watches for new job log files and forwards them. Less overhead per job, but requires administrative setup on the HPC cluster.

Where do logs go?

Redis/message queue: Stream logs to Redis (which Airflow already often uses for Celery). Fast, ephemeral, fits Airflow's architecture
Shared object storage: S3/MinIO/similar - survives restarts, but adds latency for live streaming
Database: Direct to Airflow's metadata DB - simple but potentially heavy
HTTP endpoint: POST directly to Airflow's web server or a custom log collection service
Implementation sketch for Option 1:

Your Slurm job submission would be something like:

bash
#!/bin/bash
# Wrapper script submitted to Slurm

# Start the actual task in background
python /path/to/airflow/task/command > task.log 2>&1 &
TASK_PID=$!

# Start log forwarder
log-forwarder --task-id ${AIRFLOW_TASK_ID} --file task.log --endpoint ${LOG_ENDPOINT} &
FORWARDER_PID=$!

# Wait for task to complete
wait $TASK_PID
TASK_EXIT=$?

# Give forwarder time to flush remaining logs
sleep 2
kill $FORWARDER_PID

exit $TASK_EXIT
The log forwarder could be a simple Go binary - something you could build quickly given your background. It would tail the file and stream chunks via HTTP or write to Redis.

Advantages of this approach:

No cluster-wide daemon deployment needed
Logs associated with specific jobs, cleanup happens naturally when job ends
Works even on heterogeneous clusters where not all nodes have the daemon
Challenges:

Adds a bit of overhead to every job (small binary, but still)
The forwarder binary needs to be available on all compute nodes
Need to handle forwarder failures independently of task failures
Given your infrastructure background, you've probably got opinions on log aggregation patterns. Does Redis as the log destination feel right, or would you prefer something else?

Option 1 would make more sense, what is the preferred way in Airflow to accept these logs?

9 Dec
Good question - Airflow has a specific system for this that you'll want to integrate with properly.

Airflow's logging architecture:

Airflow uses pluggable log handlers through its TaskLogHandler interface. The standard flow is:

Tasks write logs to a location (local filesystem, S3, GCS, etc.)
The web server retrieves logs through the configured log handler when users view task logs in the UI
Log handlers implement read() method to fetch logs for display
The relevant integration point:

You'd want to implement a custom TaskLogHandler that knows how to retrieve logs from wherever your sidecar is sending them. Airflow already has handlers for:

Local filesystem (FileTaskHandler)
S3 (S3TaskHandler)
GCS (GCSTaskHandler)
Azure Blob Storage
Elasticsearch
Most common pattern for remote executors:

Looking at how Kubernetes executor handles this - it writes logs to a remote location (typically S3/GCS), and the web server reads from there. Redis is actually used by Celery executor, but primarily for task queuing, not log storage.

Recommendation for your use case:

S3-compatible storage (S3, MinIO, Ceph) is probably your best bet:

Airflow already has mature S3TaskHandler
Your sidecar can stream logs by appending to an S3 object (or writing chunks)
Persists beyond job lifetime (useful for debugging)
Scales well - doesn't overload Airflow's database
Most HPC sites already have object storage infrastructure
Implementation approach:

Your sidecar streams log chunks to S3 at s3://bucket/dags/{dag_id}/{task_id}/{execution_date}/{try_number}.log
Configure Airflow to use S3TaskHandler (or your custom variant)
Airflow's UI reads from S3 when users view logs
The live streaming challenge:

The tricky bit is Airflow's UI expects to poll for new log content while a task is running. S3TaskHandler does this by re-reading the file periodically. Your sidecar would need to:

Append to the same S3 object as logs arrive (streaming upload)
Or write timestamped chunks that the handler concatenates
Alternative - Custom TaskLogHandler:

You could write a custom handler that reads from wherever your sidecar writes - could be a custom HTTP endpoint, Redis, or anything else. You'd inherit from FileTaskHandler or TaskLogHandler and implement the read() method to fetch from your backend.

Practical question: What storage infrastructure is typically available on the HPC clusters you've worked with? That might dictate the most pragmatic choice.

I suppose the Local filesystem might work if we pass the path that airflow expects down to slurm, considering that most HPC clusters have shared storage, perhaps fallback to S3 as a secondary option.

9 Dec
That's actually a really elegant solution - leverage the shared filesystem that HPC clusters already have. This is simpler and more performant than streaming to object storage.

The approach:

Airflow executor determines the log path using Airflow's standard log path template (e.g., /shared/airflow/logs/dags/{dag_id}/{task_id}/{execution_date}/{try_number}.log)
Pass this path to the Slurm job as an environment variable
The Slurm job writes directly to that path (or your wrapper script redirects stdout/stderr there)
Airflow's standard FileTaskHandler just works - no custom code needed
This gives you:

Zero additional infrastructure - uses what's already there
Live log streaming works natively - Airflow just tails the file
No S3 API calls or network overhead
Simpler deployment story
Configuration validation:

You'd want to check at executor initialization that:

The configured Airflow log path is on shared storage accessible from compute nodes
Permissions are set up correctly for Slurm jobs to write there
When to fallback to S3:

Good to have S3 as a configuration option for sites that:

Don't have shared filesystem access between head/compute nodes
Have security policies preventing compute node writes to shared storage
Run multi-cluster Slurm where filesystem isn't universally shared
Implementation would be clean:

python
# In your executor's submit logic
log_path = self.log_handler.get_log_path(task_instance)

# Submit to Slurm with environment
env = {
    'AIRFLOW_LOG_PATH': log_path,
    # ... other Airflow env vars
}
Then your Slurm job wrapper just redirects:

bash
#!/bin/bash
python /path/to/task > "${AIRFLOW_LOG_PATH}" 2>&1
Does this mesh with your experience on HPC clusters? Usually Lustre or NFS shared across the whole cluster, right? Any edge cases where this wouldn't work?

My thoughts were to set the slurm stdout and err to the path that airflow expects, would that cause issues?

9 Dec
That's even cleaner - just use Slurm's native --output and --error flags (or the REST API equivalents) to write directly to Airflow's expected log paths. No wrapper script needed.

This should work fine:

python
# In your job submission
job_spec = {
    'script': task_command,
    'environment': {...},
    'standard_output': f'/shared/airflow/logs/dag/{dag_id}/task/{task_id}/{execution_date}/{try_number}.log',
    'standard_error': f'/shared/airflow/logs/dag/{dag_id}/task/{task_id}/{execution_date}/{try_number}.log',  # or separate .err file
}
Potential considerations:

Merged vs separate streams: Airflow typically expects stdout and stderr merged into one log file. Using the same path for both accomplishes this. Just need to verify Slurm handles this correctly (it should - appending to the same file).
Slurm filename patterns: Slurm supports patterns like %j (job ID) in output paths. You probably don't want these since Airflow expects specific paths, but worth being aware of.
Directory creation: Slurm won't create parent directories - the path must exist. Your executor would need to ensure the log directory exists before submitting the job (which Airflow's log handler probably does already).
Buffering: Slurm typically buffers stdout/stderr. For truly live log streaming, you might see a slight delay vs direct writes, but this is usually acceptable (seconds, not minutes).
File permissions: The Slurm job runs as some user (the Airflow worker user, presumably). Just need to ensure permissions work for both writing (from compute nodes) and reading (from web server).
This is actually how most batch systems work - SGE, PBS, LSF all do the same thing. It's battle-tested.

The main question: Do you want stdout and stderr in the same file or separate? Airflow's UI typically shows one unified log stream, so merging makes sense. But having them separate can be useful for debugging. What's your preference?

Slurm supports merging them

9 Dec
Perfect - then that's definitely the way to go. Just set both to the same path and Slurm handles the merging natively.

This simplifies your implementation significantly:

No wrapper scripts needed
No sidecar processes
No custom log streaming infrastructure
Airflow's existing FileTaskHandler works out of the box
Live log tailing works naturally since it's just a file on shared storage
Your executor just needs to:

Get the log path from Airflow's log handler configuration
Ensure the parent directory exists
Set standard_output and standard_error to that path in the Slurm job submission
Let Slurm and Airflow do their thing
Fallback to S3:

For the S3 fallback case (non-shared filesystem environments), you'd need the sidecar approach we discussed earlier. But you could make this a configuration option - detect whether the log path is on shared storage accessible to compute nodes, and choose the strategy accordingly.

This makes the MVP much more achievable - one less complex component to build and maintain. The sidecar/S3 approach can be a "phase 2" feature for environments that need it.

Given your Slurm expertise, you've probably already validated that the REST API exposes standard_output and standard_error parameters properly? I'd assume so since you've built the Go client, but worth confirming those map cleanly to the API.

Ok let's go back to considering the implementation, would it make sense to implement a native python client for the slurm API or cauld I wrap the compiled go slurm client?

9 Dec
Given your existing Go client, wrapping it could get you to MVP faster, but there are trade-offs worth thinking through.

Arguments for wrapping your Go client:

You've already solved the hard problems - API quirks, error handling, edge cases
Faster to MVP - Just need Python bindings via subprocess calls or CGo/ctypes
Proven code - It's battle-tested from your other projects
Potential code reuse - If you're using it in your exporter/TUI, keeping one canonical implementation makes maintenance easier
Arguments for native Python:

Airflow ecosystem fit - Pure Python is more idiomatic, easier for contributors
Dependency simplicity - No compiled binaries to distribute, no CGo/cross-compilation headaches
Easier debugging - Python stacktraces vs FFI boundary issues
Distribution - pip install vs bundling platform-specific binaries
Community adoption - Lower barrier for others to contribute or fork
Hybrid approach:

You could build a minimal Python client for the specific Slurm REST API calls the executor needs (job submit, status, cancel), while keeping your Go client for your other tools. The API surface for an Airflow executor is actually pretty small - you don't need full Slurm API coverage.

Practical consideration:

How complex is your Go client? If it's thousands of lines handling every Slurm API endpoint, rewriting in Python is significant work. If it's relatively focused, a Python implementation might not be that painful.

My take: For an open-source project where you want adoption, native Python probably wins. The executor itself is the novel contribution - the Slurm API client is almost commodity code. A pure Python package is much easier for the Airflow community to work with, contribute to, and trust.

What's your instinct? Does the Go client represent significant enough IP/complexity that rewriting feels wasteful?

A minimal python client is fine then focusing on the specific feature set but it would be important to support all the latest slurm API versions while auto detecting the correct version to use as the slurm API changes significantly between versions.

9 Dec
Smart - version detection and compatibility is critical since Slurm deployments are notoriously heterogeneous. Sites often run older versions for stability, and the API has indeed changed significantly.

Version detection strategy:

The Slurm REST API exposes its version info at the /slurm/ endpoint (or you can hit /openapi/v3 or similar). You'd want to:

Probe on initialization - When the executor starts, query the API to determine available versions
Select best compatible version - Choose the newest API version your client supports that the server also supports
Cache the decision - Don't re-detect on every request
Implementation approach:

python
class SlurmAPIClient:
    SUPPORTED_VERSIONS = ['v0.0.40', 'v0.0.39', 'v0.0.38']  # Ordered newest to oldest
    
    def __init__(self, base_url):
        self.base_url = base_url
        self.api_version = self._detect_version()
        
    def _detect_version(self):
        # Query server for available versions
        # Return best match from SUPPORTED_VERSIONS
        
    def submit_job(self, job_spec):
        # Route to version-specific implementation
        handler = getattr(self, f'_submit_job_{self.api_version.replace(".", "_")}')
        return handler(job_spec)
Key API differences between versions to handle:

From your experience building the Go client, you know the pain points - things like:

Job submission payload structure changes
Different field names for the same concept
Authentication mechanism changes (especially around JWT)
Response format variations
Adapter pattern might work well:

python
class SlurmAPIAdapter:
    """Base adapter interface"""
    
class SlurmAPI_v0_0_40(SlurmAPIAdapter):
    """Implements latest API"""
    
class SlurmAPI_v0_0_39(SlurmAPIAdapter):
    """Handles v0.0.39 specifics"""
Each adapter implements the same interface but handles version-specific quirks internally.

Testing strategy:

This is where it gets painful - ideally you'd test against multiple Slurm versions. Could you:

Use Docker containers with different Slurm versions for CI?
Document which versions you've actually tested vs theoretically support?
Start with supporting just the latest 2-3 versions to limit scope?
Configuration option:

Also worth having a manual override: SLURM_API_VERSION=v0.0.39 to force a specific version, for troubleshooting or when auto-detection fails.

What's the oldest Slurm version you think is worth supporting? Slurm 20.x? 21.x? The further back you go, the more adapter code you need to maintain.

0.0.41 or 42 probably so fairly modern 24.x or newer

9 Dec
That's a reasonable scope - Slurm 24.x (API v0.0.41/42) is recent enough that most sites either have it or are planning to upgrade to it. You avoid the nightmare of supporting ancient API versions while still covering the majority of modern deployments.

This keeps your implementation manageable:

Supporting just a couple of recent API versions means:

Less adapter code to write and maintain
Differences between v0.0.41 and v0.0.42 are likely incremental rather than fundamental
You can document clearly: "Requires Slurm 24.05+ with REST API v0.0.41 or newer"
Version detection becomes simpler:

python
class SlurmAPIClient:
    MIN_SUPPORTED_VERSION = 'v0.0.41'
    SUPPORTED_VERSIONS = ['v0.0.42', 'v0.0.41']
    
    def _detect_version(self):
        available = self._query_available_versions()
        
        for version in self.SUPPORTED_VERSIONS:
            if version in available:
                return version
        
        raise UnsupportedSlurmVersion(
            f"Slurm API must be {self.MIN_SUPPORTED_VERSION} or newer. "
            f"Found: {available}"
        )
Practical question:

Between v0.0.41 and v0.0.42, are the differences significant enough that you need separate adapters, or could you write one implementation that handles both with minor conditional logic? From your Go client experience, do you recall if there were breaking changes between these versions?

Documentation will be important:

Since you're targeting modern Slurm, you should clearly document:

Minimum Slurm version (24.05 or whatever maps to v0.0.41)
How to check their Slurm REST API version
Migration path for sites on older versions (upgrade Slurm, or wait for broader version support)
This positioning actually helps with adoption - "modern Slurm REST API executor" sounds better than "works with any Slurm version but might be buggy on old ones."

Does this version scope feel right for an MVP?

41 has a fairly different API to 40 42 43 and 44, it's a bit of an odd one out of these

9 Dec
Interesting - so v0.0.41 is the awkward middle child. That changes the calculus.

Given that, you have a few options:

Option 1: Skip v0.0.41, support v0.0.42+ If v0.0.42/43/44 are similar to each other but different from v0.0.41, it might make sense to just skip v0.0.41 entirely and target the more stable v0.0.42+ API family. Less adapter code, cleaner implementation.

Option 2: Support v0.0.40 and v0.0.42+ If v0.0.40 has good adoption and is relatively stable (sounds like it based on earlier examples), you could support:

v0.0.40 (older but common, represents Slurm 23.x?)
v0.0.42/43/44 (modern family)
Skip the oddball v0.0.41. Two adapter implementations instead of trying to handle the quirky middle version.

Option 3: v0.0.42+ only Be opinionated - require the latest stable API family. Simpler to build and maintain. Users on older Slurm can upgrade or wait.

From your Go client experience, which versions are you actually seeing in the wild? If most sites are either on v0.0.40 or have jumped to v0.0.42+, supporting just those two makes sense.

My recommendation: Start with v0.0.42+ only for MVP. Get something working well on the modern API. If there's demand, add v0.0.40 support later as a separate adapter. Skip v0.0.41 unless someone specifically requests it.

What's the adoption story for v0.0.42+? Is that Slurm 24.05 or later? How recent is that release?

I might go with option 3 it would ease the implementation

9 Dec
Option 3 is the right call for an MVP. Supporting v0.0.42+ only means:

Immediate benefits:

Single, clean implementation without abstraction layers
Faster development - you can ship something useful sooner
Less testing surface area
Clearer documentation and requirements
It's defensible: You're building a new integration for modern workflows (Airflow + HPC). It's reasonable to require modern Slurm. Sites running ancient Slurm versions probably aren't rushing to adopt new Airflow executors anyway.

Clear upgrade path: If someone needs v0.0.40 support after release, you've got the architecture to add an adapter. But don't build it speculatively - wait for actual demand.

Implementation becomes straightforward:

python
class SlurmAPIClient:
    MIN_API_VERSION = 'v0.0.42'
    
    def __init__(self, base_url, auth_token):
        self.base_url = base_url
        self.auth_token = auth_token
        self._validate_api_version()
    
    def _validate_api_version(self):
        # Check server supports v0.0.42+
        # Fail fast with clear error if not
    
    def submit_job(self, job_spec):
        # Single implementation for v0.0.42+ API
Documentation upfront: Just be explicit in README: "Requires Slurm 24.05+ with REST API v0.0.42 or newer"

This lets you focus on the interesting problems - the executor logic, state management, error handling - rather than API version gymnastics.

Ready to start sketching out the executor architecture itself? The core state machine for task lifecycle management is probably the meatiest part of this project.

Yes, sounds good

9 Dec
Great. Let's think through the executor architecture.

Core components you'll need:

1. SlurmExecutor class Inherits from airflow.executors.base_executor.BaseExecutor. This is your main orchestrator.

2. Key methods to implement:

python
class SlurmExecutor(BaseExecutor):
    def start(self):
        # Initialize connection to Slurm REST API
        # Start any background threads for polling
        
    def execute_async(self, key, command, queue=None, executor_config=None):
        # Submit task to Slurm
        # Store mapping of Airflow task key -> Slurm job ID
        
    def sync(self):
        # Poll Slurm for job status updates
        # Update Airflow task states (success/failed/running)
        # This gets called periodically by Airflow scheduler
        
    def end(self):
        # Clean shutdown
        # Cancel any remaining jobs if needed
        
    def terminate(self):
        # Hard stop - kill all jobs immediately
3. State tracking

You need to maintain mappings between Airflow's world and Slurm's world:

python
{
    airflow_task_key: {
        'slurm_job_id': 12345,
        'state': 'RUNNING',
        'submit_time': datetime,
        'command': 'airflow task run...',
    }
}
Key architectural decisions:

Polling strategy: The sync() method is called periodically (every few seconds). You need to:

Query Slurm for status of all tracked jobs
Avoid overwhelming the API with requests
Handle cases where jobs disappear from Slurm's active queue (completed jobs)
Batch status queries: Rather than querying each job individually, you probably want to:

python
# Get all jobs for the user in one API call
GET /slurm/v0.0.42/jobs
Then filter/match against your tracked jobs. More efficient than N individual queries.

State mapping: Slurm job states → Airflow states:

PENDING/CONFIGURING → keep as queued in Airflow
RUNNING → running
COMPLETED (exit code 0) → success
FAILED/TIMEOUT/CANCELLED/NODE_FAIL → failed
PREEMPTED → ? (retry or fail?)
Configuration: You'll need settings for:

Slurm REST API endpoint
Authentication token (or token service endpoint)
Default partition/QoS
Polling interval
Resource defaults (cpus, memory, time limits)
Executor config: Airflow tasks can pass executor_config dict to customize execution. You'd support things like:

python
@task(executor_config={
    "partition": "gpu",
    "cpus_per_task": 8,
    "mem": "32G",
    "time_limit": "04:00:00",
})
def my_task():
    ...
Error handling:

API is down when trying to submit → queue task for retry
API is down during sync → keep existing state, retry poll
Job vanishes from Slurm → check accounting DB or mark as failed
Where do you want to start? The submit logic, the sync/polling logic, or the configuration/initialization?

The submit logic

9 Dec
Good choice - submit is the foundation. Get this right and the rest builds naturally.

Submit flow breakdown:

python
def execute_async(self, key, command, queue=None, executor_config=None):
    """
    Called by Airflow scheduler to execute a task
    
    key: TaskInstanceKey (dag_id, task_id, execution_date, try_number)
    command: List of command arguments, e.g. ['airflow', 'tasks', 'run', 'dag_id', ...]
    queue: Airflow queue name (can map to Slurm partition)
    executor_config: Dict of Slurm-specific parameters
    """
Step 1: Build the job specification

You need to construct the Slurm job submission payload:

python
def _build_job_spec(self, key, command, executor_config):
    # Extract task instance info
    ti = self._task_instances[key]  # Or however you access it
    
    # Determine log path
    log_path = self._get_log_path(ti)
    
    # Build job name
    job_name = f"airflow-{ti.dag_id}-{ti.task_id}-{ti.try_number}"
    
    # Construct Slurm job spec
    job_spec = {
        'name': job_name,
        'environment': self._build_environment(ti),
        'script': self._build_script(command),  # Or direct command execution?
        'standard_output': log_path,
        'standard_error': log_path,
        'current_working_directory': self.airflow_home,  # Or task-specific?
    }
    
    # Apply resource requirements
    job_spec.update(self._apply_resources(executor_config))
    
    return job_spec
Step 2: Resource configuration

python
def _apply_resources(self, executor_config):
    """Map executor_config to Slurm parameters"""
    config = executor_config or {}
    
    resources = {
        'partition': config.get('partition', self.default_partition),
        'tasks': 1,  # Typically 1 task per Airflow task
        'cpus_per_task': config.get('cpus_per_task', self.default_cpus),
        'memory_per_node': config.get('mem', self.default_mem),
        'time_limit': config.get('time_limit', self.default_time_limit),
    }
    
    # Optional: QoS, account, reservation
    if 'qos' in config:
        resources['qos'] = config['qos']
    if 'account' in config:
        resources['account'] = config['account']
        
    return resources
Step 3: Environment variables

Airflow tasks need specific environment to run correctly:

python
def _build_environment(self, ti):
    """Build environment dict for the Slurm job"""
    env = {
        'AIRFLOW_HOME': self.airflow_home,
        'AIRFLOW__CORE__DAGS_FOLDER': self.dags_folder,
        'AIRFLOW__CORE__EXECUTOR': 'SlurmExecutor',
        # Any other Airflow env vars needed
    }
    
    # Propagate specific env vars from scheduler
    for var in ['PYTHONPATH', 'PATH']:
        if var in os.environ:
            env[var] = os.environ[var]
    
    return env
Step 4: Command execution approach

You have two options here:

Option A: Direct command execution

python
'script': '#!/bin/bash\n' + ' '.join(command)
Option B: Wrapper script Create a wrapper that handles setup/teardown:

python
def _build_script(self, command):
    wrapper = f"""#!/bin/bash
set -e

# Setup
cd {self.airflow_home}
source venv/bin/activate  # If using venv

# Execute task
{' '.join(command)}

# Cleanup if needed
"""
    return wrapper
Step 5: Submit to Slurm

python
def _submit_job(self, job_spec):
    """Submit job via REST API"""
    try:
        response = self.slurm_client.submit_job(job_spec)
        job_id = response['job_id']
        return job_id
    except SlurmAPIError as e:
        # Handle API errors
        self.log.error(f"Failed to submit job: {e}")
        return None
Step 6: Track the submission

python
def execute_async(self, key, command, queue=None, executor_config=None):
    job_spec = self._build_job_spec(key, command, executor_config)
    
    job_id = self._submit_job(job_spec)
    
    if job_id:
        # Store mapping for sync() to track
        self.running[key] = {
            'slurm_job_id': job_id,
            'command': command,
            'submit_time': datetime.now(),
        }
        self.log.info(f"Submitted task {key} as Slurm job {job_id}")
    else:
        # Failed to submit - Airflow will retry
        self.fail(key)
Questions to resolve:

Script vs command: Should you use Slurm's script field (bash script as string) or direct command execution? Script gives you more control for setup/teardown.
Working directory: Where should Slurm jobs execute? Same as Airflow scheduler? Task-specific temp dirs?
Virtual environment: How do you ensure the Python environment on compute nodes matches the scheduler? Module system? Venv activation in wrapper?
Shared filesystem assumptions: The command paths (like airflow binary) need to be valid on compute nodes. Document this requirement or handle it somehow?
Which of these areas do you want to dig into first? The environment/venv setup is probably the trickiest operational bit.

Script seem more flexible, what is the pattern for working dir in airflow? In slurm when submitting the job through the API the working dir can be specified. Venv activation in wrapper seem reasonable. 4 airflow binary seems reasonable as a fallback slurm supports starting containers with a --containter flag that could be used to run things in a containerised airflow environment.

9 Dec
Good thinking on the container option - that's a clean way to handle environment isolation.

Working directory in Airflow:

Airflow tasks typically execute in the AIRFLOW_HOME directory by default, though this can vary by executor. Looking at other executors:

LocalExecutor: runs in scheduler's working directory
KubernetesExecutor: typically /opt/airflow or similar in the container
CeleryExecutor: worker's AIRFLOW_HOME
For your case, setting current_working_directory to AIRFLOW_HOME makes sense as a default. Tasks that need different working directories typically handle cd themselves in their code.

Container support is elegant:

python
def _apply_resources(self, executor_config):
    config = executor_config or {}
    
    resources = {
        'partition': config.get('partition', self.default_partition),
        'cpus_per_task': config.get('cpus_per_task', self.default_cpus),
        'memory_per_node': config.get('mem', self.default_mem),
        'time_limit': config.get('time_limit', self.default_time_limit),
        'current_working_directory': config.get('working_dir', self.airflow_home),
    }
    
    # Container support
    if 'container' in config:
        resources['container'] = config['container']
    elif self.default_container:
        resources['container'] = self.default_container
    
    return resources
This gives you two deployment modes:

Mode 1: Shared filesystem + venv

python
def _build_script(self, command):
    script = f"""#!/bin/bash
set -euo pipefail

# Activate environment
source {self.airflow_venv}/bin/activate

# Execute task
{' '.join(shlex.quote(arg) for arg in command)}
"""
    return script
Mode 2: Container-based

python
# In executor_config or defaults
executor_config = {
    'container': 'docker://myregistry/airflow:2.8.0',
}

# Script is simpler since environment is in container
def _build_script(self, command):
    script = f"""#!/bin/bash
set -euo pipefail

# Container already has airflow installed
{' '.join(shlex.quote(arg) for arg in command)}
"""
    return script
Configuration structure:

python
# airflow.cfg or environment
[slurm]
api_url = https://slurm-head.example.com:6820
auth_token = /path/to/token/file  # or token service URL
default_partition = compute
default_cpus = 1
default_mem = 4G
default_time_limit = 01:00:00
airflow_home = /shared/airflow
airflow_venv = /shared/airflow/venv  # Optional, for non-container mode
default_container =  # Optional, e.g. docker://myregistry/airflow:2.8.0
Script building with proper escaping:

python
def _build_script(self, command, executor_config=None):
    """Build bash script for Slurm execution"""
    config = executor_config or {}
    
    script_lines = [
        '#!/bin/bash',
        'set -euo pipefail',  # Exit on error, undefined vars, pipe failures
        ''
    ]
    
    # Add venv activation if not using container and venv is configured
    if not config.get('container') and not self.default_container:
        if self.airflow_venv:
            script_lines.append(f'source {self.airflow_venv}/bin/activate')
            script_lines.append('')
    
    # Add environment variable exports if needed
    # (though these should be in 'environment' field)
    
    # Execute the command with proper quoting
    escaped_command = ' '.join(shlex.quote(arg) for arg in command)
    script_lines.append(f'# Execute Airflow task')
    script_lines.append(escaped_command)
    
    return '\n'.join(script_lines)
The container approach has nice properties:

Consistent environment across all compute nodes
No shared venv management headaches
Easy versioning (different DAGs can use different container versions via executor_config)
Slurm handles pulling/caching containers
Does this structure feel right? The flexibility to support both modes (shared venv vs containers) covers most deployment scenarios. You could even detect automatically: if container is specified, skip venv activation; otherwise require venv path.

Yeah makes sense

9 Dec
Good. Let's move on to the sync/polling logic - this is where you reconcile Slurm's view of the world with Airflow's.

The sync() method responsibilities:

Query Slurm for status of all tracked jobs
Update Airflow task states based on Slurm job states
Handle jobs that have disappeared (completed, failed, or purged)
Clean up internal tracking for finished jobs
Basic structure:

python
def sync(self):
    """
    Called periodically by Airflow scheduler
    Updates task states based on Slurm job status
    """
    if not self.running:
        return  # Nothing to check
    
    # Get current status of all our jobs from Slurm
    job_statuses = self._fetch_job_statuses()
    
    # Update each tracked task
    for key, job_info in list(self.running.items()):
        slurm_job_id = job_info['slurm_job_id']
        
        if slurm_job_id in job_statuses:
            slurm_state = job_statuses[slurm_job_id]
            self._handle_job_state(key, slurm_state)
        else:
            # Job not in active queue - check if completed or failed
            self._handle_missing_job(key, job_info)
Fetching job statuses efficiently:

python
def _fetch_job_statuses(self):
    """
    Query Slurm for all jobs belonging to this executor
    Returns dict: {job_id: job_info}
    """
    try:
        # Get all jobs for our user
        response = self.slurm_client.get_jobs()
        
        # Build lookup dict for our tracked jobs
        our_job_ids = {info['slurm_job_id'] for info in self.running.values()}
        
        statuses = {}
        for job in response.get('jobs', []):
            job_id = job['job_id']
            if job_id in our_job_ids:
                statuses[job_id] = {
                    'state': job['job_state'],
                    'exit_code': job.get('exit_code'),
                    'reason': job.get('state_reason'),
                }
        
        return statuses
        
    except SlurmAPIError as e:
        self.log.warning(f"Failed to fetch job statuses: {e}")
        # Return empty dict - we'll retry next sync
        return {}
State mapping logic:

python
def _handle_job_state(self, key, slurm_state):
    """Map Slurm job state to Airflow task state"""
    state = slurm_state['state']
    
    if state in ['PENDING', 'CONFIGURING']:
        # Job is queued in Slurm, keep as running in Airflow
        # (Airflow doesn't distinguish between queued and running)
        pass
    
    elif state == 'RUNNING':
        # Job is executing, no state change needed in Airflow
        pass
    
    elif state == 'COMPLETED':
        # Check exit code
        exit_code = slurm_state.get('exit_code', 0)
        if exit_code == 0:
            self.success(key)
            self.log.info(f"Task {key} succeeded (Slurm job {self.running[key]['slurm_job_id']})")
        else:
            self.fail(key)
            self.log.error(f"Task {key} failed with exit code {exit_code}")
        
        # Remove from tracking
        del self.running[key]
    
    elif state in ['FAILED', 'TIMEOUT', 'CANCELLED', 'NODE_FAIL', 'OUT_OF_MEMORY']:
        self.fail(key)
        self.log.error(f"Task {key} failed: {state} - {slurm_state.get('reason', 'unknown')}")
        del self.running[key]
    
    elif state == 'PREEMPTED':
        # Job was preempted - could retry or fail
        # For now, treat as failure and let Airflow's retry logic handle it
        self.fail(key)
        self.log.warning(f"Task {key} was preempted")
        del self.running[key]
    
    else:
        self.log.warning(f"Unknown Slurm state '{state}' for task {key}")
Handling missing jobs:

When a job isn't in the active queue, it might be:

Completed and purged from active list
Failed and purged
Lost due to Slurm controller restart
python
def _handle_missing_job(self, key, job_info):
    """
    Job not found in active queue - check accounting or mark as failed
    """
    slurm_job_id = job_info['slurm_job_id']
    
    # Try accounting database for historical info
    try:
        job_history = self.slurm_client.get_job_history(slurm_job_id)
        
        if job_history:
            # Found in accounting - process final state
            state = job_history['state']
            exit_code = job_history.get('exit_code', 0)
            
            if state == 'COMPLETED' and exit_code == 0:
                self.success(key)
            else:
                self.fail(key)
            
            self.log.info(f"Task {key} found in accounting: {state}")
            del self.running[key]
        else:
            # Not in accounting yet - might still be processing
            # Check how long it's been missing
            self._handle_prolonged_missing(key, job_info)
            
    except SlurmAPIError as e:
        self.log.warning(f"Could not query accounting for job {slurm_job_id}: {e}")
        self._handle_prolonged_missing(key, job_info)
Handling prolonged missing jobs:

python
def _handle_prolonged_missing(self, key, job_info):
    """
    Job has been missing for too long - mark as failed
    """
    # Track when job went missing
    if 'missing_since' not in job_info:
        job_info['missing_since'] = datetime.now()
        return
    
    missing_duration = datetime.now() - job_info['missing_since']
    
    # If missing for more than threshold, give up
    if missing_duration > timedelta(minutes=5):
        self.fail(key)
        self.log.error(
            f"Task {key} job {job_info['slurm_job_id']} "
            f"missing from Slurm for {missing_duration}"
        )
        del self.running[key]
Polling frequency consideration:

Airflow calls sync() every few seconds (configurable). You might want to throttle actual Slurm API calls:

python
def sync(self):
    # Only query Slurm every N seconds, not every sync call
    now = time.time()
    if now - self.last_sync_time < self.min_sync_interval:
        return
    
    self.last_sync_time = now
    
    # ... rest of sync logic
Questions:

Accounting database queries: The Slurm REST API's accounting endpoints - are they performant enough for frequent queries? Or should you rely on sacct command as fallback?
Preemption handling: Should preempted jobs automatically retry (by marking as failed and letting Airflow retry), or should there be a separate state/config?
Polling interval: What feels reasonable? 10 seconds? 30 seconds? Trade-off between responsiveness and API load.
Want to tackle the cleanup/termination logic next, or dive deeper into any of these state handling edge cases?

Let's assume they are, and focus on the API approach. 2. Automatic retry seems reasonable 3. 10 to 30 seconds should be fine
9 Dec
Good - keeping it simple with API-only and reasonable defaults.

Let's move to cleanup and termination logic.

This covers graceful shutdown and forceful termination scenarios.

The end() method - graceful shutdown:

python
def end(self):
    """
    Called when Airflow scheduler is shutting down gracefully
    Should wait for running jobs or cancel them cleanly
    """
    if not self.running:
        return
    
    self.log.info(f"Executor shutting down with {len(self.running)} jobs still running")
    
    # Decision: wait or cancel?
    # Option 1: Cancel all remaining jobs
    self._cancel_all_jobs()
    
    # Option 2: Wait for jobs to finish (with timeout)
    # self._wait_for_jobs(timeout=300)
Cancelling jobs:

python
def _cancel_all_jobs(self):
    """Cancel all tracked Slurm jobs"""
    for key, job_info in list(self.running.items()):
        slurm_job_id = job_info['slurm_job_id']
        try:
            self.slurm_client.cancel_job(slurm_job_id)
            self.log.info(f"Cancelled Slurm job {slurm_job_id} for task {key}")
        except SlurmAPIError as e:
            self.log.warning(f"Failed to cancel job {slurm_job_id}: {e}")
        
        # Mark as failed in Airflow
        self.fail(key)
    
    self.running.clear()
The terminate() method - hard stop:

python
def terminate(self):
    """
    Emergency shutdown - kill everything immediately
    Called on SIGTERM or fatal errors
    """
    self.log.warning("Executor terminate() called - killing all jobs")
    
    # Best effort cancellation, don't wait for responses
    for key, job_info in self.running.items():
        slurm_job_id = job_info['slurm_job_id']
        try:
            self.slurm_client.cancel_job(slurm_job_id)
        except Exception as e:
            # Swallow all exceptions during emergency shutdown
            self.log.error(f"Error cancelling job {slurm_job_id}: {e}")
    
    self.running.clear()
Individual task cancellation:

Airflow can also request cancellation of individual running tasks:

python
def try_adopt_task_instances(self, tis):
    """
    Not typically implemented for custom executors
    Return empty list - we don't support adoption
    """
    return []

def cleanup_stuck_queued_tasks(self, tis):
    """
    Called to clean up tasks stuck in queued state
    """
    # Cancel corresponding Slurm jobs if they exist
    for ti in tis:
        key = ti.key
        if key in self.running:
            job_info = self.running[key]
            try:
                self.slurm_client.cancel_job(job_info['slurm_job_id'])
                self.log.info(f"Cleaned up stuck task {key}")
            except SlurmAPIError as e:
                self.log.warning(f"Failed to cancel stuck task {key}: {e}")
            
            del self.running[key]
SlurmAPIClient cancel method:

python
class SlurmAPIClient:
    def cancel_job(self, job_id):
        """
        Cancel a Slurm job via REST API
        DELETE /slurm/v0.0.42/job/{job_id}
        """
        url = f"{self.base_url}/slurm/{self.api_version}/job/{job_id}"
        
        response = requests.delete(
            url,
            headers=self._auth_headers(),
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # Job already finished/doesn't exist
            self.log.debug(f"Job {job_id} not found for cancellation")
            return None
        else:
            raise SlurmAPIError(f"Failed to cancel job {job_id}: {response.text}")
Configuration for shutdown behavior:

python
[slurm]
# ... other config
shutdown_mode = cancel  # Options: cancel, wait
shutdown_wait_timeout = 300  # seconds to wait if shutdown_mode=wait
Implementation with configurable behavior:

python
def end(self):
    if not self.running:
        return
    
    if self.shutdown_mode == 'cancel':
        self._cancel_all_jobs()
    elif self.shutdown_mode == 'wait':
        self._wait_for_jobs(timeout=self.shutdown_wait_timeout)
    else:
        self.log.warning(f"Unknown shutdown_mode: {self.shutdown_mode}, defaulting to cancel")
        self._cancel_all_jobs()

def _wait_for_jobs(self, timeout):
    """Wait for jobs to complete naturally"""
    start_time = time.time()
    
    while self.running and (time.time() - start_time) < timeout:
        self.sync()  # Update job states
        time.sleep(5)
    
    # Cancel any remaining jobs after timeout
    if self.running:
        self.log.warning(f"Timeout waiting for jobs, cancelling {len(self.running)} remaining")
        self._cancel_all_jobs()
Error handling in cancel operations:

Jobs might already be finished when you try to cancel them, which is fine:

python
def _cancel_job_safe(self, slurm_job_id):
    """Cancel job, handling common error cases"""
    try:
        self.slurm_client.cancel_job(slurm_job_id)
        return True
    except SlurmAPIError as e:
        if 'not found' in str(e).lower() or '404' in str(e):
            # Job already finished - not an error
            self.log.debug(f"Job {slurm_job_id} already finished")
            return True
        else:
            # Real error
            self.log.error(f"Failed to cancel job {slurm_job_id}: {e}")
            return False
```

**Putting it together - full lifecycle:**
```
start() → initialize API client, load config
  ↓
execute_async() → submit jobs to Slurm (repeats for each task)
  ↓
sync() → poll job status, update Airflow (called periodically)
  ↓
end() or terminate() → cleanup on shutdown
Next steps for a complete implementation:

Configuration loading - parse airflow.cfg or environment variables
Authentication - token loading/refreshing
Error resilience - retry logic, circuit breakers for API failures
Logging - proper integration with Airflow's logging
Metrics - optional: expose executor metrics (queue depth, job states)
Which area do you want to tackle next? Configuration and initialization would make sense to flesh out the complete picture, or we could dive into authentication/token management since you mentioned building an auth service.

2, to start with for simplicity I suppose we could just call scontrol token as needed to issue a token on demand as an MVP?

9 Dec
Yes, that's a pragmatic MVP approach. scontrol token is straightforward and avoids building auth infrastructure upfront.

Simple token management:

python
import subprocess
import json
from datetime import datetime, timedelta

class SlurmTokenManager:
    def __init__(self, username=None, lifespan=3600):
        """
        username: Slurm username (defaults to current user)
        lifespan: Token lifespan in seconds (default 1 hour)
        """
        self.username = username or self._get_current_user()
        self.lifespan = lifespan
        self.token = None
        self.token_expiry = None
    
    def _get_current_user(self):
        """Get current system username"""
        import getpass
        return getpass.getuser()
    
    def get_token(self):
        """Get valid token, refreshing if necessary"""
        if self._token_is_valid():
            return self.token
        
        return self._fetch_new_token()
    
    def _token_is_valid(self):
        """Check if current token is still valid"""
        if not self.token or not self.token_expiry:
            return False
        
        # Refresh 5 minutes before expiry to avoid edge cases
        return datetime.now() < (self.token_expiry - timedelta(minutes=5))
    
    def _fetch_new_token(self):
        """Generate new token using scontrol"""
        try:
            cmd = ['scontrol', 'token', f'lifespan={self.lifespan}']
            if self.username:
                cmd.append(f'username={self.username}')
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=10
            )
            
            # Parse output: "SLURM_JWT=token_string"
            output = result.stdout.strip()
            if output.startswith('SLURM_JWT='):
                self.token = output.split('=', 1)[1]
                self.token_expiry = datetime.now() + timedelta(seconds=self.lifespan)
                return self.token
            else:
                raise SlurmTokenError(f"Unexpected scontrol output: {output}")
        
        except subprocess.CalledProcessError as e:
            raise SlurmTokenError(f"Failed to generate token: {e.stderr}")
        except subprocess.TimeoutExpired:
            raise SlurmTokenError("Token generation timed out")

class SlurmTokenError(Exception):
    pass
Integration with API client:

python
class SlurmAPIClient:
    def __init__(self, base_url, token_manager):
        self.base_url = base_url
        self.token_manager = token_manager
        self.api_version = 'v0.0.42'
    
    def _auth_headers(self):
        """Get headers with fresh auth token"""
        token = self.token_manager.get_token()
        return {
            'X-SLURM-USER-TOKEN': token,
            'Content-Type': 'application/json',
        }
    
    def submit_job(self, job_spec):
        url = f"{self.base_url}/slurm/{self.api_version}/job/submit"
        
        response = requests.post(
            url,
            headers=self._auth_headers(),
            json={'job': job_spec},
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise SlurmAPIError(f"Job submission failed: {response.text}")
In the executor initialization:

python
class SlurmExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        self.slurm_api_url = None
        self.slurm_client = None
        self.token_manager = None
        # ... other attrs
    
    def start(self):
        """Initialize executor"""
        super().start()
        
        # Load config
        self._load_config()
        
        # Initialize token manager
        self.token_manager = SlurmTokenManager(
            username=self.slurm_username,
            lifespan=self.token_lifespan
        )
        
        # Initialize API client
        self.slurm_client = SlurmAPIClient(
            base_url=self.slurm_api_url,
            token_manager=self.token_manager
        )
        
        # Validate connection
        self._validate_connection()
        
        self.log.info("SlurmExecutor started successfully")
Configuration:

python
def _load_config(self):
    """Load Slurm executor configuration"""
    from airflow.configuration import conf
    
    self.slurm_api_url = conf.get('slurm', 'api_url')
    self.slurm_username = conf.get('slurm', 'username', fallback=None)
    self.token_lifespan = conf.getint('slurm', 'token_lifespan', fallback=3600)
    
    self.default_partition = conf.get('slurm', 'default_partition', fallback='compute')
    self.default_cpus = conf.getint('slurm', 'default_cpus', fallback=1)
    self.default_mem = conf.get('slurm', 'default_mem', fallback='4G')
    self.default_time_limit = conf.get('slurm', 'default_time_limit', fallback='01:00:00')
    
    self.airflow_home = conf.get('core', 'airflow_home')
    self.airflow_venv = conf.get('slurm', 'airflow_venv', fallback=None)
    self.default_container = conf.get('slurm', 'default_container', fallback=None)
    
    self.min_sync_interval = conf.getint('slurm', 'sync_interval', fallback=10)
    self.shutdown_mode = conf.get('slurm', 'shutdown_mode', fallback='cancel')
Error handling for token failures:

python
def _auth_headers(self):
    """Get headers with fresh auth token, with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            token = self.token_manager.get_token()
            return {
                'X-SLURM-USER-TOKEN': token,
                'Content-Type': 'application/json',
            }
        except SlurmTokenError as e:
            if attempt == max_retries - 1:
                raise
            self.log.warning(f"Token generation attempt {attempt + 1} failed: {e}")
            time.sleep(1)
Advantages of this approach:

No separate auth service to deploy/manage
Uses standard Slurm tooling
Token refresh is automatic and transparent
Works wherever scontrol is available
Limitations to document:

Requires scontrol binary accessible from Airflow scheduler
Adds slight latency on first request and token refreshes
Subprocess overhead (minimal but worth noting)
Future improvement path:

Later, you could swap in a proper auth service by just replacing SlurmTokenManager with a version that calls your Go auth service - the API client interface stays the same.

Does this token approach work for your MVP? It's clean, self-contained, and easy to test.

It works for the MVP

9 Dec
Perfect. Let's put together the configuration and initialization to tie everything together.

Configuration file structure (airflow.cfg):

ini
[slurm]
# Slurm REST API endpoint
api_url = https://slurm-head.example.com:6820

# Authentication
username =  # Optional, defaults to current user
token_lifespan = 3600  # Token validity in seconds

# Default resource allocations
default_partition = compute
default_cpus = 1
default_mem = 4G
default_time_limit = 01:00:00
default_account =  # Optional Slurm account

# Environment setup
airflow_venv =  # Path to Python venv, optional if using containers
default_container =  # Optional container image, e.g., docker://registry/airflow:2.8.0

# Executor behavior
sync_interval = 10  # Seconds between Slurm status polls
shutdown_mode = cancel  # Options: cancel, wait
shutdown_wait_timeout = 300  # Seconds to wait if shutdown_mode=wait

# API timeouts and retries
api_timeout = 30  # Seconds for API requests
api_max_retries = 3
Complete executor initialization:

python
class SlurmExecutor(BaseExecutor):
    def __init__(self):
        super().__init__()
        
        # API components
        self.slurm_api_url = None
        self.slurm_client = None
        self.token_manager = None
        
        # Configuration
        self.slurm_username = None
        self.token_lifespan = 3600
        self.default_partition = None
        self.default_cpus = None
        self.default_mem = None
        self.default_time_limit = None
        self.default_account = None
        self.airflow_home = None
        self.airflow_venv = None
        self.default_container = None
        
        # Executor behavior
        self.min_sync_interval = 10
        self.shutdown_mode = 'cancel'
        self.shutdown_wait_timeout = 300
        self.api_timeout = 30
        self.api_max_retries = 3
        
        # Runtime state
        self.running = {}  # {TaskInstanceKey: job_info}
        self.last_sync_time = 0
        
    def start(self):
        """Initialize the executor"""
        super().start()
        
        self.log.info("Starting SlurmExecutor")
        
        try:
            # Load configuration
            self._load_config()
            
            # Validate required config
            self._validate_config()
            
            # Initialize token manager
            self.token_manager = SlurmTokenManager(
                username=self.slurm_username,
                lifespan=self.token_lifespan
            )
            
            # Initialize API client
            self.slurm_client = SlurmAPIClient(
                base_url=self.slurm_api_url,
                token_manager=self.token_manager,
                timeout=self.api_timeout,
                max_retries=self.api_max_retries
            )
            
            # Test connection to Slurm
            self._validate_connection()
            
            self.log.info("SlurmExecutor started successfully")
            
        except Exception as e:
            self.log.error(f"Failed to start SlurmExecutor: {e}")
            raise
    
    def _load_config(self):
        """Load configuration from airflow.cfg"""
        from airflow.configuration import conf
        
        # Required settings
        self.slurm_api_url = conf.get('slurm', 'api_url')
        
        # Authentication
        self.slurm_username = conf.get('slurm', 'username', fallback=None)
        self.token_lifespan = conf.getint('slurm', 'token_lifespan', fallback=3600)
        
        # Resource defaults
        self.default_partition = conf.get('slurm', 'default_partition', fallback='compute')
        self.default_cpus = conf.getint('slurm', 'default_cpus', fallback=1)
        self.default_mem = conf.get('slurm', 'default_mem', fallback='4G')
        self.default_time_limit = conf.get('slurm', 'default_time_limit', fallback='01:00:00')
        self.default_account = conf.get('slurm', 'default_account', fallback=None)
        
        # Environment
        self.airflow_home = conf.get('core', 'airflow_home')
        self.dags_folder = conf.get('core', 'dags_folder')
        self.airflow_venv = conf.get('slurm', 'airflow_venv', fallback=None)
        self.default_container = conf.get('slurm', 'default_container', fallback=None)
        
        # Executor behavior
        self.min_sync_interval = conf.getint('slurm', 'sync_interval', fallback=10)
        self.shutdown_mode = conf.get('slurm', 'shutdown_mode', fallback='cancel')
        self.shutdown_wait_timeout = conf.getint('slurm', 'shutdown_wait_timeout', fallback=300)
        self.api_timeout = conf.getint('slurm', 'api_timeout', fallback=30)
        self.api_max_retries = conf.getint('slurm', 'api_max_retries', fallback=3)
    
    def _validate_config(self):
        """Validate required configuration is present"""
        if not self.slurm_api_url:
            raise ValueError("slurm.api_url must be configured")
        
        if not self.default_container and not self.airflow_venv:
            self.log.warning(
                "Neither default_container nor airflow_venv is configured. "
                "Tasks will execute in whatever environment is available on compute nodes."
            )
        
        # Validate URL format
        if not self.slurm_api_url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid slurm.api_url: {self.slurm_api_url}")
    
    def _validate_connection(self):
        """Test connection to Slurm REST API"""
        try:
            # Try to get API version/ping
            version = self.slurm_client.get_api_version()
            self.log.info(f"Connected to Slurm REST API version {version}")
            
            # Verify we can authenticate
            # Try a simple query like getting job list
            self.slurm_client.get_jobs()
            self.log.info("Successfully authenticated with Slurm")
            
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Slurm API at {self.slurm_api_url}: {e}")
Enhanced API client with configuration:

python
class SlurmAPIClient:
    def __init__(self, base_url, token_manager, timeout=30, max_retries=3):
        self.base_url = base_url.rstrip('/')
        self.token_manager = token_manager
        self.timeout = timeout
        self.max_retries = max_retries
        self.api_version = 'v0.0.42'
        self.session = requests.Session()
    
    def get_api_version(self):
        """Get Slurm REST API version"""
        url = f"{self.base_url}/openapi/v3"
        response = self.session.get(url, timeout=self.timeout)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('info', {}).get('version', 'unknown')
        
        # Fallback - just return configured version
        return self.api_version
    
    def get_jobs(self, job_ids=None):
        """
        Get job information
        job_ids: Optional list of specific job IDs to query
        """
        url = f"{self.base_url}/slurm/{self.api_version}/jobs"
        
        params = {}
        if job_ids:
            params['job_id'] = ','.join(str(jid) for jid in job_ids)
        
        response = self._request('GET', url, params=params)
        return response.json()
    
    def get_job_history(self, job_id):
        """Query accounting database for completed job"""
        url = f"{self.base_url}/slurm/{self.api_version}/job/{job_id}"
        
        try:
            response = self._request('GET', url)
            return response.json().get('jobs', [{}])[0]
        except SlurmAPIError:
            return None
    
    def _request(self, method, url, **kwargs):
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                kwargs.setdefault('timeout', self.timeout)
                kwargs.setdefault('headers', {}).update(self._auth_headers())
                
                response = self.session.request(method, url, **kwargs)
                
                if response.status_code < 400:
                    return response
                elif response.status_code == 401:
                    # Auth failed - force token refresh and retry
                    self.token_manager.token = None
                    if attempt < self.max_retries - 1:
                        continue
                
                # Non-retryable error
                raise SlurmAPIError(
                    f"API request failed: {response.status_code} - {response.text}"
                )
                
            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise SlurmAPIError(f"API request failed after {self.max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff

class SlurmAPIError(Exception):
    pass
Usage in Airflow:

Users would set the executor in airflow.cfg:

ini
[core]
executor = airflow.contrib.executors.slurm_executor.SlurmExecutor
# Or wherever you place it in the package structure
```

**Package structure:**
```
airflow_provider_slurm/
├── __init__.py
├── slurm_executor.py      # Main SlurmExecutor class
├── slurm_api_client.py    # SlurmAPIClient class
├── slurm_token_manager.py # SlurmTokenManager class
├── exceptions.py          # Custom exceptions
└── README.md
What's left for MVP:

Log path handling - implement _get_log_path() method
Script building - finalize _build_script() with proper escaping
Testing - unit tests and integration tests
Documentation - installation, configuration, usage examples
Packaging - setup.py for pip installation
Does this configuration structure cover everything you need? We could start fleshing out one of the remaining pieces, or discuss the testing/packaging strategy.

Yes, I think it makes sense

9 Dec
Excellent. This gives you a solid foundation for the MVP.

Summary of what we've designed:

Core architecture:

SlurmExecutor - main executor inheriting from BaseExecutor
SlurmAPIClient - handles REST API communication with retry logic
SlurmTokenManager - manages JWT tokens via scontrol token
Key features:

Job submission with configurable resources (CPU, memory, time, partition)
Log streaming via shared filesystem (Slurm stdout/stderr to Airflow log paths)
Container support via Slurm's --container flag
Fallback to venv activation for non-container deployments
State synchronization with configurable polling interval
Graceful shutdown with cancellation or wait modes
Support for Slurm REST API v0.0.42+
Configuration:

All settings via airflow.cfg [slurm] section
Reasonable defaults for most parameters
Clear validation and error messages
What makes this a good project for your goals:

Demonstrates systems integration - connecting two complex systems (Airflow + Slurm)
Production considerations - error handling, state management, cleanup
Real-world utility - fills actual gap in the ecosystem
Open source contribution - something the community would actually use
Shows your HPC expertise - leverages your deep Slurm API knowledge
Next practical steps:

Set up a basic project structure and repository
Implement the core classes we've outlined
Write some integration tests (might need a test Slurm cluster or mock)
Create example DAGs showing usage
Write documentation
For your job search positioning, you could frame this as:

"Built open-source Airflow executor enabling ML workflows on HPC infrastructure"
"Integrated Apache Airflow with Slurm via REST API, solving environment gap for orgs with existing HPC investments"
Shows you can ship complete, production-ready integrations
Want to discuss implementation priorities, testing strategy, or how to structure the repo for maximum impact?

Do we have enough of a plan to write a detailed specification that could be used for development?

9 Dec
Yes, absolutely. We have enough to write a comprehensive specification. Let me structure this as a development-ready document.

Slurm Executor for Apache Airflow - Technical Specification
1. Overview
1.1 Purpose
Implement a custom Airflow executor that enables task execution on HPC clusters running Slurm, using the Slurm REST API for job submission and monitoring.

1.2 Goals
Enable Airflow workflows to leverage existing Slurm/HPC infrastructure
Provide seamless integration using modern Slurm REST API (v0.0.42+)
Support both containerized and virtual environment execution modes
Maintain compatibility with Airflow's executor interface and logging system
1.3 Non-Goals (for MVP)
Support for Slurm API versions older than v0.0.42
MPI or multi-node job support
Slurm job arrays for mapped tasks
Advanced Slurm features (reservations, licenses, GRES beyond basic resources)
2. Architecture
2.1 Component Diagram
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
2.2 Key Classes
2.2.1 SlurmExecutor
Inheritance: airflow.executors.base_executor.BaseExecutor

Responsibilities:

Translate Airflow task instances to Slurm jobs
Submit jobs via SlurmAPIClient
Poll job status and update Airflow state
Handle graceful and emergency shutdown
Manage task-to-job-ID mapping
State:

python
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
2.2.2 SlurmAPIClient
Responsibilities:

HTTP communication with Slurm REST API
Request retry logic with exponential backoff
Authentication header management
API version compatibility
Methods:

submit_job(job_spec: dict) -> dict - Submit job, return job_id
get_jobs(job_ids: Optional[List[int]]) -> dict - Query active jobs
get_job_history(job_id: int) -> Optional[dict] - Query accounting DB
cancel_job(job_id: int) -> dict - Cancel a job
get_api_version() -> str - Discover API version
2.2.3 SlurmTokenManager
Responsibilities:

Generate JWT tokens via scontrol token
Cache tokens until near expiration
Automatic token refresh
Methods:

get_token() -> str - Get valid token (refresh if needed)
_fetch_new_token() -> str - Call scontrol to generate token
_token_is_valid() -> bool - Check if cached token is still valid
3. Detailed Design
3.1 Task Submission Flow
3.1.1 execute_async() Implementation
python
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
3.1.2 Job Specification Building
Function: _build_job_spec(key, command, queue, executor_config) -> dict

Logic:

Extract task instance metadata (dag_id, task_id, execution_date, try_number)
Determine log file path using Airflow's log handler
Build job name: airflow-{dag_id}-{task_id}-{try_number}
Construct bash script with proper escaping
Apply resource requirements from executor_config or defaults
Set environment variables
Configure working directory
Job Spec Structure (Slurm REST API v0.0.42):

json
{
  "script": "#!/bin/bash\nset -euo pipefail\n...",
  "job": {
    "name": "airflow-my_dag-my_task-1",
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
    "account": "airflow_account",  // Optional
    "qos": "normal",  // Optional
    "container": "docker://registry/airflow:2.8.0"  // Optional
  }
}
3.1.3 Script Generation
Two modes based on configuration:

Mode 1: Container-based (if container specified)

bash
#!/bin/bash
set -euo pipefail

# Airflow command (container has environment)
airflow tasks run my_dag my_task 2024-01-01T00:00:00+00:00 --local --pool default --subdir /shared/airflow/dags/my_dag.py
Mode 2: Virtual environment (if venv specified, no container)

bash
#!/bin/bash
set -euo pipefail

# Activate virtual environment
source /shared/airflow/venv/bin/activate

# Execute Airflow task
airflow tasks run my_dag my_task 2024-01-01T00:00:00+00:00 --local --pool default --subdir /shared/airflow/dags/my_dag.py
Command escaping: Use shlex.quote() for all command arguments

3.1.4 Resource Mapping
Executor config schema:

python
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
Defaults (from config):

partition: default_partition (config: 'compute')
cpus_per_task: default_cpus (config: 1)
mem: default_mem (config: '4G')
time_limit: default_time_limit (config: '01:00:00')
Precedence: executor_config > defaults > Slurm cluster defaults

3.2 Status Synchronization
3.2.1 sync() Implementation
Called periodically by Airflow scheduler (every few seconds)

python
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
3.2.2 Fetching Job Statuses
Function: _fetch_job_statuses() -> Dict[int, dict]

python
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
3.2.3 State Mapping
Slurm job states → Airflow task states:

Slurm State	Airflow Action	Notes
PENDING	No action	Job queued, keep as running in Airflow
CONFIGURING	No action	Job being configured
RUNNING	No action	Job executing
COMPLETED (exit_code=0)	success(key)	Task succeeded
COMPLETED (exit_code≠0)	fail(key)	Task failed with error
FAILED	fail(key)	Job failed
TIMEOUT	fail(key)	Job exceeded time limit
CANCELLED	fail(key)	Job was cancelled
NODE_FAIL	fail(key)	Node failure
OUT_OF_MEMORY	fail(key)	OOM kill
PREEMPTED	fail(key)	Job preempted (Airflow will retry)
Implementation:

python
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
3.2.4 Missing Job Handling
Function: _handle_missing_job(key, job_info) -> None

Logic:

Job not in active queue - might be completed and purged
Query accounting database for historical record
If found in accounting: process final state
If not found: track how long it's been missing
If missing > 5 minutes: mark as failed
python
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
Implementation:

python
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
3.3.2 Shared Filesystem Requirements
Prerequisites:

Shared filesystem accessible from:
Airflow scheduler (writes job specs, reads config)
Slurm compute nodes (write logs)
Airflow web server (reads logs for UI)
Common setups:

NFS mounted at same path on all nodes
Lustre parallel filesystem
GPFS or other cluster filesystems
Validation on startup:

python
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
3.3.3 Log Streaming Behavior
How it works:

Slurm writes stdout/stderr to log file on shared FS
Airflow's FileTaskHandler tails this file when user views logs in UI
Live streaming works because file is actively being written to
Buffering consideration:

Slurm buffers stdout/stderr (typically line-buffered)
Minor delay (seconds) between task output and log visibility
Acceptable for most use cases
3.4 Authentication
3.4.1 Token Generation
Method: Use scontrol token command

Command format:

bash
scontrol token lifespan=3600 username=airflow_user
```

**Output format:**
```
SLURM_JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
3.4.2 SlurmTokenManager Implementation
python
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
3.4.3 API Authentication Headers
Header format:

python
headers = {
    'X-SLURM-USER-TOKEN': token,
    'Content-Type': 'application/json',
}
Implementation in SlurmAPIClient:

python
def _auth_headers(self) -> dict:
    """Get headers with fresh authentication token"""
    token = self.token_manager.get_token()
    return {
        'X-SLURM-USER-TOKEN': token,
        'Content-Type': 'application/json',
    }
3.5 Cleanup and Shutdown
3.5.1 Graceful Shutdown: end()
python
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
Cancel all jobs:

python
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
Wait for jobs:

python
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
3.5.2 Emergency Shutdown: terminate()
python
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
3.5.3 Job Cancellation API
SlurmAPIClient method:

python
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
4. Configuration
4.1 Configuration File (airflow.cfg)
ini
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
4.2 Executor Configuration in Tasks
Per-task overrides using executor_config:

python
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
Supported executor_config keys:

partition (str): Slurm partition name
cpus_per_task (int): Number of CPU cores
mem (str): Memory allocation (e.g., '16G', '32768M')
time_limit (str): Wall time limit (HH:MM:SS format)
account (str): Slurm account
qos (str): Quality of Service
container (str): Container image (docker://...)
working_dir (str): Working directory override
4.3 Environment Variables
Alternative to airflow.cfg (useful for containerized deployments):

bash
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
5. Error Handling
5.1 API Errors
Retry strategy:

python
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
5.2 Token Generation Failures
Handling:

Log error with details
Retry with exponential backoff
After max retries, raise exception (executor will fail)
Common causes:

scontrol binary not in PATH
User doesn't have permission to generate tokens
Slurm controller unreachable
5.3 Job Submission Failures
Possible failures:

API unreachable
Invalid job specification
User over quota
Partition doesn't exist
Insufficient resources
Handling:

python
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
5.4 State Synchronization Failures
If sync() fails (API down):

Log warning
Return without updating state
Tasks remain in current state
Will retry on next sync() call
Tasks eventually timeout if Slurm stays down
If individual job disappears unexpectedly:

Check accounting database
Track how long it's been missing
After 5 minutes, mark as failed
5.5 Shutdown Failures
If jobs can't be cancelled during shutdown:

Log errors but continue
Don't block shutdown
Jobs will continue running on Slurm
Orphaned jobs can be cleaned up manually with scancel
6. Testing Strategy
6.1 Unit Tests
Mock components:

Mock SlurmAPIClient responses
Mock subprocess.run for token generation
Test state transitions
Test configuration loading
Test error handling paths
Example test structure:

python
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
6.2 Integration Tests
Requirements:

Access to Slurm cluster with REST API
Or Docker container running Slurm (e.g., slurm-docker-cluster)
Test scenarios:

Submit simple job, wait for completion
Submit job that fails (exit code 1)
Submit job that times out
Cancel running job
Executor shutdown with running jobs
Token refresh during long-running test
API unavailable during sync
Example integration test:

python
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

## 7. Implementation Roadmap

### 7.1 Phase 1: Core Implementation (MVP)

**Deliverables:**
- [ ] Project structure and packaging setup
- [ ] `SlurmTokenManager` class
- [ ] `SlurmAPIClient` class (submit, query, cancel)
- [ ] `SlurmExecutor` class (execute_async, sync, end, terminate)
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
- [ ] Support for older API versions (v0.0.40)
- [ ] Job arrays for mapped tasks
- [ ] Advanced Slurm features (GRES, licenses)
- [ ] Metrics and monitoring
- [ ] Alternative auth methods (beyond scontrol token)
- [ ] S3 log backend (for non-shared-filesystem setups)

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
9.3 System Dependencies
Python 3.8+
Access to Slurm cluster with REST API (slurmrestd) v0.0.42+
scontrol binary in PATH (for token generation)
Shared filesystem between scheduler and compute nodes (for logs)
10. API Reference
10.1 SlurmExecutor
Public Methods:

python
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
10.2 SlurmAPIClient
python
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
10.3 SlurmTokenManager
python
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
11. Troubleshooting Guide
11.1 Common Issues
Issue: "Failed to connect to Slurm API"

Verify api_url is correct
Check slurmrestd is running: systemctl status slurmrestd
Test connectivity: curl -k https://slurm-head:6820/openapi/v3
Check firewall rules
Issue: "Token generation failed"

Verify scontrol is in PATH: which scontrol
Check user has permission: scontrol token lifespan=60
Verify Slurm is running: scontrol ping
Issue: "Jobs not appearing in Airflow logs"

Verify log path is on shared filesystem
Check file permissions on log directory
Verify compute nodes can write to log path
Check Slurm job output: cat slurm-12345.out
Issue: "Tasks stuck in running state"

Check if jobs are actually running: squeue -u $USER
Check scheduler logs for sync errors
Verify API is responding: curl -k https://slurm-head:6820/slurm/v0.0.42/jobs
Increase sync_interval if API is slow
Issue: "Container not found"

Verify Slurm has container support: scontrol show config | grep PluginDir
Check container image is accessible from compute nodes
Test manually: srun --container=docker://image:tag /bin/bash
11.2 Debugging Tips
Enable debug logging:

python
import logging
logging.getLogger('airflow_provider_slurm').setLevel(logging.DEBUG)
Check Slurm job details:

bash
scontrol show job 12345
sacct -j 12345 -o JobID,JobName,State,ExitCode,Reason
Inspect job script:

bash
scontrol write batch_script 12345 /tmp/job_script.sh
cat /tmp/job_script.sh
Test job submission manually:

bash
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
12. Success Criteria
12.1 Functional Requirements
 Submit Airflow tasks as Slurm jobs via REST API ✓
 Monitor job status and update Airflow task states ✓
 Stream logs to Airflow UI ✓
 Support resource customization per task ✓
 Handle failures and retries ✓
 Graceful shutdown ✓
12.2 Non-Functional Requirements
Performance: Sync interval of 10-30 seconds acceptable
Reliability: Handle API failures without losing task state
Scalability: Support hundreds of concurrent jobs
Usability: Simple configuration, clear error messages
Maintainability: Clean code, well documented, tested
12.3 Documentation Requirements
Installation guide
Configuration reference
Usage examples
API documentation
Troubleshooting guide
Development/contribution guide
Appendices
A. Slurm REST API Endpoints Used
Base URL: https://slurm-head:6820/slurm/v0.0.42

Endpoint	Method	Purpose
/job/submit	POST	Submit new job
/jobs	GET	List active jobs
/job/{job_id}	GET	Get job details (including accounting)
/job/{job_id}	DELETE	Cancel job
/openapi/v3	GET	Get API version info
B. Slurm Job State Reference
State	Description	Terminal?
PENDING	Job waiting in queue	No
CONFIGURING	Job being configured	No
RUNNING	Job executing	No
COMPLETED	Job finished	Yes
FAILED	Job failed	Yes
TIMEOUT	Job exceeded time limit	Yes
CANCELLED	Job was cancelled	Yes
NODE_FAIL	Node failure	Yes
PREEMPTED	Job preempted by higher priority	Yes
OUT_OF_MEMORY	Job killed for OOM	Yes
C. Example DAGs
Simple DAG:

python
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
Resource-intensive DAG:

python
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
This specification provides a complete blueprint for implementing the Slurm executor. It covers architecture, detailed design, configuration, error handling, testing strategy, and implementation roadmap. The document can be used directly for development and serves as comprehensive documentation for the project.

Anything else we need to cover?

9 Dec
I think we've covered the essentials comprehensively. Let me do a quick check of potential gaps:

Covered thoroughly:

✓ Architecture and components
✓ Job submission, state sync, cleanup
✓ Logging via shared filesystem
✓ Authentication with scontrol token
✓ Configuration and executor_config
✓ Error handling and retry logic
✓ Testing strategy
✓ Implementation roadmap
Potential additions (minor, not blockers for MVP):

Metrics/observability - Not essential for MVP, but you might want to mention:
Executor could expose metrics (jobs submitted, success rate, sync duration)
Could integrate with Airflow's StatsD if desired
Add logging of key events for operational visibility
Security considerations - Brief mention of:
JWT tokens should be handled securely (not logged)
Log files may contain sensitive data
User permissions model (who can submit to which partitions)
Performance tuning guidance - Quick note on:
Batch size for status queries if tracking 100+ jobs
Tradeoffs of sync_interval setting
Impact of shared filesystem performance on log streaming
Migration/compatibility - If this matters:
How users would migrate from LocalExecutor or CeleryExecutor
Any Airflow version compatibility notes beyond "2.5+"
Future architecture considerations - Optional section on:
How to extend for job arrays (mapped tasks)
Hook points for custom behavior
Plugin architecture if you want others to extend
My assessment: The spec is development-ready as-is. The items above are polish that could be added during implementation or in documentation, not prerequisites for starting development.

One practical question: Do you want to add a section on development environment setup (how to set up a local Slurm cluster for testing, recommended IDE setup, etc.)? That might help future contributors, but isn't strictly part of the technical spec.

Otherwise, I think you have everything you need to start building. Want to proceed with any of these additions, or are you ready to begin implementation?

If we compare to kubernetes, celery or dask executors is there some main functionality in those that we are not implementing in the slurm executor?

9 Dec
Good question - let me compare against those three executors to identify any functional gaps.

Comparison Analysis
1. KubernetesExecutor
Features they have that we don't (yet):

Pod template customization - They allow full Kubernetes pod specs (volumes, init containers, sidecars, node selectors, tolerations, etc.). Our executor_config is more limited to basic Slurm parameters.
Impact: Medium. We could expand executor_config to support more Slurm features (constraints, GRES, etc.)
Per-task Docker images - They support different images per task natively. We support this via executor_config['container'], so we're covered.
Resource quotas and namespaces - K8s has namespace isolation. Slurm has accounts/QoS which we support, so roughly equivalent.
Task logs via Kubernetes API - They can stream logs from pods even without shared filesystem. We require shared filesystem or would need the sidecar approach we discussed.
Impact: Medium. This limits deployment flexibility. Worth noting as a known limitation.
Automatic cleanup of completed pods - K8s executor can delete pods after task completion. Slurm jobs disappear from active queue automatically, so covered.
Features we have that they don't:

Native HPC resource concepts (partitions, QoS, accounts)
Job history via accounting database
2. CeleryExecutor
Features they have that we don't:

Task queues with routing - Celery has sophisticated queue routing (task → queue → worker pool). We map Airflow queues to Slurm partitions, which is simpler but functional.
Worker autoscaling - Celery can dynamically scale workers. Slurm handles this at the cluster level (elastic scaling), not something the executor controls. Different model, not a gap.
Task revocation - Celery can revoke queued tasks before execution. We can cancel submitted Slurm jobs, so covered.
Task result backend - Celery stores task results in Redis/database. Airflow handles this via XCom regardless of executor, so not relevant.
Heartbeat/worker health monitoring - Celery tracks worker health. Slurm does this at the cluster level. Different model.
Features we have that they don't:

No need for separate worker infrastructure
Native resource guarantees (CPU, memory)
3. DaskExecutor
Features they have that we don't:

Dynamic resource allocation - Dask can dynamically request more workers. Slurm allocates resources per job at submit time. Different model - ours is more predictable, theirs is more flexible.
Task clustering - Dask can group small tasks together for efficiency. We submit one Slurm job per Airflow task.
Impact: High for workloads with many tiny tasks (seconds each). Could be a performance issue.
Mitigation: Document that this executor is better suited for longer-running tasks (minutes+)
Distributed data structures - Dask has built-in distributed arrays/dataframes. Not relevant to executor functionality.
Adaptive scaling - Dask can scale workers based on workload. Slurm does this at cluster level. Different model.
Features we have that they don't:

Better integration with existing HPC infrastructure
Resource guarantees and queuing
Key Functional Gaps
Gap 1: Task Adoption (Medium Priority)
What it is: When Airflow scheduler restarts, executors can "adopt" tasks that were already running.

Status in other executors:

KubernetesExecutor: Supports adoption (reconnects to running pods)
CeleryExecutor: Supports adoption (queries workers)
DaskExecutor: Limited support
Our spec: We return empty list from try_adopt_task_instances()

Impact: If Airflow scheduler restarts, running Slurm jobs continue but Airflow loses track of them. They'll eventually complete but Airflow won't know.

Fix: We could implement adoption by:

On startup, query Slurm for jobs with names matching our pattern (airflow-*)
Parse job names to reconstruct TaskInstanceKeys
Rebuild self.running dict
Continue tracking those jobs
Should we add this? Yes, for production readiness. Not critical for MVP but relatively straightforward.

Gap 2: Task Parallelism at Task Level (Low Priority)
What it is: Some executors support running multiple instances of the same task in parallel (task parallelism, not DAG parallelism).

Status: KubernetesExecutor and CeleryExecutor support this via pool slots.

Our implementation: Each Airflow task = one Slurm job. We support DAG-level parallelism (multiple tasks running concurrently) but not task-level.

Impact: Minimal. This is rarely used in practice.

Gap 3: Logging Without Shared Filesystem (Medium Priority)
What it is: Support for deployments without shared filesystem.

Status in other executors:

KubernetesExecutor: Uses K8s API to fetch logs from pods
CeleryExecutor: Workers can push logs to S3/GCS
DaskExecutor: Can use remote log storage
Our spec: Requires shared filesystem for logs (or future S3 sidecar option)

Impact: Limits deployment flexibility. Some environments might not have shared FS.

Mitigation: Document as requirement, keep S3 sidecar as Phase 4 enhancement.

Gap 4: Small Task Efficiency (Low-Medium Priority)
What it is: Overhead for very short tasks (seconds).

Issue: Slurm has per-job overhead (submission, scheduling, startup). For tasks that run in seconds, this overhead is proportionally high.

Status in other executors:

DaskExecutor: Can batch small tasks
CeleryExecutor: Workers are persistent, no startup overhead per task
KubernetesExecutor: Also has pod startup overhead
Impact: Performance degradation for DAGs with hundreds of tiny tasks.

Mitigation:

Document that executor is optimized for medium-to-long running tasks (minutes+)
Could add task batching in Phase 4 (submit multiple tasks as Slurm job array)
Recommendation
Add to spec now (for MVP completeness):

Task adoption - This is important for production reliability. Add a section:
python
def try_adopt_task_instances(self, tis: List[TaskInstance]) -> List[TaskInstance]:
    """
    Adopt running tasks after scheduler restart
    
    On startup, query Slurm for jobs matching our naming pattern,
    reconstruct task tracking, and continue monitoring them.
    """
    # Query Slurm for jobs with name pattern "airflow-*"
    # Parse job names to extract dag_id, task_id, execution_date, try_number
    # Rebuild self.running dict
    # Return list of successfully adopted TIs
Document as known limitations:

Shared filesystem requirement - Note that logs require shared FS (or future S3 option)
Task granularity - Best for tasks that run minutes or longer; high overhead for second-long tasks
No task-level parallelism - Each Airflow task = one Slurm job
Keep as Phase 4 enhancements:

Job arrays for mapped tasks (Airflow 2.3+ dynamic task mapping)
S3 log backend for non-shared-FS deployments
Task batching for small tasks
Updated Spec Section
I'd add this to Section 3:

3.6 Task Adoption (Scheduler Restart Recovery)
Purpose: When Airflow scheduler restarts, reconnect to jobs that were already running.

Implementation:

python
def try_adopt_task_instances(self, tis: List[TaskInstance]) -> List[TaskInstance]:
    """
    Adopt tasks that are already running after scheduler restart
    
    Args:
        tis: List of task instances to potentially adopt
        
    Returns:
        List of successfully adopted task instances
    """
    adopted = []
    
    # Build set of task keys we're looking for
    target_keys = {ti.key for ti in tis}
    
    try:
        # Query Slurm for all our jobs (name pattern: airflow-*)
        # This requires job name filtering, which may need separate queries
        response = self.slurm_client.get_jobs()
        
        for job in response.get('jobs', []):
            job_name = job.get('name', '')
            if not job_name.startswith('airflow-'):
                continue
            
            # Parse: "airflow-{dag_id}-{task_id}-{try_number}"
            try:
                parts = job_name.split('-', 3)  # ['airflow', dag_id, task_id, try_number]
                if len(parts) < 4:
                    continue
                
                dag_id, task_id, try_number = parts[1], parts[2], int(parts[3])
                
                # Find matching TI
                for ti in tis:
                    if (ti.dag_id == dag_id and 
                        ti.task_id == task_id and 
                        ti.try_number == try_number):
                        
                        # Adopt this job
                        key = ti.key
                        self.running[key] = {
                            'slurm_job_id': job['job_id'],
                            'command': [],  # Unknown, but not critical
                            'submit_time': datetime.now(),  # Approximate
                        }
                        adopted.append(ti)
                        self.log.info(f"Adopted task {key} as Slurm job {job['job_id']}")
                        break
                        
            except (ValueError, IndexError) as e:
                self.log.debug(f"Could not parse job name {job_name}: {e}")
                continue
        
        return adopted
        
    except SlurmAPIError as e:
        self.log.error(f"Failed to query jobs for adoption: {e}")
        return []
Limitations:

Job name must encode dag_id, task_id, try_number (already in our design)
Cannot recover exact command or submit_time (but these aren't critical for monitoring)
Execution date not in job name - we rely on try_number being unique enough
Alternative: include execution_date hash in job name for uniqueness
Bottom line: The spec is solid. The main functional gap is task adoption which you should add for production use. The other differences are either architectural (different models, not gaps) or acceptable limitations for the use case (shared FS requirement, task granularity).

Want me to write the full task adoption section to add to the spec?

We should add the full task adoption. In regards to slurm array jobs and short jobs what more could we do there?

9 Dec
Yes, let me write the full task adoption section and then explore job arrays for short tasks.

Full Task Adoption Section
Add this as Section 3.6 in the spec:

3.6 Task Adoption (Scheduler Restart Recovery)
3.6.1 Purpose
When the Airflow scheduler restarts (due to crash, deployment, or maintenance), tasks that were running continue to execute on Slurm. Task adoption allows the restarted scheduler to reconnect to these in-flight jobs and continue monitoring them, rather than losing track and potentially re-submitting them.

3.6.2 Design Approach
Strategy: Query Slurm for jobs matching our naming convention and reconstruct tracking state.

Job Name Encoding: Job names must contain sufficient information to uniquely identify the task instance. Our format:

airflow-{dag_id}-{task_id}-{run_id_hash}-{try_number}
Where:

dag_id: DAG identifier
task_id: Task identifier
run_id_hash: First 8 chars of SHA256 hash of run_id (handles execution_date/logical_date uniqueness)
try_number: Attempt number
Example: airflow-ml_pipeline-train_model-a3f5b2c1-1

Why hash run_id? Slurm job names have length limits and run_ids can be long (especially with manual run_ids). The hash ensures uniqueness while keeping names manageable.

3.6.3 Implementation
Update job name generation in _build_job_spec():

python
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
Implement try_adopt_task_instances():

python
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
Update execute_async() to use new job name:

python
def _build_job_spec(self, key, command, queue, executor_config):
    # ... other code ...
    
    job_name = self._build_job_name(key)
    
    job_spec = {
        'name': job_name,  # Use structured name instead of simple concatenation
        # ... rest of spec
    }
    
    return job_spec
3.6.4 Edge Cases and Limitations
Name collisions:

Hash collisions are extremely unlikely (SHA256 with 8 chars = 2^32 possibilities)
Within same DAG, run_ids are unique by design
If collision somehow occurs, try_number provides additional differentiation
Job name length limits:

Slurm typically allows 256 character job names
Our implementation truncates long dag_id/task_id if needed
Hash remains intact to preserve uniqueness
Unknown command:

Adopted jobs have empty command in tracking dict
Not needed for monitoring (only job_id matters)
Prevents re-execution but doesn't affect success/failure detection
Timing windows:

Small window between scheduler crash and restart where job might complete
If job finishes before adoption scan, it won't be adopted (acceptable - sync will handle)
If job finishes during adoption scan, handled gracefully (terminal state check)
Multiple schedulers:

In multi-scheduler setups (HA), both schedulers see same Slurm jobs
Airflow's DB coordination prevents double-adoption
Job name uniqueness ensures correct scheduler adopts correct tasks
3.6.5 Testing Task Adoption
Unit test:

python
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
Integration test:

python
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
Job Arrays for Short Tasks
Now let's tackle the job arrays optimization for handling many short tasks efficiently.

3.7 Job Arrays for Short Task Optimization (Phase 4 Enhancement)
3.7.1 Problem Statement
Issue: Slurm has per-job overhead:

Job submission: ~100-500ms
Scheduler evaluation: ~50-200ms
Job startup on compute node: ~500ms-2s
For tasks that run in seconds, this overhead becomes significant. A DAG with 100 tasks that each run 5 seconds could spend:

Job overhead: 100 × 1s = 100s
Actual work: 100 × 5s = 500s
Efficiency: 500/(500+100) = 83%
Solution: Use Slurm job arrays to batch multiple tasks into a single job submission.

3.7.2 Slurm Job Arrays Overview
What they are: A single job submission that creates multiple job array elements, each running the same script with different parameters.

Example:

bash
sbatch --array=0-99 task_script.sh
# Creates jobs: jobid_0, jobid_1, ... jobid_99
# Each gets environment variable: SLURM_ARRAY_TASK_ID
Benefits:

Single API call submits N tasks
Scheduler processes array as unit (faster)
Shared startup overhead
Better cluster utilization
3.7.3 Design Approach
Strategy: Detect when multiple tasks can be batched and submit as job array.

Batching criteria:

Tasks ready to submit at same time
Similar resource requirements (CPU, memory, partition)
Same container/environment
Short estimated duration (configurable threshold)
Implementation options:

Option A: Implicit batching (aggressive)

Executor automatically batches compatible tasks
Transparent to users
Risk: Unexpected behavior
Option B: Explicit batching (conservative)

Users opt-in via executor_config flag
More predictable
Requires user awareness
Recommendation: Start with Option B (explicit) for safety.

3.7.4 Implementation Design
Configuration:

ini
[slurm]
# Enable job array support
enable_job_arrays = True

# Maximum tasks per array (Slurm typically limits to 1000-10000)
max_array_size = 1000

# Batch window: wait this long to accumulate tasks before submitting array
batch_window_seconds = 5

# Only batch tasks estimated shorter than this
batch_task_duration_threshold = 300  # 5 minutes
Executor_config for opt-in:

python
@task(executor_config={
    'cpus_per_task': 2,
    'mem': '4G',
    'enable_batching': True,  # Opt into job arrays
    'estimated_duration': 60,  # Help executor decide if worth batching
})
def short_task(index: int):
    # Task that runs ~60 seconds
    pass
Modified execute_async() with batching:

python
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
Build array job spec:

python
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
API client support for arrays:

python
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
Enhanced status querying:

python
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
3.7.5 Logging for Array Jobs
Challenge: Each array element needs its own log file.

Solution: Use Slurm's array-aware output path patterns:

python
def _get_array_log_path(self, key, array_index):
    """Get log path for array job element"""
    base_log_path = self._get_log_path(key)
    # Slurm pattern: %A = array job ID, %a = array task ID
    # But we're specifying exact path, so include index in filename
    log_dir = os.path.dirname(base_log_path)
    log_name = os.path.basename(base_log_path)
    
    return os.path.join(log_dir, f"array_{array_index}_{log_name}")
In job spec:

python
'standard_output': f'/logs/airflow-array-%A_%a.log'  # %A=job_id, %a=array_index
Actually, better approach - the wrapper script should redirect to task-specific log:

python
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
3.7.6 Benefits and Trade-offs
Benefits:

Reduced overhead: Single API call for N tasks
Faster scheduling: Slurm scheduler processes array as unit
Better throughput: For workloads with many small tasks, 2-5x speedup possible
Trade-offs:

Complexity: More complex code, harder to debug
Batching delay: Tasks wait up to batch_window seconds before submission
All-or-nothing: If array job fails to submit, all tasks in batch affected
Resource uniformity: All tasks in array must have same resources
When to use:

DAGs with 50+ similar short tasks (< 5 min each)
High-throughput data processing pipelines
Parameter sweeps, batch inference
When not to use:

Long-running tasks (overhead already small %)
Tasks with varied resource needs
Critical path tasks (latency-sensitive)
3.7.7 Configuration Example
Example DAG using batching:

python
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
Expected behavior:

Without batching: 100 separate Slurm jobs, ~100s overhead + 3000s work = 3100s total
With batching: ~10 job arrays of 10 tasks each (limited by batch window), ~10s overhead + 3000s work = 3010s total
Actual batching depends on task submission timing and batch_window
3.7.8 Alternative: Dynamic Task Mapping Integration
Airflow 2.3+ introduced dynamic task mapping. This could integrate naturally:

python
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
Executor could detect mapped tasks and automatically batch them since:

They're all from same task definition (same resources)
They're submitted at once
They're independent
This could be even more transparent than explicit opt-in.

Summary
Task Adoption: Full implementation added to spec. This is important for production reliability.

Job Arrays: Comprehensive design for Phase 4 enhancement. Key points:

Opt-in initially (executor_config flag) for safety
Batch compatible tasks (same resources, short duration)
Use manifest file to map array indices to task commands
Wrapper script handles task lookup and execution
Best for workloads with 50+ similar short tasks
Recommendation for MVP:

Include task adoption (Section 3.6) - straightforward and important
Document job arrays as Phase 4 - it's complex and can wait
Trade-off assessment:

Task adoption: High value, low complexity → include in MVP
Job arrays: High value for specific workloads, high complexity → defer to Phase 4
Want me to add anything else to either section?

Looks good, anything else we need to add?

9 Dec
I think we have a comprehensive, development-ready specification. Let me do a final checklist:

Core functionality: ✓

Job submission, status sync, cleanup
Logging integration
Authentication
Configuration
Error handling
Production features: ✓

Task adoption for scheduler restarts
Graceful and emergency shutdown
Resource customization
Documentation: ✓

API reference
Configuration guide
Troubleshooting
Testing strategy
Implementation roadmap
Future enhancements documented: ✓

Job arrays for short tasks
S3 log backend option
Older API version support
One small addition I'd suggest: A "Quick Start" section at the beginning of the spec for developers jumping in. Something like:

Section 1.4: Quick Start for Developers
Prerequisites:

Python 3.8+
Access to Slurm cluster with REST API v0.0.42+
scontrol binary in PATH
Basic development workflow:

Clone and setup:
bash
git clone <repo>
cd airflow-slurm-executor
pip install -e .
pip install -r requirements-dev.txt
Configure Airflow:
ini
# airflow.cfg
[core]
executor = airflow_provider_slurm.SlurmExecutor

[slurm]
api_url = https://your-slurm-cluster:6820
default_partition = compute
Run tests:
bash
pytest tests/unit/
pytest tests/integration/  # Requires Slurm access
Test with example DAG:
bash
cp examples/simple_dag.py ~/airflow/dags/
airflow dags test simple_slurm_dag
Key files to start with:

slurm_executor.py - Main executor logic
slurm_api_client.py - REST API wrapper
tests/unit/test_executor.py - Test examples
Otherwise, the specification is complete and ready for implementation. It covers:

✅ All core executor functionality
✅ Production reliability features
✅ Clear API contracts
✅ Testing approach
✅ Configuration schema
✅ Error handling patterns
✅ Future enhancement paths
✅ Comparison with other executors
You have everything you need to:

Start coding immediately
Know what to build and in what order
Handle edge cases properly
Test thoroughly
Document for users
For your job search: This spec itself demonstrates strong technical planning and systems thinking. You could share it with potential employers as evidence of your approach to building production infrastructure.

Ready to start building? The MVP roadmap suggests 2-3 weeks for core implementation, which feels achievable given your Slurm API experience. Good luck with the project!

Can you put this all together in a downloadable markdown file?

10 Dec
I'll create a complete markdown file with all the content we've developed.


Creating comprehensive technical specification document for Slurm Executor for Apache Airflow
slurm-executor-specification.md

Presented file
Perfect! I've created a comprehensive markdown specification document that includes everything we discussed:

Contents:

Complete architecture and component design
Detailed implementation for all core features
Task adoption for scheduler restart recovery
Job arrays design for short task optimization (Phase 4)
Full configuration reference
Error handling strategies
Testing approach
Implementation roadmap
Troubleshooting guide
Example DAGs
Comparison with other executors
The document is production-ready and can serve as:

Development blueprint - Complete technical specs for implementation
Project documentation - Reference for users and contributors
Portfolio piece - Demonstrates your technical planning and systems thinking skills
You can download it now and start building immediately. Good luck with the project!

Slurm executor specification
Document · MD 





Claude is AI and can make mistakes. Please double-check responses.
Claude
