#!/usr/bin/env python3
"""Quick diagnostic test to check why jobs stay in PENDING."""

import sys
from unittest.mock import MagicMock

sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from tests.utils.cluster_helpers import (
    fetch_token_via_ssh,
    get_cluster_config,
    is_cluster_available,
)



def main():
    print("🔍 Quick Diagnostic Test")
    print("-" * 30)

    # Check cluster availability
    print("Checking cluster availability...")
    if not is_cluster_available():
        print("❌ Cluster is not available or accessible")
        print("   Check SLURM_TEST_CLUSTER_HOST and SSH configuration")
        return False

    config = get_cluster_config()
    print(f"✅ Cluster {config['host']} is available\n")

    # Fetch token automatically
    print("Fetching authentication token...")
    token = fetch_token_via_ssh(
        host=config["host"], user=config["user"], ssh_key=config["ssh_key"]
    )

    if not token:
        print("❌ Failed to fetch authentication token")
        return False

    print(f"✅ Token fetched: {token[:20]}...\n")

    # Create API client with real token
    token_manager = MagicMock()
    token_manager.get_token.return_value = token
    base_url = f"http://{config['host']}:{config['port']}"
    client = SlurmAPIClient(base_url, token_manager)

    # Check current queue
    print("1. Checking current job queue...")
    try:
        response = client.get_jobs()
        jobs = response.get("jobs", [])
        print(f"   Found {len(jobs)} jobs in queue")

        for job in jobs:
            job_id = job.get("job_id")
            name = job.get("name", "unknown")
            state = job.get("job_state", "unknown")
            partition = job.get("partition", "unknown")
            print(f"   Job {job_id}: {name} - {state} on {partition}")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Submit minimal test job
    print("\\n2. Submitting minimal test job...")
    minimal_job = {
        "script": "#!/bin/bash\\necho 'Minimal test'\\ndate\\nhostname",
        "job": {
            "name": "minimal-test",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "10M",
            "time_limit": 30,
            "current_working_directory": "/tmp",
        },
    }

    try:
        response = client.submit_job(minimal_job)
        job_id = response.get("job_id")
        print(f"   ✅ Submitted job {job_id}")

        # Check its details immediately
        job_info = client.get_job(job_id)
        if job_info:
            print(f"   Job details:")
            print(f"     State: {job_info.get('job_state')}")
            print(f"     Partition: {job_info.get('partition')}")
            print(f"     Reason: {job_info.get('state_reason', 'none')}")

        # Cancel it
        client.cancel_job(job_id)
        print(f"   ✅ Cancelled job {job_id}")

    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Summary
    print("\n" + "=" * 40)
    print("📊 Diagnostic Summary")
    print("=" * 40)
    print("✅ Cluster connectivity: OK")
    print("✅ Authentication: OK")
    print("✅ Job submission: OK")
    print("✅ Job querying: OK")
    print("✅ Job cancellation: OK")
    print("=" * 40)

    return True



if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
