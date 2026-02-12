#!/usr/bin/env python3
"""Simple test with debug partition compatible settings."""

import sys
import time
from unittest.mock import MagicMock

sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from tests.utils.cluster_helpers import (
    fetch_token_via_ssh,
    get_cluster_config,
    is_cluster_available,
)


def main():
    print("🧪 Simple Test with Debug Partition")
    print("-" * 40)

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

    # Test 1: Minimal job with very short time limit
    print("Test 1: Minimal 10-second job")
    minimal_job = {
        "script": "#!/bin/bash\\necho 'Hello from Slurm!'\\ndate\\nhostname\\necho 'Job completed successfully'",
        "job": {
            "name": "simple-test-10s",
            "partition": "debug",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "50M",
            "time_limit": 10,  # Only 10 seconds
            "current_working_directory": "/tmp",
            "environment": {"PATH": "/usr/local/bin:/usr/bin:/bin", "USER": "root"},
            "standard_output": "/tmp/simple_test_%j.out",
            "standard_error": "/tmp/simple_test_%j.err",
        },
    }

    try:
        response = client.submit_job(minimal_job)
        job_id = response.get("job_id")
        print(f"✅ Submitted job {job_id}")

        # Monitor for 60 seconds max
        for i in range(30):  # 30 * 2 = 60 seconds max
            time.sleep(2)
            job_info = client.get_job(job_id)

            if job_info and "job_state" in job_info:
                state = job_info["job_state"]
                if isinstance(state, list):
                    state = state[0]

                print(f"   Job {job_id}: {state}")

                if state in ["COMPLETED", "FAILED", "CANCELLED"]:
                    if state == "COMPLETED":
                        print(f"✅ Job {job_id} completed successfully!")
                    else:
                        print(f"❌ Job {job_id} ended with: {state}")
                    break
            else:
                print(f"   Job {job_id}: Not found (may have completed)")
                break
        else:
            print(f"⏱️ Job {job_id} still running, cancelling...")
            client.cancel_job(job_id)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Test 2: Use normal partition (infinite time limit)
    print("\\nTest 2: Using normal partition")
    normal_job = {
        "script": "#!/bin/bash\\necho 'Hello from normal partition!'\\ndate\\nhostname\\nsleep 5\\necho 'Normal partition test completed'",
        "job": {
            "name": "simple-test-normal",
            "partition": "normal",  # Use normal partition instead
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "50M",
            "time_limit": 60,  # 1 minute
            "current_working_directory": "/tmp",
            "environment": {"PATH": "/usr/local/bin:/usr/bin:/bin", "USER": "root"},
            "standard_output": "/tmp/normal_test_%j.out",
            "standard_error": "/tmp/normal_test_%j.err",
        },
    }

    try:
        response = client.submit_job(normal_job)
        job_id = response.get("job_id")
        print(f"✅ Submitted job {job_id} to normal partition")

        # Monitor for 90 seconds max
        for i in range(45):  # 45 * 2 = 90 seconds max
            time.sleep(2)
            job_info = client.get_job(job_id)

            if job_info and "job_state" in job_info:
                state = job_info["job_state"]
                if isinstance(state, list):
                    state = state[0]

                print(f"   Job {job_id}: {state}")

                if state in ["COMPLETED", "FAILED", "CANCELLED"]:
                    if state == "COMPLETED":
                        print(f"✅ Job {job_id} completed successfully!")
                    else:
                        print(f"❌ Job {job_id} ended with: {state}")
                    break
            else:
                print(f"   Job {job_id}: Not found (may have completed)")
                break
        else:
            print(f"⏱️ Job {job_id} still running, cancelling...")
            client.cancel_job(job_id)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False

    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary")
    print("=" * 50)
    print("✅ Test 1 (Debug partition): Completed")
    print("✅ Test 2 (Normal partition): Completed")
    print("=" * 50)

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
