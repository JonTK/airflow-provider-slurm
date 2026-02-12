#!/usr/bin/env python3
"""Test with properly formatted script."""

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
    print("🧪 Fixed Script Format Test")
    print("-" * 30)

    # Check cluster availability
    print("Checking cluster availability...")
    if not is_cluster_available():
        print("❌ Cluster is not available or accessible")
        print("   Check SLURM_TEST_CLUSTER_HOST and SSH configuration")
        return False

    config = get_cluster_config()
    print(f"✅ Cluster {config['host']} is available")

    # Fetch token automatically
    print("Fetching authentication token...")
    token = fetch_token_via_ssh(
        host=config["host"], user=config["user"], ssh_key=config["ssh_key"]
    )

    if not token:
        print("❌ Failed to fetch authentication token")
        return False

    print(f"✅ Token fetched: {token[:20]}...")

    # Create API client with real token
    token_manager = MagicMock()
    token_manager.get_token.return_value = token
    base_url = f"http://{config['host']}:{config['port']}"
    client = SlurmAPIClient(base_url, token_manager)

    # Properly formatted multi-line script
    script_content = """#!/bin/bash
echo 'Hello from Slurm!'
date
hostname
echo 'Script completed successfully'
"""

    job_spec = {
        "script": script_content,
        "job": {
            "name": "fixed-script-test",
            "partition": "normal",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "50M",
            "time_limit": 60,
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root",
            },
            "standard_output": "/tmp/fixed_test_%j.out",
            "standard_error": "/tmp/fixed_test_%j.err",
        },
    }

    success = False
    try:
        response = client.submit_job(job_spec)
        job_id = response.get("job_id")
        print(f"✅ Submitted job {job_id} with properly formatted script")
        print(f"Monitor with: scontrol show job {job_id}")
        print(f"Check output: cat /tmp/fixed_test_{job_id}.out")
        print(f"Check errors: cat /tmp/fixed_test_{job_id}.err")
        success = True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        success = False

    # Summary
    print("\n" + "=" * 40)
    print("📊 Test Summary")
    print("=" * 40)
    if success:
        print("✅ All tests passed - job submitted successfully")
    else:
        print("❌ Test failed - see errors above")
    print("=" * 40)

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
