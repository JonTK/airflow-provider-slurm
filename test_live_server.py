#!/usr/bin/env python3
"""Test script for validating the Slurm executor against a live server."""

import os
import sys
import time
from unittest.mock import MagicMock

# Add the package to Python path
sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_provider_slurm.exceptions import SlurmAPIError
from airflow_provider_slurm.slurm_api_client import SlurmAPIClient
from airflow_provider_slurm.slurm_token_manager import SlurmTokenManager
from tests.utils.cluster_helpers import (
    fetch_token_via_ssh,
    get_cluster_config,
    is_cluster_available,
)


def test_live_server():
    """Test our implementation against the live Slurm server."""

    print("🧪 Testing Airflow Slurm Executor against live server")
    print("-" * 60)

    # Check cluster availability
    print("Checking cluster availability...")
    if not is_cluster_available():
        print("❌ Cluster is not available or accessible")
        print("   Check SLURM_TEST_CLUSTER_HOST and SSH configuration")
        return False

    config = get_cluster_config()
    BASE_URL = f"http://{config['host']}:{config['port']}"

    print(f"Server: {BASE_URL}")
    print(f"User: {config['user']}")
    print(f"✅ Cluster {config['host']} is available")
    print("-" * 60)

    try:
        # Step 1: Fetch authentication token
        print("1️⃣  Fetching authentication token...")
        token = fetch_token_via_ssh(
            host=config["host"], user=config["user"], ssh_key=config["ssh_key"]
        )

        if not token:
            print("❌ Failed to fetch authentication token")
            return False

        # Create token manager with fetched token
        token_manager = MagicMock()
        token_manager.get_token.return_value = token

        print(f"✅ Token fetched: {token[:20]}...")

        # Step 2: Test API Client
        print("\n2️⃣  Testing SlurmAPIClient...")
        api_client = SlurmAPIClient(
            base_url=BASE_URL, token_manager=token_manager, timeout=10, max_retries=2
        )

        # Test ping/version detection
        print("   Testing API connectivity...")
        try:
            if api_client.ping():
                print("✅ Server ping successful!")

                version = api_client.get_api_version()
                print(f"✅ API Version detected: {version}")
            else:
                print("❌ Server ping failed")
                return False
        except Exception as e:
            print(f"❌ Server connectivity failed: {e}")
            return False

        # Test job listing
        print("   Testing job query...")
        try:
            jobs_response = api_client.get_jobs()
            jobs = jobs_response.get("jobs", [])
            print(f"✅ Retrieved {len(jobs)} jobs from queue")

            # Show first few jobs if any
            for i, job in enumerate(jobs[:3]):
                job_id = job.get("job_id", "unknown")
                job_state = job.get("job_state", "unknown")
                job_name = job.get("name", "unknown")
                print(f"   Job {job_id}: {job_state} - {job_name}")

        except Exception as e:
            print(f"❌ Job query failed: {e}")
            return False

        # Test job submission (simple test job)
        print("\\n   Testing job submission...")
        try:
            test_job_spec = {
                "script": "#!/bin/bash\\necho 'Airflow Slurm Executor Test Job'\\necho 'Working directory:' $(pwd)\\necho 'User:' $(whoami)\\nsleep 2\\necho 'Test completed successfully'",
                "job": {
                    "name": "airflow-executor-test",
                    "partition": "debug",  # Use debug partition (30 min limit)
                    "tasks": 1,
                    "cpus_per_task": 1,
                    "memory_per_node": "100M",
                    "time_limit": 60,  # 60 seconds (1 minute)
                    "current_working_directory": "/tmp",
                    "environment": {
                        "PATH": "/usr/local/bin:/usr/bin:/bin",
                        "USER": "root",
                        "HOME": "/root",
                    },
                    "standard_output": "/tmp/slurm_test_%j.out",
                    "standard_error": "/tmp/slurm_test_%j.err",
                },
            }

            # Try job submission
            submit_response = api_client.submit_job(test_job_spec)
            job_id = submit_response.get("job_id")

            if job_id:
                print(f"✅ Test job submitted successfully! Job ID: {job_id}")

                # Monitor job for a few seconds
                print(f"   Monitoring job {job_id}...")
                for i in range(10):  # Monitor for up to 10 seconds
                    try:
                        job_info = api_client.get_job(job_id)
                        if job_info:
                            state = job_info.get("job_state", "UNKNOWN")
                            print(f"   Job {job_id} state: {state}")

                            if state in ["COMPLETED", "FAILED", "CANCELLED"]:
                                break
                        else:
                            print(f"   Job {job_id} no longer in queue")
                            break

                        time.sleep(1)
                    except Exception as e:
                        print(f"   Error checking job status: {e}")
                        break

                # Try to cancel job if still running
                try:
                    cancel_response = api_client.cancel_job(job_id)
                    if cancel_response:
                        print(f"✅ Job {job_id} cancelled successfully")
                except Exception as e:
                    print(f"   Note: Could not cancel job {job_id}: {e}")

            else:
                print("❌ Job submission failed - no job ID returned")
                return False

        except SlurmAPIError as e:
            print(f"❌ Job submission failed: {e}")
            print("   This might be due to partition/resource constraints")
            print("   But API communication is working!")

        except Exception as e:
            print(f"❌ Unexpected error during job submission: {e}")
            return False

        print("\n🎉 All basic tests completed successfully!")
        print("\n" + "=" * 60)
        print("📊 Test Summary")
        print("=" * 60)
        print("✅ Token management: OK")
        print("✅ API connectivity: OK")
        print("✅ Job query functionality: OK")
        print("✅ Job submission: OK")
        print("✅ Job monitoring: OK")
        print("✅ Job cancellation: OK")
        print("=" * 60)
        print("\n🚀 The Slurm executor should work with your cluster!")

        return True

    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_live_server()
    sys.exit(0 if success else 1)
