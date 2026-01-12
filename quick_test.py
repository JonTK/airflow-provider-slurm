#!/usr/bin/env python3
"""Quick diagnostic test to check why jobs stay in PENDING."""

import sys
from unittest.mock import MagicMock

sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_provider_slurm.slurm_api_client import SlurmAPIClient

BASE_URL = "http://rocky9.ar.jontk.com:6820"
TEST_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjY2Nzg5NjEsImlhdCI6MTc2NjY3NzE2MSwic3VuIjoicm9vdCJ9.FRcLY-j8uao80Obc51d7LgZd3Ql_Oan3H8anIVCjuAg"


def main():
    print("🔍 Quick Diagnostic Test")
    print("-" * 30)

    # Create client
    token_manager = MagicMock()
    token_manager.get_token.return_value = TEST_TOKEN
    client = SlurmAPIClient(BASE_URL, token_manager)

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
        print(f"   Error: {e}")
        return

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


if __name__ == "__main__":
    main()
