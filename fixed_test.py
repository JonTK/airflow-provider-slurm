#!/usr/bin/env python3
"""Test with properly formatted script."""

import sys
from unittest.mock import MagicMock

sys.path.insert(0, "/home/jontk/src/github.com/jontk/airflow-slurm-executor")

from airflow_slurm_executor.slurm_api_client import SlurmAPIClient

BASE_URL = "http://rocky9.ar.jontk.com:6820"
TEST_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjY2Nzg5NjEsImlhdCI6MTc2NjY3NzE2MSwic3VuIjoicm9vdCJ9.FRcLY-j8uao80Obc51d7LgZd3Ql_Oan3H8anIVCjuAg"


def main():
    print("🧪 Fixed Script Format Test")
    print("-" * 30)

    token_manager = MagicMock()
    token_manager.get_token.return_value = TEST_TOKEN
    client = SlurmAPIClient(BASE_URL, token_manager)

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

    try:
        response = client.submit_job(job_spec)
        job_id = response.get("job_id")
        print(f"✅ Submitted job {job_id} with properly formatted script")
        print(f"Monitor with: scontrol show job {job_id}")
        print(f"Check output: cat /tmp/fixed_test_{job_id}.out")
        print(f"Check errors: cat /tmp/fixed_test_{job_id}.err")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
