#!/usr/bin/env python3
"""Comprehensive test jobs for validating the Slurm executor."""

import sys
import time
import json
from unittest.mock import MagicMock
from datetime import datetime

# Add the package to Python path
sys.path.insert(0, '/home/jontk/src/github.com/jontk/airflow-slurm-executor')

from airflow_slurm_executor.slurm_api_client import SlurmAPIClient

# Server configuration
BASE_URL = "http://rocky9.ar.jontk.com:6820"
TEST_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjY2Nzg5NjEsImlhdCI6MTc2NjY3NzE2MSwic3VuIjoicm9vdCJ9.FRcLY-j8uao80Obc51d7LgZd3Ql_Oan3H8anIVCjuAg"

def create_api_client():
    """Create API client with test token."""
    token_manager = MagicMock()
    token_manager.get_token.return_value = TEST_TOKEN
    
    return SlurmAPIClient(
        base_url=BASE_URL,
        token_manager=token_manager,
        timeout=30,
        max_retries=2
    )

def test_basic_echo_job():
    """Test 1: Basic echo job (quickest validation)."""
    print("🧪 Test 1: Basic Echo Job")
    print("-" * 40)
    
    job_spec = {
        "script": "#!/bin/bash\\necho 'Hello from Slurm!'\\necho 'Job ID: $SLURM_JOB_ID'\\necho 'Node: $SLURM_JOB_NODELIST'",
        "job": {
            "name": "airflow-test-echo",
            "partition": "debug",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "50M",
            "time_limit": 30,  # 30 seconds
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root"
            },
            "standard_output": "/tmp/slurm_echo_test_%j.out",
            "standard_error": "/tmp/slurm_echo_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "Basic Echo")

def test_resource_usage_job():
    """Test 2: Job with specific resource requirements."""
    print("\\n🧪 Test 2: Resource Usage Job")
    print("-" * 40)
    
    job_spec = {
        "script": """#!/bin/bash
echo 'Testing resource allocation...'
echo 'CPUs allocated: $SLURM_CPUS_PER_TASK'
echo 'Memory info:'
free -h
echo 'CPU info:'
nproc
echo 'Hostname: $(hostname)'
echo 'Using 2 CPUs for 10 seconds...'
# Simple CPU load test
for i in {1..2}; do
  (yes > /dev/null &)
done
sleep 10
killall yes 2>/dev/null || true
echo 'Resource test completed!'""",
        "job": {
            "name": "airflow-test-resources",
            "partition": "debug", 
            "tasks": 1,
            "cpus_per_task": 2,  # Request 2 CPUs
            "memory_per_node": "200M",  # Request 200MB
            "time_limit": 120,  # 2 minutes
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root"
            },
            "standard_output": "/tmp/slurm_resource_test_%j.out",
            "standard_error": "/tmp/slurm_resource_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "Resource Usage")

def test_environment_job():
    """Test 3: Job that tests environment variables."""
    print("\\n🧪 Test 3: Environment Variables Job")
    print("-" * 40)
    
    job_spec = {
        "script": """#!/bin/bash
echo 'Environment Variable Test'
echo '=========================='
echo 'PATH: $PATH'
echo 'USER: $USER'
echo 'HOME: $HOME'
echo 'SLURM_JOB_ID: $SLURM_JOB_ID'
echo 'SLURM_JOB_NAME: $SLURM_JOB_NAME'
echo 'SLURM_JOB_PARTITION: $SLURM_JOB_PARTITION'
echo 'SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK'
echo 'CUSTOM_VAR: $CUSTOM_VAR'
echo 'TEST_ENV: $TEST_ENV'
echo ''
echo 'All environment variables:'
env | sort
echo 'Environment test completed!'""",
        "job": {
            "name": "airflow-test-env",
            "partition": "debug",
            "tasks": 1, 
            "cpus_per_task": 1,
            "memory_per_node": "100M",
            "time_limit": 60,
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root",
                "CUSTOM_VAR": "AirflowSlurmExecutor",
                "TEST_ENV": "production_test"
            },
            "standard_output": "/tmp/slurm_env_test_%j.out",
            "standard_error": "/tmp/slurm_env_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "Environment Variables")

def test_file_operations_job():
    """Test 4: Job that creates and manipulates files."""
    print("\\n🧪 Test 4: File Operations Job")
    print("-" * 40)
    
    job_spec = {
        "script": """#!/bin/bash
echo 'File Operations Test'
echo '==================='

# Create a test directory
TEST_DIR="/tmp/slurm_test_$$"
mkdir -p "$TEST_DIR"
echo "Created test directory: $TEST_DIR"

# Create some test files
echo "Creating test files..."
echo "Job ID: $SLURM_JOB_ID" > "$TEST_DIR/job_info.txt"
echo "Timestamp: $(date)" >> "$TEST_DIR/job_info.txt"
echo "Node: $(hostname)" >> "$TEST_DIR/job_info.txt"

# Create a data file
echo "Generating data file..."
for i in {1..100}; do
    echo "Line $i: $(date +%s.%N)" >> "$TEST_DIR/data.txt"
done

# Process the data
echo "Processing data..."
wc -l "$TEST_DIR/data.txt"
head -5 "$TEST_DIR/data.txt"
tail -5 "$TEST_DIR/data.txt"

# Create a summary
echo "Creating summary..."
{
    echo "=== Job Summary ==="
    echo "Job ID: $SLURM_JOB_ID"
    echo "Files created: $(ls -1 $TEST_DIR | wc -l)"
    echo "Total data lines: $(wc -l < $TEST_DIR/data.txt)"
    echo "Directory size: $(du -sh $TEST_DIR)"
    echo "Completion time: $(date)"
} > "$TEST_DIR/summary.txt"

cat "$TEST_DIR/summary.txt"

# Cleanup
echo "Cleaning up test directory..."
rm -rf "$TEST_DIR"
echo 'File operations test completed!'""",
        "job": {
            "name": "airflow-test-files",
            "partition": "debug",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "100M", 
            "time_limit": 180,  # 3 minutes
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root"
            },
            "standard_output": "/tmp/slurm_files_test_%j.out",
            "standard_error": "/tmp/slurm_files_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "File Operations")

def test_python_job():
    """Test 5: Job that runs Python code."""
    print("\\n🧪 Test 5: Python Execution Job")
    print("-" * 40)
    
    job_spec = {
        "script": """#!/bin/bash
echo 'Python Execution Test'
echo '====================='

# Check Python availability
echo 'Python version:'
python3 --version

# Run a simple Python script
echo 'Running Python calculations...'
python3 << 'EOF'
import os
import sys
import time
from datetime import datetime

print(f"Python executable: {sys.executable}")
print(f"Python version: {sys.version}")
print(f"Current working directory: {os.getcwd()}")
print(f"Process ID: {os.getpid()}")
print(f"Slurm Job ID: {os.environ.get('SLURM_JOB_ID', 'Not set')}")

# Do some calculations
print("\\nPerforming calculations...")
result = sum(i*i for i in range(1000))
print(f"Sum of squares 1-999: {result}")

# Test file I/O
test_file = "/tmp/python_test_output.txt"
with open(test_file, 'w') as f:
    f.write(f"Test completed at {datetime.now()}\\n")
    f.write(f"Result: {result}\\n")

print(f"Results written to: {test_file}")

# Read it back
with open(test_file, 'r') as f:
    print("File contents:")
    print(f.read())

# Cleanup
os.remove(test_file)
print("Python test completed successfully!")
EOF

echo 'Python execution test completed!'""",
        "job": {
            "name": "airflow-test-python",
            "partition": "debug",
            "tasks": 1,
            "cpus_per_task": 1, 
            "memory_per_node": "150M",
            "time_limit": 120,
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root",
                "PYTHONPATH": "/usr/local/lib/python3.9/site-packages"
            },
            "standard_output": "/tmp/slurm_python_test_%j.out",
            "standard_error": "/tmp/slurm_python_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "Python Execution")

def test_failing_job():
    """Test 6: Job designed to fail (test error handling)."""
    print("\\n🧪 Test 6: Failing Job (Error Handling Test)")
    print("-" * 40)
    
    job_spec = {
        "script": """#!/bin/bash
echo 'Failure Test Job'
echo '================'
echo 'This job is designed to fail to test error handling...'
echo 'Job ID: $SLURM_JOB_ID'
echo 'Starting operations...'
sleep 2
echo 'About to fail...'
# Intentionally fail
exit 42""",
        "job": {
            "name": "airflow-test-failure",
            "partition": "debug",
            "tasks": 1,
            "cpus_per_task": 1,
            "memory_per_node": "50M", 
            "time_limit": 60,
            "current_working_directory": "/tmp",
            "environment": {
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "USER": "root",
                "HOME": "/root"
            },
            "standard_output": "/tmp/slurm_failure_test_%j.out",
            "standard_error": "/tmp/slurm_failure_test_%j.err"
        }
    }
    
    return submit_and_monitor_job(job_spec, "Failure Test", expect_failure=True)

def submit_and_monitor_job(job_spec, test_name, expect_failure=False, max_wait=300):
    """Submit a job and monitor it to completion."""
    client = create_api_client()
    
    try:
        print(f"Submitting {test_name} job...")
        response = client.submit_job(job_spec)
        job_id = response.get('job_id')
        
        if not job_id:
            print(f"❌ {test_name}: Job submission failed - no job ID")
            return False
            
        print(f"✅ {test_name}: Job {job_id} submitted successfully")
        
        # Monitor job status
        print(f"Monitoring job {job_id}...")
        start_time = time.time()
        final_state = None
        
        while time.time() - start_time < max_wait:
            try:
                job_info = client.get_job(job_id)
                if job_info and 'job_state' in job_info:
                    state = job_info['job_state']
                    if isinstance(state, list):
                        state = state[0] if state else 'UNKNOWN'
                    
                    print(f"   Job {job_id}: {state}")
                    
                    if state in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT']:
                        final_state = state
                        break
                        
                else:
                    print(f"   Job {job_id}: Not found in queue (may have completed)")
                    break
                    
            except Exception as e:
                print(f"   Error checking job {job_id}: {e}")
                break
                
            time.sleep(2)
        
        # Evaluate results
        if expect_failure:
            if final_state == 'FAILED':
                print(f"✅ {test_name}: Job {job_id} failed as expected (exit code test)")
                return True
            else:
                print(f"⚠️ {test_name}: Job {job_id} should have failed but got state: {final_state}")
                return False
        else:
            if final_state == 'COMPLETED':
                print(f"✅ {test_name}: Job {job_id} completed successfully")
                return True
            elif final_state in ['FAILED', 'CANCELLED', 'TIMEOUT']:
                print(f"❌ {test_name}: Job {job_id} ended with state: {final_state}")
                return False
            else:
                print(f"⏱️ {test_name}: Job {job_id} still running after {max_wait}s, cancelling...")
                try:
                    client.cancel_job(job_id)
                    print(f"   Job {job_id} cancelled")
                except:
                    pass
                return False
                
    except Exception as e:
        print(f"❌ {test_name}: Exception occurred - {e}")
        return False

def run_all_tests():
    """Run comprehensive test suite."""
    print("🚀 Starting Comprehensive Slurm Executor Test Suite")
    print("=" * 60)
    print(f"Target: {BASE_URL}")
    print(f"Time: {datetime.now()}")
    print("=" * 60)
    
    tests = [
        ("Basic Echo", test_basic_echo_job),
        ("Resource Usage", test_resource_usage_job), 
        ("Environment Variables", test_environment_job),
        ("File Operations", test_file_operations_job),
        ("Python Execution", test_python_job),
        ("Error Handling", test_failing_job)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\\n{'='*20} {test_name} {'='*20}")
        try:
            results[test_name] = test_func()
        except KeyboardInterrupt:
            print(f"\\n⚠️ Test interrupted by user")
            break
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {e}")
            results[test_name] = False
    
    # Print summary
    print(f"\\n{'='*60}")
    print("🏁 TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = 0
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL" 
        print(f"{test_name:25} {status}")
        if success:
            passed += 1
    
    print(f"\\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("\\n🎉 All tests PASSED! Slurm executor is working perfectly!")
    else:
        print(f"\\n⚠️ {total - passed} test(s) failed. Check individual results above.")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)