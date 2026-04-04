#!/bin/bash
set -e

export AIRFLOW_HOME=/home/jontk/src/github.com/jontk/airflow-slurm-executor/test_airflow_home
VENV=/home/jontk/src/github.com/jontk/airflow-slurm-executor/venv/bin

# Start API server
$VENV/airflow api-server > /tmp/af-test-api.log 2>&1 &
API_PID=$!
sleep 8

# Start scheduler
$VENV/airflow scheduler > /tmp/af-test-sched.log 2>&1 &
SCHED_PID=$!
sleep 10

# Trigger DAG
$VENV/airflow dags trigger test_slurm_executor 2>&1 | tail -1

# Wait for execution
sleep 40

# Show results
echo ""
echo "=== Key log lines ==="
grep -iE "submit|succeeded|Task.*failed|Task.*success" /tmp/af-test-sched.log | grep -v "Deprecat" | tail -20

echo ""
echo "=== DAG run status ==="
$VENV/airflow dags list-runs -d test_slurm_executor 2>&1 | tail -10

# Cleanup
kill $API_PID $SCHED_PID 2>/dev/null
