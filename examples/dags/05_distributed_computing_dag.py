"""
Distributed Computing with Dynamic Resource Allocation.

This example demonstrates advanced Slurm features like dynamic task generation,
different partition usage, and complex dependency patterns.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "distributed-computing-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    "distributed_computing_slurm",
    default_args=default_args,
    description="Advanced distributed computing with dynamic resource allocation",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    catchup=False,
    max_active_runs=1,
    tags=["slurm", "distributed", "dynamic", "hpc"],
)


def determine_workload_size(**context):
    """Determine the size of computational workload based on data or parameters."""
    import random

    # Simulate workload determination (could be based on file size, data volume, etc.)
    workload_types = ["small", "medium", "large"]
    workload = random.choice(workload_types)

    workload_configs = {
        "small": {"partitions": 4, "compute_time": "00:10:00", "memory": "2G"},
        "medium": {"partitions": 8, "compute_time": "00:30:00", "memory": "4G"},
        "large": {"partitions": 16, "compute_time": "01:00:00", "memory": "8G"},
    }

    config = workload_configs[workload]
    config["workload_type"] = workload

    print(f"Determined workload: {workload}")
    print(f"Configuration: {config}")

    # Store configuration for downstream tasks
    context["task_instance"].xcom_push(key="workload_config", value=config)
    return config


# Start
start = DummyOperator(task_id="start", dag=dag)

# Determine computational requirements
analyze_workload = PythonOperator(
    task_id="analyze_workload",
    python_callable=determine_workload_size,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 1,
            "mem": "256M",
            "time_limit": "00:02:00",
        }
    },
    dag=dag,
)

# Prepare distributed computing environment
setup_environment = BashOperator(
    task_id="setup_distributed_environment",
    bash_command="""
    echo "=== Setting Up Distributed Computing Environment ==="

    WORK_DIR="/tmp/distributed_computing_{{ ds_nodash }}"
    mkdir -p "$WORK_DIR"/{input,output,temp,logs,results}

    echo "Workspace created: $WORK_DIR"

    # Generate input data based on workload size
    PARTITIONS="{{ ti.xcom_pull(task_ids='analyze_workload', key='workload_config')['partitions'] }}"
    WORKLOAD_TYPE="{{ ti.xcom_pull(task_ids='analyze_workload', key='workload_config')['workload_type'] }}"

    echo "Generating input data for $WORKLOAD_TYPE workload with $PARTITIONS partitions"

    # Create a large dataset to be processed in parallel
    for i in $(seq 1 $PARTITIONS); do
        INPUT_FILE="$WORK_DIR/input/partition_${i}.dat"

        echo "Creating partition $i..."

        # Generate different sized datasets based on workload
        case $WORKLOAD_TYPE in
            small)
                # 1000 lines per partition
                head -c $((1000 * 50)) /dev/urandom | base64 > "$INPUT_FILE"
                ;;
            medium)
                # 5000 lines per partition
                head -c $((5000 * 50)) /dev/urandom | base64 > "$INPUT_FILE"
                ;;
            large)
                # 10000 lines per partition
                head -c $((10000 * 50)) /dev/urandom | base64 > "$INPUT_FILE"
                ;;
        esac

        echo "Partition $i created: $(wc -l < "$INPUT_FILE") lines"
    done

    # Create metadata file
    cat > "$WORK_DIR/metadata.json" << EOF
{
    "workload_type": "$WORKLOAD_TYPE",
    "partitions": $PARTITIONS,
    "created_date": "{{ ds }}",
    "total_input_files": $PARTITIONS,
    "estimated_processing_time": "{{ ti.xcom_pull(task_ids='analyze_workload', key='workload_config')['compute_time'] }}"
}
EOF

    echo "Environment setup completed"
    echo "Metadata:"
    cat "$WORK_DIR/metadata.json"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 2,
            "mem": "1G",
            "time_limit": "00:05:00",
        }
    },
    dag=dag,
)

# Dynamic task generation using TaskGroup for parallel processing
with TaskGroup("parallel_processing", dag=dag) as parallel_processing:
    # We'll create tasks for different processing types
    # In a real scenario, you might dynamically generate these based on workload

    # CPU-intensive tasks (normal partition)
    cpu_intensive_tasks = []
    for i in range(4):  # Process first 4 partitions on CPU
        task = BashOperator(
            task_id=f"cpu_process_partition_{i+1}",
            bash_command=f"""
            echo "=== CPU Processing Partition {i+1} ==="

            WORK_DIR="/tmp/distributed_computing_{{{{ ds_nodash }}}}"
            INPUT_FILE="$WORK_DIR/input/partition_{i+1}.dat"
            OUTPUT_FILE="$WORK_DIR/output/cpu_result_{i+1}.out"
            LOG_FILE="$WORK_DIR/logs/cpu_process_{i+1}.log"

            if [ ! -f "$INPUT_FILE" ]; then
                echo "Error: Input file not found: $INPUT_FILE"
                exit 1
            fi

            echo "Processing $INPUT_FILE on CPU cores..." | tee "$LOG_FILE"
            echo "Started at: $(date)" >> "$LOG_FILE"

            # CPU-intensive processing simulation
            {{
                echo "Input size: $(wc -l < "$INPUT_FILE") lines"
                echo "Processing with CPU-intensive algorithms..."

                # Simulate complex mathematical computations
                LINE_COUNT=$(wc -l < "$INPUT_FILE")
                PROCESSED_LINES=0

                while IFS= read -r line; do
                    # Simulate CPU work (hash computation, sorting, etc.)
                    echo "$line" | md5sum | cut -d' ' -f1
                    PROCESSED_LINES=$((PROCESSED_LINES + 1))

                    # Progress reporting
                    if [ $((PROCESSED_LINES % 100)) -eq 0 ]; then
                        echo "Processed $PROCESSED_LINES / $LINE_COUNT lines..."
                    fi
                done < "$INPUT_FILE"

                echo "CPU processing completed for partition {i+1}"
                echo "Total lines processed: $PROCESSED_LINES"
                echo "Completed at: $(date)"
            }} > "$OUTPUT_FILE" 2>> "$LOG_FILE"

            echo "CPU processing completed for partition {i+1}"
            echo "Output: $OUTPUT_FILE"
            echo "Log: $LOG_FILE"
            """,
            executor_config={
                {
                    "slurm": {
                        {
                            "partition": "normal",
                            "cpus_per_task": 4,  # More CPU cores for intensive work
                            "mem": '{{{{ ti.xcom_pull(task_ids="analyze_workload", key="workload_config")["memory"] }}}}',
                            "time_limit": '{{{{ ti.xcom_pull(task_ids="analyze_workload", key="workload_config")["compute_time"] }}}}',
                        }
                    }
                }
            },
            dag=dag,
        )
        cpu_intensive_tasks.append(task)

    # Memory-intensive tasks
    memory_intensive_tasks = []
    for i in range(4, 8):  # Process partitions 5-8 with memory focus
        task = BashOperator(
            task_id=f"memory_process_partition_{i+1}",
            bash_command=f"""
            echo "=== Memory-Intensive Processing Partition {i+1} ==="

            WORK_DIR="/tmp/distributed_computing_{{{{ ds_nodash }}}}"
            INPUT_FILE="$WORK_DIR/input/partition_{i+1}.dat"
            OUTPUT_FILE="$WORK_DIR/output/memory_result_{i+1}.out"
            LOG_FILE="$WORK_DIR/logs/memory_process_{i+1}.log"

            if [ ! -f "$INPUT_FILE" ]; then
                echo "Warning: Input file not found: $INPUT_FILE, skipping..."
                echo "Skipped - no input file" > "$OUTPUT_FILE"
                exit 0
            fi

            echo "Processing $INPUT_FILE with memory-intensive algorithms..." | tee "$LOG_FILE"
            echo "Started at: $(date)" >> "$LOG_FILE"

            # Memory-intensive processing simulation
            {{
                echo "Input size: $(wc -l < "$INPUT_FILE") lines"
                echo "Loading data into memory for sorting and aggregation..."

                # Simulate memory-intensive operations (sorting large datasets)
                TEMP_FILE="$WORK_DIR/temp/memory_temp_{i+1}.tmp"
                mkdir -p "$WORK_DIR/temp"

                # Sort the entire dataset (memory-intensive)
                sort "$INPUT_FILE" > "$TEMP_FILE"

                # Process sorted data
                echo "Processing sorted data..."
                LINE_COUNT=$(wc -l < "$TEMP_FILE")

                {{
                    echo "=== Memory Processing Results ==="
                    echo "Original file size: $(du -h "$INPUT_FILE" | cut -f1)"
                    echo "Sorted file size: $(du -h "$TEMP_FILE" | cut -f1)"
                    echo "Total lines: $LINE_COUNT"
                    echo "Memory processing completed"
                    echo "First 5 sorted lines:"
                    head -5 "$TEMP_FILE"
                    echo "Last 5 sorted lines:"
                    tail -5 "$TEMP_FILE"
                }}

                # Cleanup temp file
                rm -f "$TEMP_FILE"

                echo "Memory processing completed for partition {i+1}"
                echo "Completed at: $(date)"
            }} > "$OUTPUT_FILE" 2>> "$LOG_FILE"

            echo "Memory processing completed for partition {i+1}"
            """,
            executor_config={
                {
                    "slurm": {
                        {
                            "partition": "normal",
                            "cpus_per_task": 2,
                            "mem": "16G",  # High memory requirement
                            "time_limit": '{{{{ ti.xcom_pull(task_ids="analyze_workload", key="workload_config")["compute_time"] }}}}',
                        }
                    }
                }
            },
            dag=dag,
        )
        memory_intensive_tasks.append(task)

    # Long-running tasks (using long partition if available)
    long_running_tasks = []
    for i in range(8, 10):  # Process remaining partitions as long jobs
        task = BashOperator(
            task_id=f"long_process_partition_{i+1}",
            bash_command=f"""
            echo "=== Long-Running Processing Partition {i+1} ==="

            WORK_DIR="/tmp/distributed_computing_{{{{ ds_nodash }}}}"
            INPUT_FILE="$WORK_DIR/input/partition_{i+1}.dat"
            OUTPUT_FILE="$WORK_DIR/output/long_result_{i+1}.out"
            LOG_FILE="$WORK_DIR/logs/long_process_{i+1}.log"

            if [ ! -f "$INPUT_FILE" ]; then
                echo "Warning: Input file not found: $INPUT_FILE, creating dummy output..."
                echo "Skipped - no input file" > "$OUTPUT_FILE"
                exit 0
            fi

            echo "Starting long-running analysis for partition {i+1}..." | tee "$LOG_FILE"
            echo "Started at: $(date)" >> "$LOG_FILE"

            # Simulate long-running computation
            {{
                echo "Input file: $INPUT_FILE"
                echo "Performing iterative analysis..."

                # Simulate multiple processing passes
                for pass in {{1..3}}; do
                    echo "=== Processing Pass $pass ==="
                    echo "Pass $pass started at: $(date)"

                    # Simulate work with sleep (replace with actual computation)
                    LINE_COUNT=$(wc -l < "$INPUT_FILE")
                    echo "Processing $LINE_COUNT lines in pass $pass"

                    # Simulate progressive refinement
                    case $pass in
                        1) echo "Initial data parsing and validation..."
                           sleep 5 ;;
                        2) echo "Statistical analysis and feature extraction..."
                           sleep 10 ;;
                        3) echo "Final optimization and result generation..."
                           sleep 8 ;;
                    esac

                    echo "Pass $pass completed at: $(date)"
                done

                echo "=== Final Results ==="
                echo "Long-running analysis completed for partition {i+1}"
                echo "Total passes: 3"
                echo "Input lines: $(wc -l < "$INPUT_FILE")"
                echo "Final result: Analysis successful"
                echo "Completed at: $(date)"
            }} > "$OUTPUT_FILE" 2>> "$LOG_FILE"

            echo "Long-running processing completed for partition {i+1}"
            """,
            executor_config={
                {
                    "slurm": {
                        {
                            "partition": "long",  # Use long partition for extended jobs
                            "cpus_per_task": 2,
                            "mem": "4G",
                            "time_limit": "02:00:00",  # Longer time limit
                        }
                    }
                }
            },
            dag=dag,
        )
        long_running_tasks.append(task)

# Aggregation and analysis
aggregate_results = BashOperator(
    task_id="aggregate_distributed_results",
    bash_command="""
    echo "=== Aggregating Distributed Results ==="

    WORK_DIR="/tmp/distributed_computing_{{ ds_nodash }}"
    RESULTS_DIR="$WORK_DIR/results"
    FINAL_REPORT="$RESULTS_DIR/aggregated_results.txt"

    echo "Collecting results from all processing tasks..."

    # Initialize report
    {
        echo "Distributed Computing Results Summary"
        echo "===================================="
        echo "Processing Date: {{ ds }}"
        echo "Workload Type: $(jq -r '.workload_type' "$WORK_DIR/metadata.json")"
        echo "Total Partitions: $(jq -r '.partitions' "$WORK_DIR/metadata.json")"
        echo ""
    } > "$FINAL_REPORT"

    # Aggregate CPU processing results
    echo "CPU Processing Results:" >> "$FINAL_REPORT"
    echo "-----------------------" >> "$FINAL_REPORT"
    CPU_COUNT=0
    CPU_SUCCESS=0

    for result_file in "$WORK_DIR/output/cpu_result_"*.out; do
        if [ -f "$result_file" ]; then
            CPU_COUNT=$((CPU_COUNT + 1))
            partition_num=$(basename "$result_file" | grep -o '[0-9]\\+')

            if grep -q "CPU processing completed" "$result_file"; then
                CPU_SUCCESS=$((CPU_SUCCESS + 1))
                status="SUCCESS"
            else
                status="FAILED"
            fi

            lines=$(grep "Total lines processed" "$result_file" 2>/dev/null | awk '{print $4}' || echo "N/A")
            echo "  Partition $partition_num: $status (Lines: $lines)" >> "$FINAL_REPORT"
        fi
    done

    echo "  Total CPU tasks: $CPU_COUNT, Successful: $CPU_SUCCESS" >> "$FINAL_REPORT"
    echo "" >> "$FINAL_REPORT"

    # Aggregate memory processing results
    echo "Memory Processing Results:" >> "$FINAL_REPORT"
    echo "--------------------------" >> "$FINAL_REPORT"
    MEMORY_COUNT=0
    MEMORY_SUCCESS=0

    for result_file in "$WORK_DIR/output/memory_result_"*.out; do
        if [ -f "$result_file" ]; then
            MEMORY_COUNT=$((MEMORY_COUNT + 1))
            partition_num=$(basename "$result_file" | grep -o '[0-9]\\+')

            if grep -q "Memory processing completed" "$result_file"; then
                MEMORY_SUCCESS=$((MEMORY_SUCCESS + 1))
                status="SUCCESS"
            else
                status="FAILED"
            fi

            lines=$(grep "Total lines:" "$result_file" 2>/dev/null | awk '{print $3}' || echo "N/A")
            echo "  Partition $partition_num: $status (Lines: $lines)" >> "$FINAL_REPORT"
        fi
    done

    echo "  Total Memory tasks: $MEMORY_COUNT, Successful: $MEMORY_SUCCESS" >> "$FINAL_REPORT"
    echo "" >> "$FINAL_REPORT"

    # Aggregate long-running results
    echo "Long-Running Processing Results:" >> "$FINAL_REPORT"
    echo "--------------------------------" >> "$FINAL_REPORT"
    LONG_COUNT=0
    LONG_SUCCESS=0

    for result_file in "$WORK_DIR/output/long_result_"*.out; do
        if [ -f "$result_file" ]; then
            LONG_COUNT=$((LONG_COUNT + 1))
            partition_num=$(basename "$result_file" | grep -o '[0-9]\\+')

            if grep -q "Long-running analysis completed" "$result_file"; then
                LONG_SUCCESS=$((LONG_SUCCESS + 1))
                status="SUCCESS"
            else
                status="FAILED"
            fi

            passes=$(grep "Total passes:" "$result_file" 2>/dev/null | awk '{print $3}' || echo "N/A")
            echo "  Partition $partition_num: $status (Passes: $passes)" >> "$FINAL_REPORT"
        fi
    done

    echo "  Total Long tasks: $LONG_COUNT, Successful: $LONG_SUCCESS" >> "$FINAL_REPORT"
    echo "" >> "$FINAL_REPORT"

    # Overall summary
    TOTAL_TASKS=$((CPU_COUNT + MEMORY_COUNT + LONG_COUNT))
    TOTAL_SUCCESS=$((CPU_SUCCESS + MEMORY_SUCCESS + LONG_SUCCESS))
    SUCCESS_RATE=$(echo "scale=1; $TOTAL_SUCCESS * 100 / $TOTAL_TASKS" | bc)

    {
        echo "Overall Summary:"
        echo "================="
        echo "Total processing tasks: $TOTAL_TASKS"
        echo "Successful tasks: $TOTAL_SUCCESS"
        echo "Success rate: $SUCCESS_RATE%"
        echo ""
        echo "Performance by type:"
        echo "  CPU tasks: $CPU_SUCCESS/$CPU_COUNT"
        echo "  Memory tasks: $MEMORY_SUCCESS/$MEMORY_COUNT"
        echo "  Long tasks: $LONG_SUCCESS/$LONG_COUNT"
        echo ""
        echo "Processing completed at: $(date)"
    } >> "$FINAL_REPORT"

    echo "Results aggregated successfully"
    echo "Final report: $FINAL_REPORT"
    echo ""
    echo "=== SUMMARY ==="
    echo "Total tasks: $TOTAL_TASKS"
    echo "Success rate: $SUCCESS_RATE%"

    # Display the report
    echo ""
    echo "=== FINAL REPORT ==="
    cat "$FINAL_REPORT"
    """,
    executor_config={
        "slurm": {
            "partition": "normal",
            "cpus_per_task": 2,
            "mem": "2G",
            "time_limit": "00:10:00",
        }
    },
    dag=dag,
)

# Performance analysis
performance_analysis = BashOperator(
    task_id="analyze_performance",
    bash_command="""
    echo "=== Performance Analysis ==="

    WORK_DIR="/tmp/distributed_computing_{{ ds_nodash }}"
    PERF_REPORT="$WORK_DIR/results/performance_analysis.json"

    # Analyze processing logs for performance metrics
    echo "Analyzing processing performance..."

    # Initialize performance data
    cat > "$PERF_REPORT" << EOF
{
    "analysis_date": "{{ ds }}",
    "performance_metrics": {
EOF

    # Analyze CPU task performance
    echo '    "cpu_tasks": [' >> "$PERF_REPORT"
    first_cpu=true

    for log_file in "$WORK_DIR/logs/cpu_process_"*.log; do
        if [ -f "$log_file" ]; then
            task_num=$(basename "$log_file" | grep -o '[0-9]\\+')

            if [ "$first_cpu" = true ]; then
                first_cpu=false
            else
                echo ',' >> "$PERF_REPORT"
            fi

            # Extract timing information (simulate performance metrics)
            start_time=$(grep "Started at:" "$log_file" 2>/dev/null | head -1)
            end_time=$(grep "Completed at:" "$log_file" 2>/dev/null | head -1)

            cat >> "$PERF_REPORT" << EOF
      {
        "task_id": "cpu_process_partition_$task_num",
        "partition_type": "normal",
        "cpu_cores": 4,
        "memory_gb": 4,
        "estimated_duration_minutes": 10
      }EOF
        fi
    done

    echo '' >> "$PERF_REPORT"
    echo '    ],' >> "$PERF_REPORT"

    # Analyze memory task performance
    echo '    "memory_tasks": [' >> "$PERF_REPORT"
    first_mem=true

    for log_file in "$WORK_DIR/logs/memory_process_"*.log; do
        if [ -f "$log_file" ]; then
            task_num=$(basename "$log_file" | grep -o '[0-9]\\+')

            if [ "$first_mem" = true ]; then
                first_mem=false
            else
                echo ',' >> "$PERF_REPORT"
            fi

            cat >> "$PERF_REPORT" << EOF
      {
        "task_id": "memory_process_partition_$task_num",
        "partition_type": "normal",
        "cpu_cores": 2,
        "memory_gb": 16,
        "estimated_duration_minutes": 15
      }EOF
        fi
    done

    echo '' >> "$PERF_REPORT"
    echo '    ],' >> "$PERF_REPORT"

    # Analyze long task performance
    echo '    "long_tasks": [' >> "$PERF_REPORT"
    first_long=true

    for log_file in "$WORK_DIR/logs/long_process_"*.log; do
        if [ -f "$log_file" ]; then
            task_num=$(basename "$log_file" | grep -o '[0-9]\\+')

            if [ "$first_long" = true ]; then
                first_long=false
            else
                echo ',' >> "$PERF_REPORT"
            fi

            cat >> "$PERF_REPORT" << EOF
      {
        "task_id": "long_process_partition_$task_num",
        "partition_type": "long",
        "cpu_cores": 2,
        "memory_gb": 4,
        "estimated_duration_minutes": 60
      }EOF
        fi
    done

    echo '' >> "$PERF_REPORT"
    echo '    ]' >> "$PERF_REPORT"
    echo '  },' >> "$PERF_REPORT"

    # Add summary metrics
    TOTAL_CPU_CORES=$(echo "4 * $(ls "$WORK_DIR/logs/cpu_process_"*.log 2>/dev/null | wc -l) + 2 * $(ls "$WORK_DIR/logs/memory_process_"*.log 2>/dev/null | wc -l) + 2 * $(ls "$WORK_DIR/logs/long_process_"*.log 2>/dev/null | wc -l)" | bc)
    TOTAL_MEMORY_GB=$(echo "4 * $(ls "$WORK_DIR/logs/cpu_process_"*.log 2>/dev/null | wc -l) + 16 * $(ls "$WORK_DIR/logs/memory_process_"*.log 2>/dev/null | wc -l) + 4 * $(ls "$WORK_DIR/logs/long_process_"*.log 2>/dev/null | wc -l)" | bc)

    cat >> "$PERF_REPORT" << EOF
  "resource_summary": {
    "total_cpu_cores_used": $TOTAL_CPU_CORES,
    "total_memory_gb_used": $TOTAL_MEMORY_GB,
    "partitions_utilized": ["normal", "long"],
    "parallel_tasks": $(ls "$WORK_DIR/output/"*.out 2>/dev/null | wc -l)
  }
}
EOF

    echo "Performance analysis completed"
    echo "Report: $PERF_REPORT"
    echo ""
    echo "Resource Summary:"
    echo "  Total CPU cores used: $TOTAL_CPU_CORES"
    echo "  Total memory used: ${TOTAL_MEMORY_GB}GB"
    echo "  Parallel tasks executed: $(ls "$WORK_DIR/output/"*.out 2>/dev/null | wc -l)"
    """,
    dag=dag,
)

# Cleanup with resource reporting
cleanup = BashOperator(
    task_id="cleanup_and_report",
    bash_command="""
    echo "=== Cleanup and Final Resource Report ==="

    WORK_DIR="/tmp/distributed_computing_{{ ds_nodash }}"

    if [ -d "$WORK_DIR" ]; then
        echo "Final workspace analysis:"
        echo "========================="
        echo "Total workspace size: $(du -sh "$WORK_DIR" | cut -f1)"
        echo ""
        echo "Space usage by directory:"
        du -sh "$WORK_DIR"/*
        echo ""
        echo "File count by type:"
        echo "  Input files: $(find "$WORK_DIR/input" -name "*.dat" 2>/dev/null | wc -l)"
        echo "  Output files: $(find "$WORK_DIR/output" -name "*.out" 2>/dev/null | wc -l)"
        echo "  Log files: $(find "$WORK_DIR/logs" -name "*.log" 2>/dev/null | wc -l)"
        echo "  Result files: $(find "$WORK_DIR/results" -name "*" -type f 2>/dev/null | wc -l)"

        # Clean up large temporary files but keep results
        echo ""
        echo "Cleaning up temporary files..."
        rm -rf "$WORK_DIR/input" "$WORK_DIR/temp" 2>/dev/null

        echo "Post-cleanup size: $(du -sh "$WORK_DIR" | cut -f1)"

        echo ""
        echo "Preserved files:"
        find "$WORK_DIR" -type f | head -10
    else
        echo "No workspace found to analyze"
    fi

    echo ""
    echo "✓ Distributed computing pipeline completed successfully"
    """,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# End
end = DummyOperator(
    task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, dag=dag
)

# Define dependencies
start >> analyze_workload >> setup_environment >> parallel_processing
parallel_processing >> aggregate_results >> performance_analysis >> cleanup >> end
