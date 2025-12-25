"""
Parallel Processing with Slurm Executor

This example demonstrates parallel data processing using multiple Slurm jobs
with different resource requirements and partitions.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'parallel_processing_slurm',
    default_args=default_args,
    description='Parallel data processing using Slurm executor',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['slurm', 'parallel', 'data-processing']
)

def generate_partitioned_data():
    """Generate partitioned data files for parallel processing."""
    import os
    base_dir = f"/tmp/parallel_processing_{{{{ ds_nodash }}}}"
    os.makedirs(base_dir, exist_ok=True)
    
    for partition in range(4):
        file_path = f"{base_dir}/partition_{partition}.csv"
        with open(file_path, 'w') as f:
            f.write("id,value,timestamp\\n")
            for i in range(1000):
                f.write(f"{partition * 1000 + i},{i % 100},2024-01-01 {i % 24:02d}:00:00\\n")
        print(f"Created partition {partition} with 1000 records")

# Start and data preparation
start = DummyOperator(task_id='start', dag=dag)

prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=generate_partitioned_data,
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 1,
            'mem': '256M',
            'time_limit': '00:02:00',
        }
    },
    dag=dag
)

# Parallel processing tasks - different resource requirements
process_partition_tasks = []

for i in range(4):
    # CPU-intensive partition processing
    if i < 2:
        task = BashOperator(
            task_id=f'process_partition_{i}_cpu',
            bash_command=f'''
            echo "=== Processing Partition {i} (CPU Intensive) ==="
            PARTITION_FILE="/tmp/parallel_processing_{{{{ ds_nodash }}}}/partition_{i}.csv"
            OUTPUT_FILE="/tmp/parallel_processing_{{{{ ds_nodash }}}}/processed_{i}.csv"
            
            if [ ! -f "$PARTITION_FILE" ]; then
                echo "Error: Partition file not found: $PARTITION_FILE"
                exit 1
            fi
            
            echo "Input file: $PARTITION_FILE"
            echo "Records in partition: $(wc -l < $PARTITION_FILE)"
            
            # Simulate CPU-intensive processing
            echo "id,value,timestamp,computed_value" > $OUTPUT_FILE
            tail -n +2 $PARTITION_FILE | while IFS=',' read -r id value timestamp; do
                # Simulate computation
                computed_value=$((value * value + id % 100))
                echo "$id,$value,$timestamp,$computed_value" >> $OUTPUT_FILE
                # Simulate processing time
                if [ $((id % 100)) -eq 0 ]; then
                    echo "Processed $id records..."
                fi
            done
            
            echo "Completed processing partition {i}"
            echo "Output records: $(tail -n +2 $OUTPUT_FILE | wc -l)"
            ''',
            executor_config={{
                'slurm': {{
                    'partition': 'normal',
                    'cpus_per_task': 2,  # More CPUs for intensive work
                    'mem': '1G',
                    'time_limit': '00:10:00',
                }}
            }},
            dag=dag
        )
    else:
        # Memory-intensive partition processing
        task = BashOperator(
            task_id=f'process_partition_{i}_memory',
            bash_command=f'''
            echo "=== Processing Partition {i} (Memory Intensive) ==="
            PARTITION_FILE="/tmp/parallel_processing_{{{{ ds_nodash }}}}/partition_{i}.csv"
            OUTPUT_FILE="/tmp/parallel_processing_{{{{ ds_nodash }}}}/processed_{i}.csv"
            
            if [ ! -f "$PARTITION_FILE" ]; then
                echo "Error: Partition file not found: $PARTITION_FILE"
                exit 1
            fi
            
            echo "Input file: $PARTITION_FILE"
            
            # Simulate memory-intensive processing (sorting, aggregating)
            echo "id,value,timestamp,aggregated_value" > $OUTPUT_FILE
            
            # Load all data into memory and process (simulated with sort)
            tail -n +2 $PARTITION_FILE | sort -t',' -k2,2n | while IFS=',' read -r id value timestamp; do
                # Simulate memory-intensive aggregation
                aggregated_value=$((value + (id % 1000)))
                echo "$id,$value,$timestamp,$aggregated_value" >> $OUTPUT_FILE
            done
            
            echo "Completed memory-intensive processing for partition {i}"
            echo "Output records: $(tail -n +2 $OUTPUT_FILE | wc -l)"
            ''',
            executor_config={{
                'slurm': {{
                    'partition': 'normal', 
                    'cpus_per_task': 1,
                    'mem': '2G',  # More memory for intensive work
                    'time_limit': '00:15:00',
                }}
            }},
            dag=dag
        )
    
    process_partition_tasks.append(task)

# Aggregation task that waits for all partitions
aggregate_results = BashOperator(
    task_id='aggregate_results',
    bash_command='''
    echo "=== Aggregating Results ==="
    BASE_DIR="/tmp/parallel_processing_{{ ds_nodash }}"
    FINAL_OUTPUT="$BASE_DIR/final_results.csv"
    
    echo "id,value,timestamp,processed_value,partition" > $FINAL_OUTPUT
    
    total_records=0
    for i in {0..3}; do
        PROCESSED_FILE="$BASE_DIR/processed_$i.csv"
        if [ -f "$PROCESSED_FILE" ]; then
            echo "Merging partition $i..."
            tail -n +2 "$PROCESSED_FILE" | while IFS=',' read -r id value timestamp processed_value; do
                echo "$id,$value,$timestamp,$processed_value,$i" >> $FINAL_OUTPUT
            done
            partition_records=$(tail -n +2 "$PROCESSED_FILE" | wc -l)
            total_records=$((total_records + partition_records))
            echo "Partition $i: $partition_records records"
        else
            echo "Warning: Processed file not found for partition $i"
        fi
    done
    
    echo "=== Final Statistics ==="
    echo "Total records processed: $(tail -n +2 $FINAL_OUTPUT | wc -l)"
    echo "Output file: $FINAL_OUTPUT"
    
    # Generate summary statistics
    echo "=== Summary by Partition ==="
    tail -n +2 $FINAL_OUTPUT | cut -d',' -f5 | sort | uniq -c | while read count partition; do
        echo "Partition $partition: $count records"
    done
    
    # Save processing summary
    SUMMARY_FILE="$BASE_DIR/processing_summary.txt"
    {
        echo "Parallel Processing Summary"
        echo "=========================="
        echo "Date: {{ ds }}"
        echo "Processing completed at: $(date)"
        echo "Total records: $(tail -n +2 $FINAL_OUTPUT | wc -l)"
        echo "Partitions processed: 4"
        echo "Output file: $FINAL_OUTPUT"
    } > $SUMMARY_FILE
    
    echo "Summary saved to: $SUMMARY_FILE"
    ''',
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 1,
            'mem': '512M',
            'time_limit': '00:05:00',
        }
    },
    dag=dag
)

# Quality check task
quality_check = BashOperator(
    task_id='quality_check',
    bash_command='''
    echo "=== Data Quality Check ==="
    BASE_DIR="/tmp/parallel_processing_{{ ds_nodash }}"
    FINAL_OUTPUT="$BASE_DIR/final_results.csv"
    
    if [ ! -f "$FINAL_OUTPUT" ]; then
        echo "Error: Final output file not found"
        exit 1
    fi
    
    # Check record counts
    expected_records=4000  # 4 partitions * 1000 records each
    actual_records=$(tail -n +2 "$FINAL_OUTPUT" | wc -l)
    
    echo "Expected records: $expected_records"
    echo "Actual records: $actual_records"
    
    if [ "$actual_records" -eq "$expected_records" ]; then
        echo "✓ Record count check PASSED"
    else
        echo "✗ Record count check FAILED"
        exit 1
    fi
    
    # Check for duplicates
    duplicate_count=$(tail -n +2 "$FINAL_OUTPUT" | cut -d',' -f1 | sort | uniq -d | wc -l)
    echo "Duplicate IDs found: $duplicate_count"
    
    if [ "$duplicate_count" -eq 0 ]; then
        echo "✓ Duplicate check PASSED"
    else
        echo "✗ Duplicate check FAILED"
        exit 1
    fi
    
    echo "✓ All quality checks PASSED"
    ''',
    dag=dag
)

# Cleanup
cleanup = BashOperator(
    task_id='cleanup',
    bash_command='''
    echo "=== Cleanup ==="
    BASE_DIR="/tmp/parallel_processing_{{ ds_nodash }}"
    
    if [ -d "$BASE_DIR" ]; then
        echo "Cleaning up processing directory: $BASE_DIR"
        echo "Files to remove:"
        ls -la "$BASE_DIR/"
        
        # Keep summary but remove data files
        find "$BASE_DIR" -name "*.csv" -delete
        echo "Data files removed, keeping summary files"
    else
        echo "No cleanup needed - directory not found"
    fi
    ''',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Define dependencies
start >> prepare_data
prepare_data >> process_partition_tasks
process_partition_tasks >> aggregate_results >> quality_check >> cleanup >> end