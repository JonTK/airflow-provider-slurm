# Airflow Slurm Executor - DAG Examples

This directory contains comprehensive examples demonstrating various use cases and patterns for the Airflow Slurm Executor. Each example showcases different aspects of distributed computing with Slurm.

## 🚀 Quick Start

1. **Install the Slurm Executor**:
   ```bash
   pip install airflow-slurm-executor
   ```

2. **Configure Airflow** (see [Configuration Guide](#configuration) below)

3. **Copy examples to your DAGs folder**:
   ```bash
   cp examples/dags/* $AIRFLOW_HOME/dags/
   ```

4. **Enable the Slurm Executor** in `airflow.cfg`:
   ```ini
   [core]
   executor = airflow_slurm_executor.slurm_executor.SlurmExecutor
   ```

## 📁 Example DAGs Overview

### 1. Basic Slurm DAG (`01_basic_slurm_dag.py`)
**Complexity**: Beginner  
**Use Case**: Introduction to Slurm executor basics

**Features**:
- Simple system information gathering
- Basic data processing pipeline
- Resource configuration examples
- File cleanup patterns

**Resources**: 1 CPU, 512MB-1GB RAM, normal partition  
**Duration**: ~10 minutes

```python
# Example task configuration
executor_config = {
    'slurm': {
        'partition': 'normal',
        'cpus_per_task': 1,
        'mem': '512M',
        'time_limit': '00:05:00',
    }
}
```

### 2. Parallel Processing DAG (`02_parallel_processing_dag.py`)
**Complexity**: Intermediate  
**Use Case**: Data processing with parallel tasks

**Features**:
- Dynamic data partitioning
- Parallel task execution
- Different resource requirements per task
- Data aggregation and quality checks
- Resource optimization patterns

**Resources**: 1-2 CPUs, 512MB-2GB RAM per task  
**Duration**: ~20 minutes

**Key Concepts**:
- CPU-intensive vs memory-intensive task configuration
- Parallel processing patterns
- Data validation and quality assurance

### 3. Machine Learning Pipeline (`03_machine_learning_pipeline.py`)
**Complexity**: Advanced  
**Use Case**: Complete ML workflow with different computational stages

**Features**:
- Conditional execution based on data availability
- Feature engineering with scikit-learn
- Model training and evaluation
- Model deployment preparation
- Resource scaling based on workload

**Resources**: 1-8 CPUs, 256MB-8GB RAM depending on stage  
**Duration**: ~45 minutes

**Stages**:
1. **Data Preparation** (2 CPUs, 2GB) - Data generation and preprocessing
2. **Feature Engineering** (4 CPUs, 4GB) - Feature extraction and transformation
3. **Model Training** (8 CPUs, 8GB) - Multiple algorithm training
4. **Evaluation** (2 CPUs, 2GB) - Model assessment and selection
5. **Deployment Prep** (1 CPU, 512MB) - Artifact packaging

### 4. Bioinformatics Workflow (`04_bioinformatics_workflow.py`)
**Complexity**: Advanced  
**Use Case**: Genomic data processing pipeline

**Features**:
- File-based conditional execution
- Quality control analysis
- Parallel sample processing
- Memory-intensive alignment tasks
- Variant calling and annotation
- Comprehensive reporting

**Resources**: 1-8 CPUs, 1GB-16GB RAM per task  
**Duration**: 1-3 hours

**Typical Bioinformatics Steps**:
1. **Quality Control** (2 CPUs, 2GB) - FastQC simulation
2. **Read Alignment** (8 CPUs, 16GB) - BWA mem simulation
3. **Variant Calling** (4 CPUs, 8GB) - GATK simulation
4. **Annotation** (2 CPUs, 4GB) - VEP simulation
5. **Reporting** (1 CPU, 512MB) - HTML report generation

### 5. Distributed Computing DAG (`05_distributed_computing_dag.py`)
**Complexity**: Expert  
**Use Case**: Large-scale distributed processing with dynamic resource allocation

**Features**:
- Dynamic workload determination
- TaskGroups for complex workflows
- Multiple partition utilization
- Performance monitoring and analysis
- Resource optimization strategies

**Resources**: Variable (2-16 partitions, different CPU/memory configs)  
**Duration**: 30 minutes - 2 hours

**Processing Types**:
- **CPU-Intensive Tasks**: 4 CPUs, normal partition
- **Memory-Intensive Tasks**: 2 CPUs, 16GB RAM, normal partition  
- **Long-Running Tasks**: 2 CPUs, 4GB RAM, long partition

## ⚙️ Configuration

### Airflow Configuration (`airflow.cfg`)

```ini
[core]
executor = airflow_slurm_executor.slurm_executor.SlurmExecutor

[slurm]
# Slurm REST API configuration
api_url = http://your-slurm-server:6820
username = your_username
token_lifespan = 3600

# Default resource allocation
default_partition = normal
default_cpus = 1
default_mem = 1G
default_time_limit = 01:00:00
default_account = your_account

# Optional: Virtual environment
airflow_venv = /path/to/your/airflow/venv

# Optional: Container support
# default_container = your_container_image

# API settings
api_timeout = 30
api_max_retries = 3
sync_interval = 10.0

# Shutdown behavior
shutdown_mode = cancel  # or 'wait'
shutdown_wait_timeout = 300
```

### Environment Variables

```bash
# Alternative to airflow.cfg settings
export AIRFLOW__SLURM__API_URL="http://your-slurm-server:6820"
export AIRFLOW__SLURM__USERNAME="your_username"
export AIRFLOW__SLURM__DEFAULT_PARTITION="normal"
export AIRFLOW__SLURM__DEFAULT_CPUS="1"
export AIRFLOW__SLURM__DEFAULT_MEM="1G"
export AIRFLOW__SLURM__DEFAULT_TIME_LIMIT="01:00:00"
```

### Slurm Cluster Requirements

**Required**:
- Slurm REST API (slurmrestd) running and accessible
- JWT token authentication enabled
- Shared filesystem between Airflow and compute nodes

**Recommended Partitions**:
- `normal`: General purpose computing (default)
- `debug`: Short test jobs (limited time)
- `long`: Extended duration jobs
- `gpu`: GPU-accelerated computing (optional)

**Network Access**:
- Airflow scheduler → Slurm REST API (typically port 6820)
- Compute nodes → Airflow log directory (shared filesystem)

## 🎯 Task Configuration Patterns

### Basic Task Configuration
```python
task = BashOperator(
    task_id='basic_task',
    bash_command='echo "Hello Slurm!"',
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 1,
            'mem': '1G',
            'time_limit': '00:10:00',
        }
    }
)
```

### CPU-Intensive Task
```python
cpu_task = BashOperator(
    task_id='cpu_intensive',
    bash_command='compute_heavy_algorithm.sh',
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 8,
            'mem': '4G',
            'time_limit': '01:00:00',
        }
    }
)
```

### Memory-Intensive Task
```python
memory_task = BashOperator(
    task_id='memory_intensive',
    bash_command='process_large_dataset.py',
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 2,
            'mem': '32G',
            'time_limit': '02:00:00',
        }
    }
)
```

### GPU Task
```python
gpu_task = BashOperator(
    task_id='gpu_training',
    bash_command='python train_model.py --gpu',
    executor_config={
        'slurm': {
            'partition': 'gpu',
            'cpus_per_task': 4,
            'mem': '16G',
            'time_limit': '04:00:00',
            'gres': 'gpu:1',  # Request 1 GPU
        }
    }
)
```

### Container Task
```python
container_task = BashOperator(
    task_id='containerized_task',
    bash_command='python analysis.py',
    executor_config={
        'slurm': {
            'partition': 'normal',
            'cpus_per_task': 2,
            'mem': '4G',
            'time_limit': '00:30:00',
            'container': 'docker://tensorflow/tensorflow:latest'
        }
    }
)
```

## 📊 Monitoring and Debugging

### View Job Status
```bash
# Check Slurm queue
squeue

# Check specific job
scontrol show job <job_id>

# View job output
cat /path/to/airflow/logs/dag_id/task_id/run_id/1.log
```

### Common Issues and Solutions

**Issue**: Jobs stuck in PENDING  
**Solution**: Check partition availability and resource requests
```bash
sinfo  # Check partition status
squeue --start  # Check estimated start times
```

**Issue**: Jobs fail immediately  
**Solution**: Check file permissions and shared filesystem
```bash
# Test shared filesystem
touch /shared/test_file_$(date +%s)
```

**Issue**: Authentication failures  
**Solution**: Verify JWT token and scontrol access
```bash
scontrol token  # Generate new token
```

## 🔧 Customization

### Custom Resource Profiles

Create reusable resource profiles in your DAGs:

```python
# Resource profiles
RESOURCE_PROFILES = {
    'small': {
        'partition': 'normal',
        'cpus_per_task': 1,
        'mem': '1G',
        'time_limit': '00:15:00'
    },
    'medium': {
        'partition': 'normal',
        'cpus_per_task': 4,
        'mem': '8G',
        'time_limit': '01:00:00'
    },
    'large': {
        'partition': 'normal',
        'cpus_per_task': 16,
        'mem': '32G',
        'time_limit': '04:00:00'
    },
    'gpu': {
        'partition': 'gpu',
        'cpus_per_task': 8,
        'mem': '16G',
        'time_limit': '02:00:00',
        'gres': 'gpu:1'
    }
}

# Use in tasks
task = BashOperator(
    task_id='my_task',
    bash_command='...',
    executor_config={'slurm': RESOURCE_PROFILES['medium']}
)
```

### Dynamic Resource Allocation

```python
def get_resource_config(data_size):
    if data_size < 1000:
        return RESOURCE_PROFILES['small']
    elif data_size < 10000:
        return RESOURCE_PROFILES['medium']
    else:
        return RESOURCE_PROFILES['large']

# Use in PythonOperator
def process_data(**context):
    data_size = context['ti'].xcom_pull(key='data_size')
    config = get_resource_config(data_size)
    # Process data with appropriate resources
```

## 🚀 Best Practices

### 1. Resource Planning
- Start with conservative resource requests
- Monitor actual usage and adjust accordingly
- Use appropriate partitions for different workloads
- Consider cost implications of resource requests

### 2. Error Handling
- Implement proper retry logic
- Use trigger rules for complex dependencies  
- Include cleanup tasks for temporary files
- Monitor for stuck jobs and implement timeouts

### 3. Performance Optimization
- Parallelize independent tasks
- Use shared filesystem efficiently
- Batch small tasks when possible
- Monitor queue times and adjust schedules

### 4. Security
- Use minimal required permissions
- Secure token management
- Implement proper data access controls
- Regular security audits

## 📚 Additional Resources

- **Slurm Documentation**: https://slurm.schedmd.com/documentation.html
- **Airflow Documentation**: https://airflow.apache.org/docs/
- **Executor Repository**: https://github.com/JonTK/airflow-provider-slurm
- **Issues and Support**: https://github.com/JonTK/airflow-provider-slurm/issues

## 🤝 Contributing

We welcome contributions! Please see our examples as templates for:
- New use case patterns
- Performance optimizations
- Integration examples
- Documentation improvements

Submit pull requests with new examples following the established patterns and documentation standards.