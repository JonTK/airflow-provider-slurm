# Docker Development Environment

This directory contains Docker configuration for developing and testing the airflow-provider-slurm package.

## Quick Start

```bash
# Build and start all services
docker-compose up -d

# Access Airflow web UI
open http://localhost:8080
# Login: admin / admin

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## Services

### Airflow Webserver
- **Port**: 8080
- **URL**: http://localhost:8080
- **Credentials**: admin / admin
- Access the Airflow UI to monitor DAGs and tasks

### Airflow Scheduler
- Runs in the background
- Executes tasks using the Slurm executor
- Submits jobs to the mock Slurm API

### PostgreSQL
- **Port**: 5432 (internal)
- Airflow metadata database
- Data persisted in `postgres-data` volume

### Slurm Mock API
- **Port**: 6820
- **URL**: http://localhost:6820
- Mock Slurm REST API for development/testing
- Simulates job submission, status, and cancellation

### Dev Container
- Development environment for running tests
- Access with: `docker-compose exec dev bash`

## Development Workflow

### Running Tests

```bash
# Enter dev container
docker-compose exec dev bash

# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_executor.py

# Run with coverage
pytest --cov=airflow_provider_slurm --cov-report=html
```

### Code Formatting

```bash
# Enter dev container
docker-compose exec dev bash

# Format code
black airflow_provider_slurm tests
isort airflow_provider_slurm tests

# Check types
mypy airflow_provider_slurm
```

### Testing with Example DAGs

```bash
# Copy example DAGs to Airflow
cp examples/dags/*.py /airflow/dags/

# Trigger a DAG
docker-compose exec airflow-scheduler airflow dags trigger basic_slurm_dag

# Check task logs
docker-compose exec airflow-scheduler airflow tasks logs basic_slurm_dag task_id run_id
```

## Configuration

### Environment Variables

Edit `docker-compose.yml` to customize:

- `AIRFLOW__SLURM__API_URL`: Slurm API endpoint
- `AIRFLOW__SLURM__DEFAULT_PARTITION`: Default partition
- `AIRFLOW__SLURM__DEFAULT_CPUS`: Default CPU count
- `AIRFLOW__SLURM__DEFAULT_MEM`: Default memory
- `AIRFLOW__SLURM__SYNC_INTERVAL`: Job status sync interval

### Using Real Slurm Cluster

To test with a real Slurm cluster:

1. Update `AIRFLOW__SLURM__API_URL` in `docker-compose.yml`
2. Ensure network connectivity to Slurm REST API
3. Mount shared filesystem if needed

```yaml
airflow-scheduler:
  environment:
    AIRFLOW__SLURM__API_URL: https://your-slurm-cluster:6820
  volumes:
    - /shared/filesystem:/shared:rw
```

## Mock Slurm API

The mock API configuration is in `mockoon-slurm-api.json`.

### Customizing Mock Responses

Edit `mockoon-slurm-api.json` to:
- Add new endpoints
- Modify response data
- Simulate different job states
- Add latency for testing

### Mock API Endpoints

- `GET /openapi/v3` - API version
- `GET /slurm/v0.0.42/ping` - Health check
- `POST /slurm/v0.0.42/job/submit` - Submit job
- `GET /slurm/v0.0.42/jobs` - List jobs
- `GET /slurm/v0.0.42/job/:job_id` - Get job status
- `DELETE /slurm/v0.0.42/job/:job_id` - Cancel job

## Volumes

- `postgres-data`: PostgreSQL database files
- `airflow-logs`: Airflow task logs
- `dev-venv`: Python virtual environment for dev container

## Troubleshooting

### Airflow not starting

```bash
# Check logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler

# Restart services
docker-compose restart
```

### Database migration issues

```bash
# Reset database
docker-compose down -v
docker-compose up -d
```

### Mock API not responding

```bash
# Check mock API logs
docker-compose logs slurm-mock

# Test manually
curl http://localhost:6820/openapi/v3
```

### Permission issues with volumes

```bash
# Fix permissions
sudo chown -R $USER:$USER airflow-logs/
```

## Clean Up

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (deletes data!)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## Production Deployment

This Docker setup is for **development only**. For production:

1. Use a production-grade database (PostgreSQL, MySQL)
2. Configure proper secrets management
3. Use CeleryExecutor or KubernetesExecutor for scaling
4. Set up proper monitoring and logging
5. Use environment-specific configurations
6. Connect to real Slurm cluster with proper authentication

See the main [README.md](../README.md) for production deployment guidance.
