# Batch Orchestration (Airflow)

Apache Airflow DAGs for orchestrating batch transaction processing workflows. Coordinates the execution of batch processing jobs, manages dependencies, and provides monitoring and alerting capabilities.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [DAGs](#dags)
- [Usage](#usage)
- [Monitoring](#monitoring)

## 🎯 Overview

The orchestration module provides Apache Airflow DAGs that:

1. **Schedule** - Run one batch job per monthly data partition, backfilling the dataset with catchup
2. **Coordinate** - Manage dependencies between extraction, validation, prediction, and loading steps
3. **Monitor** - Track job execution status and provide alerting on failures
4. **Retry** - Automatically retry failed tasks with exponential backoff

**Key DAGs:**
- `batch_pipeline_dag` - Main daily batch processing workflow

## ✨ Features

- **Docker-based Execution** - Runs batch processing in isolated Docker containers
- **Dynamic Configuration** - Pulls environment variables from Airflow connections
- **Task Dependencies** - Manages proper sequencing of pipeline stages
- **Error Handling** - Comprehensive retry logic and failure notifications
- **Monitoring** - Integration with Airflow UI for job tracking and debugging

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Airflow Scheduler                           │
│                                                          │
│  ┌───────────────────────────────────────────────────┐ │
│  │  batch_pipeline_dag (Daily @ 2 AM UTC)           │ │
│  │                                                   │ │
│  │  1. Extract credentials from Airflow Connection  │ │
│  │  2. Run Docker container with batch service      │ │
│  │  3. Monitor execution and handle failures        │ │
│  └───────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 📋 Prerequisites

- Docker and Docker Compose
- Apache Airflow 3.1.5+
- Access to Docker registry with batch service image
- Configured Airflow connections for:
  - MinIO/S3 credentials
  - PostgreSQL credentials
  - ML API endpoint

## 🚀 Installation

### Local Development

```bash
# Navigate to orchestration directory
cd pipeline/application/batch/orchestration

# Build Airflow image
docker build -t batch-orchestration:latest .

# Start Airflow services (via docker-compose at project root)
cd /path/to/project/root
docker-compose up airflow-webserver airflow-scheduler
```

### Production Deployment

```bash
# Deploy to Kubernetes/Cloud
kubectl apply -f k8s/airflow-deployment.yaml
```

## ⚙️ Configuration

### Airflow Connections

Configure the following connections in Airflow UI (`Admin > Connections`):

**1. MinIO Connection (`minio_default`)**
```
Connection Type: Generic
Host: minio
Port: 9000
Login: <MINIO_USER>
Password: <MINIO_PASSWORD>
Extra: {"secure": false}
```

**2. PostgreSQL Connection (`postgres_default`)**
```
Connection Type: Postgres
Host: postgres
Port: 5432
Schema: transactions
Login: <DB_USER>
Password: <DB_PASSWORD>
```

**3. ML API Connection (`ml_api_default`)**
```
Connection Type: HTTP
Host: http://ml-api:8000
```

### DAG Configuration

Edit `dags/batch_pipeline_dag.py` to customize:

```python
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
```

## 📊 DAGs

### batch_pipeline_dag

**Purpose:** Process one monthly partition of transaction data per run

**Schedule:** Monthly data intervals (`CronDataIntervalTimetable("@monthly")`) from 2023-01 to
2024-04, with catchup and `max_active_runs=1`: enabling the DAG backfills the 16 months one at a
time. The run for month M processes `s3://transactions/raw/month=M/transactions.csv` and writes
records it can't process to `s3://transactions/rejects/month=M/`. Writes are idempotent, so clearing a
run reprocesses that month safely.

**Tasks:**
1. `get_environment_vars` - Extract credentials from Airflow connections and compute the partition paths
2. `run_batch_processing` - Execute batch service in Docker container (exit 1 if predictions failed, so Airflow retries)
3. `log_completion` - Log the run

**Dependencies:**
```
prepare_environment >> run_batch_processing
```

**Configuration:**
- Task timeout: 1 hour
- Retries: 3 times with 5-minute delays
- Email alerts on failure

## 💻 Usage

### Access Airflow UI

```bash
# Start Airflow
docker-compose up airflow-webserver

# Access UI
open http://localhost:8080

# Default credentials (change in production!)
Username: airflow
Password: airflow
```

### Trigger Manual DAG Run

```bash
# Via UI: DAGs > batch_pipeline_dag > Play button

# Via CLI
docker exec -it airflow-scheduler \
  airflow dags trigger batch_pipeline_dag
```

### View Logs

```bash
# Via UI: DAGs > batch_pipeline_dag > Graph > Task > Logs

# Via CLI
docker exec -it airflow-scheduler \
  airflow tasks logs batch_pipeline_dag run_batch_processing 2026-01-12
```

## 📈 Monitoring

### Airflow UI Dashboards

- **DAGs View** - Overview of all DAGs and their status
- **Graph View** - Visual representation of task dependencies
- **Gantt View** - Timeline of task execution
- **Task Duration** - Historical performance metrics

### Alerts

Configure email alerts in `airflow.cfg`:

```ini
[email]
email_backend = airflow.utils.email.send_email_smtp

[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your-email@gmail.com
smtp_password = your-app-password
smtp_port = 587
smtp_mail_from = airflow@yourdomain.com
```

### Health Checks

```bash
# Check scheduler health
docker exec airflow-scheduler airflow jobs check --hostname <hostname>

# Check database connectivity
docker exec airflow-scheduler airflow db check

# List active DAGs
docker exec airflow-scheduler airflow dags list
```

## 🔧 Development

### Testing DAGs Locally

```bash
# Install dependencies
uv sync

# Test DAG integrity
python dags/batch_pipeline_dag.py

# Run specific task
docker exec -it airflow-scheduler \
  airflow tasks test batch_pipeline_dag run_batch_processing 2026-01-12
```

### Adding New DAGs

1. Create new DAG file in `dags/` directory
2. Follow naming convention: `{purpose}_dag.py`
3. Import required operators and utilities
4. Define DAG with proper configuration
5. Test locally before deploying

## 📝 Best Practices

- **Idempotency** - Ensure tasks can be safely retried
- **Atomicity** - Keep tasks focused on single responsibilities
- **Monitoring** - Add logging and metrics to track performance
- **Documentation** - Document DAG purpose, schedule, and dependencies
- **Testing** - Validate DAG logic before production deployment

## 🐛 Troubleshooting

### DAG Not Appearing in UI

```bash
# Check DAG file for syntax errors
python dags/batch_pipeline_dag.py

# Refresh DAGs in Airflow
docker exec airflow-scheduler airflow dags list-import-errors
```

### Task Failing with Connection Error

```bash
# Verify connection exists
docker exec airflow-scheduler airflow connections list

# Test connection
docker exec airflow-scheduler airflow connections test <connection-id>
```

### Docker Container Fails to Start

```bash
# Check Docker daemon is running
docker ps

# Verify Docker socket is mounted correctly
docker exec airflow-scheduler ls -la /var/run/docker.sock

# Check container logs
docker logs <container-id>
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Provider Documentation](https://airflow.apache.org/docs/apache-airflow-providers-docker/)
- [Best Practices Guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
