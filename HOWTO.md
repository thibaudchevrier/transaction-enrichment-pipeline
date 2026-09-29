# HOWTO Guide - Transaction Classification Pipeline

Complete guide for running, managing, and testing the transaction classification data pipeline.

## 📋 Table of Contents

- [Prerequisites](#prerequisites)
- [Running Pipelines with Docker Compose](#running-pipelines-with-docker-compose)
- [Running Pipelines with Make](#running-pipelines-with-make)
- [Service Endpoints & UI Access](#service-endpoints--ui-access)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Testing & Code Quality](#testing--code-quality)
- [Common Tasks](#common-tasks)

---

## Prerequisites

### Required Software

#### 1. Docker & Docker Compose

**Installation:**

```bash
# macOS (using Homebrew)
brew install --cask docker

# Or download Docker Desktop from:
# https://www.docker.com/products/docker-desktop

# Verify installation
docker --version
docker-compose --version
```

**Minimum Requirements:**
- Docker Desktop 4.0+
- Docker Compose v2.0+
- 8 GB RAM allocated to Docker
- 20 GB free disk space

#### 2. Make (for simplified commands)

**Installation:**

```bash
# macOS (usually pre-installed, verify with)
make --version

# If not installed
xcode-select --install

# Linux (Debian/Ubuntu)
sudo apt-get install build-essential

# Linux (RHEL/CentOS)
sudo yum groupinstall "Development Tools"
```

#### 3. uv (for Python development & testing)

**Installation:**

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Verify installation
uv --version
```

#### 4. pre-commit (for code quality checks)

**Installation:**

```bash
# Using uv (recommended)
uv tool install pre-commit

# Or using pip
pip install pre-commit

# Verify installation
pre-commit --version
```

---

## Running Pipelines with Docker Compose

### Infrastructure Only (Core Services)

Start core services without pipeline applications:

```bash
docker-compose up -d postgres flyway adminer minio minio-init ml-api
```

**What starts:**
- PostgreSQL database (transactions storage)
- Flyway (database migrations)
- Adminer (database UI)
- MinIO (S3-compatible object storage)
- ML API (transaction classification service)

### Batch Pipeline

Start the batch processing pipeline:

```bash
# Start infrastructure + batch
docker-compose --profile batch up -d

# Or just batch (if infrastructure already running)
docker-compose up -d batch-processor airflow
```

**What starts:**
- Infrastructure services (if not already running)
- Batch processor service (Docker image for Airflow)
- Apache Airflow (scheduler + webserver)

**Check batch logs:**

```bash
docker-compose logs -f batch-processor airflow
```

### Streaming Pipeline

Start the streaming pipeline:

```bash
# Start infrastructure + streaming
docker-compose --profile streaming up -d

# Check streaming logs
docker-compose logs -f streaming-producer-1 streaming-producer-2 streaming-consumer
```

**What starts:**
- Infrastructure services (if not already running)
- Apache Kafka (message broker with KRaft mode)
- Kafka UI (broker management interface)
- 2x Producers (generate transaction events)
- 1x Consumer (process transactions in real-time)

**Producer Configurations:**
- **Producer 1**: Produces 1-10 records every 0.5 seconds
- **Producer 2**: Produces 5-20 records every 1.0 second

### Both Pipelines (Complete System)

Start everything:

```bash
# Start all services
docker-compose --profile all up -d

# Check all pipeline logs
docker-compose logs -f streaming-producer-1 streaming-producer-2 streaming-consumer batch-processor airflow
```

### Stopping Services

```bash
# Stop all services (keeps data)
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Stop specific profile
docker-compose --profile streaming down
docker-compose --profile batch down
```

---

## Running Pipelines with Make

### Quick Start Commands

```bash
# View all available commands
make help

# Start infrastructure only
make infra
# or
make up

# Start streaming pipeline
make streaming

# Start batch pipeline
make batch

# Start everything (infra + streaming + batch)
make all

# Check status of all services
make status

# Stop all services (keeps volumes)
make down

# Stop and clean everything (including volumes)
make clean

# Fix network issues (if services won't start)
make clean-network
```

### Best Practices for Clean Docker State

**To avoid network conflicts and orphaned containers:**

```bash
# Always use --remove-orphans when stopping
make down          # Already includes --remove-orphans

# Use clean instead of down when switching configurations
make clean         # Removes volumes and networks

# If you encounter network errors
make clean-network # Fixes orphaned containers/networks
make all           # Restart everything

# Weekly maintenance (optional)
make clean-all     # Deep clean unused Docker resources
```

**Common scenarios:**

```bash
# Switching from streaming to batch
make down
make batch

# Fresh start with clean data
make clean
make all

# Network error occurred
make clean-network
make all

# Free up disk space
make clean-all  # Removes ALL unused Docker resources
```

### Viewing Logs

```bash
# All services
make logs

# Streaming services only
make logs-streaming

# Batch services only
make logs-batch

# Consumer only
make logs-consumer

# Producers only
make logs-producers

# ML API
make logs-ml-api

# Airflow
make logs-airflow
```

### Rebuilding Services

```bash
# Rebuild streaming services
make rebuild-streaming

# Rebuild batch service
make rebuild-batch

# Rebuild all services
make rebuild-all

# Full rebuild with no cache
make rebuild-clean
```

### Restarting Services

```bash
# Restart consumer
make restart-consumer

# Restart producers
make restart-producers
```

---

## Service Endpoints & UI Access

### 🗄️ Database & Storage

#### PostgreSQL Database

- **Connection:** `localhost:5432`
- **Database:** `transactions`
- **Username:** `pipeline`
- **Password:** `pipeline_password`
- **JDBC URL:** `jdbc:postgresql://localhost:5432/transactions`

**Connect with psql:**

```bash
psql -h localhost -p 5432 -U pipeline -d transactions
```

#### Adminer (Database UI)

- **URL:** http://localhost:8080
- **System:** PostgreSQL
- **Server:** `postgres` (or `localhost` from outside Docker)
- **Username:** `pipeline`
- **Password:** `pipeline_password`
- **Database:** `transactions`

**What to check:**
- `transactions` table: All processed transactions with UUIDs
- `predictions` table: Transaction classification results
- Row counts and recent records

#### MinIO (S3 Storage)

- **Console:** http://localhost:9001
- **API:** http://localhost:9000
- **Username:** `minioadmin`
- **Password:** `minioadmin`

**What to check:**
- `transactions` bucket: `raw/month=YYYY-MM/transactions.csv` (16 monthly partitions, 10,000 records); `rejects/month=YYYY-MM/` holds records a batch run could not process
- Object browser and file download

### 📊 Streaming Services

#### Kafka UI

- **URL:** http://localhost:8081
- **No authentication required**

**What to check:**
- **Topics:**
  - `transactions`: Transaction messages from producers
  - `predictions`: Prediction results (if implemented)
  - `failed-transactions`: Failed processing records (DLQ)
- **Messages:** View message contents, offsets, partitions
- **Consumer Groups:** `transaction-consumer-group` lag and status
- **Brokers:** Single broker health status

**Key Metrics to Monitor:**
- Message rate (messages/second)
- Consumer lag (should be low, < 100 messages)
- Partition distribution

### 🔄 Batch Services

#### Apache Airflow

- **URL:** http://localhost:8082
- **Username:** `admin`
- **Password:** `airflow123`

**What to check:**
- **DAGs:**
  - `batch_pipeline_dag`: Main batch processing DAG
  - Status: Enabled/Disabled
  - Last run: Success/Failed
  - Next run: Scheduled time
- **Graph View:** Task dependencies and execution flow
- **Gantt Chart:** Task duration and timeline
- **Task Logs:** Detailed execution logs for debugging

**Manual Trigger:**

1. Click on DAG name
2. Click play button (▶️) on top right
3. Click "Trigger DAG"

### 🤖 ML API

- **URL:** http://localhost:8000
- **Health:** http://localhost:8000/health
- **Docs:** http://localhost:8000/docs
- **No authentication required**

**What to check:**
- Health endpoint returns 200 OK
- API documentation (Swagger UI)
- POST `/predict` endpoint for batch predictions

---

## Monitoring & Troubleshooting

### Health Checks

```bash
# Check all container status
docker ps

# Check specific service health
docker-compose ps

# Check service logs
docker-compose logs <service-name>

# Follow logs in real-time
docker-compose logs -f <service-name>
```

### Common Issues

#### Container Won't Start

```bash
# Check container status
docker-compose ps

# View detailed logs
docker-compose logs <service-name>

# Restart service
docker-compose restart <service-name>

# Rebuild and restart
docker-compose up -d --build <service-name>
```

#### Docker Network Issues

**Symptom:** Error like "failed to set up container networking: network XXX not found"

**Cause:** Orphaned containers still attached to old networks

**Solution:**

```bash
# Quick fix
make clean-network
make all

# Or manual steps
docker-compose down --remove-orphans
docker ps -a | grep transaction-enrichment  # Check for orphaned containers
docker rm -f <container-names>       # Remove any orphaned containers
docker network prune -f              # Clean up networks
docker-compose up -d                 # Restart
```

**Prevention:**

```bash
# Always use --remove-orphans (built into Makefile)
make down              # Instead of: docker-compose down

# Clean state between runs
make clean             # Removes volumes too
make all

# Regular maintenance
make clean-all         # Weekly cleanup of unused resources
```

#### Database Connection Issues

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check PostgreSQL logs
docker-compose logs postgres

# Test connection
psql -h localhost -p 5432 -U pipeline -d transactions -c "SELECT COUNT(*) FROM transactions;"
```

#### Kafka Issues

```bash
# Check Kafka broker health
docker-compose logs kafka

# Check if topics exist
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Check consumer lag
docker exec kafka-broker /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group transaction-consumer-group --describe
```

#### Airflow Issues

```bash
# Check Airflow logs
docker-compose logs airflow

# Restart Airflow
docker-compose restart airflow

# Access Airflow CLI
docker exec -it airflow-standalone bash
airflow dags list
airflow tasks list batch_pipeline_dag
```

### Performance Monitoring

#### Database Queries

```sql
-- Check transaction counts
SELECT COUNT(*) FROM transactions;
SELECT COUNT(*) FROM predictions;

-- Recent transactions
SELECT * FROM transactions ORDER BY created_at DESC LIMIT 10;

-- Predictions by category
SELECT category, COUNT(*) 
FROM predictions 
GROUP BY category;

-- Check processing lineage
SELECT processing_type, COUNT(*) 
FROM transactions 
GROUP BY processing_type;
```

#### Streaming Metrics

```bash
# Producer message rate
docker-compose logs streaming-producer-1 | grep "Produced"

# Consumer processing rate
docker-compose logs streaming-consumer | grep "Processed"

# Check consumer lag (should be low)
# Via Kafka UI: http://localhost:8081 > Consumer Groups
```

---

## Testing & Code Quality

### Setup Development Environment

```bash
# Navigate to library directory
cd pipeline/library

# Install dependencies
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

### Running Tests

#### All Tests

```bash
# From library directory
cd pipeline/library
uv run pytest tests

# With verbose output
uv run pytest tests -v

# With coverage report
uv run pytest tests --cov=src --cov-report=html
```

#### Specific Test Files

```bash
# Data validation tests
uv run pytest tests/core/test_data_validation.py -v

# Model tests
uv run pytest tests/core/test_model.py -v

# Orchestration tests
uv run pytest tests/core/test_orchestrate.py -v
```

#### Quick Test Task

```bash
# Using poe task
uv run poe test
```

### Code Quality Checks

#### Pre-commit (All Checks)

```bash
# Run all checks on staged files
pre-commit run

# Run all checks on all files
pre-commit run --all-files

# Run specific hook
pre-commit run ruff --all-files
```

#### Individual Tools

```bash
# Format code
uv run poe linter-fix
uv run ruff format .

# Check linting
uv run poe lint
uv run ruff check .

# Type checking
uv run poe types
uv run pyright

# Documentation style
uv run pydocstyle src/
```

#### Complete Check Workflow

```bash
# Format, type check, and lint
uv run poe check

# Run tests after checks
uv run poe test
```

### Project-Wide Tasks (from root)

```bash
# Navigate to project root
cd /path/to/transaction-enrichment-pipeline

# Test library only
uv run poe test-library

# Test all modules
uv run poe test-all

# Format all modules
uv run poe format-all

# Lint all modules
uv run poe lint-all

# Type check all modules
uv run poe types-all

# Run all checks on all modules
uv run poe check-all
```

---

## Common Tasks

### Load Fresh Data

```bash
# Stop all services
make down

# Clean volumes (removes all data)
make clean

# Restart infrastructure (re-initializes MinIO with fresh CSV)
make infra

# Verify data loaded
curl http://localhost:9001  # Check MinIO console
```

### Reset Database

```bash
# Stop services
docker-compose stop postgres

# Remove PostgreSQL volume
docker-compose down -v

# Restart (Flyway will re-run migrations)
docker-compose up -d postgres flyway
```

### View Real-Time Processing

```bash
# Terminal 1: Producer logs
make logs-producers

# Terminal 2: Consumer logs
make logs-consumer

# Terminal 3: Database queries
watch -n 2 'psql -h localhost -U pipeline -d transactions -c "SELECT COUNT(*) FROM transactions;"'
```

### Debug Service Issues

```bash
# Check service status
docker-compose ps

# View specific service logs with timestamps
docker-compose logs --timestamps <service-name>

# Follow logs from last 100 lines
docker-compose logs --tail=100 -f <service-name>

# Execute command in running container
docker exec -it <container-name> bash

# Check environment variables
docker exec <container-name> env
```

### Backup Database

```bash
# Dump entire database
docker exec transactions-db pg_dump -U pipeline transactions > backup.sql

# Dump specific table
docker exec transactions-db pg_dump -U pipeline -t transactions transactions > transactions_backup.sql

# Restore from backup
cat backup.sql | docker exec -i transactions-db psql -U pipeline transactions
```

### Scale Services

```bash
# Scale producers to 5 instances
docker-compose --profile streaming up -d --scale streaming-producer-1=5

# Scale consumers to 3 instances
docker-compose --profile streaming up -d --scale streaming-consumer=3
```

---

## Quick Reference

### Essential Commands

```bash
# Start everything
make all

# Stop everything (properly stops all profiles)
make down

# Clean everything (including volumes)
make clean

# View logs
make logs

# Check service status
make status

# Run tests
cd pipeline/library && uv run pytest tests

# Fix network issues
make clean-network
```

### Why `make down` Now Works Properly

**Previous Issue:** The `! Network ... Resource is still in use` warning appeared because `docker-compose down` without profiles only stopped infrastructure containers, leaving streaming/batch containers running and attached to the network.

**Solution:** All cleanup commands now use `--profile all` to ensure ALL containers (infrastructure + streaming + batch) are stopped together, allowing the network to be cleanly removed.

```bash
# Before (problematic)
docker-compose down                    # Only stops infra, network warning appears

# After (fixed)
docker-compose --profile all down      # Stops everything, network removed cleanly
# Or just use: make down
```

# Code quality
cd pipeline/library && uv run pre-commit run --all-files


### Port Reference

| Service | Port | URL |
|---------|------|-----|
| PostgreSQL | 5432 | `localhost:5432` |
| Adminer | 8080 | http://localhost:8080 |
| Kafka UI | 8081 | http://localhost:8081 |
| Airflow | 8082 | http://localhost:8082 |
| MinIO API | 9000 | http://localhost:9000 |
| MinIO Console | 9001 | http://localhost:9001 |
| ML API | 8000 | http://localhost:8000 |
| Kafka Broker | 9092 | `localhost:9092` |

### Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| PostgreSQL | `pipeline` | `pipeline_password` |
| Adminer | `pipeline` | `pipeline_password` |
| Airflow | `admin` | `airflow123` |
| MinIO | `minioadmin` | `minioadmin` |
| Kafka UI | - | (none) |
| ML API | - | (none) |

---

## Additional Resources

- **Project README:** `/README.md`
- **Library Documentation:** `/pipeline/library/README.md`
- **Batch Service Docs:** `/pipeline/application/batch/service/README.md`
- **Streaming Producer Docs:** `/pipeline/application/streaming/producer/README.md`
- **Streaming Consumer Docs:** `/pipeline/application/streaming/consumer/README.md`
- **Orchestration Docs:** `/pipeline/application/batch/orchestration/README.md`

---

## Support & Troubleshooting

If you encounter issues:

1. Check service logs: `make logs-<service>`
2. Verify all containers are running: `docker-compose ps`
3. Check health endpoints (ML API, Kafka UI)
4. Review relevant service README for specific issues
5. Clean and restart: `make clean && make all`

**Common Solutions:**

- **Port conflicts:** Change port mappings in `docker-compose.yml`
- **Memory issues:** Increase Docker memory allocation (Docker Desktop > Settings > Resources)
- **Network issues:** Restart Docker Desktop
- **Stale containers:** `docker system prune -a` (⚠️ removes all unused images)
