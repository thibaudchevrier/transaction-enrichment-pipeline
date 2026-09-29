# Batch Processing Pipeline

Batch processing application for transaction classification on historical transaction data. Loads transactions from S3/MinIO, performs parallel ML predictions, and persists results to PostgreSQL.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Docker Deployment](#docker-deployment)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The batch pipeline processes large volumes of historical transaction data in an efficient, parallel manner:

1. **Load** - Streams transactions from S3/MinIO in configurable batches
2. **Validate** - Validates each transaction against Pydantic models
3. **Predict** - Sends batches to ML API with parallel workers (ThreadPoolExecutor)
4. **Persist** - Bulk writes results to PostgreSQL for efficient storage

**Typical Performance:**
- 10,000 transactions in ~30-60 seconds
- 5 parallel API workers
- Batch sizes: 5000 (source) → 100 (API) → 1000 (database)

## ✨ Features

- **Memory Efficient**: Streaming batch processing with generators
- **Parallel Processing**: Concurrent API calls with ThreadPoolExecutor
- **Error Handling**: Tracks invalid and failed transactions separately
- **Retry Logic**: Automatic retry with exponential backoff for API failures
- **Bulk Operations**: PostgreSQL bulk insert/upsert for performance
- **Progress Tracking**: Detailed logging with batch-level metrics
- **Docker Ready**: Multi-stage build with optimized image size

## 🏗️ Architecture

```
┌─────────────┐
│   S3/MinIO  │
│ (CSV Files) │
└──────┬──────┘
       │ Stream in batches (5000 rows)
       ▼
┌─────────────────┐
│ BatchService    │
│ .read()         │
└──────┬──────────┘
       │ Valid transactions
       ▼
┌─────────────────────────────────┐
│  orchestrate_service()          │
│  ┌─────────────────────────┐   │
│  │  ThreadPoolExecutor     │   │
│  │  ├─ Worker 1 (API)      │   │
│  │  ├─ Worker 2 (API)      │   │
│  │  ├─ Worker 3 (API)      │   │
│  │  ├─ Worker 4 (API)      │   │
│  │  └─ Worker 5 (API)      │   │
│  └─────────────────────────┘   │
└──────┬──────────────────────────┘
       │ Predictions
       ▼
┌─────────────────┐
│   PostgreSQL    │
│  (Bulk Write)   │
└─────────────────┘
```

### Component Flow

1. **BatchService** (`main.py`)
   - Extends `BaseService` from library
   - Implements `read()` method for S3 data loading
   - Delegates to `orchestrate_service()` for processing

2. **Orchestration** (`library/core/orchestrate.py`)
   - Manages workflow: validate → predict → write
   - Parallel API calls with configurable workers
   - Accumulates results for bulk database writes

3. **Data Flow**
   - Source: CSV files in S3/MinIO buckets
   - Validation: Pydantic models ensure data quality
   - API: ML classification service
   - Storage: PostgreSQL with transactions + predictions tables

## 📋 Prerequisites

- Python 3.13+
- Access to S3/MinIO storage
- PostgreSQL database (with Flyway migrations applied)
- ML API service running
- Docker (for containerized deployment)

## 🚀 Installation

### Local Development

```bash
cd pipeline/batch

# Install dependencies with uv
uv sync

# Or with pip
pip install -e ../library
pip install -r requirements.txt
```

### Docker Build

```bash
docker build -t batch-pipeline -f Dockerfile ../
```

## ⚙️ Configuration

### Environment Variables

Required configuration:

```bash
# S3/MinIO Configuration
KEY=minioadmin                        # MinIO access key
SECRET=minioadmin                     # MinIO secret key
ENDPOINT_URL=http://minio:9000        # MinIO endpoint

# Database Configuration
DATABASE_URL=postgresql://user:password@postgres:5432/transactions

# ML API Configuration
ML_API_URL=http://ml-api:8000         # ML prediction service

# Batch Processing Configuration (Optional)
ROW_BATCH_SIZE=5000                   # Rows per batch from source
API_BATCH_SIZE=100                    # Transactions per API call
API_MAX_WORKERS=5                     # Parallel API workers
DB_ROW_BATCH_SIZE=1000               # Threshold for bulk DB writes
```

### Configuration Details

| Variable | Default | Description |
|----------|---------|-------------|
| `ROW_BATCH_SIZE` | 5000 | Number of transactions to load from S3 per batch |
| `API_BATCH_SIZE` | 100 | Number of transactions sent to ML API per request |
| `API_MAX_WORKERS` | 5 | Maximum parallel workers for API calls |
| `DB_ROW_BATCH_SIZE` | 1000 | When to trigger bulk database writes |

**Tuning Guidelines:**
- Increase `API_MAX_WORKERS` for faster processing (if API can handle load)
- Decrease `API_BATCH_SIZE` if API has request size limits
- Increase `ROW_BATCH_SIZE` for fewer S3 reads (more memory usage)
- Adjust `DB_ROW_BATCH_SIZE` based on database performance

## 🎮 Usage

### Local Execution

```bash
cd pipeline/batch

# Set environment variables
export KEY=minioadmin
export SECRET=minioadmin
export ENDPOINT_URL=http://localhost:9000
export DATABASE_URL=postgresql://user:pass@localhost:5432/transactions
export ML_API_URL=http://localhost:8000

# Run pipeline
uv run main.py
```

### Expected Output

```
2026-01-11 10:00:00 - INFO - Starting batch pipeline
2026-01-11 10:00:00 - INFO - Pipeline Run ID: batch_20260111_100000
2026-01-11 10:00:05 - INFO - Batch 0: processed 5000 transactions
2026-01-11 10:00:10 - INFO - Batch 1: processed 5000 transactions
2026-01-11 10:00:12 - INFO - Successfully wrote 10000 transactions to database
2026-01-11 10:00:12 - INFO - Pipeline complete: 10000 processed, 0 failed, 0 invalid
```

## 🐳 Docker Deployment

### Using docker-compose

```yaml
services:
  batch:
    build:
      context: ./pipeline
      dockerfile: ./batch/Dockerfile
    environment:
      KEY: minioadmin
      SECRET: minioadmin
      ENDPOINT_URL: http://minio:9000
      DATABASE_URL: postgresql://user:pass@postgres:5432/transactions
      ML_API_URL: http://ml-api:8000
      ROW_BATCH_SIZE: 5000
      API_BATCH_SIZE: 100
      API_MAX_WORKERS: 5
    depends_on:
      postgres:
        condition: service_healthy
      minio:
        condition: service_healthy
      ml-api:
        condition: service_started
```

### Manual Execution

```bash
# Build image
cd pipeline
docker build -t batch-pipeline -f batch/Dockerfile .

# Run container
docker run --rm \
  -e KEY=minioadmin \
  -e SECRET=minioadmin \
  -e ENDPOINT_URL=http://minio:9000 \
  -e DATABASE_URL=postgresql://user:pass@postgres:5432/transactions \
  -e ML_API_URL=http://ml-api:8000 \
  --network=host \
  batch-pipeline
```

### Scheduled Execution

For cron-like scheduling in docker-compose:

```bash
# Run once
docker-compose run --rm batch

# Schedule with cron on host
0 2 * * * cd /path/to/project && docker-compose run --rm batch
```

Or use Kubernetes CronJob for production scheduling.

## 📊 Monitoring

### Logs

View logs in real-time:

```bash
# Local
tail -f logs/batch_pipeline.log

# Docker
docker logs -f batch-container

# docker-compose
docker-compose logs -f batch
```

### Metrics

Key metrics to monitor:

- **Processing Rate**: Transactions per second
- **API Latency**: Time per API batch call
- **Error Rate**: Failed transactions / total transactions
- **Memory Usage**: Should remain stable with streaming
- **Database Write Time**: Bulk operation performance

### Database Queries

Check processing results:

```sql
-- Total transactions processed
SELECT COUNT(*) FROM transactions;

-- Classification summary
SELECT 
    prediction,
    COUNT(*) as count,
    AVG(fraud_score) as avg_score
FROM predictions
GROUP BY prediction;

-- Recent pipeline runs
SELECT 
    DATE(created_at) as date,
    COUNT(*) as transactions
FROM transactions
GROUP BY DATE(created_at)
ORDER BY date DESC;
```

## 🔧 Troubleshooting

### Issue: "Connection refused to ML API"

**Symptom:**
```
Failed to get predictions: Connection refused (http://localhost:8000)
```

**Solution:**
- Check ML API is running: `docker ps | grep ml-api`
- Verify `ML_API_URL` uses correct hostname (e.g., `ml-api` not `localhost` in Docker)
- Test connectivity: `curl http://ml-api:8000/health`

### Issue: "S3/MinIO access denied"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (403)
```

**Solution:**
- Verify `KEY` and `SECRET` environment variables
- Check MinIO is running: `docker ps | grep minio`
- Verify bucket exists and has files
- Test access: `aws --endpoint-url=$ENDPOINT_URL s3 ls s3://transactions/`

### Issue: "Database connection timeout"

**Symptom:**
```
sqlalchemy.exc.OperationalError: could not connect to server
```

**Solution:**
- Ensure PostgreSQL is healthy: `docker ps | grep postgres`
- Verify `DATABASE_URL` connection string
- Check Flyway migrations completed: `docker logs flyway`
- Test connection: `psql $DATABASE_URL -c "SELECT 1;"`

### Issue: "Out of memory"

**Symptom:**
```
MemoryError or killed by OOM
```

**Solution:**
- Reduce `ROW_BATCH_SIZE` (e.g., from 5000 to 2000)
- Check for memory leaks in long-running processes
- Increase Docker memory limit: `--memory=4g`
- Verify generator pattern is streaming (not loading all data)

### Issue: "Slow processing"

**Symptom:**
Processing takes much longer than expected

**Solution:**
- Increase `API_MAX_WORKERS` (e.g., from 5 to 10)
- Check ML API performance: `curl http://ml-api:8000/metrics`
- Monitor database connection pool
- Consider increasing `API_BATCH_SIZE` if API allows
- Profile with: `python -m cProfile main.py`

## 📁 File Structure

```
batch/
├── Dockerfile              # Multi-stage Docker build
├── README.md              # This file
├── main.py                # Application entry point
├── pyproject.toml         # Dependencies
└── .python-version        # Python version (3.13)
```

## 🤝 Related Components

- **Library** (`../library/`) - Shared business logic and infrastructure
- **ML API** (`../../ml_api/`) - Transaction classification service
- **Streaming Producer** (`../streaming/producer/`) - Real-time transaction simulation
- **Streaming Consumer** (`../streaming/consumer/`) - Real-time processing
