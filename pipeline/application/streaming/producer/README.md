# Streaming Producer

Kafka producer application that continuously streams transaction data to simulate real-time transaction events for transaction classification pipelines.

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

The streaming producer simulates real-time transaction events by:

1. **Loading** all transactions from S3/MinIO into memory (10,000 records)
2. **Sampling** randomly with replacement at configurable intervals
3. **Producing** to Kafka topic with parallel async operations
4. **Scaling** horizontally - multiple producers with different configurations

**Typical Configuration:**
- Produce every 0.5-1.0 seconds
- 1-20 transactions per batch
- Random sampling with replacement
- Parallel message sending with `asyncio.gather()`

## ✨ Features

- **Async/Await**: Built with `asyncio` and `AIOProducer` for high performance
- **Random Sampling**: Realistic transaction patterns with variable batch sizes
- **Parallel Sending**: Messages sent concurrently using `asyncio.gather()`
- **Configurable Rate**: Adjustable intervals and batch sizes via environment variables
- **Multiple Producers**: Run multiple instances with different configurations
- **Docker Ready**: Containerized with proper Kafka networking
- **Graceful Shutdown**: Clean resource cleanup on SIGTERM/SIGINT

## 🏗️ Architecture

```
┌──────────────────┐
│   S3/MinIO       │
│ (transactions.csv)│
└────────┬─────────┘
         │ Load all (10k) into memory at startup
         ▼
┌─────────────────────────────┐
│  In-Memory Dataset          │
│  [10,000 transactions]      │
└────────┬────────────────────┘
         │ Random sampling
         ▼
┌─────────────────────────────┐
│  Timer Loop (asyncio)       │
│  - Every N seconds          │
│  - Sample K transactions    │
│  - Parallel async send      │
└────────┬────────────────────┘
         │ asyncio.gather()
         ▼
┌─────────────────────────────┐
│  AIOProducer                │
│  └─ Kafka: transactions     │
└─────────────────────────────┘
```

### Message Flow

1. **Initialization**:
   - Load all transactions from S3/MinIO
   - Validate with Pydantic models
   - Store in memory for fast random access

2. **Production Loop**:
   - Timer triggers every `PRODUCE_INTERVAL` seconds
   - Randomly select `MIN_RECORDS` to `MAX_RECORDS` transactions
   - Create async tasks for each message
   - Send all messages in parallel with `asyncio.gather()`

3. **Message Format**:
   ```json
   {
     "id": "uuid-v4",
     "description": "Transaction description",
     "amount": 123.45,
     "timestamp": "2026-01-11T10:00:00",
     "merchant": "Merchant Name",
     "operation_type": "debit",
     "side": "outgoing"
   }
   ```

## 📋 Prerequisites

- Python 3.13+
- Kafka cluster running (with topics created)
- Access to S3/MinIO storage with transaction data
- Docker (for containerized deployment)

## 🚀 Installation

### Local Development

```bash
cd pipeline/streaming/producer

# Install dependencies with uv
uv sync

# Or with pip
pip install -e ../../library
pip install -r requirements.txt
```

### Docker Build

```bash
cd pipeline
docker build -t streaming-producer -f streaming/producer/Dockerfile .
```

## ⚙️ Configuration

### Environment Variables

```bash
# S3/MinIO Configuration
KEY=minioadmin                              # MinIO access key
SECRET=minioadmin                           # MinIO secret key
ENDPOINT_URL=http://minio:9000              # MinIO endpoint

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092      # Kafka brokers
KAFKA_TOPIC=transactions                    # Target topic

# Producer Configuration
PRODUCE_INTERVAL=0.5                        # Seconds between batches (default: 0.5)
MIN_RECORDS_PER_BATCH=1                     # Min transactions per batch (default: 1)
MAX_RECORDS_PER_BATCH=10                    # Max transactions per batch (default: 10)
```

### Configuration Presets

#### High Volume Producer
Simulates peak transaction periods:
```bash
PRODUCE_INTERVAL=0.2        # 5 batches per second
MIN_RECORDS_PER_BATCH=10
MAX_RECORDS_PER_BATCH=20
```

#### Normal Volume Producer
Simulates regular business hours:
```bash
PRODUCE_INTERVAL=0.5        # 2 batches per second
MIN_RECORDS_PER_BATCH=1
MAX_RECORDS_PER_BATCH=10
```

#### Low Volume Producer
Simulates off-peak hours:
```bash
PRODUCE_INTERVAL=2.0        # 0.5 batches per second
MIN_RECORDS_PER_BATCH=1
MAX_RECORDS_PER_BATCH=5
```

## 🎮 Usage

### Local Execution

```bash
cd pipeline/streaming/producer

# Set environment variables
export KEY=minioadmin
export SECRET=minioadmin
export ENDPOINT_URL=http://localhost:9000
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=transactions
export PRODUCE_INTERVAL=0.5

# Run producer
uv run main.py
```

### Expected Output

```
2026-01-11 10:00:00 - INFO - Starting producer - Server: localhost:9092, Topic: transactions, Interval: 0.5s
2026-01-11 10:00:00 - INFO - Records per batch: 1-10, Total samples loaded: 10000
2026-01-11 10:00:00 - INFO - Batch complete: sent 5/5 records
2026-01-11 10:00:01 - INFO - Batch complete: sent 8/8 records
2026-01-11 10:00:01 - INFO - Batch complete: sent 3/3 records
^C
2026-01-11 10:00:05 - INFO - Producer stopped. Total messages sent: 247
```

### Testing Kafka Connectivity

```bash
# Check topic exists
kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning \
  --max-messages 10
```

## 🐳 Docker Deployment

### Using docker-compose

Run multiple producers with different configurations:

```yaml
services:
  streaming-producer-1:
    build:
      context: ./pipeline
      dockerfile: ./streaming/producer/Dockerfile
    environment:
      KEY: minioadmin
      SECRET: minioadmin
      ENDPOINT_URL: http://minio:9000
      KAFKA_BOOTSTRAP_SERVERS: kafka:19092
      KAFKA_TOPIC: transactions
      PRODUCE_INTERVAL: 0.5
      MIN_RECORDS_PER_BATCH: 1
      MAX_RECORDS_PER_BATCH: 10
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy

  streaming-producer-2:
    build:
      context: ./pipeline
      dockerfile: ./streaming/producer/Dockerfile
    environment:
      KEY: minioadmin
      SECRET: minioadmin
      ENDPOINT_URL: http://minio:9000
      KAFKA_BOOTSTRAP_SERVERS: kafka:19092
      KAFKA_TOPIC: transactions
      PRODUCE_INTERVAL: 1.0
      MIN_RECORDS_PER_BATCH: 5
      MAX_RECORDS_PER_BATCH: 20
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy
```

### Start Producers

```bash
# Start all producers
docker-compose up -d streaming-producer-1 streaming-producer-2

# View logs
docker-compose logs -f streaming-producer-1

# Stop producers
docker-compose stop streaming-producer-1 streaming-producer-2
```

### Kafka Networking in Docker

**Important**: Use `kafka:19092` (internal listener) for Docker services, not `localhost:9092`.

```yaml
# Kafka service configuration
kafka:
  environment:
    KAFKA_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

## 📊 Monitoring

### Real-time Logs

```bash
# Docker
docker logs -f streaming-producer-1

# docker-compose
docker-compose logs -f streaming-producer-1 streaming-producer-2
```

### Kafka Metrics

Check topic lag and message count:

```bash
# Topic details
kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic transactions

# Consumer groups (if any)
kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Kafka UI

Access web UI at `http://localhost:8080` (if kafka-ui service is running):

- View topic messages in real-time
- Monitor producer throughput
- Check partition distribution
- Analyze message schemas

### Production Rate Calculation

```python
# Theoretical max rate per producer
batches_per_second = 1 / PRODUCE_INTERVAL
avg_records_per_batch = (MIN_RECORDS_PER_BATCH + MAX_RECORDS_PER_BATCH) / 2
records_per_second = batches_per_second * avg_records_per_batch

# Example: INTERVAL=0.5, MIN=1, MAX=10
# = 2 batches/sec * 5.5 records = 11 records/sec per producer
```

## 🔧 Troubleshooting

### Issue: "Kafka connection refused"

**Symptom:**
```
Failed to produce message: Connection refused (localhost:9092)
```

**Solution:**
- Check Kafka is running: `docker ps | grep kafka`
- Verify `KAFKA_BOOTSTRAP_SERVERS` is correct
  - Docker: Use `kafka:19092` (internal listener)
  - Host: Use `localhost:9092` (external listener)
- Test connectivity: `telnet localhost 9092`

### Issue: "Topic does not exist"

**Symptom:**
```
UnknownTopicOrPartition: This server does not host this topic-partition
```

**Solution:**
```bash
# Create topic manually
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1

# Or enable auto-creation in Kafka config
KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
```

### Issue: "MinIO access denied"

**Symptom:**
```
botocore.exceptions.ClientError: An error occurred (403)
```

**Solution:**
- Verify `KEY` and `SECRET` environment variables
- Check MinIO is running: `docker ps | grep minio`
- Verify bucket and file exist:
  ```bash
  aws --endpoint-url=$ENDPOINT_URL s3 ls s3://transactions/
  ```

### Issue: "Messages not appearing in topic"

**Symptom:**
Producer logs show success but consumer doesn't see messages

**Solution:**
- Check producer is using correct topic name
- Verify consumer is reading from correct topic
- Check consumer group offset:
  ```bash
  kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group transaction-consumer-group \
    --describe
  ```
- Reset consumer offset to beginning:
  ```bash
  kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group transaction-consumer-group \
    --topic transactions \
    --reset-offsets --to-earliest --execute
  ```

### Issue: "High memory usage"

**Symptom:**
Container memory grows over time

**Solution:**
- This is expected - all 10k transactions are in memory
- Limit Docker memory: `--memory=512m`
- If memory continues to grow, check for leaks:
  ```bash
  docker stats streaming-producer-1
  ```

### Issue: "Slow startup"

**Symptom:**
Producer takes long time to start producing

**Solution:**
- Loading 10k records from S3 takes a few seconds (normal)
- Check MinIO network latency
- Consider pre-loading data into a local cache
- Monitor logs for validation errors

## 📁 File Structure

```
streaming/producer/
├── Dockerfile              # Multi-stage Docker build
├── README.md              # This file
├── main.py                # Application entry point
├── pyproject.toml         # Dependencies
└── .python-version        # Python version (3.13)
```

## 🎯 Performance Tuning

### Increase Throughput

1. **Multiple Producers**: Run 2-3 producer instances
2. **Larger Batches**: Increase `MAX_RECORDS_PER_BATCH`
3. **Faster Intervals**: Decrease `PRODUCE_INTERVAL`
4. **More Partitions**: Increase Kafka topic partitions

### Reduce Load

1. **Slower Intervals**: Increase `PRODUCE_INTERVAL`
2. **Smaller Batches**: Decrease `MAX_RECORDS_PER_BATCH`
3. **Single Producer**: Run only one instance

### Async Performance

The producer uses `asyncio.gather()` for parallel sending:
- All messages in a batch are sent concurrently
- No waiting for sequential ACKs
- Maximum throughput for async operations

## 🤝 Related Components

- **Library** (`../../library/`) - Shared validation and data loading
- **Streaming Consumer** (`../consumer/`) - Consumes and processes messages
- **Batch Pipeline** (`../../batch/`) - Historical processing
- **Kafka** - Message broker
