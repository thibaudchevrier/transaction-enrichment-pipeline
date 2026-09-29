# Streaming Consumer

Kafka consumer application that processes real-time transaction messages for transaction classification. Validates transactions, calls ML API for predictions, and persists results to PostgreSQL.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Current Status](#current-status)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Docker Deployment](#docker-deployment)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Development Roadmap](#development-roadmap)

## 🎯 Overview

The streaming consumer provides real-time transaction processing by:

1. **Consuming** messages from Kafka `transactions` topic
2. **Validating** each transaction against Pydantic models
3. **Classifying** transactions via ML API
4. **Persisting** results to PostgreSQL
5. **Handling** errors by producing failed messages to error topic

**Design Goals:**
- Low latency processing (<100ms per message)
- At-least-once delivery semantics
- Graceful error handling
- Consumer group coordination for scalability

## ✨ Features

- **Consumer Groups**: Coordinate multiple consumers for parallel processing
- **Offset Management**: Automatic offset commit with Kafka
- **Error Handling**: Separate error topic for failed transactions
- **Graceful Shutdown**: Clean resource cleanup on SIGTERM/SIGINT
- **Progress Logging**: Batch-level metrics every 100 messages
- **Context Manager**: Proper resource management for Kafka consumer
- **JSON Parsing**: Automatic deserialization of message payloads

## 🏗️ Architecture

```
┌─────────────────────┐
│  Kafka Topic        │
│  "transactions"     │
└──────┬──────────────┘
       │ Poll (1s timeout)
       ▼
┌─────────────────────────────┐
│  Consumer                   │
│  (Consumer Group)           │
│  - Deserialize JSON         │
│  - Validate with Pydantic   │
└──────┬──────────────────────┘
       │ Valid transactions
       ▼
┌─────────────────────────────┐
│  ML API                     │
│  - POST /predict            │
│  - Get classification       │
└──────┬──────────────────────┘
       │ Predictions
       ▼
┌─────────────────────────────┐
│  PostgreSQL                 │
│  - transactions table       │
│  - predictions table        │
└─────────────────────────────┘
       
       (Error Path)
┌─────────────────────────────┐
│  Kafka Topic                │
│  "transactions-failed"      │
│  - Invalid transactions     │
│  - API failures             │
└─────────────────────────────┘
```

### Processing Flow

1. **Message Consumption**:
   - Poll Kafka with 1 second timeout
   - Deserialize JSON payload
   - Log message receipt

2. **Validation** (TODO):
   - Validate against Pydantic `Transaction` model
   - Separate valid from invalid transactions
   - Send invalid to error topic

3. **Prediction** (TODO):
   - Call ML API with valid transaction
   - Handle API errors with retry logic
   - Track failed predictions

4. **Persistence** (TODO):
   - Write transaction to database
   - Write prediction to database
   - Commit Kafka offset on success

5. **Error Handling** (TODO):
   - Produce failed transactions to error topic
   - Log detailed error information
   - Continue processing next messages

## 🚧 Current Status

**Implementation Status: 40% Complete**

### ✅ Implemented
- Kafka consumer with context manager
- Consumer group configuration
- Message polling loop
- JSON deserialization
- Progress logging
- Graceful shutdown

### 🚧 TODO
- [ ] Transaction validation with Pydantic
- [ ] ML API integration for predictions
- [ ] Database write operations
- [ ] Error topic producer for failed transactions
- [ ] Retry logic for transient failures
- [ ] Offset commit strategy
- [ ] Performance metrics collection
- [ ] Integration tests

See [Development Roadmap](#development-roadmap) for details.

## 📋 Prerequisites

- Python 3.13+
- Kafka cluster with `transactions` topic
- PostgreSQL database (with Flyway migrations applied)
- ML API service running
- Docker (for containerized deployment)

## 🚀 Installation

### Local Development

```bash
cd pipeline/streaming/consumer

# Install dependencies with uv
uv sync

# Or with pip
pip install -e ../../library
pip install -r requirements.txt
```

### Docker Build

```bash
cd pipeline
docker build -t streaming-consumer -f streaming/consumer/Dockerfile .
```

## ⚙️ Configuration

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092      # Kafka brokers
KAFKA_CONSUMER_GROUP=transaction-consumer-group  # Consumer group ID
KAFKA_TOPIC=transactions                    # Source topic
KAFKA_ERROR_TOPIC=transactions-failed       # Error topic (TODO)

# Database Configuration (TODO)
DATABASE_URL=postgresql://user:password@postgres:5432/transactions

# ML API Configuration (TODO)
ML_API_URL=http://ml-api:8000

# Processing Configuration (TODO)
BATCH_SIZE=100                              # Transactions per DB write
MAX_RETRIES=3                               # API retry attempts
RETRY_BACKOFF=2                             # Retry backoff multiplier
```

### Consumer Group Behavior

- **Group ID**: All consumers with same `KAFKA_CONSUMER_GROUP` coordinate
- **Partition Assignment**: Kafka automatically assigns partitions
- **Offset Management**: Offsets committed automatically or manually
- **Rebalancing**: Automatic when consumers join/leave

Example: 3 consumers, 3 partitions
```
Consumer 1 → Partition 0
Consumer 2 → Partition 1
Consumer 3 → Partition 2
```

## 🎮 Usage

### Local Execution

```bash
cd pipeline/streaming/consumer

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP=transaction-consumer-group
export KAFKA_TOPIC=transactions
export DATABASE_URL=postgresql://user:pass@localhost:5432/transactions
export ML_API_URL=http://localhost:8000

# Run consumer
uv run main.py
```

### Expected Output (Current Implementation)

```
2026-01-11 10:00:00 - INFO - Consumer started - Server: localhost:9092, Group: transaction-consumer-group, Topic: transactions
2026-01-11 10:00:01 - DEBUG - Received message #1: uuid-123
2026-01-11 10:00:01 - DEBUG - Received message #2: uuid-456
2026-01-11 10:00:05 - INFO - Processed 100 messages
2026-01-11 10:00:10 - INFO - Processed 200 messages
^C
2026-01-11 10:00:15 - INFO - Consumer stopped. Total messages: 247
```

### Testing Message Consumption

```bash
# Check consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group transaction-consumer-group

# View consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group transaction-consumer-group

# Reset offset to beginning
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group transaction-consumer-group \
  --topic transactions \
  --reset-offsets --to-earliest --execute
```

## 🐳 Docker Deployment

### Using docker-compose (TODO)

```yaml
services:
  streaming-consumer:
    build:
      context: ./pipeline
      dockerfile: ./streaming/consumer/Dockerfile
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:19092
      KAFKA_CONSUMER_GROUP: transaction-consumer-group
      KAFKA_TOPIC: transactions
      KAFKA_ERROR_TOPIC: transactions-failed
      DATABASE_URL: postgresql://user:pass@postgres:5432/transactions
      ML_API_URL: http://ml-api:8000
      BATCH_SIZE: 100
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
      ml-api:
        condition: service_started
    restart: unless-stopped
```

### Scaling Consumers

Run multiple consumer instances for parallel processing:

```bash
# Scale to 3 consumers
docker-compose up -d --scale streaming-consumer=3

# Check consumer group partition assignment
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group transaction-consumer-group
```

**Note**: Number of consumers should not exceed number of partitions.

## 📊 Monitoring

### Consumer Lag

Critical metric for streaming health:

```bash
# Check lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group transaction-consumer-group

# Output shows:
# - Current offset
# - Log end offset
# - Lag (messages behind)
```

**Healthy lag**: 0-100 messages  
**Warning**: 100-1000 messages  
**Critical**: >1000 messages (consumer can't keep up)

### Logs

```bash
# Docker
docker logs -f streaming-consumer

# docker-compose
docker-compose logs -f streaming-consumer
```

### Metrics (TODO)

Track in production:
- **Throughput**: Messages processed per second
- **Latency**: Time from message arrival to DB write
- **Error Rate**: Failed transactions / total transactions
- **Consumer Lag**: Offset difference from producer
- **Rebalance Events**: Consumer group coordination

### Database Queries (TODO)

```sql
-- Recent transactions processed
SELECT COUNT(*) 
FROM transactions 
WHERE created_at > NOW() - INTERVAL '1 hour';

-- Classification rate
SELECT 
    prediction,
    COUNT(*) as count,
    AVG(fraud_score) as avg_score
FROM predictions
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY prediction;

-- Processing rate (transactions per minute)
SELECT 
    DATE_TRUNC('minute', created_at) as minute,
    COUNT(*) as transactions
FROM transactions
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute DESC;
```

## 🔧 Troubleshooting

### Issue: "No messages being consumed"

**Symptom:**
Consumer starts but no messages appear in logs

**Solution:**
- Check producer is running and producing messages
- Verify consumer is subscribed to correct topic
- Check consumer group offset (might be at end of topic):
  ```bash
  kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group transaction-consumer-group \
    --describe
  ```
- Reset offset to beginning if needed (see Usage section)

### Issue: "Kafka connection refused"

**Symptom:**
```
Failed to connect to broker: Connection refused (localhost:9092)
```

**Solution:**
- Check Kafka is running: `docker ps | grep kafka`
- Verify `KAFKA_BOOTSTRAP_SERVERS` is correct
  - Docker: Use `kafka:19092` (internal listener)
  - Host: Use `localhost:9092` (external listener)
- Test connectivity: `telnet localhost 9092`

### Issue: "Consumer group rebalancing constantly"

**Symptom:**
```
Rebalancing consumer group transaction-consumer-group
```

**Solution:**
- Increase `session.timeout.ms` in consumer config
- Increase `max.poll.interval.ms` if processing takes long
- Check network stability between consumer and Kafka
- Ensure consumer is processing messages and calling `poll()` regularly

### Issue: "Database connection errors" (TODO)

**Symptom:**
```
sqlalchemy.exc.OperationalError: could not connect to server
```

**Solution:**
- Ensure PostgreSQL is running: `docker ps | grep postgres`
- Verify `DATABASE_URL` connection string
- Test connection: `psql $DATABASE_URL -c "SELECT 1;"`
- Check database has required tables (Flyway migrations)

### Issue: "ML API timeouts" (TODO)

**Symptom:**
```
requests.exceptions.ReadTimeout: Request timeout
```

**Solution:**
- Check ML API is healthy: `curl http://ml-api:8000/health`
- Increase API timeout in config
- Scale ML API horizontally if overloaded
- Implement circuit breaker pattern
- Queue failed predictions for retry

## 📁 File Structure

```
streaming/consumer/
├── Dockerfile              # Multi-stage Docker build (TODO)
├── README.md              # This file
├── main.py                # Application entry point
├── pyproject.toml         # Dependencies
└── .python-version        # Python version (3.13)
```

## 🛣️ Development Roadmap

### Phase 1: Core Processing (Current)
- [x] Kafka consumer setup
- [x] Consumer group configuration
- [x] Message polling and deserialization
- [ ] Transaction validation with Pydantic
- [ ] ML API integration
- [ ] Database persistence

### Phase 2: Error Handling
- [ ] Error topic producer
- [ ] Retry logic with exponential backoff
- [ ] Dead letter queue for permanent failures
- [ ] Detailed error logging

### Phase 3: Performance
- [ ] Batch processing (N messages → 1 DB write)
- [ ] Connection pooling (DB, API)
- [ ] Async processing with asyncio
- [ ] Performance metrics collection

### Phase 4: Reliability
- [ ] Manual offset commit strategy
- [ ] Exactly-once semantics with transactions
- [ ] Circuit breaker for ML API
- [ ] Health check endpoint

### Phase 5: Observability
- [ ] Prometheus metrics export
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Structured logging (JSON)
- [ ] Grafana dashboards

### Phase 6: Testing
- [ ] Unit tests for validation logic
- [ ] Integration tests with test containers
- [ ] Load testing with high message volume
- [ ] Chaos testing (kill consumers, Kafka)

## 🎯 Implementation Guide

### Step 1: Add Validation

```python
from core.data_validation import validate_transaction_records

# In main loop
transaction = json.loads(msg.value().decode('utf-8'))
valid, invalid = validate_transaction_records([transaction])

if invalid:
    # Send to error topic
    logger.warning(f"Invalid transaction: {transaction['id']}")
    continue
```

### Step 2: Add ML API Call

```python
from infrastructure.api import predict_batch

# After validation
predictions = predict_batch(valid, ml_api_url)
```

### Step 3: Add Database Write

```python
from infrastructure.database import get_db_session, db_write_results

# After prediction
with get_db_session(database_url) as session:
    db_write_results(session, valid, predictions)
    session.commit()
```

### Step 4: Add Error Topic Producer

```python
from confluent_kafka import Producer

error_producer = Producer({'bootstrap.servers': bootstrap_servers})

# On error
error_producer.produce(
    error_topic,
    value=json.dumps({
        'transaction': transaction,
        'error': str(error),
        'timestamp': datetime.now().isoformat()
    }).encode('utf-8')
)
```

## 🤝 Related Components

- **Library** (`../../library/`) - Shared validation, API, database logic
- **Streaming Producer** (`../producer/`) - Produces messages to consume
- **Batch Pipeline** (`../../batch/`) - Similar processing for historical data
- **ML API** (`../../../ml_api/`) - Transaction classification service

## 📝 Next Steps

1. **Implement Transaction Validation**: Add Pydantic validation (see Step 1)
2. **Integrate ML API**: Call prediction service (see Step 2)
3. **Add Database Persistence**: Write results to PostgreSQL (see Step 3)
4. **Create Dockerfile**: Package for Docker deployment
5. **Add to docker-compose**: Integrate with existing services
6. **Test End-to-End**: Producer → Consumer → Database verification
