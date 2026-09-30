# Design Notes & Architecture

## Table of Contents

- [Business Context & Assumptions](#business-context--assumptions)
- [Architecture Overview](#architecture-overview)
- [Current Architecture](#current-architecture)
- [Target Architecture](#target-architecture)
- [Technology Choices](#technology-choices)
- [Data Modeling & Schema Design](#data-modeling--schema-design)
- [Trade-offs & Design Decisions](#trade-offs--design-decisions)
- [Code Organization](#code-organization)
- [Quality Assurance](#quality-assurance)
- [Future Improvements](#future-improvements)

---

## Architecture Overview

This project implements a dual-pipeline system for transaction classification: a **batch pipeline** for historical data processing and a **streaming pipeline** for real-time event processing. Both pipelines share common business logic through a clean architecture approach, ensuring code reusability and maintainability.

---

## Current Architecture

See the architecture diagram in the [README](README.md#architecture): batch (Airflow, one monthly
partition per run) and streaming (Kafka, with a DLQ) pipelines sharing one core library.

### Key Components

#### Data Storage
- **MinIO (Object Storage)**: Centralized storage for CSV transactions
  - **Rationale**: Provides a single source of truth accessible by any application
  - **Benefit**: Avoids mounting files as volumes for each application
  - **S3-Compatible**: Easy migration to cloud storage if needed

#### Batch Pipeline
- **Apache Airflow**: Orchestration and scheduling
  - **DAG-based workflow**: Visual representation of batch processing
  - **Scheduling**: Cron-based execution for periodic processing
  - **Monitoring**: Built-in UI for job tracking and debugging

#### Streaming Pipeline
- **2 Kafka Producers**: Simulate real-time transaction events
  - **Producer 1**: Generates 1-10 records every 0.5 seconds
  - **Producer 2**: Generates 5-20 records every 1.0 second
  - **Purpose**: Realistic simulation of varying event rates

- **Kafka Topics**: 
  - `transactions`: Incoming transaction events
  - `failed-transactions`: Dead Letter Queue (DLQ) for error handling
  - **Replication Factor**: 1 locally (a single broker in Docker Compose); 3 in production, see
    [Replication Strategy](#replication-strategy)

- **Consumer Group**: Parallel processing with load distribution

#### Backend
- **PostgreSQL**: Relational database for storing transactions and predictions
  - **ACID compliance**: Ensures data consistency for financial transactions
  - **Lineage tracking**: Captures processing metadata (batch_id, run_id, processing_type)

### Design Decisions

#### Buffering vs. Latency Trade-off
**Decision**: Buffer streaming data before API calls and database writes

**Rationale**:
- **Optimized API calls**: Batch multiple transactions in single requests
- **Efficient database operations**: Bulk inserts reduce transaction overhead
- **Reduced network overhead**: Fewer round trips to external services

**Trade-off**:
- ✅ Better throughput and resource utilization
- ✅ Lower operational costs (fewer API calls)
- ❌ Increased latency (buffering delay)
- ⚖️ **Acceptable for use case**: Depends on client SLA requirements

**Alternative**: Lower latency by increasing API call frequency and database transactions at the cost of higher resource usage.

---

## Target Architecture

![Target Architecture](docs/images/target.png)

*Figure 2: Proposed architecture with improved separation of concerns and connector pattern*

### Key Improvements

#### 1. Decoupled Processing (Single Responsibility Principle)

**Current**: Consumer reads from Kafka → validates → predicts → writes to PostgreSQL (all in one)

**Target**: Separate concerns into independent components
- **Prediction Service**: Focus solely on calling ML API and publishing results
- **Kafka Connectors**: Handle data persistence to various backends
- **Benefit**: Each component has a single, well-defined responsibility

#### 2. Connector-Based Architecture (Fan-out Pattern)

**Approach**: Stream processing writes to Kafka topics, connectors handle backend integration

**Components**:
- **Topic: transactions** → Original transaction events
- **Topic: predictions** → Classification results from ML API
- **Topic: errors** → Failed records for monitoring

**Connectors** (fan-out from topics):
- **PostgreSQL Connector**: Persist to relational database
- **Elasticsearch Connector**: Full-text search and analytics
- **Prometheus/InfluxDB Connector**: Metrics and monitoring
- **S3 Connector**: Archival and data lake

**Benefits**:
- ✅ **Flexibility**: Add/remove backends without changing streaming application
- ✅ **Scalability**: Each connector scales independently
- ✅ **Separation of concerns**: Streaming app focuses on transformations, connectors handle persistence
- ✅ **Reusability**: Same topic consumed by multiple downstream systems

#### 3. API Output Validation

**Current**: API output validation is "all or nothing" (entire batch fails on malformed response)

**Target**: Add Pydantic models for API output validation
- **Predict Model**: Validate individual prediction responses
- **Partial failure handling**: Process valid predictions, log invalid ones
- **Resilience**: Application continues even with partial API failures

**Implementation**:
```python
class PredictOutput(BaseModel):
    transaction_id: str
    category: str
    confidence_score: float
    
    @field_validator('confidence_score')
    def validate_confidence(cls, v):
        if not 0 <= v <= 1:
            raise ValueError('Confidence must be between 0 and 1')
        return v
```

#### 4. Unified Model Across Services

**Current**: ML API uses separate models from application code (code duplication)

**Target**: Shared model library used by both API and applications
- **Single source of truth**: `Transaction` and `Predict` models defined once
- **Dependency management**: Use `uv` for ML API (consistent with applications)
- **Task execution**: Consolidate quality/test scripts in single `pyproject.toml` using `poe`

**Benefits**:
- ✅ No model drift between API and consumers
- ✅ Consistent validation across services
- ✅ Easier maintenance and updates

---

## Technology Choices

### Package Management: uv

**Decision**: Use `uv` instead of pip or Poetry

**Rationale**:
- **Speed**: Written in Rust, significantly faster than Poetry
- **Completeness**: More feature-rich than plain pip (lock files, workspace support, virtual env management)
- **Modern**: Designed for modern Python workflows (PEP 621, pyproject.toml-first)
- **Reliability**: Deterministic dependency resolution with lock files

**Comparison**:
| Feature | pip | Poetry | uv |
|---------|-----|--------|-----|
| Speed | ⚡ | ⚡⚡ | ⚡⚡⚡⚡⚡ |
| Lock files | ❌ | ✅ | ✅ |
| Workspace support | ❌ | ⚠️ | ✅ |
| Virtual env management | ⚠️ | ✅ | ✅ |

### Orchestration: Apache Airflow

**Decision**: Use Airflow for batch orchestration

**Rationale**:
- **Industry standard**: Proven solution for batch processing
- **Visual monitoring**: DAG UI for workflow tracking
- **Scheduling**: Built-in cron-based scheduling
- **Extensibility**: Rich ecosystem of operators (DockerOperator used here)

### Message Broker: Apache Kafka

**Decision**: Use Kafka for streaming pipeline

**Rationale**:
- **Durability**: Messages persisted to disk (reliable event log)
- **Scalability**: Horizontal scaling via partitions (3 partitions configured)
- **Replayability**: Can reprocess messages from any offset
- **Ecosystem**: Rich connector ecosystem for target architecture

### Replication Strategy

**Decision**: Replication factor of 3 for all Kafka topics in production. The local Compose setup
runs a single broker, so its topics use a replication factor of 1.

**Rationale** (production):
- **Fault tolerance**: Survives up to 2 broker failures
- **Data durability**: No data loss even with broker failures
- **Industry best practice**: Standard for production Kafka clusters

**Trade-off**: Higher storage requirements (3x data replication)

### Database Migration: Flyway

**Decision**: Use Flyway for database schema evolution

**Rationale**:
- **Version control for database**: SQL migrations tracked in Git
- **Incremental evolution**: Apply changes incrementally (V1, V2, V3...)
- **Repeatable**: Migrations are idempotent and can be replayed
- **Clear audit trail**: History of all schema changes with timestamps
- **Team coordination**: Multiple developers can work on schema changes safely

**Implementation**:
```sql
-- migrations/V1__create_transactions_table.sql
CREATE TABLE transactions (...);

-- migrations/V2__add_lineage_columns.sql
ALTER TABLE transactions ADD COLUMN batch_id VARCHAR(255);
```

**Benefits**:
- ✅ **Controlled evolution**: Each change is reviewed and versioned
- ✅ **Environment parity**: Dev, staging, prod have same schema
- ✅ **Rollback capability**: Can track which version is deployed
- ✅ **Documentation**: Migrations serve as schema documentation

**Alternative considered**: Alembic (SQLAlchemy-native)
- **Why Flyway**: Language-agnostic, plain SQL, simpler for this use case

---

## Data Modeling & Schema Design

### Separated Tables: Transactions vs. Predictions

**Decision**: Store transactions and predictions in separate tables with foreign key relationship

**Schema**:
```sql
-- Transactions table (source data)
CREATE TABLE transactions (
    id VARCHAR(255) PRIMARY KEY,
    description TEXT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    side VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Predictions table (enriched data)
CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL UNIQUE,  -- One prediction per transaction
    category VARCHAR(100) NOT NULL,
    confidence_score DECIMAL(5, 4) DEFAULT 1.0,
    model_version VARCHAR(50) DEFAULT 'v1.0',
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id)
);
```

### Rationale for Separation

#### 1. **Decoupled Ingestion and Enrichment**

**Benefit**: Can ingest transactions independently from predictions

**Use case**:
- Stream transactions immediately → Available for monitoring
- Predict in batch later → Enrichment happens asynchronously
- Failed predictions don't block transaction ingestion

**Example flow**:
```
Transaction arrives → Store in transactions table ✅
ML API call fails → DLQ / rejects file (today the transaction is not persisted either)
Retry later → Add prediction to predictions table ✅
```

#### 2. **Independent Monitoring and Observability**

**Queries enabled**:
```sql
-- Monitor transaction volume (no join needed)
SELECT COUNT(*) FROM transactions WHERE created_at > NOW() - INTERVAL '1 hour';

-- Monitor prediction coverage (what % have predictions)
SELECT 
    COUNT(DISTINCT t.id) as total_transactions,
    COUNT(p.id) as predicted_transactions,
    ROUND(100.0 * COUNT(p.id) / COUNT(DISTINCT t.id), 2) as coverage_pct
FROM transactions t
LEFT JOIN predictions p ON t.id = p.transaction_id;

-- Find transactions without predictions (data quality check)
SELECT t.* 
FROM transactions t
LEFT JOIN predictions p ON t.id = p.transaction_id
WHERE p.id IS NULL;
```

**Benefit**: Different teams can monitor different aspects
- **Data engineering**: Transaction ingestion rate, pipeline health
- **ML team**: Prediction coverage, model performance
- **Business**: Combined view with predictions

#### 3. **Different Constraint Requirements**

**Transactions table**:
- ✅ **PRIMARY KEY** on `id` → Prevents duplicate transactions
- ✅ **ON CONFLICT** strategies possible:
  ```sql
  INSERT INTO transactions (...) 
  ON CONFLICT (id) DO NOTHING;  -- Idempotent ingestion
  ```

**Predictions table**:
- ✅ **UNIQUE** on `transaction_id` → One prediction per transaction (current model)
- ✅ **Can be relaxed later** for prediction history:
  ```sql
  -- Drop UNIQUE constraint
  ALTER TABLE predictions DROP CONSTRAINT predictions_transaction_id_key;
  
  -- Now multiple predictions per transaction (model versioning)
  -- Query latest: SELECT DISTINCT ON (transaction_id) ...
  ```

**Flexibility**: Can handle duplicates differently per table

#### 4. **Support for Prediction History**

**Current**: One prediction per transaction (UNIQUE constraint)

**Future**: Track prediction history across model versions
```sql
-- Remove UNIQUE constraint
ALTER TABLE predictions DROP CONSTRAINT predictions_transaction_id_key;

-- Now can store multiple predictions per transaction
INSERT INTO predictions (transaction_id, category, model_version, predicted_at)
VALUES 
  ('txn-123', 'groceries', 'v1.0', '2026-01-01'),
  ('txn-123', 'food', 'v2.0', '2026-01-15');  -- Model v2 re-predicted

-- Query latest prediction per transaction
SELECT DISTINCT ON (transaction_id) 
    transaction_id, category, model_version, predicted_at
FROM predictions
ORDER BY transaction_id, predicted_at DESC;
```

**Benefits**:
- ✅ Track model evolution (A/B testing, performance comparison)
- ✅ Audit trail (when did category change?)
- ✅ Rollback capability (revert to previous model predictions)

#### 5. **Data Lifecycle Management**

**Different retention policies**:
- **Transactions**: Keep forever (source of truth, compliance)
- **Predictions**: Archive after 90 days (can be regenerated)

**Storage optimization**:
```sql
-- Archive old predictions
CREATE TABLE predictions_archive AS 
SELECT * FROM predictions WHERE predicted_at < NOW() - INTERVAL '90 days';

DELETE FROM predictions WHERE predicted_at < NOW() - INTERVAL '90 days';

-- Transactions remain untouched
```

**Benefit**: Lower storage costs while maintaining source data

#### 6. **Schema Evolution Independence**

**Transactions schema** (stable):
- Defined by upstream systems (payment processor)
- Rarely changes

**Predictions schema** (evolves):
- Depends on ML model outputs
- May add: `confidence_score`, `alternative_categories`, `explanation`, `feature_importance`

**Example evolution**:
```sql
-- Add new prediction metadata without touching transactions table
ALTER TABLE predictions ADD COLUMN explanation TEXT;
ALTER TABLE predictions ADD COLUMN feature_importance JSONB;
```

**Benefit**: Can evolve prediction schema independently

### Trade-offs of Separated Tables

| Aspect | Benefit | Trade-off |
|--------|---------|-----------|
| **Queries** | Simple monitoring per table | Joins needed for combined view |
| **Writes** | Independent ingestion | Two INSERTs instead of one |
| **Schema** | Independent evolution | More complex migrations |
| **Storage** | Optimize per table | Slight overhead (foreign keys) |

**Chosen approach**: Separation benefits outweigh join overhead for this use case

### Alternative: Single Denormalized Table

**Not chosen**:
```sql
CREATE TABLE transactions_with_predictions (
    id VARCHAR(255) PRIMARY KEY,
    -- Transaction fields
    description TEXT,
    amount DECIMAL,
    -- Prediction fields (nullable)
    category VARCHAR(100),
    confidence_score DECIMAL,
    predicted_at TIMESTAMP
);
```

**Why rejected**:
- ❌ Can't track prediction history (overwrites previous predictions)
- ❌ Tight coupling (transaction ingestion waits for prediction)
- ❌ Mixed concerns (source data + enrichment in same table)
- ❌ Complex NULL handling (unpredicted transactions)
- ❌ Schema evolution requires table locks

**When it would make sense**: 
- Very small dataset
- Predictions always available immediately
- No need for prediction history
- Extreme query performance requirements (avoid joins)

---

## Trade-offs & Design Decisions

### 1. Buffering Strategy

**Decision**: Buffer events before processing (batch API calls and DB writes)

| Approach | Latency | Throughput | Cost | Use Case |
|----------|---------|------------|------|----------|
| **Buffered (Current)** | Higher (seconds) | Higher | Lower | Analytics, reporting |
| **Immediate** | Lower (milliseconds) | Lower | Higher | Real-time alerts, monitoring |

**Chosen**: Buffered approach for better resource utilization
**Depends on**: Client SLA requirements (acceptable latency window)

### 2. Single-Node vs. Distributed Processing

**Current**: Single-node Python applications (vertical scaling)

**Appropriate for**:
- Low to medium data volumes
- Cost-sensitive environments
- Simpler operational overhead

**Limitations**:
- Vertical scaling has limits
- Single point of failure
- Cannot leverage distributed computing benefits

**Alternative** (for higher volumes): 
- Apache Flink or Spark/Spark Streaming
- Lambda architecture pattern (unified batch/streaming logic)
- Horizontal scaling across multiple nodes

### 3. Error Handling Strategy

**Current**:
- **ML API calls**: exponential backoff retries. Transactions still failing after the last attempt
  go to the DLQ (streaming) or `rejects/month=…/failed.jsonl` (batch, and the run exits 1 so Airflow
  retries it).
- **Invalid records**: DLQ (streaming, `error_type=invalid`) or `rejects/month=…/invalid.jsonl` (batch).
- **Database errors**: not retried in place. A failed statement leaves the Postgres transaction
  aborted, so the whole unit is retried instead: the Airflow task for batch, redelivery of the
  uncommitted Kafka window for streaming.
- **Idempotency makes these retries safe**: transaction ids are deterministic (UUID5 of the source
  id), transactions are inserted with `ON CONFLICT DO NOTHING` and predictions are upserted.
- **Streaming delivery**: auto-commit is off; offsets are committed only after the database commit
  and the DLQ flush (at-least-once, no loss).

**Rationale**:
- **Retry the unit that can succeed**: HTTP calls in place, database work as a whole transaction
- **Nothing silently dropped**: every record ends up in Postgres, the DLQ or a rejects file
- **Graceful degradation**: Backs off when service is struggling

**Limitation**: 
- Generic approach doesn't differentiate between error types
- Could implement more targeted solutions (e.g., circuit breakers for sustained failures)

**Future improvement**: Targeted error handling strategies per failure type

### 4. Storage Backend Choice

**Current**: PostgreSQL for all data

**Appropriate for**:
- Structured, relational data
- ACID transaction requirements
- SQL-based analytics

**Future considerations** (based on volume and use case):
- **Time-series DB** (InfluxDB, Prometheus): Transactions are inherently time-series
- **NoSQL** (Cassandra, MongoDB): Better horizontal scaling for massive volumes
- **Data Lakehouse** (Delta Lake, Apache Iceberg): Analytics-first, cost-effective storage
- **Choice depends on**: Query patterns, latency requirements, data volume, analytics needs

---

## Code Organization

### Clean Architecture Implementation

The codebase follows **Hexagonal Architecture** (Ports & Adapters pattern) to separate business logic from infrastructure concerns.

```
pipeline/
├── library/                 # Shared business logic
│   ├── src/
│   │   ├── core/           # Domain layer (business logic)
│   │   │   ├── model.py            # Pydantic data models (Transaction, Predict)
│   │   │   ├── protocol.py         # Interface definitions (DataSource, MLService)
│   │   │   ├── orchestrate.py      # Business orchestration logic
│   │   │   └── data_validation.py  # Validation rules
│   │   │
│   │   └── infrastructure/ # Adapter layer (external services)
│   │       ├── api.py              # ML API client (with retry logic)
│   │       ├── database.py         # PostgreSQL models & operations
│   │       ├── files.py            # CSV/S3 file operations
│   │       └── service.py          # Service implementations
│   │
│   └── tests/              # Comprehensive test suite (48 tests, 100% passing)
│
└── application/            # Application-specific implementations
    ├── batch/
    │   ├── service/        # Batch processing application
    │   └── orchestration/  # Airflow DAGs
    │
    └── streaming/
        ├── producer/       # Kafka event generators
        └── consumer/       # Kafka consumer application
```

### Architecture Layers

#### 1. Core Layer (Domain Logic)
**Purpose**: Pure business logic, independent of infrastructure

**Components**:
- **Models**: Pydantic data models (`Transaction`, `Predict`)
- **Protocols**: Interface definitions (structural subtyping, no inheritance)
- **Orchestration**: Processing workflow coordination
- **Validation**: Business rule validation

**Benefits**:
- ✅ No external dependencies (database, API, Kafka)
- ✅ Easy to test (no mocking needed for pure logic)
- ✅ Portable across different infrastructures

#### 2. Infrastructure Layer (Adapters)
**Purpose**: Handle interactions with external services

**Components**:
- **API adapter**: ML service communication with retry logic
- **Database adapter**: PostgreSQL operations (SQLAlchemy)
- **File adapter**: CSV reading, S3/MinIO integration
- **Service adapter**: Base service implementation

**Serialization/Deserialization**:
- External service formats → Internal Pydantic models
- Pydantic models → External service formats

### Future Refactoring

#### Separate Libraries
**Current**: Core and infrastructure in single library

**Target**: Split into two independent libraries
- `dataeng-core`: Domain models and business logic
- `dataeng-infrastructure`: Adapters for external services

**Benefits**:
- ✅ True separation via interface abstraction
- ✅ Core can be tested without any infrastructure
- ✅ Infrastructure adapters can be swapped independently

**Implementation**:
- Define interfaces in core library
- Infrastructure implements interfaces (dependency inversion)
- Applications depend only on core abstractions

#### Library Versioning & Distribution
**Target**: Release libraries to artifact storage
- **Artifact repositories**: AWS CodeArtifact, GitLab Artifact Registry, Azure Artifacts
- **Semantic versioning**: Major.Minor.Patch (e.g., 1.0.0)
- **Release pipeline**: Automated via CI/CD
  - Run tests
  - Build packages
  - Publish to artifact storage
  - Tag release in Git

**Benefits**:
- ✅ Version pinning for applications
- ✅ Controlled dependency updates
- ✅ Reusability across multiple projects

---

## Quality Assurance

### CI/CD Simulation with Pre-commit

The project simulates CI/CD quality gates using **pre-commit hooks**:

```yaml
# .pre-commit-config.yaml
- ruff        # Code quality & formatting (replaces flake8, black, isort)
- pyright     # Static type checking
- pydocstyle  # Documentation style (Google/NumPy docstrings)
- pytest      # Unit test execution
```

**Execution**: Runs automatically before each commit

**Benefits**:
- ✅ Catches issues before code review
- ✅ Consistent code style across team
- ✅ Type safety guarantees
- ✅ Documentation quality enforcement

### Testing Strategy

**Current**: Focused testing on critical components

**Priority**: Core library (business logic)
- **Coverage**: 48 tests, 100% passing
- **Approach**: Protocol-based testing (minimal mocking)
- **Focus**: Data validation, orchestration, model validation

**Future**: Expand to infrastructure layer
- **Challenge**: Requires extensive mocking (database, API, Kafka)
- **Value**: Integration tests ensure services work together
- **Approach**: Test core within real service contexts

**Testing pyramid**:
```
    /\      E2E Tests (Future)
   /  \     
  /    \    Integration Tests (Infrastructure - Future)
 /      \   
/________\  Unit Tests (Core - Current ✅)
```

### Dependency Injection & Configuration

**Current**: Manual dependency injection in applications

**Future**: Factory pattern for cleaner instantiation
```python
# Current approach (manual)
service = BatchService(
    s3_path="s3://...",
    storage_options={...},
    ml_api_url="http://...",
    db_session=session
)

# Target approach (factory)
service = ServiceFactory.create_batch_service(config)
```

**Benefits**:
- ✅ Centralized configuration
- ✅ Easier testing (inject test configs)
- ✅ Cleaner application code

### Code Separation

**Future**: Extract ThreadPoolExecutor from orchestration library

**Current**: Thread pool management mixed with orchestration logic

**Target**: Separate execution strategy from orchestration
- **Orchestration**: Define *what* to process and *when*
- **Executor**: Handle *how* to execute (threading, async, distributed)

**Benefits**:
- ✅ Single responsibility principle
- ✅ Pluggable execution strategies
- ✅ Easier to test orchestration logic independently

---

## Future Improvements

### 1. Performance Optimizations

#### Asynchronous Streaming Consumer
**Current**: Synchronous consumer (blocking I/O)

**Target**: Async consumer with `asyncio`
- **Non-blocking I/O**: Handle multiple messages concurrently
- **Better resource utilization**: Single thread handles many connections
- **Higher throughput**: Process more messages per second

**Implementation**:
```python
async def consume_messages():
    async for message in consumer:
        await process_transaction(message)
```

#### Memory-Efficient CSV Reading
**Current**: Load entire CSV into memory with Polars

**Target**: Streaming CSV reader
```python
# Instead of
df = pl.read_csv("large_file.csv")  # Loads all data

# Use streaming
for batch in pl.read_csv_batched("large_file.csv", batch_size=10000):
    process_batch(batch)  # Process incrementally
```

**Benefits**:
- ✅ Constant memory usage regardless of file size
- ✅ Can process files larger than available RAM
- ✅ Faster time to first result

### 2. Error Reprocessing

**Current**: Failed and invalid records are persisted (DLQ topic for streaming, `rejects/` files on
MinIO for batch) but nothing consumes them yet.

**Target**:
- A DLQ consumer that retries `prediction_failed` messages after a delay and alerts on `invalid` ones
- Carry the validation error message with invalid records (today only the record and the
  `error_type` header)

### 3. Streaming Predictions Topic

**Current**: Streaming consumer writes predictions directly to PostgreSQL

**Target**: Write predictions to Kafka topic
- **Topic**: `predictions`
- **Schema**: Validated prediction results
- **Consumers**: PostgreSQL connector, analytics services, monitoring tools

**Benefits**:
- ✅ Decouples prediction from persistence
- ✅ Multiple consumers can use predictions
- ✅ Enables real-time analytics pipelines

### 4. Schema Registry for Kafka Topics

**Current**: JSON serialization with Pydantic validation at application level

**Target**: Confluent Schema Registry with Avro or Protobuf serialization

#### Implementation

**Schema Registry setup**:
```yaml
# docker-compose.yml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  ports:
    - "8081:8081"
  environment:
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
```

**Avro schema definition**:
```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.dataeng.models",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "description", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "merchant", "type": "string"},
    {"name": "operation_type", "type": "string"},
    {"name": "side", "type": {"type": "enum", "symbols": ["debit", "credit"]}}
  ]
}
```

**Producer with Schema Registry**:
```python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Register schema and produce
producer = AvroProducer({
    'bootstrap.servers': 'kafka:9092',
    'schema.registry.url': 'http://schema-registry:8081'
}, default_value_schema=transaction_schema)

producer.produce(topic='transactions', value=transaction_dict)
```

#### Benefits

##### 1. **Strong Interface Contracts**
- ✅ **Schema enforced**: Producers must conform to registered schema
- ✅ **Validation at write**: Invalid messages rejected at production time
- ✅ **Type safety**: Schema defines field types explicitly
- ✅ **Documentation**: Schema serves as API contract

**Current problem**: JSON allows any shape, validation only at consumer

##### 2. **Schema Evolution with Compatibility**
- ✅ **Controlled evolution**: Add/remove fields with compatibility rules
- ✅ **Forward compatible**: Old consumers can read new data
- ✅ **Backward compatible**: New consumers can read old data
- ✅ **Version tracking**: All schema versions stored with history

**Compatibility modes**:
```
BACKWARD: New schema can read old data (add optional fields)
FORWARD: Old schema can read new data (remove fields)
FULL: Both backward and forward (add optional, remove optional)
```

**Example evolution**:
```json
// v1 schema
{"name": "amount", "type": "double"}

// v2 schema (backward compatible - added optional field)
{"name": "amount", "type": "double"}
{"name": "currency", "type": "string", "default": "USD"}

// v3 schema (forward compatible - removed field with default)
{"name": "amount", "type": "double"}
```

##### 3. **Efficient Serialization**
- ✅ **Binary format**: Avro is compact (vs. JSON text)
- ✅ **Schema not included**: Schema ID sent instead of full schema
- ✅ **Compression friendly**: Binary data compresses better
- ✅ **Performance**: Faster serialization/deserialization

**Size comparison**:
```
JSON:     ~250 bytes per transaction
Avro:     ~80 bytes per transaction (68% reduction)
```

**Throughput impact**:
- Lower network bandwidth
- Faster Kafka writes/reads
- Lower storage costs

##### 4. **Cross-Language Support**
- ✅ **Polyglot systems**: Python, Java, Go all use same schema
- ✅ **No model drift**: Schema is single source of truth
- ✅ **Code generation**: Auto-generate models from schema

**Current limitation**: Pydantic models only work in Python

**Multi-language example**:
```python
# Python producer (Avro)
producer.produce(topic='transactions', value=transaction)

# Java consumer (same schema)
ConsumerRecord<String, Transaction> record = consumer.poll();
Transaction txn = record.value();  // Auto-deserialized
```

##### 5. **Governance and Discovery**
- ✅ **Schema catalog**: Browse all schemas in UI
- ✅ **Usage tracking**: See which topics use which schemas
- ✅ **Impact analysis**: Find consumers affected by schema changes
- ✅ **Compliance**: Audit trail of all schema changes

**Schema Registry UI**:
- Browse schemas by topic
- View version history
- Test compatibility before deploying

#### When to Implement

**Triggers**:
- 🎯 **Multiple teams** producing/consuming same topics
- 🎯 **Polyglot services** (Python, Java, Go, etc.)
- 🎯 **Frequent schema changes** requiring coordination
- 🎯 **Compliance requirements** for schema governance
- 🎯 **Performance issues** from large JSON payloads

**Not needed if**:
- ✅ Single team, single language (current state)
- ✅ Stable schemas with infrequent changes
- ✅ Low message volume (serialization cost negligible)

#### Alternative: Protobuf

**Confluent Schema Registry also supports Protobuf**:

```protobuf
// transaction.proto
syntax = "proto3";

message Transaction {
  string id = 1;
  string description = 2;
  double amount = 3;
  int64 timestamp = 4;
  string merchant = 5;
  string operation_type = 6;
  enum Side {
    DEBIT = 0;
    CREDIT = 1;
  }
  Side side = 7;
}
```

**Protobuf vs. Avro**:
| Feature | Avro | Protobuf |
|---------|------|----------|
| Schema evolution | Better (dynamic) | Good (requires recompilation) |
| Serialization speed | Fast | Faster |
| Size | Compact | More compact |
| Ecosystem | Kafka-native | Google ecosystem |
| Code generation | Optional | Required |

**Choice depends on**: Existing infrastructure, team preference, performance requirements

#### Migration Path

**Phase 1**: Keep JSON, add Schema Registry
- Register JSON schemas
- Validate at producer
- No consumer changes

**Phase 2**: Migrate to Avro/Protobuf
- Deploy dual-format producers (JSON + Avro)
- Migrate consumers one by one
- Deprecate JSON once all migrated

**Phase 3**: Enforce schema validation
- Reject messages without schema
- Block incompatible schema changes
- Full governance in place

### 5. Distributed Processing for High Volume

**Current**: Single-node applications (vertical scaling)

**Target**: Distributed processing frameworks

**Options**:

#### Apache Flink
- **Strengths**: True streaming, low latency, stateful processing
- **Use case**: Real-time processing with complex event processing

#### Apache Spark (Batch) / Spark Streaming
- **Strengths**: Unified API for batch and streaming (Lambda architecture)
- **Use case**: Same code runs in batch and streaming modes
- **Benefits**: 
  - Code reuse across pipelines
  - Horizontal scaling (distribute across nodes)
  - Fault tolerance built-in

**When to migrate**:
- Data volume exceeds single-node capacity
- Sub-second latency required
- Need for stateful computations (windowing, aggregations)

### 6. Alternative Storage Backends

**Current**: PostgreSQL for all data

**Future options** (based on use case):

#### Time-Series Databases
- **InfluxDB**: Purpose-built for time-series data
- **Prometheus**: Metrics and monitoring
- **Use case**: Real-time monitoring dashboards
- **Benefits**: 
  - Optimized for time-series queries
  - Better compression for sequential data
  - Built-in downsampling and retention policies

#### Data Lakehouse
- **Delta Lake** / **Apache Iceberg** / **Apache Hudi**
- **Storage**: Parquet files in S3/MinIO
- **Use case**: Analytics workloads, data science
- **Benefits**:
  - Cost-effective (cheap object storage)
  - ACID transactions on data lake
  - Time travel (query historical data)
  - Schema evolution

#### NoSQL Databases
- **Cassandra**: Massive scale, write-heavy workloads
- **MongoDB**: Flexible schema, document-oriented
- **Use case**: Very high volume, distributed writes
- **Benefits**: Horizontal scaling, high availability

**Decision criteria**:
- **Query patterns**: OLTP vs. OLAP
- **Latency requirements**: Real-time vs. batch analytics
- **Data volume**: GB vs. TB vs. PB
- **Cost**: Operational cost vs. storage cost

### 7. Enhanced Test Coverage

**Current**: 48 tests focused on core library

**Future**:
1. **Infrastructure tests**: Test adapters with mocked services
2. **Integration tests**: Test core with real services (Docker containers)
3. **Contract tests**: Ensure API contract compliance
4. **Load tests**: Validate performance under high volume
5. **Chaos engineering**: Test failure scenarios

**Target**: >80% code coverage across all layers

### 8. Advanced Error Handling

**Current**: Generic exponential backoff for all errors

**Future**: Targeted error handling strategies

**Circuit Breaker Pattern**:
```python
# Stop calling failing service after threshold
if error_rate > threshold:
    return cached_response  # Fail fast
# Try again after cooldown period
```

**Error Classification**:
- **Retriable**: Network timeouts, rate limits → Retry
- **Non-retriable**: Invalid data, auth errors → Send to DLQ
- **Transient**: Temporary overload → Exponential backoff
- **Permanent**: Service down → Circuit breaker

### 9. Observability Enhancements

**Current**: Logs and UI tools (Kafka UI, Adminer, Airflow)

**Future**: Full observability stack

**Metrics** (Prometheus):
- Processing throughput (records/sec)
- Error rates by type
- API latency (p50, p95, p99)
- Database connection pool stats
- Kafka consumer lag

**Dashboards** (Grafana):
- Real-time pipeline health
- Historical trends and patterns
- SLA compliance tracking
- Alert thresholds visualization

**Tracing** (OpenTelemetry):
- End-to-end request tracking
- Service dependency mapping
- Performance bottleneck identification

**Alerting**:
- Consumer lag > threshold
- Error rate spike
- API latency degradation
- Database connection issues

---

## Summary

This implementation demonstrates production-grade data engineering practices with:

✅ **Clean Architecture**: Separation of domain logic and infrastructure  
✅ **Dual Pipelines**: Batch and streaming with shared business logic  
✅ **Resilience**: Idempotent writes, at-least-once streaming, DLQ and rejects, retries  
✅ **Scalability**: Horizontal scaling ready, distributed processing path  
✅ **Quality**: Unit tests (including delivery guarantees), pre-commit hooks, type checking  
✅ **Documentation**: Comprehensive READMEs and design notes  

The architecture provides a **solid foundation** with clear paths for:
- Enhanced observability
- Distributed processing for scale
- Flexible backend options via connector pattern
- DLQ reprocessing
- Full test coverage and CI/CD integration

**Design philosophy**: Balance pragmatism with best practices, delivering a functional system while maintaining extensibility for future requirements.
