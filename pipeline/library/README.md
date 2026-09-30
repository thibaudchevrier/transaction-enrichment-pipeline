# Transaction Processing Library

A shared Python library providing core business logic and infrastructure components for batch and streaming transaction processing pipelines.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture Principles](#architecture-principles)
- [Module Structure](#module-structure)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [API Reference](#api-reference)
- [Development](#development)

## 🎯 Overview

This library implements a **Hexagonal Architecture** (Ports & Adapters) design pattern, separating domain logic from infrastructure concerns. It provides reusable components for:

- Transaction data validation with Pydantic models
- ML API integration with retry logic
- Database operations with SQLAlchemy
- S3/MinIO data loading
- Service orchestration for parallel processing

The library is shared across **batch** and **streaming** pipelines, ensuring consistency and reducing code duplication.

## 🏗️ Architecture Principles

### Hexagonal Architecture (Clean Architecture)

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
│              (batch/main.py, streaming/*.py)                 │
└─────────────────┬───────────────────────────────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
┌───▼─────────────┐    ┌────────▼──────────────┐
│   Core Domain   │    │   Infrastructure      │
│  (Business Logic)│◄───│    (Adapters)        │
└─────────────────┘    └───────────────────────┘
```

**Key Principles:**

1. **Dependency Inversion**: Infrastructure depends on Core, not vice versa
2. **Protocol-Based Design**: Core defines interfaces (`ServiceProtocol`), infrastructure implements them
3. **Domain Purity**: Core contains only business logic, no external dependencies
4. **Testability**: Easy to mock infrastructure for unit testing

### Layer Responsibilities

#### Core Layer (`src/core/`)
**Purpose**: Pure business logic and domain models

- **What it contains**: 
  - Pydantic models (`Transaction`)
  - Validation logic
  - Orchestration workflow
  - Service protocols (interfaces)
  
- **What it doesn't contain**:
  - Database connections
  - HTTP clients
  - File I/O operations
  - External service calls

- **Dependencies**: Only Python stdlib and Pydantic

#### Infrastructure Layer (`src/infrastructure/`)
**Purpose**: External adapters and technical implementations

- **What it contains**:
  - API clients (ML predictions)
  - Database operations (SQLAlchemy)
  - S3/MinIO data loading
  - Retry mechanisms
  - Service implementations

- **What it doesn't contain**:
  - Business rules
  - Domain validation
  - Workflow orchestration

- **Dependencies**: SQLAlchemy, requests, polars, boto3

## 📦 Module Structure

```
library/
├── pyproject.toml              # Package configuration
├── README.md                   # This file
└── src/
    ├── core/                   # 🧠 Domain Layer
    │   ├── __init__.py
    │   ├── model.py           # Pydantic Transaction model
    │   ├── data_validation.py # Validation logic
    │   ├── protocol.py        # ServiceProtocol interface
    │   └── orchestrate.py     # Orchestration workflow
    │
    └── infrastructure/         # 🔌 Adapter Layer
        ├── __init__.py
        ├── api.py             # ML API client
        ├── database.py        # SQLAlchemy models & operations
        ├── generator.py       # S3/MinIO data loading
        ├── utils.py           # Retry decorator
        └── service.py         # BaseService implementation
```

### Core Modules

#### `core/model.py`
Pydantic models for transactions with validation and deterministic ids: a UUID is kept, any other
source id is mapped to a UUID5, so reprocessing a record yields the same id (idempotent writes).

```python
class Transaction(BaseModel):
    id: str              # UUID, or UUID5 of the source id
    description: str
    amount: float
    timestamp: str       # ISO format
    merchant: str | None
    operation_type: str
    side: str
```

#### `core/protocol.py`
Service interface definition using Python Protocol (PEP 544).

```python
class ServiceProtocol(Protocol):
    def read(source: str) -> Iterator[tuple[list[dict], list[dict]]]
    def predict(transactions: list[dict]) -> list[dict]
    def bulk_write(transactions: list[dict], predictions: list[dict]) -> None
```

#### `core/orchestrate.py`
Main orchestration logic for batch processing with parallel API calls.

- Coordinates read → validate → predict → write workflow
- Parallel API calls using `ThreadPoolExecutor`
- Tracks valid, invalid, and failed transactions
- Bulk database writes for efficiency

#### `core/data_validation.py`
Transaction validation using Pydantic models.

- Validates batches of transactions
- Returns valid and invalid records separately
- Detailed error logging for debugging

### Infrastructure Modules

#### `infrastructure/api.py`
ML API client with retry logic.

- `predict_batch()`: Send transactions for classification prediction
- Automatic retry with exponential backoff
- Handles API failures gracefully

#### `infrastructure/database.py`
SQLAlchemy models and database operations.

- `TransactionDB`: Transaction table model
- `PredictionDB`: Prediction table model
- `get_db_session()`: Context manager for sessions
- `db_write_results()`: Bulk insert/upsert operations

#### `infrastructure/generator.py`
S3/MinIO data loading with Polars.

- `load_and_validate_transactions()`: Generator for streaming reads
- Memory-efficient batch processing
- Automatic validation during load

#### `infrastructure/service.py`
Base service implementation.

- `BaseService`: Implements `ServiceProtocol`
- Encapsulates common operations (predict, bulk_write)
- Subclassed by batch/streaming applications

## 🚀 Installation

### As Editable Package (Development)

```bash
cd pipeline/library
pip install -e .
```

### In Docker (Multi-stage Build)

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.13-bookworm AS uv_build

WORKDIR /app
COPY library/ library/
COPY your_app/ your_app/

RUN --mount=type=cache,target=/root/.cache/uv \
    cd your_app && uv sync --frozen

FROM python:3.13-slim
COPY --from=uv_build /app /app
WORKDIR /app/your_app
CMD ["uv", "run", "main.py"]
```

## 💡 Usage Examples

### Example 1: Batch Processing Service

```python
from infrastructure import BaseService, load_and_validate_transactions, get_db_session
from core import orchestrate_service
from typing import Iterator

class BatchService(BaseService):
    def read(self, batch_size: int) -> Iterator[tuple[list[dict], list[dict]]]:
        return load_and_validate_transactions(
            s3_path=self.s3_path,
            storage_options=self.storage_options,
            batch_size=batch_size
        )

# Usage
with get_db_session(database_url) as session:
    service = BatchService(
        s3_path="s3://bucket/data.csv",
        storage_options={"key": "...", "secret": "..."},
        ml_api_url="http://ml-api:8000",
        db_session=session
    )
    
    total, failed, invalid = orchestrate_service(
        service=service,
        row_batch_size=5000,
        api_batch_size=100,
        api_max_workers=5,
        db_row_batch_size=1000
    )
```

### Example 2: Direct Validation

```python
from core.data_validation import validate_transaction_records

records = [
    {"id": "1", "amount": 100.0, "description": "Purchase", ...},
    {"id": "2", "amount": -50.0, "description": "Invalid", ...}
]

valid, invalid = validate_transaction_records(records)
print(f"Valid: {len(valid)}, Invalid: {len(invalid)}")
```

### Example 3: ML API Predictions

```python
from infrastructure.api import predict_batch

transactions = [
    {"id": "uuid-1", "amount": 100.0, ...},
    {"id": "uuid-2", "amount": 250.0, ...}
]

predictions = predict_batch(
    transactions=transactions,
    ml_api_url="http://ml-api:8000",
    batch_id=1
)

for pred in predictions:
    print(f"Transaction {pred['transaction_id']}: {pred['category']}")
```

## 📚 API Reference

### Core API

#### `validate_transaction_records(records: list[dict]) -> tuple[list[dict], list[dict]]`
Validate a batch of transaction records using Pydantic models.

**Returns:** `(valid_transactions, invalid_transactions)`

#### `orchestrate_service(service, row_batch_size, api_batch_size, api_max_workers, db_row_batch_size)`
Orchestrate batch processing with parallel API calls and bulk writes.

**Returns:** `(total_processed, failed_transactions, invalid_transactions)`

### Infrastructure API

#### `predict_batch(transactions: list[dict], ml_api_url: str, batch_id: int = 0) -> list[dict]`
Get classification predictions from ML API with automatic retry.

#### `db_write_results(session: Session, transactions: list[dict], predictions: list[dict]) -> None`
Bulk insert/upsert transactions and predictions to database.

#### `load_and_validate_transactions(s3_path: str, storage_options: dict, batch_size: int)`
Generator that loads and validates transactions from S3/MinIO in batches.

**Yields:** `(valid_transactions, invalid_transactions)`

#### `get_db_session(database_url: str) -> ContextManager[Session]`
Context manager for SQLAlchemy database sessions.

## 🛠️ Development

### Running Tests

```bash
cd pipeline/library
uv run pytest tests/
```

### Code Quality

```bash
# Type checking
uv run mypy src/

# Linting
uv run ruff check src/

# Formatting
uv run ruff format src/
```

### Dependencies

**Core Dependencies:**
- `pydantic >= 2.0` - Data validation
- `python >= 3.13` - Type hints with `|` syntax

**Infrastructure Dependencies:**
- `sqlalchemy >= 2.0` - Database ORM
- `psycopg2-binary` - PostgreSQL driver
- `requests` - HTTP client
- `polars` - Fast DataFrame library
- `s3fs` - S3 file system

### Environment Variables

Required for applications using this library:

```bash
# S3/MinIO Configuration
KEY=minio_access_key
SECRET=minio_secret_key
ENDPOINT_URL=http://minio:9000

# Database Configuration
DATABASE_URL=postgresql://user:pass@host:5432/db

# ML API Configuration
ML_API_URL=http://ml-api:8000
```

## 🤝 Contributing

1. Follow the architecture principles (Hexagonal Architecture)
2. Keep core/ pure - no infrastructure dependencies
3. Add NumPy-style docstrings to all functions
4. Write tests for new functionality
5. Update this README for API changes
