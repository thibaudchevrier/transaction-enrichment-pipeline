# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Batch (Airflow) and streaming (Kafka) pipelines that enrich transactions with a category from an ML API
and store them in Postgres. `ml_api/` is a stub whose prediction logic (`CATEGORIES[hash(id) % len]`)
must stay as it is. The work here is in the pipelines, not the model.

## Commands

The repo is a uv workspace (root `pyproject.toml`). Each component under `pipeline/` has its own
`pyproject.toml`, uv lock and Docker image, so run tools **from the component directory**.

```bash
# Full stack (creates .env from .env.example on first run)
make all                                   # or: make infra / make streaming / make batch
make down                                  # make clean also removes volumes
ML_API_PUBLISHED_PORT=8010 make all        # if host port 8000 is taken

# Tests
cd pipeline/library && uv run pytest                                   # library
cd pipeline/library && uv run pytest tests/core/test_model.py::TestTransactionModel::test_id_is_deterministic
cd pipeline/application/streaming/consumer && uv run pytest            # delivery-guarantee tests

# Lint / types (per component; library pyright is strict about missing stubs)
uv run ruff check --fix . && uv run ruff format . && uv run pyright && uv run pydocstyle
uv run poe check-all                        # from the root: format + types + lint for every component
```

The installed git pre-commit hook uses `pipeline/library/.pre-commit-config.yaml`: it formats,
type-checks and tests **the library only**. Changes to the batch service, DAG, producer or consumer
aren't checked on commit, so run ruff and pyright in those directories yourself.

## Architecture

**Shared library, thin applications.** `pipeline/library` holds `core` (Pydantic `Transaction` model,
validation, `orchestrate_service`) and `infrastructure` (ML API client with retry, Postgres writes,
fsspec/polars file loading, `BaseService`). An application only subclasses `BaseService` and implements
`read(batch_size)`, which yields `(valid, invalid)` batches. `orchestrate_service` does the rest:
parallel API calls (ThreadPoolExecutor), bulk writes, and returns `(processed, failed, invalid)`.
Libraries are imported as top-level `core` / `infrastructure` (see `tests/conftest.py`).

**Idempotency is the core invariant.** `Transaction.normalize_id` keeps a UUID id and maps any other
source id to a UUID5 (`TRANSACTION_ID_NAMESPACE`, which must never change). Transactions are inserted
with `ON CONFLICT DO NOTHING` and predictions are upserted. Every retry strategy depends on this, so
never reintroduce random ids in validation. The streaming producer deliberately assigns a fresh
`uuid4` per event, because it simulates a source system.

**Retries happen per unit, not in place.** `retry_with_backoff` only retries `requests` errors (HTTP
calls). It returns `(transactions, None)` when every attempt fails, and `orchestrate_service` treats
that as failed. Database errors are not retried inside the transaction (Postgres aborts it). They
propagate, and the whole unit is retried:
- **Streaming**: `process_window` in `consumer/main.py` runs, in this order, DB transaction → DLQ
  publish + flush → synchronous offset commit. Kafka auto-commit is off. A crash before the commit
  means redelivery. `StreamingService.read` must add every polled message to the window before
  closing it, because its offset is committed with the window.
- **Batch**: one Airflow run = one monthly partition. The DAG uses
  `CronDataIntervalTimetable("@monthly")` with catchup over 2023-01 → 2024-04 and `max_active_runs=1`.
  `get_environment_vars` derives `SOURCE_PATH` (`s3://transactions/raw/month=YYYY-MM/…`) and
  `REJECTS_PATH` from `data_interval_start`, and passes them via XCom to a `DockerOperator` running
  the `batch-processor:latest` image. The service writes `rejects/month=…/{failed,invalid}.jsonl`
  (removing kinds with nothing to report) and exits 1 on failed predictions so Airflow retries.

**Failure routing.** Invalid records → DLQ `error_type=invalid` / `invalid.jsonl` (the run doesn't
fail). Predictions still failing after retries → DLQ `error_type=prediction_failed` / `failed.jsonl`
(the batch run fails).

**Infrastructure notes.**
- The compose project name is `transaction-enrichment-pipeline`. The network has the fixed name
  `transaction-enrichment-network`, because the DockerOperator attaches batch containers to it by
  name (`PIPELINE_DOCKER_NETWORK`).
- The batch image is built by the `batch-processor` compose service (it exits immediately). After
  changing the batch service, rebuild it (`docker compose --profile batch build batch-processor`)
  before re-running DAG tasks.
- MinIO uses `bitnamilegacy/*` images, because the official `minio/*` images can no longer be pulled.
  Data lives under `/bitnami/minio/data`, and `mc` needs `MC_CONFIG_DIR` (it runs as a non-root user).
- Postgres runs on `tmpfs`: data resets when the container is recreated. The Airflow volume
  (run history) and the MinIO volume persist.
- The schema is managed by Flyway (`migrations/V*__*.sql`). The SQLAlchemy models in
  `infrastructure/database.py` must match it.
- Source data is `data/transactions/month=YYYY-MM/transactions.csv` (`;` separator, decimal comma),
  uploaded to MinIO `raw/` by `minio-init`.

Design rationale and trade-offs are in `NOTES.md`; operating details are in `HOWTO.md`.
