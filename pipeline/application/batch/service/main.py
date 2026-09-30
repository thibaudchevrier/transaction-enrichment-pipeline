"""Batch processing pipeline for transaction data.

This module implements a batch processing pipeline that loads transactions
from S3/MinIO, validates them, performs ML classification predictions in parallel,
and persists results to PostgreSQL database.
"""

import json
import logging
import os
import posixpath
import sys
from collections.abc import Iterator

import fsspec
from core import orchestrate_service
from infrastructure import BaseService, get_db_session
from infrastructure.generator import load_and_validate_transactions
from sqlalchemy.orm import Session

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BatchService(BaseService):
    """
    Batch processing service for transaction pipelines.

    Extends BaseService to implement batch-specific data loading from
    S3/MinIO storage. Reads CSV transaction data in configurable batch
    sizes and validates records before yielding to orchestration layer.

    Attributes
    ----------
    s3_path : str
        S3/MinIO path to transaction CSV file.
    storage_options : dict
        S3 configuration (access key, secret, endpoint URL).
    ml_api_url : str
        ML API endpoint URL (inherited from BaseService).
    db_session : Session
        SQLAlchemy session for database operations (inherited from BaseService).

    Methods
    -------
    read(batch_size: int) -> Iterator[tuple[list[dict], list[dict]]]
        Load and validate transactions in batches from S3/MinIO.

    Notes
    -----
    Uses load_and_validate_transactions generator from infrastructure layer
    for CSV parsing and validation logic reuse.
    """

    def __init__(self, s3_path: str, storage_options: dict, ml_api_url: str, db_session: Session, run_id: str) -> None:
        """
        Initialize BatchService with S3 and database configuration.

        Parameters
        ----------
        s3_path : str
            S3/MinIO path to transaction CSV file.
        storage_options : dict
            S3 configuration dictionary containing:
            - key: MinIO access key
            - secret: MinIO secret key
            - client_kwargs: Dict with endpoint_url
        ml_api_url : str
            ML API endpoint URL for classification predictions.
        db_session : Session
            SQLAlchemy session for database operations.

        Notes
        -----
        Calls parent BaseService.__init__() with ml_api_url and db_session,
        then stores S3-specific configuration as instance attributes.
        """
        super().__init__(ml_api_url=ml_api_url, db_session=db_session)
        self.s3_path = s3_path
        self.storage_options = storage_options
        self.run_id = run_id

    def read(self, batch_size: int) -> Iterator[tuple[list[dict], list[dict]]]:
        """
        Load and validate transactions in batches from S3/MinIO.

        Implements the abstract read() method from BaseService for
        batch processing. Reads CSV data from S3/MinIO in chunks,
        parses and validates each batch.

        Parameters
        ----------
        batch_size : int
            Number of transactions to load per batch.

        Yields
        ------
        tuple[list[dict], list[dict]]
            Tuple containing:
            - List of valid transaction dictionaries
            - List of invalid transaction dictionaries with error details

        Notes
        -----
        Delegates to load_and_validate_transactions generator which handles:
        - CSV parsing from S3 (pandas with s3fs)
        - Schema validation
        - Data type coercion
        - Error collection

        Uses self.s3_path and self.storage_options configured during init.
        """
        return load_and_validate_transactions(
            s3_path=self.s3_path,
            storage_options=self.storage_options,
            batch_size=batch_size,
            run_id=self.run_id,
            processing_type="batch",
        )


def write_rejects(rejects_path: str, storage_options: dict, failed: list[dict], invalid: list[dict]) -> None:
    """
    Write the records a run could not process next to the source data.

    Parameters
    ----------
    rejects_path : str
        Prefix for this run's reject files (e.g. 's3://transactions/rejects/run_id=...').
    storage_options : dict
        fsspec options for the filesystem (S3/MinIO credentials).
    failed : list[dict]
        Valid transactions whose prediction failed after all retries.
    invalid : list[dict]
        Records that failed validation.

    Notes
    -----
    One JSON Lines file per kind (``failed.jsonl``, ``invalid.jsonl``). Each run
    owns its month's rejects: files are overwritten, and a kind with nothing
    to report is removed, so a successful retry doesn't leave the previous
    attempt's failures behind.
    """
    for name, records in (("failed", failed), ("invalid", invalid)):
        path = f"{rejects_path.rstrip('/')}/{name}.jsonl"
        fs, fs_path = fsspec.url_to_fs(path, **storage_options)
        if not records:
            if fs.exists(fs_path):
                fs.rm(fs_path)
                logger.info(f"Removed stale {path}")
            continue
        fs.makedirs(posixpath.dirname(fs_path), exist_ok=True)  # no-op on object storage
        with fs.open(fs_path, "w") as f:
            f.writelines(json.dumps(record, default=str) + "\n" for record in records)
        logger.warning(f"Wrote {len(records)} {name} records to {path}")


def main() -> int:
    """
    Execute batch processing pipeline.

    This function orchestrates the complete batch processing workflow:
    - Loads configuration from environment variables
    - Creates database session
    - Initializes BatchService with S3/MinIO credentials
    - Runs orchestrate_service for parallel processing
    - Writes failed and invalid records to the rejects location

    Environment Variables
    ---------------------
    ML_API_URL : str
        ML API endpoint (default: 'http://localhost:8000').
    ROW_BATCH_SIZE : int
        Transactions per batch from source (default: 5000).
    API_BATCH_SIZE : int
        Transactions per API request (default: 100).
    API_MAX_WORKERS : int
        Parallel API workers (default: 5).
    DB_ROW_BATCH_SIZE : int
        Threshold for bulk database writes (default: 1000).
    DATABASE_URL : str
        PostgreSQL connection string (required).
    KEY : str
        MinIO access key (required).
    SECRET : str
        MinIO secret key (required).
    ENDPOINT_URL : str
        MinIO endpoint URL (required).
    SOURCE_PATH : str
        Partition to process, e.g. 's3://transactions/raw/month=2023-01/transactions.csv' (required).
    REJECTS_PATH : str
        Where to write this run's rejected records (required).
    BATCH_RUN_ID : str
        Airflow run id, stored on each row for lineage (default: 'batch_unknown').

    Returns
    -------
    int
        Exit code: 1 if some predictions failed (so Airflow retries the run;
        writes are idempotent), 0 otherwise. Invalid records don't fail the
        run: retrying can't fix them, they are in the rejects file.
    """
    logger.info("Starting batch pipeline")

    ml_api_url = os.getenv("ML_API_URL", "http://localhost:8000")
    row_batch_size = int(os.getenv("ROW_BATCH_SIZE", "5000"))
    api_batch_size = int(os.getenv("API_BATCH_SIZE", "100"))
    api_max_workers = int(os.getenv("API_MAX_WORKERS", "5"))
    db_row_batch_size = int(os.getenv("DB_ROW_BATCH_SIZE", "1000"))
    run_id = os.getenv("BATCH_RUN_ID", "batch_unknown")
    s3_path = os.environ["SOURCE_PATH"]
    rejects_path = os.environ["REJECTS_PATH"]
    logger.info(f"Run {run_id}: processing {s3_path}")

    storage_options = {
        "key": os.environ["KEY"],
        "secret": os.environ["SECRET"],
        "client_kwargs": {"endpoint_url": os.environ["ENDPOINT_URL"]},
    }

    with get_db_session(os.environ["DATABASE_URL"]) as session:
        _, failed, invalid = orchestrate_service(
            service=BatchService(
                s3_path=s3_path,
                storage_options=storage_options,
                ml_api_url=ml_api_url,
                db_session=session,
                run_id=run_id,
            ),
            row_batch_size=row_batch_size,
            api_batch_size=api_batch_size,
            api_max_workers=api_max_workers,
            db_row_batch_size=db_row_batch_size,
        )

    write_rejects(rejects_path, storage_options, failed, invalid)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
