"""Data loading and validation module.

This module provides functions for loading transaction data from S3/MinIO,
validating transactions using Pydantic models, and yielding batches for
processing in the pipeline.
"""

import logging
from collections.abc import Iterator

import fsspec  # pyright: ignore[reportMissingTypeStubs]  # fsspec ships no stubs
import polars as pl

from core.data_validation import validate_transaction_records

logger = logging.getLogger(__name__)


def __validate_transaction_dataframe(df: pl.DataFrame, run_id: str, processing_type: str) -> pl.DataFrame:
    # Check for required columns
    required_cols = {"id", "description", "amount", "timestamp", "merchant", "operation_type", "side"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Add lineage tracking columns
    df = df.with_columns([pl.lit(run_id).alias("run_id"), pl.lit(processing_type).alias("processing_type")])

    # Check for null values
    null_counts = df.null_count()
    total_nulls = sum([null_counts[col][0] for col in null_counts.columns])
    if total_nulls > 0:
        logger.warning(f"Found {total_nulls} null values in data - these rows will be filtered")
        df = df.drop_nulls()
        logger.info(f"After filtering nulls: {len(df)} transactions remain")
    return df


def load_and_validate_transactions(
    s3_path: str, storage_options: dict, run_id: str, processing_type: str, batch_size: int = 100
) -> Iterator[tuple[list[dict], list[dict]]]:
    """
    Load transactions from S3/MinIO and yield validated batches.

    This function reads CSV data from S3, validates each transaction
    with Pydantic (auto-assigning UUIDs), and yields batches of
    validated transactions.

    Parameters
    ----------
    s3_path : str
        S3 path to a CSV file (e.g., 's3://bucket/raw/month=2023-01/transactions.csv'),
        or a glob over several (e.g., 's3://bucket/raw/month=*/transactions.csv').
    storage_options : dict
        fsspec options for the filesystem, e.g. S3/MinIO credentials
        ('key', 'secret', 'client_kwargs' with 'endpoint_url').
    batch_size : int, optional
        Number of transactions per batch, by default 100.

    Yields
    ------
    tuple[list[dict], list[dict]]
        Tuple of (validated_transactions, invalid_transactions).

    Raises
    ------
    ValueError
        If required columns are missing from the CSV.
    FileNotFoundError
        If no file matches ``s3_path``.

    Notes
    -----
    Invalid transactions are collected and yielded alongside
    valid transactions for error reporting.
    """
    logger.info(f"Reading data from {s3_path}")

    fs, _, paths = fsspec.get_fs_token_paths(s3_path, storage_options=storage_options)
    if not paths:
        raise FileNotFoundError(f"No file matches {s3_path}")

    frames = []
    for path in paths:
        with fs.open(path, "rb") as f:
            frames.append(pl.read_csv(f, separator=";", decimal_comma=True))
    df = pl.concat(frames)

    logger.info(f"Loaded {len(df)} raw transactions from CSV")

    for sub_df in __validate_transaction_dataframe(df, run_id=run_id, processing_type=processing_type).iter_slices(
        n_rows=batch_size
    ):
        yield validate_transaction_records(sub_df.to_dicts())
