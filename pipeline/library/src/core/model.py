"""Pydantic data models for transaction processing.

This module defines Pydantic models for transactions and predictions,
providing automatic validation, type checking, and UUID generation.
"""

from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, field_validator

# Namespace for deriving transaction UUIDs from source identifiers. Changing it
# changes every derived id, so it must stay fixed once data has been written.
TRANSACTION_ID_NAMESPACE = uuid5(NAMESPACE_URL, "transaction-enrichment-pipeline/transaction")


class Transaction(BaseModel):
    """
    Transaction model with validation.

    Used for both CSV/Kafka input. The id is deterministic: a UUID is kept as
    is, any other source id is mapped to a UUID5, so reprocessing the same
    record always yields the same id and database writes stay idempotent.

    Attributes
    ----------
    id : str
        Transaction identifier (UUID, derived from the source id).
    description : str
        Transaction description text.
    amount : float
        Transaction amount.
    timestamp : str
        ISO format timestamp of transaction.
    merchant : str | None
        Merchant name (optional).
    operation_type : str
        Type of operation (e.g., 'debit', 'credit').
    side : str
        Transaction side indicator.
    processing_type : str
        Type of processing ('batch' or 'streaming').
    run_id : str
        Unique identifier for the processing run.
    """

    model_config = ConfigDict(extra="ignore")

    id: str  # Will be replaced with UUID
    description: str
    amount: float
    timestamp: str  # Will be converted to datetime
    merchant: str | None
    operation_type: str
    side: str

    # Lineage tracking fields
    processing_type: str
    run_id: str

    @field_validator("id", mode="before")
    @classmethod
    def normalize_id(cls, value: object) -> str:
        """
        Map the incoming id to a deterministic UUID.

        Parameters
        ----------
        value : object
            Incoming id: a UUID (kept) or any other source identifier.

        Returns
        -------
        str
            The UUID as a string.

        Raises
        ------
        ValueError
            If the id is missing or blank.

        Notes
        -----
        The ML API requires UUIDs, while sources such as the CSV use integer
        ids. Deriving a UUID5 (instead of drawing a random UUID) means a
        re-run or a Kafka redelivery produces the same id, so ``ON CONFLICT``
        deduplicates it.
        """
        if value is None or not str(value).strip():
            raise ValueError("Transaction id is required")
        source_id = str(value).strip()
        try:
            return str(UUID(source_id))
        except ValueError:
            return str(uuid5(TRANSACTION_ID_NAMESPACE, source_id))

    @field_validator("timestamp")
    @classmethod
    def parse_timestamp(cls, v: str) -> str:
        """
        Parse timestamp string to ISO format for database.

        Parameters
        ----------
        v : str
            Timestamp string in various formats.

        Returns
        -------
        str
            ISO format timestamp string.

        Raises
        ------
        ValueError
            If timestamp format is not recognized.

        Notes
        -----
        Supports formats: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DDTHH:MM:SS',
        and 'YYYY-MM-DDTHH:MM:SS.ffffff' (with microseconds).
        Converts to ISO format for consistent database storage.
        """
        if isinstance(v, str):
            # Handle various timestamp formats
            try:
                # Try parsing with microseconds first
                dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
                return dt.isoformat()
            except ValueError:
                try:
                    # Try space-separated format
                    dt = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                    return dt.isoformat()
                except ValueError:
                    try:
                        # Try basic ISO format
                        dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
                        return dt.isoformat()
                    except ValueError as exc:
                        raise ValueError(f"Invalid timestamp format: {v}") from exc
        return v
