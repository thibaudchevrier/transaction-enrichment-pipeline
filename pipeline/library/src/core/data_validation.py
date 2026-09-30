"""Transaction validation module.

This module provides Pydantic-based validation for transaction records,
ensuring data quality and automatically generating UUIDs for valid
transactions.
"""

import logging

from pydantic import ValidationError as PydanticValidationError

from .model import Transaction

logger = logging.getLogger(__name__)


def validate_transaction_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Validate a list of transaction records using Pydantic.

    Parameters
    ----------
    records : list[dict]
        List of transaction records as dictionaries to validate.

    Returns
    -------
    tuple[list[dict], list[dict]]
        Tuple containing:
        - List of validated transaction dictionaries (ids normalized to deterministic UUIDs).
        - List of invalid transaction error dictionaries.

    Notes
    -----
    The Transaction model maps source ids to deterministic UUIDs.
    Validation errors are logged for debugging purposes.
    """
    validated_transactions = []
    invalid_transactions = []

    logger.info("Validating transactions...")

    for record in records:
        try:
            validated_transactions.append(Transaction(**record).model_dump())

        except PydanticValidationError as e:
            # Collect validation errors
            invalid_transactions.append(record)
            logger.error(f"Validation failed for record: {e}")

    logger.info(f"Validation complete: {len(validated_transactions)} valid, {len(invalid_transactions)} invalid")

    if invalid_transactions:
        logger.warning(f"Found {len(invalid_transactions)} invalid transactions")
        # Log a sample of invalid records
        for record in invalid_transactions[:5]:
            record_id = record.get("id", "unknown")
            logger.warning(f"  - Record ID: {record_id}")

    return validated_transactions, invalid_transactions
