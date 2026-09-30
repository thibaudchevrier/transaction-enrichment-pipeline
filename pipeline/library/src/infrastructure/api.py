"""ML API client module.

This module provides functions for sending transaction batches to the ML API
for transaction classification with automatic retry logic and exponential backoff.
"""

import logging
import os

import requests

from .utils import retry_with_backoff

logger = logging.getLogger(__name__)


@retry_with_backoff(max_retries=int(os.getenv("MAX_RETRIES", "3")), initial_delay=1.0)
def predict_batch(transactions: list[dict], ml_api_url: str) -> tuple[list[dict], list[dict]]:
    """
    Send a batch of transactions to the ML API for prediction.

    Parameters
    ----------
    transactions : list[dict]
        List of transaction dictionaries to be predicted.
    ml_api_url : str
        ML API URL (e.g., 'http://ml-api:8000').

    Returns
    -------
    tuple[list[dict], list[dict]]
        Tuple containing (predictions, transactions).

    Raises
    ------
    requests.HTTPError
        If the API request fails or returns an error status.
    """
    response = requests.post(
        f"{ml_api_url}/predict", json=transactions, headers={"Content-Type": "application/json"}, timeout=30
    )
    response.raise_for_status()
    predictions = response.json()
    logger.debug(f"Received {len(predictions)} predictions")
    return transactions, predictions
