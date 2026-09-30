"""Utility functions for infrastructure operations.

This module provides decorator functions for retry logic with exponential
backoff for handling transient failures in API calls and database operations.
"""

import logging
import time
from collections.abc import Callable
from functools import wraps

import requests

logger = logging.getLogger(__name__)


def retry_with_backoff(max_retries: int = 3, initial_delay: float = 1.0):
    """
    Decorate a function with exponential backoff retry logic.

    Parameters
    ----------
    max_retries : int, optional
        Maximum number of retry attempts, by default 3.
    initial_delay : float, optional
        Initial delay in seconds for exponential backoff, by default 1.0.

    Returns
    -------
    Callable
        Decorated function that returns the wrapped function's result on
        success, or ``(transactions, None)`` once all attempts failed, where
        ``transactions`` is its first argument.

    Notes
    -----
    The delay doubles after each failed attempt (exponential backoff).
    Only ``requests`` errors are retried: this is meant for HTTP calls.
    Failed batches are logged and their transactions are returned for
    potential reprocessing.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> tuple[list[dict] | None, list[dict] | None]:
            delay = initial_delay
            transactions = args[0] if args else None
            size = len(transactions) if transactions is not None else 0

            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)

                    if attempt > 0:
                        logger.info(f"Batch of {size}: retry succeeded on attempt {attempt + 1}")

                    return result

                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Batch of {size}: attempt {attempt + 1}/{max_retries} failed - {e}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                    else:
                        logger.error(
                            f"Batch of {size}: all {max_retries} attempts failed - {e}. "
                            f"Moving transactions to failed queue."
                        )
                        return transactions, None

            return transactions, None

        return wrapper

    return decorator
