"""Database operations module for PostgreSQL.

This module provides SQLAlchemy models for transactions and predictions,
session management utilities, and bulk insert/upsert operations with
retry logic for resilient database interactions.
"""

import logging
from contextlib import contextmanager
from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()


class Transaction(Base):
    """Transaction table model."""

    __tablename__ = "transactions"

    id = Column(String, primary_key=True)  # UUID from Pydantic
    description = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    merchant = Column(String, nullable=False)
    operation_type = Column(String, nullable=False)
    side = Column(String, nullable=False)

    # Lineage tracking columns
    processing_type = Column(String, nullable=False)  # 'batch' or 'streaming'
    run_id = Column(String, nullable=False)  # Unique identifier for processing run

    # Relationship to predictions
    predictions = relationship("Prediction", back_populates="transaction")


class Prediction(Base):
    """Prediction table model."""

    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, ForeignKey("transactions.id"), unique=True, nullable=False)
    category = Column(String, nullable=False)
    confidence_score = Column(Float, default=1.0)
    model_version = Column(String, default="v1.0")
    predicted_at = Column(DateTime, default=lambda: datetime.now(UTC).replace(tzinfo=None))  # naive UTC column

    # Relationship to transaction
    transaction = relationship("Transaction", back_populates="predictions")


@contextmanager
def get_db_session(database_url: str):
    """
    Context manager for database session.

    Parameters
    ----------
    database_url : str
        PostgreSQL connection URL
        (e.g., 'postgresql://user:pass@host:port/dbname').

    Yields
    ------
    Session
        SQLAlchemy session object for database operations.

    Notes
    -----
    Automatically commits on success, rolls back on error,
    and closes the session in all cases.

    Examples
    --------
    >>> with get_db_session(url) as session:
    ...     bulk_insert_transactions(session, transactions)
    """
    engine = create_engine(database_url, pool_pre_ping=True, echo=False)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        logger.info("Database connection established")
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        session.close()
        logger.info("Database connection closed")


@contextmanager
def db_transaction(session: Session):
    """
    Context manager for database transaction with automatic commit/rollback.

    This context manager provides transaction boundaries for an existing
    database session, allowing fine-grained control over when transactions
    are committed in long-running processes.

    Parameters
    ----------
    session : Session
        Existing SQLAlchemy session object.

    Yields
    ------
    Session
        The same session object for use within the transaction.

    Notes
    -----
    Automatically commits on success and rolls back on error.
    Does not close the session - that remains the responsibility
    of the session owner (e.g., get_db_session context manager).

    Examples
    --------
    >>> with get_db_session(url) as session:
    ...     while True:  # Long-running process
    ...         with db_transaction(session):
    ...             bulk_insert_transactions(session, batch)
    ...         # Transaction committed, session still open
    """
    try:
        yield session
        session.commit()
        logger.debug("Transaction committed")
    except Exception as exc:
        session.rollback()
        logger.error(f"Transaction rolled back due to error: {exc}")
        raise


def __bulk_insert_transactions(session: Session, transactions: list[dict]):
    """
    Insert transactions with ON CONFLICT DO NOTHING (idempotent).

    Parameters
    ----------
    session : Session
        SQLAlchemy session object.
    transactions : list[dict]
        List of transaction dictionaries with keys matching
        Transaction model columns.

    Notes
    -----
    Re-running with the same transaction IDs won't create
    duplicates due to ON CONFLICT DO NOTHING clause.
    """
    if not transactions:
        return

    stmt = insert(Transaction).values(transactions)
    stmt = stmt.on_conflict_do_nothing(index_elements=["id"])

    session.execute(stmt)
    logger.info(f"Inserted {len(transactions)} transactions (skipped duplicates)")


def __bulk_upsert_predictions(session: Session, predictions: list[dict]):
    """
    UPSERT predictions: insert new ones, update existing ones.

    Parameters
    ----------
    session : Session
        SQLAlchemy session object.
    predictions : list[dict]
        List of prediction dictionaries with keys matching
        Prediction model columns.

    Notes
    -----
    If transaction_id exists, updates with latest prediction.
    Otherwise, inserts as new prediction.
    """
    if not predictions:
        return

    # Prepare data - ensure transaction_id is a string (UUID)
    for pred in predictions:
        if "transaction_id" in pred and not isinstance(pred["transaction_id"], str):
            pred["transaction_id"] = str(pred["transaction_id"])

    stmt = insert(Prediction).values(predictions)
    stmt = stmt.on_conflict_do_update(
        index_elements=["transaction_id"],
        set_={
            "category": stmt.excluded.category,
            "confidence_score": stmt.excluded.confidence_score,
            "model_version": stmt.excluded.model_version,
            "predicted_at": stmt.excluded.predicted_at,
        },
    )

    session.execute(stmt)
    logger.info(f"Upserted {len(predictions)} predictions")


def db_write_results(session: Session, all_valid_transactions: list[dict], all_predictions: list[dict]):
    """
    Write valid transactions and predictions to the database.

    Parameters
    ----------
    session : Session
        SQLAlchemy session object.
    all_valid_transactions : list[dict]
        List of validated transaction dictionaries to persist.
    all_predictions : list[dict]
        List of prediction dictionaries to persist.

    Notes
    -----
    Clears the input lists after successful persistence.
    Transactions are inserted idempotently (no duplicates).
    Predictions are upserted (insert or update).

    Database errors are not retried here: a statement that fails leaves the
    Postgres transaction aborted, so retrying inside it can't succeed. Errors
    propagate, the transaction rolls back, and the run is retried as a whole
    (Airflow task retries for batch, redelivery of uncommitted offsets for
    streaming). Idempotent writes make those retries safe.
    """
    if all_valid_transactions:
        # Insert transactions to database (idempotent)
        __bulk_insert_transactions(session, all_valid_transactions)
        logger.info(f"Persisted {len(all_valid_transactions)} transactions to database")
        all_valid_transactions.clear()

    if all_predictions:
        # Persist predictions to database (upsert)
        __bulk_upsert_predictions(session, all_predictions)
        logger.info(f"Persisted {len(all_predictions)} predictions to database")
        all_predictions.clear()
