"""
Tests for the Transaction Pydantic model.

Covers validation, deterministic ids, timestamp parsing, and lineage fields.
"""

import re
import uuid

import pytest
from pydantic import ValidationError

from core.model import TRANSACTION_ID_NAMESPACE, Transaction


class TestTransactionModel:
    """Test suite for Transaction model validation and field handling."""

    @pytest.mark.parametrize(
        "transaction_data,expected_fields",
        [
            # Valid transaction with all required fields
            (
                {
                    "id": "123",  # Will be replaced with UUID
                    "description": "Payment for services",
                    "amount": 100.50,
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Acme Corp",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "airflow-run-123",
                },
                {
                    "description": "Payment for services",
                    "amount": 100.50,
                    "merchant": "Acme Corp",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "airflow-run-123",
                },
            ),
            # Merchant can be None
            (
                {
                    "id": "456",
                    "description": "Online purchase",
                    "amount": 25.99,
                    "timestamp": "2026-01-11T11:30:00",
                    "merchant": None,
                    "operation_type": "credit",
                    "side": "merchant",
                    "processing_type": "streaming",
                    "run_id": "kafka-offset-456",
                },
                {
                    "description": "Online purchase",
                    "amount": 25.99,
                    "merchant": None,
                    "operation_type": "credit",
                    "side": "merchant",
                    "processing_type": "streaming",
                    "run_id": "kafka-offset-456",
                },
            ),
            # Different timestamp formats
            (
                {
                    "id": "789",
                    "description": "ATM withdrawal",
                    "amount": 200.00,
                    "timestamp": "2026-01-11 15:45:30",
                    "merchant": "ATM Network",
                    "operation_type": "withdrawal",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "batch-20260111",
                },
                {
                    "description": "ATM withdrawal",
                    "amount": 200.00,
                    "processing_type": "batch",
                    "run_id": "batch-20260111",
                },
            ),
        ],
    )
    def test_valid_transaction_creation(self, transaction_data, expected_fields):
        """Test creation of valid transactions with various field combinations."""
        transaction = Transaction(**transaction_data)

        # Check the source id was mapped to a UUID
        assert transaction.id != transaction_data["id"]
        assert re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", transaction.id)

        # Check expected fields match
        for field, value in expected_fields.items():
            assert getattr(transaction, field) == value

    @staticmethod
    def _data(transaction_id: object) -> dict:
        return {
            "id": transaction_id,
            "description": "Test",
            "amount": 10.0,
            "timestamp": "2026-01-11T10:00:00",
            "merchant": "Test Merchant",
            "operation_type": "debit",
            "side": "customer",
            "processing_type": "batch",
            "run_id": "test-run",
        }

    def test_id_is_deterministic(self):
        """Test that the same source id always maps to the same UUID (idempotent re-runs)."""
        assert Transaction(**self._data("42")).id == Transaction(**self._data("42")).id
        assert Transaction(**self._data(42)).id == Transaction(**self._data("42")).id

    def test_id_is_uuid5_of_source_id(self):
        """Test that a non-UUID source id maps to its UUID5 in the transaction namespace."""
        assert Transaction(**self._data("42")).id == str(uuid.uuid5(TRANSACTION_ID_NAMESPACE, "42"))

    def test_different_source_ids_get_different_uuids(self):
        """Test that distinct source ids do not collide."""
        assert Transaction(**self._data("1")).id != Transaction(**self._data("2")).id

    def test_uuid_id_is_kept(self):
        """Test that an id that is already a UUID is kept (e.g. re-validated by the consumer)."""
        source = "b9fa6684-502b-4695-8f92-247432ba610d"
        assert Transaction(**self._data(source)).id == source

    @pytest.mark.parametrize("transaction_id", [None, "", "   "])
    def test_blank_id_is_rejected(self, transaction_id):
        """Test that a missing id is a validation error, not a fresh UUID."""
        with pytest.raises(ValidationError):
            Transaction(**self._data(transaction_id))

    @pytest.mark.parametrize(
        "timestamp_str,expected_format",
        [
            ("2026-01-11T10:00:00", "2026-01-11T10:00:00"),
            ("2026-01-11 10:00:00", "2026-01-11T10:00:00"),
            ("2026-01-11T10:00:00.123456", "2026-01-11T10:00:00.123456"),
        ],
    )
    def test_timestamp_parsing(self, timestamp_str, expected_format):
        """Test timestamp parsing and normalization."""
        data = {
            "id": "123",
            "description": "Test",
            "amount": 10.0,
            "timestamp": timestamp_str,
            "merchant": "Test",
            "operation_type": "debit",
            "side": "customer",
            "processing_type": "batch",
            "run_id": "test-run",
        }

        transaction = Transaction(**data)
        assert transaction.timestamp == expected_format

    @pytest.mark.parametrize(
        "invalid_data,expected_error_field",
        [
            # Missing required field: description
            (
                {
                    "id": "123",
                    "amount": 100.0,
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Test",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "test-run",
                },
                "description",
            ),
            # Missing required field: amount
            (
                {
                    "id": "123",
                    "description": "Test",
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Test",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "test-run",
                },
                "amount",
            ),
            # Invalid amount type
            (
                {
                    "id": "123",
                    "description": "Test",
                    "amount": "not-a-number",
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Test",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                    "run_id": "test-run",
                },
                "amount",
            ),
            # Missing required field: processing_type
            (
                {
                    "id": "123",
                    "description": "Test",
                    "amount": 100.0,
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Test",
                    "operation_type": "debit",
                    "side": "customer",
                    "run_id": "test-run",
                },
                "processing_type",
            ),
            # Missing required field: run_id
            (
                {
                    "id": "123",
                    "description": "Test",
                    "amount": 100.0,
                    "timestamp": "2026-01-11T10:00:00",
                    "merchant": "Test",
                    "operation_type": "debit",
                    "side": "customer",
                    "processing_type": "batch",
                },
                "run_id",
            ),
        ],
    )
    def test_invalid_transaction_raises_validation_error(self, invalid_data, expected_error_field):
        """Test that invalid transactions raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            Transaction(**invalid_data)

        # Check that the expected field is in the error
        errors = exc_info.value.errors()
        error_fields = [error["loc"][0] for error in errors]
        assert expected_error_field in error_fields

    def test_model_dump_includes_all_fields(self):
        """Test that model_dump includes all fields including lineage."""
        data = {
            "id": "123",
            "description": "Test Transaction",
            "amount": 50.0,
            "timestamp": "2026-01-11T10:00:00",
            "merchant": "Test Merchant",
            "operation_type": "debit",
            "side": "customer",
            "processing_type": "batch",
            "run_id": "airflow-run-xyz",
        }

        transaction = Transaction(**data)
        dumped = transaction.model_dump()

        assert "id" in dumped
        assert "description" in dumped
        assert "amount" in dumped
        assert "timestamp" in dumped
        assert "merchant" in dumped
        assert "operation_type" in dumped
        assert "side" in dumped
        assert "processing_type" in dumped
        assert "run_id" in dumped

        # Verify lineage fields have correct values
        assert dumped["processing_type"] == "batch"
        assert dumped["run_id"] == "airflow-run-xyz"
