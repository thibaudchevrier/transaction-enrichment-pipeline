"""
Tests for the consumer's delivery guarantees.

``process_window`` must commit Kafka offsets only after the batch is in the
database and its failures are in the DLQ. Fakes record the order of those
steps in a shared event log.
"""

import json
from collections.abc import Iterator

import pytest
from confluent_kafka import KafkaError, KafkaException

from main import commit_offsets, process_window


class FakeService:
    """Service yielding one window, with ids in ``fail_ids`` failing prediction."""

    def __init__(self, events: list[str], valid: list[dict], invalid: list[dict], fail_ids=(), db_error=None):
        self.events = events
        self.valid = valid
        self.invalid = invalid
        self.fail_ids = set(fail_ids)
        self.db_error = db_error

    def read(self, batch_size: int) -> Iterator[tuple[list[dict], list[dict]]]:
        yield self.valid, self.invalid

    def predict(self, transactions: list[dict]) -> tuple[list[dict], list[dict] | None]:
        if any(t["id"] in self.fail_ids for t in transactions):
            return transactions, None  # what retry_with_backoff returns once retries are exhausted
        return transactions, [{"transaction_id": t["id"], "category": "Food"} for t in transactions]

    def bulk_write(self, transactions: list[dict], predictions: list[dict]) -> None:
        if self.db_error and transactions:
            raise self.db_error
        self.events.append("db_write")
        transactions.clear()
        predictions.clear()


class FakeSession:
    def __init__(self, events: list[str]):
        self.events = events

    def commit(self) -> None:
        self.events.append("db_commit")

    def rollback(self) -> None:
        self.events.append("db_rollback")


class FakeProducer:
    def __init__(self, events: list[str], undelivered: int = 0):
        self.events = events
        self.undelivered = undelivered
        self.messages: list[tuple[str, dict, dict]] = []

    def produce(self, topic: str, value: bytes, headers: list[tuple[str, bytes]]) -> None:
        self.messages.append((topic, json.loads(value), {k: v.decode() for k, v in headers}))

    def flush(self, timeout: float) -> int:
        self.events.append("dlq_flush")
        return self.undelivered


class FakeConsumer:
    def __init__(self, events: list[str], error: KafkaException | None = None):
        self.events = events
        self.error = error

    def commit(self, asynchronous: bool) -> None:
        assert asynchronous is False
        if self.error:
            raise self.error
        self.events.append("offset_commit")


def _run(events, service, producer, consumer=None):
    return process_window(
        service=service,
        session=FakeSession(events),
        consumer=consumer or FakeConsumer(events),
        dlq_producer=producer,
        dlq_topic="dlq",
        message_batch_size=10,
        api_batch_size=1,
        api_max_workers=1,
        db_row_batch_size=100,
    )


def test_offsets_committed_after_db_and_dlq():
    """Happy path: DB commit, then DLQ flush, then offset commit."""
    events: list[str] = []
    service = FakeService(events, valid=[{"id": "a"}, {"id": "b"}], invalid=[{"raw": "{", "error": "bad json"}])
    producer = FakeProducer(events)

    processed, failed, invalid = _run(events, service, producer)

    assert (processed, len(failed), len(invalid)) == (2, 0, 1)
    assert events[-3:] == ["db_commit", "dlq_flush", "offset_commit"]
    assert producer.messages == [("dlq", {"raw": "{", "error": "bad json"}, producer.messages[0][2])]
    assert producer.messages[0][2]["error_type"] == "invalid"


def test_failed_predictions_go_to_dlq():
    """Transactions whose prediction failed after retries are sent to the DLQ, not dropped."""
    events: list[str] = []
    service = FakeService(events, valid=[{"id": "a"}, {"id": "b"}], invalid=[], fail_ids={"b"})
    producer = FakeProducer(events)

    processed, failed, _ = _run(events, service, producer)

    assert processed == 1
    assert [m[1]["id"] for m in producer.messages] == ["b"]
    assert producer.messages[0][2]["error_type"] == "prediction_failed"
    assert events[-1] == "offset_commit"


def test_db_failure_commits_nothing():
    """A database error rolls back and leaves offsets uncommitted, so the window is redelivered."""
    events: list[str] = []
    service = FakeService(events, valid=[{"id": "a"}], invalid=[], db_error=RuntimeError("db down"))
    producer = FakeProducer(events)

    with pytest.raises(RuntimeError, match="db down"):
        _run(events, service, producer)

    assert "db_rollback" in events
    assert "dlq_flush" not in events
    assert "offset_commit" not in events


def test_undelivered_dlq_messages_block_offset_commit():
    """If the DLQ can't be flushed, offsets stay uncommitted (the DB write is idempotent on redelivery)."""
    events: list[str] = []
    service = FakeService(events, valid=[{"id": "a"}], invalid=[{"raw": "x", "error": "bad json"}])
    producer = FakeProducer(events, undelivered=1)

    with pytest.raises(RuntimeError, match="not delivered"):
        _run(events, service, producer)

    assert "db_commit" in events
    assert "offset_commit" not in events


def test_commit_offsets_ignores_no_offset():
    """A window with nothing new to commit is not an error."""
    events: list[str] = []
    no_offset = KafkaException(KafkaError(KafkaError._NO_OFFSET))  # pyright: ignore[reportAttributeAccessIssue]

    commit_offsets(FakeConsumer(events, error=no_offset))


def test_commit_offsets_raises_other_errors():
    """Any other commit error propagates, stopping the consumer before it reads more."""
    events: list[str] = []
    error = KafkaException(KafkaError(KafkaError._TRANSPORT))  # pyright: ignore[reportAttributeAccessIssue]

    with pytest.raises(KafkaException):
        commit_offsets(FakeConsumer(events, error=error))


class FakeMessage:
    def __init__(self, value: bytes | None):
        self._value = value

    def error(self):
        return None

    def value(self) -> bytes | None:
        return self._value


class ScriptedConsumer:
    """Consumer whose poll() returns the scripted messages, then None."""

    def __init__(self, messages: list[FakeMessage]):
        self.messages = list(messages)

    def poll(self, timeout: float) -> FakeMessage | None:
        return self.messages.pop(0) if self.messages else None


def _transaction(transaction_id: str) -> bytes:
    return json.dumps(
        {
            "id": transaction_id,
            "description": "Test",
            "amount": 10.0,
            "timestamp": "2026-01-11T10:00:00",
            "merchant": "Shop",
            "operation_type": "card_payment",
            "side": "debit",
            "processing_type": "streaming",
            "run_id": "test",
        }
    ).encode()


def test_window_closed_by_timeout_keeps_the_polled_message(monkeypatch):
    """The message polled when the buffer timeout expires belongs to the window, not dropped."""
    import main

    clock = iter(range(0, 1000, 3))  # every time.time() call advances 3 s
    monkeypatch.setattr(main.time, "time", lambda: next(clock))
    ids = [f"00000000-0000-0000-0000-00000000000{i}" for i in range(4)]
    consumer = ScriptedConsumer([FakeMessage(_transaction(i)) for i in ids])
    service = main.StreamingService(consumer=consumer, ml_api_url="", db_session=None, buffer_timeout=5.0)  # type: ignore[arg-type]

    seen: list[str] = []
    while consumer.messages:
        for valid, _ in service.read(batch_size=50):
            seen.extend(t["id"] for t in valid)

    assert seen == ids
