"""
Tests for loading transactions from partitioned CSV files.

Uses the local filesystem through fsspec, the same code path as S3/MinIO.
"""

from pathlib import Path

import pytest

from infrastructure.generator import load_and_validate_transactions

HEADER = "id;description;amount;timestamp;merchant;operation_type;side\n"


def _write_partition(root: Path, month: str, rows: list[str]) -> None:
    path = root / f"month={month}" / "transactions.csv"
    path.parent.mkdir(parents=True)
    path.write_text(HEADER + "".join(rows), encoding="utf-8")


@pytest.fixture
def partitions(tmp_path: Path) -> Path:
    _write_partition(
        tmp_path, "2023-01", ['1;"Card expense at EDF";-952,40;"2023-01-28 06:35:12";"EDF";"refund";"debit"\n']
    )
    _write_partition(
        tmp_path,
        "2023-02",
        [
            '2;"Purchase at Amazon.fr";-749,56;"2023-02-16 01:27:52";"Amazon.fr";"transfer";"debit"\n',
            '3;"Salary";2500,00;"2023-02-28 09:00:00";"ACME";"transfer";"credit"\n',
        ],
    )
    return tmp_path


def _load(path: str, batch_size: int = 100) -> list[tuple[list[dict], list[dict]]]:
    return list(
        load_and_validate_transactions(
            s3_path=path, storage_options={}, run_id="test-run", processing_type="batch", batch_size=batch_size
        )
    )


def test_loads_single_partition(partitions: Path):
    """A path to one partition loads only that month."""
    batches = _load(str(partitions / "month=2023-01" / "transactions.csv"))

    valid = [t for v, _ in batches for t in v]
    assert len(valid) == 1
    assert valid[0]["amount"] == -952.40
    assert valid[0]["run_id"] == "test-run"


def test_glob_loads_all_partitions(partitions: Path):
    """A glob loads every matching partition, in batches."""
    batches = _load(str(partitions / "month=*" / "transactions.csv"), batch_size=2)

    assert [len(v) for v, _ in batches] == [2, 1]


def test_ids_are_stable_across_runs(partitions: Path):
    """Reloading the same file yields the same ids, so re-runs are idempotent."""
    path = str(partitions / "month=2023-02" / "transactions.csv")

    first = [t["id"] for v, _ in _load(path) for t in v]
    second = [t["id"] for v, _ in _load(path) for t in v]

    assert first == second


def test_missing_partition_raises(tmp_path: Path):
    """A glob matching nothing is an error, not an empty run."""
    with pytest.raises(FileNotFoundError):
        _load(str(tmp_path / "month=*" / "transactions.csv"))
