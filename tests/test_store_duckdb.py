import os
import tempfile
from datetime import datetime

import pytest

from quarantine.core.models import FailureSeverity, FailureType, QuarantineRecord
from quarantine.stores.duckdb import DuckDBStore


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    return DuckDBStore(db_path)


def make_record(pipeline="orders", **kwargs):
    defaults = dict(
        pipeline_name=pipeline,
        stage_name="validate",
        run_id="run-1",
        record_data={"order_id": 1},
        failure_type=FailureType.NULL_VIOLATION,
        failure_severity=FailureSeverity.CRITICAL,
        failure_reason="order_id is null",
    )
    defaults.update(kwargs)
    return QuarantineRecord(**defaults)


def test_save_and_get(store):
    rec = make_record()
    store.save(rec)
    fetched = store.get(rec.id)
    assert fetched.id == rec.id
    assert fetched.pipeline_name == "orders"


def test_get_missing_raises(store):
    with pytest.raises(KeyError):
        store.get("nonexistent-id")


def test_list_all(store):
    store.save(make_record())
    store.save(make_record())
    records = store.list(limit=100)
    assert len(records) == 2


def test_list_filter_by_pipeline(store):
    store.save(make_record(pipeline="orders"))
    store.save(make_record(pipeline="events"))
    records = store.list(pipeline_name="orders")
    assert all(r.pipeline_name == "orders" for r in records)
    assert len(records) == 1


def test_list_filter_by_failure_type(store):
    store.save(make_record(failure_type=FailureType.NULL_VIOLATION))
    store.save(make_record(failure_type=FailureType.SCHEMA_VIOLATION))
    records = store.list(failure_type="null_violation")
    assert len(records) == 1


def test_mark_replayed(store):
    rec = make_record()
    store.save(rec)
    store.mark_replayed(rec.id, "success")
    fetched = store.get(rec.id)
    assert fetched.replay_status == "success"
    assert fetched.replayed_at is not None


def test_stats(store):
    store.save(make_record())
    store.save(make_record(failure_type=FailureType.SCHEMA_VIOLATION))
    stats = store.stats()
    assert stats["total"] == 2
    assert stats["unresolved"] == 2
    assert "null_violation" in stats["by_failure_type"]


def test_delete(store):
    rec = make_record()
    store.save(rec)
    store.delete(rec.id)
    with pytest.raises(KeyError):
        store.get(rec.id)


def test_count(store):
    store.save(make_record())
    store.save(make_record())
    assert store.count() == 2


def test_delete_where_replayed_only(store):
    rec1 = make_record()
    rec2 = make_record()
    store.save(rec1)
    store.save(rec2)
    store.mark_replayed(rec1.id, "success")
    deleted = store.delete_where(replayed_only=True)
    assert deleted == 1
    assert store.count() == 1
