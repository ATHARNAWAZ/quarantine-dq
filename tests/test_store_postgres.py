import os
import pytest

POSTGRES_URL = os.getenv("QUARANTINE_POSTGRES_URL", "")
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="QUARANTINE_POSTGRES_URL not set — skipping Postgres tests",
)

from quarantine.core.models import FailureSeverity, FailureType, QuarantineRecord
from quarantine.stores.postgres import PostgresStore


@pytest.fixture
def store():
    s = PostgresStore(POSTGRES_URL)
    yield s
    # cleanup
    with s._engine.begin() as con:
        from sqlalchemy import text
        con.execute(text("DELETE FROM quarantine_records"))


def make_record(**kwargs):
    defaults = dict(
        pipeline_name="pg_test",
        stage_name="validate",
        run_id="run-pg-1",
        record_data={"id": 1},
        failure_type=FailureType.NULL_VIOLATION,
        failure_severity=FailureSeverity.CRITICAL,
        failure_reason="test failure",
    )
    defaults.update(kwargs)
    return QuarantineRecord(**defaults)


def test_save_and_get(store):
    rec = make_record()
    store.save(rec)
    fetched = store.get(rec.id)
    assert fetched.id == rec.id


def test_list(store):
    store.save(make_record())
    records = store.list(pipeline_name="pg_test")
    assert len(records) >= 1


def test_mark_replayed(store):
    rec = make_record()
    store.save(rec)
    store.mark_replayed(rec.id, "success")
    fetched = store.get(rec.id)
    assert fetched.replay_status == "success"


def test_stats(store):
    store.save(make_record())
    stats = store.stats(pipeline_name="pg_test")
    assert stats["total"] >= 1
