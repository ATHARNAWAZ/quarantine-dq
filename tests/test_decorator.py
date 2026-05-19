import tempfile
import pytest
from quarantine.core.context import QuarantineException, quarantine
from quarantine.core.models import FailureType


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)


def test_decorator_passes_clean_record():
    @quarantine(
        pipeline="test",
        stage="validate",
        schema={"order_id": int},
        not_null=["order_id"],
        silent=True,
    )
    def process(record):
        return {"processed": True, **record}

    result = process({"order_id": 1})
    assert result is not None
    assert result["processed"] is True


def test_decorator_returns_none_on_violation():
    @quarantine(
        pipeline="test",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    )
    def process(record):
        return record

    result = process({"order_id": None})
    assert result is None


def test_decorator_raise_mode():
    @quarantine(
        pipeline="test",
        stage="validate",
        not_null=["order_id"],
        on_failure="raise",
        silent=True,
    )
    def process(record):
        return record

    with pytest.raises(QuarantineException) as exc_info:
        process({"order_id": None})
    assert exc_info.value.record.pipeline_name == "test"


def test_decorator_quarantine_context_accessible():
    @quarantine(
        pipeline="orders",
        stage="ingest",
        not_null=["id"],
        silent=True,
    )
    def process(record):
        return record

    process({"id": None})
    process({"id": 1})
    ctx = process._quarantine_ctx
    assert ctx._quarantined == 1
