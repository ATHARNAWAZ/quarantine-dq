import pytest
from quarantine.core.context import QuarantineContext, QuarantineException


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)


def test_context_manager_clean():
    results = []
    with QuarantineContext(
        pipeline="test",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    ) as ctx:
        result = ctx.process({"order_id": 1}, lambda r: r)
        results.append(result)

    assert results[0] is not None
    assert ctx.stats["processed"] == 1
    assert ctx.stats["quarantined"] == 0


def test_context_manager_quarantines_bad():
    with QuarantineContext(
        pipeline="test",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    ) as ctx:
        result = ctx.process({"order_id": None})

    assert result is None
    assert ctx.stats["quarantined"] == 1


def test_context_manager_raise_mode():
    with pytest.raises(QuarantineException):
        with QuarantineContext(
            pipeline="test",
            stage="validate",
            not_null=["order_id"],
            on_failure="raise",
            silent=True,
        ) as ctx:
            ctx.process({"order_id": None})


def test_context_manager_batch_stats():
    with QuarantineContext(
        pipeline="orders",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    ) as ctx:
        for i in range(5):
            ctx.process({"order_id": i})
        ctx.process({"order_id": None})

    assert ctx.stats["processed"] == 6  # all records counted
    assert ctx.stats["quarantined"] == 1
