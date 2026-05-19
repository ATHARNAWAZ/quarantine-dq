import pytest
from quarantine.core.context import QuarantineContext
from quarantine.core.replay import ReplayEngine
from quarantine.stores.duckdb import DuckDBStore


@pytest.fixture
def store(tmp_path):
    return DuckDBStore(str(tmp_path / "test.duckdb"))


@pytest.fixture(autouse=True)
def tmp_store_url(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)


def test_replay_success(store):
    with QuarantineContext(
        pipeline="orders", stage="validate", not_null=["order_id"], silent=True,
        store_url=None,
    ) as ctx:
        ctx.process({"order_id": None})

    replayed_records = []

    def transform(record):
        replayed_records.append(record)

    engine = ReplayEngine(store=store)
    result = engine.replay("orders", transform_fn=transform)
    assert result.succeeded == 1
    assert result.failed == 0


def test_replay_dry_run(store):
    with QuarantineContext(
        pipeline="orders", stage="validate", not_null=["order_id"], silent=True,
    ) as ctx:
        ctx.process({"order_id": None})

    engine = ReplayEngine(store=store)
    result = engine.replay("orders", transform_fn=lambda r: r, dry_run=True)
    assert result.dry_run is True
    assert result.succeeded == 0


def test_replay_failure_marks_record(store):
    with QuarantineContext(
        pipeline="orders", stage="validate", not_null=["order_id"], silent=True,
    ) as ctx:
        ctx.process({"order_id": None})

    def failing_transform(record):
        raise ValueError("still broken")

    engine = ReplayEngine(store=store)
    result = engine.replay("orders", transform_fn=failing_transform)
    assert result.failed == 1
    assert len(result.errors) == 1


def test_replay_only_unreplayed(store):
    with QuarantineContext(
        pipeline="orders", stage="validate", not_null=["order_id"], silent=True,
    ) as ctx:
        ctx.process({"order_id": None})
        ctx.process({"order_id": None})

    records = store.list(pipeline_name="orders")
    store.mark_replayed(records[0].id, "success")

    engine = ReplayEngine(store=store)
    result = engine.replay("orders", transform_fn=lambda r: r)
    assert result.attempted == 1
