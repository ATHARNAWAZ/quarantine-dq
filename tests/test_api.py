import pytest

httpx = pytest.importorskip("httpx")
fastapi = pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from quarantine.api.app import app, _get_store
from quarantine.core.context import QuarantineContext


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)

    import quarantine.api.app as api_module
    api_module._store = None
    yield
    api_module._store = None


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def populated_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)
    import quarantine.api.app as api_module
    api_module._store = None

    with QuarantineContext(
        pipeline="api_test",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    ) as ctx:
        ctx.process({"order_id": None})
        ctx.process({"order_id": 1})
    return url


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_list_records_empty(client):
    resp = client.get("/records")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_records(client, populated_store):
    resp = client.get("/records")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) == 1


def test_get_record(client, populated_store):
    resp = client.get("/records")
    record_id = resp.json()[0]["id"]
    resp2 = client.get(f"/records/{record_id}")
    assert resp2.status_code == 200
    assert resp2.json()["id"] == record_id


def test_get_record_not_found(client):
    resp = client.get("/records/nonexistent-id")
    assert resp.status_code == 404


def test_stats(client, populated_store):
    resp = client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1


def test_replay_one(client, populated_store):
    resp = client.get("/records")
    record_id = resp.json()[0]["id"]
    resp2 = client.post(f"/records/{record_id}/replay")
    assert resp2.status_code == 200
    assert resp2.json()["success"] is True


def test_delete_record(client, populated_store):
    resp = client.get("/records")
    record_id = resp.json()[0]["id"]
    resp2 = client.delete(f"/records/{record_id}")
    assert resp2.status_code == 200
    resp3 = client.get(f"/records/{record_id}")
    assert resp3.status_code == 404
