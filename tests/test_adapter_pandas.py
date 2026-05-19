import pytest

pd = pytest.importorskip("pandas", reason="pandas not installed")

from quarantine.adapters.pandas import quarantine_dataframe


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)


def sample_df():
    return pd.DataFrame([
        {"order_id": 1, "revenue": 100.0, "status": "confirmed"},
        {"order_id": None, "revenue": 200.0, "status": "pending"},
        {"order_id": 3, "revenue": -50.0, "status": "confirmed"},
        {"order_id": 4, "revenue": 300.0, "status": "confirmed"},
    ])


def test_clean_and_quarantined_split():
    df = sample_df()
    from quarantine.rules.business import PositiveRevenue
    clean, quarantined = quarantine_dataframe(
        df,
        pipeline="orders",
        stage="validate",
        not_null=["order_id"],
        rules=[PositiveRevenue()],
    )
    # order_id=None and revenue=-50 should be quarantined
    assert len(clean) == 2
    assert len(quarantined) == 2


def test_quarantined_has_metadata_columns():
    df = sample_df()
    clean, quarantined = quarantine_dataframe(
        df,
        pipeline="orders",
        stage="validate",
        not_null=["order_id"],
    )
    if len(quarantined) > 0:
        assert "__quarantine_id" in quarantined.columns
        assert "__failure_type" in quarantined.columns
        assert "__failure_reason" in quarantined.columns


def test_all_clean():
    df = pd.DataFrame([
        {"order_id": 1, "revenue": 100.0},
        {"order_id": 2, "revenue": 200.0},
    ])
    clean, quarantined = quarantine_dataframe(
        df, pipeline="test", stage="validate", not_null=["order_id"]
    )
    assert len(clean) == 2
    assert len(quarantined) == 0


def test_all_quarantined():
    df = pd.DataFrame([
        {"order_id": None},
        {"order_id": None},
    ])
    clean, quarantined = quarantine_dataframe(
        df, pipeline="test", stage="validate", not_null=["order_id"]
    )
    assert len(clean) == 0
    assert len(quarantined) == 2
