import pytest
from click.testing import CliRunner
from quarantine.cli.main import cli
from quarantine.core.context import QuarantineContext


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)
    return url


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def populated_store(tmp_path, monkeypatch):
    url = f"duckdb://{tmp_path}/test.duckdb"
    monkeypatch.setenv("QUARANTINE_STORE_URL", url)
    with QuarantineContext(
        pipeline="orders",
        stage="validate",
        not_null=["order_id"],
        silent=True,
    ) as ctx:
        ctx.process({"order_id": None, "revenue": 100.0})
        ctx.process({"order_id": 1, "revenue": 200.0})
    return url


def test_inspect_empty(runner):
    result = runner.invoke(cli, ["inspect"])
    assert result.exit_code == 0
    assert "No quarantined records" in result.output


def test_inspect_with_records(runner, populated_store):
    result = runner.invoke(cli, ["inspect"])
    assert result.exit_code == 0
    assert "orders" in result.output


def test_inspect_filter_pipeline(runner, populated_store):
    result = runner.invoke(cli, ["inspect", "--pipeline", "orders"])
    assert result.exit_code == 0


def test_stats_command(runner, populated_store):
    result = runner.invoke(cli, ["stats"])
    assert result.exit_code == 0
    assert "Total records" in result.output


def test_report_command(runner, populated_store, tmp_path):
    out_file = str(tmp_path / "report.md")
    result = runner.invoke(cli, ["report", "--output", out_file])
    assert result.exit_code == 0
    with open(out_file) as f:
        content = f.read()
    assert "Quarantine Report" in content


def test_replay_dry_run(runner, populated_store):
    result = runner.invoke(cli, ["replay", "--pipeline", "orders", "--dry-run"])
    assert result.exit_code == 0
    assert "DRY RUN" in result.output
