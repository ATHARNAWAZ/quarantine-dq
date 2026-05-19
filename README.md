# quarantine-dq

> The third option. When pipelines fail, records shouldn't disappear.

[![Tests](https://github.com/ATHARNAWAZ/quarantine-dq/actions/workflows/quarantine_gate.yml/badge.svg)](https://github.com/ATHARNAWAZ/quarantine-dq/actions)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## The problem

Every data pipeline has two default failure modes: **crash** or **silently drop**. Neither is acceptable in production. Crash mode blocks the entire pipeline over a single bad record — a null in one row stops ten thousand clean rows from landing. Silent drop mode is worse: bad records vanish without a trace, causing downstream inconsistencies that take days to diagnose.

**quarantine-dq** is the third option. When a record violates a rule — null constraint, schema mismatch, business logic failure, volume anomaly — it gets intercepted, stored with full failure context, and made replayable after the root cause is fixed. The pipeline continues. Nothing disappears. Everything is auditable.

---

## How it works

```
                    ┌─────────────────────────┐
                    │       Your Pipeline      │
                    └──────────┬──────────────┘
                               │ records
                    ┌──────────▼──────────────┐
                    │     RulesEngine          │
                    │  • schema validation     │
                    │  • null constraints      │
                    │  • uniqueness checks     │
                    │  • range / pattern       │
                    │  • custom BusinessRules  │
                    └──────┬───────────┬───────┘
                           │ pass      │ fail
               ┌───────────▼──┐   ┌───▼──────────────┐
               │  Pipeline    │   │  QuarantineStore  │
               │  continues   │   │  (DuckDB/Postgres)│
               └──────────────┘   │  • record_data    │
                                  │  • failure_reason │
                                  │  • failure_detail │
                                  └───────┬───────────┘
                                          │ later
                                ┌─────────▼──────────┐
                                │   ReplayEngine      │
                                │  replay after fix   │
                                └─────────────────────┘
```

---

## Install

```bash
pip install quarantine-dq
```

With optional extras:
```bash
pip install "quarantine-dq[postgres]"   # PostgreSQL backend
pip install "quarantine-dq[llm]"        # LLM-enhanced failure classification
pip install "quarantine-dq[api]"        # FastAPI REST dashboard
pip install "quarantine-dq[all]"        # everything
```

---

## 30-second quickstart

```python
from quarantine import quarantine, QuarantineContext
from quarantine.rules.business import PositiveRevenue, AllowedValues

# Option 1 — decorator style
@quarantine(
    pipeline="orders_etl",
    stage="validate",
    schema={"order_id": int, "revenue": float},
    not_null=["order_id", "customer_id"],
    pk_fields=["order_id"],
    rules=[PositiveRevenue()],
    on_failure="quarantine",   # pipeline continues; bad records saved
)
def process_order(record: dict) -> dict:
    return {**record, "processed": True}

# Records with violations return None — pipeline keeps going
result = process_order({"order_id": None, "revenue": 100.0})  # → None (quarantined)
result = process_order({"order_id": 1,    "revenue": 100.0})  # → {"order_id": 1, "processed": True}

# Option 2 — pandas batch processing
from quarantine.adapters.pandas import quarantine_dataframe
import pandas as pd

df = pd.DataFrame([...])
clean_df, quarantined_df = quarantine_dataframe(
    df,
    pipeline="orders_etl",
    stage="validate",
    not_null=["order_id"],
    rules=[PositiveRevenue()],
)
# quarantined_df has extra columns: __quarantine_id, __failure_type, __failure_reason
```

---

## Core concepts

**QuarantineRecord** — the atom of the system. Every intercepted record is stored as a `QuarantineRecord` with: the original `record_data`, `failure_type`, `failure_severity` (CRITICAL / WARNING / INFO), a human-readable `failure_reason`, structured `failure_detail` (expected vs actual), and full pipeline lineage (`pipeline_name`, `stage_name`, `run_id`, `batch_id`).

**FailureType** — an enum covering the full taxonomy of data quality failures: `SCHEMA_VIOLATION`, `NULL_VIOLATION`, `UNIQUENESS_VIOLATION`, `RANGE_VIOLATION`, `PATTERN_VIOLATION`, `VOLUME_ANOMALY`, `BUSINESS_RULE`, `UNKNOWN`. Every quarantined record is tagged with one.

**RulesEngine** — pure Python, zero external APIs. Runs all configured rules against a record and returns a `List[RuleViolation]`. Supports schema checking, null constraints, uniqueness within a batch, range bounds, regex patterns, and custom `BusinessRule` subclasses. Deterministic, testable, composable.

**ReplayEngine** — fetches unresolved quarantined records matching a filter (pipeline, failure type, date), passes them through a transform function, and marks each as `success` or `failed`. Returns a `ReplayResult` with full counts. Supports `dry_run` mode.

---

## Adapters

| Framework | Install | Import | Example |
| --- | --- | --- | --- |
| **pandas** | included | `from quarantine.adapters.pandas import quarantine_dataframe` | [examples/pandas_orders_pipeline.py](examples/pandas_orders_pipeline.py) |
| **PySpark** | `uv pip install pyspark` | `from quarantine.adapters.spark import quarantine_rdd` | [examples/spark_events_pipeline.py](examples/spark_events_pipeline.py) |
| **Airflow** | `uv pip install apache-airflow` | `from quarantine.adapters.airflow import QuarantineOperator` | [examples/airflow_dag_example.py](examples/airflow_dag_example.py) |
| **dbt** | n/a (SQL macro) | `{% from 'quarantine_test.sql' ... %}` | [quarantine/adapters/dbt_macro/](quarantine/adapters/dbt_macro/) |

---

## CLI reference

```bash
# Inspect quarantined records
quarantine inspect
quarantine inspect --pipeline orders_etl
quarantine inspect --failure-type null_violation --since 2026-05-01 --limit 20

# Replay after fixes
quarantine replay --pipeline orders_etl
quarantine replay --pipeline orders_etl --dry-run          # preview first
quarantine replay --pipeline orders_etl --failure-type schema_violation

# Dashboard
quarantine stats
quarantine stats --pipeline orders_etl

# Generate markdown report
quarantine report --pipeline orders_etl --output report.md

# Clean up replayed records
quarantine clear --pipeline orders_etl --replayed-only
```

Example `quarantine stats` output:
```
╭─────────────────────────────────────────╮
│  Quarantine Stats — orders_etl          │
│  Total records: 47   Unresolved: 12     │
╰─────────────────────────────────────────╯

 By Failure Type
 ┌─────────────────────┬───────┐
 │ null_violation      │    18 │
 │ schema_violation    │    15 │
 │ business_rule       │    10 │
 │ range_violation     │     4 │
 └─────────────────────┴───────┘
```

---

## Storage backends

| Backend | URL format | When to use |
| --- | --- | --- |
| **DuckDB** (default) | `duckdb://~/.quarantine/quarantine.duckdb` | Local dev, CI, single-machine pipelines. Zero config. |
| **PostgreSQL** | `postgresql://user:pw@host:5432/db` | Production, multi-process, shared access |

Configure via environment variable:
```bash
export QUARANTINE_STORE_URL="postgresql://user:pw@localhost/quarantine"
```

---

## REST API

Start the dashboard:
```bash
uv run uvicorn quarantine.api.app:app --reload
```

Endpoints:
- `GET /records` — list with filters (`pipeline`, `failure_type`, `since`)
- `GET /records/{id}` — full record detail
- `POST /records/{id}/replay` — replay one record
- `POST /replay` — bulk replay with filters
- `GET /stats` — failure counts by pipeline / type
- `DELETE /records/{id}` — remove a record

---

## Custom business rules

```python
from quarantine import BusinessRule

class OrderValueCap(BusinessRule):
    def validate(self, record) -> str | None:
        if record.get("revenue", 0) > 50_000:
            return f"Order revenue {record['revenue']} exceeds maximum allowed $50,000"
        return None

# Use anywhere rules are accepted
with QuarantineContext(pipeline="orders", stage="validate", rules=[OrderValueCap()]) as ctx:
    for record in records:
        ctx.process(record, my_transform)
```

---

## CI gate

Add to GitHub Actions to fail a build when critical unresolved records exist:

```yaml
- name: Check quarantine store
  run: uv run quarantine stats --fail-on-critical
```

Full workflow: [.github/workflows/quarantine_gate.yml](.github/workflows/quarantine_gate.yml)

---

## Optional: LLM-enhanced failure classification

With `pip install "quarantine-dq[llm]"`, the rules engine can optionally route `UNKNOWN` failures through Claude for richer remediation suggestions. LLM usage is always opt-in — the base package is 100% offline.

---

## Contributing

```bash
git clone https://github.com/ATHARNAWAZ/quarantine-dq
cd quarantine-dq
uv venv && uv pip install -e ".[all,dev]"
uv run pytest
```

Issues and PRs welcome.

---

## License

MIT — see [LICENSE](LICENSE).
