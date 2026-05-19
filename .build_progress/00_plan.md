# quarantine-dq Build Plan

## Component Checklist

- [x] 0. Repo + directory structure
- [ ] 1. pyproject.toml scaffold + uv setup
- [ ] 2. Core data models (quarantine/core/models.py)
- [ ] 3. Rules engine (schema, quality, business, engine)
- [ ] 4. Storage backends (DuckDB, Postgres, factory)
- [ ] 5. Quarantine decorator + context manager
- [ ] 6. Replay engine
- [ ] 7. Pandas adapter
- [ ] 8. PySpark adapter
- [ ] 9. dbt macro adapter
- [ ] 10. Airflow operator adapter
- [ ] 11. CLI (Click + Rich)
- [ ] 12. GitHub Actions gate
- [ ] 13. FastAPI REST API
- [ ] 14. Test suite + examples
- [ ] 15. README + docs
- [ ] 16. Reality check + release v0.1.0

## Architecture Overview

```
quarantine-dq/
├── quarantine/
│   ├── __init__.py              # exports: quarantine, QuarantineContext, ReplayEngine
│   ├── core/
│   │   ├── models.py            # QuarantineRecord, FailureType, FailureSeverity
│   │   ├── context.py           # @quarantine decorator + QuarantineContext
│   │   └── replay.py            # ReplayEngine
│   ├── rules/
│   │   ├── schema.py
│   │   ├── quality.py
│   │   ├── business.py
│   │   └── engine.py
│   ├── stores/
│   │   ├── base.py
│   │   ├── duckdb.py
│   │   ├── postgres.py
│   │   └── factory.py
│   ├── adapters/
│   │   ├── pandas.py
│   │   ├── spark.py
│   │   ├── airflow.py
│   │   └── dbt_macro/
│   ├── api/
│   │   └── app.py
│   └── cli/
│       └── main.py
├── tests/
├── examples/
├── .github/workflows/
├── .build_progress/
├── pyproject.toml
├── README.md
└── LICENSE
```

## Key Design Decisions

- DuckDB is zero-config default; Postgres for production
- Rules engine is 100% pure Python — no external API calls
- LLM (anthropic) is optional extras only
- All adapters (pandas, spark, airflow, dbt) are thin wrappers
- CLI built with Click + Rich for beautiful terminal output
