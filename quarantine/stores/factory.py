from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from quarantine.stores.base import QuarantineStore

_DEFAULT_URL = f"duckdb://{Path.home() / '.quarantine' / 'quarantine.duckdb'}"


def get_store(url: Optional[str] = None) -> QuarantineStore:
    url = url or os.getenv("QUARANTINE_STORE_URL", _DEFAULT_URL)

    if url.startswith("postgresql") or url.startswith("postgres"):
        from quarantine.stores.postgres import PostgresStore
        return PostgresStore(url)

    db_path = url.replace("duckdb://", "", 1)
    from quarantine.stores.duckdb import DuckDBStore
    return DuckDBStore(db_path)
