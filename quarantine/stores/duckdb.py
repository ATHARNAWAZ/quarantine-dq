from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from quarantine.core.models import QuarantineRecord
from quarantine.stores.base import QuarantineStore

_DEFAULT_PATH = str(Path.home() / ".quarantine" / "quarantine.duckdb")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS quarantine_records (
    id VARCHAR PRIMARY KEY,
    pipeline_name VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    batch_id VARCHAR,
    record_data TEXT NOT NULL,
    failure_type VARCHAR NOT NULL,
    failure_severity VARCHAR NOT NULL,
    failure_reason TEXT NOT NULL,
    failure_detail TEXT NOT NULL,
    created_at VARCHAR NOT NULL,
    replayed_at VARCHAR,
    replay_status VARCHAR,
    resolved_by VARCHAR,
    tags TEXT NOT NULL
)
"""


class DuckDBStore(QuarantineStore):
    def __init__(self, db_path: str = _DEFAULT_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_table()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._connect() as con:
            con.execute(_CREATE_TABLE_SQL)

    def save(self, record: QuarantineRecord) -> None:
        d = record.to_store_dict()
        with self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO quarantine_records VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    d["id"], d["pipeline_name"], d["stage_name"], d["run_id"],
                    d["batch_id"], d["record_data"], d["failure_type"],
                    d["failure_severity"], d["failure_reason"], d["failure_detail"],
                    d["created_at"], d["replayed_at"], d["replay_status"],
                    d["resolved_by"], d["tags"],
                ],
            )

    def get(self, record_id: str) -> QuarantineRecord:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM quarantine_records WHERE id = ?", [record_id]
            ).fetchone()
        if row is None:
            raise KeyError(f"Record {record_id!r} not found")
        cols = [
            "id", "pipeline_name", "stage_name", "run_id", "batch_id",
            "record_data", "failure_type", "failure_severity", "failure_reason",
            "failure_detail", "created_at", "replayed_at", "replay_status",
            "resolved_by", "tags",
        ]
        return QuarantineRecord.from_store_dict(dict(zip(cols, row)))

    def list(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[QuarantineRecord]:
        conditions = []
        params = []
        if pipeline_name:
            conditions.append("pipeline_name = ?")
            params.append(pipeline_name)
        if failure_type:
            conditions.append("failure_type = ?")
            params.append(failure_type)
        if replay_status:
            conditions.append("replay_status = ?")
            params.append(replay_status)
        if since:
            conditions.append("created_at >= ?")
            params.append(since.isoformat())

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM quarantine_records {where} ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        cols = [
            "id", "pipeline_name", "stage_name", "run_id", "batch_id",
            "record_data", "failure_type", "failure_severity", "failure_reason",
            "failure_detail", "created_at", "replayed_at", "replay_status",
            "resolved_by", "tags",
        ]
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        return [QuarantineRecord.from_store_dict(dict(zip(cols, r))) for r in rows]

    def mark_replayed(self, record_id: str, status: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE quarantine_records SET replay_status = ?, replayed_at = ? WHERE id = ?",
                [status, datetime.utcnow().isoformat(), record_id],
            )

    def stats(self, pipeline_name: Optional[str] = None) -> Dict[str, Any]:
        where = "WHERE pipeline_name = ?" if pipeline_name else ""
        params = [pipeline_name] if pipeline_name else []

        with self._connect() as con:
            total = con.execute(
                f"SELECT COUNT(*) FROM quarantine_records {where}", params
            ).fetchone()[0]

            by_type = con.execute(
                f"SELECT failure_type, COUNT(*) FROM quarantine_records {where} GROUP BY failure_type",
                params,
            ).fetchall()

            by_severity = con.execute(
                f"SELECT failure_severity, COUNT(*) FROM quarantine_records {where} GROUP BY failure_severity",
                params,
            ).fetchall()

            by_pipeline = con.execute(
                "SELECT pipeline_name, COUNT(*) FROM quarantine_records GROUP BY pipeline_name"
            ).fetchall()

            unresolved_where = (
                f"{where} AND replay_status IS NULL" if where else "WHERE replay_status IS NULL"
            )
            unresolved = con.execute(
                f"SELECT COUNT(*) FROM quarantine_records {unresolved_where}",
                params,
            ).fetchone()[0]

        return {
            "total": total,
            "unresolved": unresolved,
            "by_failure_type": dict(by_type),
            "by_severity": dict(by_severity),
            "by_pipeline": dict(by_pipeline),
        }

    def delete(self, record_id: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM quarantine_records WHERE id = ?", [record_id])

    def count(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
    ) -> int:
        conditions = []
        params = []
        if pipeline_name:
            conditions.append("pipeline_name = ?")
            params.append(pipeline_name)
        if failure_type:
            conditions.append("failure_type = ?")
            params.append(failure_type)
        if replay_status == "unresolved":
            conditions.append("replay_status IS NULL")
        elif replay_status:
            conditions.append("replay_status = ?")
            params.append(replay_status)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connect() as con:
            return con.execute(
                f"SELECT COUNT(*) FROM quarantine_records {where}", params
            ).fetchone()[0]

    def delete_where(
        self,
        pipeline_name: Optional[str] = None,
        replayed_only: bool = False,
    ) -> int:
        conditions = []
        params = []
        if pipeline_name:
            conditions.append("pipeline_name = ?")
            params.append(pipeline_name)
        if replayed_only:
            conditions.append("replay_status = 'success'")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connect() as con:
            count = con.execute(
                f"SELECT COUNT(*) FROM quarantine_records {where}", params
            ).fetchone()[0]
            con.execute(f"DELETE FROM quarantine_records {where}", params)
        return count
