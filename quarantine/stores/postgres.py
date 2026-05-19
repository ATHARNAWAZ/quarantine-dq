from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text

from quarantine.core.models import QuarantineRecord
from quarantine.stores.base import QuarantineStore

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


class PostgresStore(QuarantineStore):
    def __init__(self, url: str):
        self.url = url
        self._engine = create_engine(url, pool_pre_ping=True)
        self._ensure_table()

    def _ensure_table(self) -> None:
        with self._engine.begin() as con:
            con.execute(text(_CREATE_TABLE_SQL))

    def save(self, record: QuarantineRecord) -> None:
        d = record.to_store_dict()
        with self._engine.begin() as con:
            con.execute(
                text("""
                    INSERT INTO quarantine_records VALUES (
                        :id, :pipeline_name, :stage_name, :run_id, :batch_id,
                        :record_data, :failure_type, :failure_severity, :failure_reason,
                        :failure_detail, :created_at, :replayed_at, :replay_status,
                        :resolved_by, :tags
                    ) ON CONFLICT (id) DO UPDATE SET
                        replay_status = EXCLUDED.replay_status,
                        replayed_at = EXCLUDED.replayed_at,
                        resolved_by = EXCLUDED.resolved_by
                """),
                d,
            )

    def get(self, record_id: str) -> QuarantineRecord:
        with self._engine.connect() as con:
            row = con.execute(
                text("SELECT * FROM quarantine_records WHERE id = :id"),
                {"id": record_id},
            ).mappings().fetchone()
        if row is None:
            raise KeyError(f"Record {record_id!r} not found")
        return QuarantineRecord.from_store_dict(dict(row))

    def list(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[QuarantineRecord]:
        conditions = []
        params: Dict[str, Any] = {"limit": limit}

        if pipeline_name:
            conditions.append("pipeline_name = :pipeline_name")
            params["pipeline_name"] = pipeline_name
        if failure_type:
            conditions.append("failure_type = :failure_type")
            params["failure_type"] = failure_type
        if replay_status:
            conditions.append("replay_status = :replay_status")
            params["replay_status"] = replay_status
        if since:
            conditions.append("created_at >= :since")
            params["since"] = since.isoformat()

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM quarantine_records {where} ORDER BY created_at DESC LIMIT :limit"

        with self._engine.connect() as con:
            rows = con.execute(text(sql), params).mappings().fetchall()
        return [QuarantineRecord.from_store_dict(dict(r)) for r in rows]

    def mark_replayed(self, record_id: str, status: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                text("""
                    UPDATE quarantine_records
                    SET replay_status = :status, replayed_at = :replayed_at
                    WHERE id = :id
                """),
                {"status": status, "replayed_at": datetime.utcnow().isoformat(), "id": record_id},
            )

    def stats(self, pipeline_name: Optional[str] = None) -> Dict[str, Any]:
        where = "WHERE pipeline_name = :pipeline_name" if pipeline_name else ""
        params = {"pipeline_name": pipeline_name} if pipeline_name else {}

        with self._engine.connect() as con:
            total = con.execute(
                text(f"SELECT COUNT(*) FROM quarantine_records {where}"), params
            ).scalar()

            by_type = con.execute(
                text(f"SELECT failure_type, COUNT(*) FROM quarantine_records {where} GROUP BY failure_type"),
                params,
            ).fetchall()

            by_severity = con.execute(
                text(f"SELECT failure_severity, COUNT(*) FROM quarantine_records {where} GROUP BY failure_severity"),
                params,
            ).fetchall()

            by_pipeline = con.execute(
                text("SELECT pipeline_name, COUNT(*) FROM quarantine_records GROUP BY pipeline_name")
            ).fetchall()

            unresolved = con.execute(
                text(f"SELECT COUNT(*) FROM quarantine_records {where} WHERE replay_status IS NULL"),
                params,
            ).scalar()

        return {
            "total": total,
            "unresolved": unresolved,
            "by_failure_type": dict(by_type),
            "by_severity": dict(by_severity),
            "by_pipeline": dict(by_pipeline),
        }

    def delete(self, record_id: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                text("DELETE FROM quarantine_records WHERE id = :id"),
                {"id": record_id},
            )

    def count(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
    ) -> int:
        conditions = []
        params: Dict[str, Any] = {}
        if pipeline_name:
            conditions.append("pipeline_name = :pipeline_name")
            params["pipeline_name"] = pipeline_name
        if failure_type:
            conditions.append("failure_type = :failure_type")
            params["failure_type"] = failure_type
        if replay_status == "unresolved":
            conditions.append("replay_status IS NULL")
        elif replay_status:
            conditions.append("replay_status = :replay_status")
            params["replay_status"] = replay_status

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._engine.connect() as con:
            return con.execute(
                text(f"SELECT COUNT(*) FROM quarantine_records {where}"), params
            ).scalar()

    def delete_where(
        self,
        pipeline_name: Optional[str] = None,
        replayed_only: bool = False,
    ) -> int:
        conditions = []
        params: Dict[str, Any] = {}
        if pipeline_name:
            conditions.append("pipeline_name = :pipeline_name")
            params["pipeline_name"] = pipeline_name
        if replayed_only:
            conditions.append("replay_status = 'success'")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._engine.begin() as con:
            count = con.execute(
                text(f"SELECT COUNT(*) FROM quarantine_records {where}"), params
            ).scalar()
            con.execute(text(f"DELETE FROM quarantine_records {where}"), params)
        return count
