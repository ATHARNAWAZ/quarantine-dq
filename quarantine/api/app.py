from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel as ApiModel
except ImportError:
    raise ImportError("FastAPI is required: uv pip install 'quarantine-dq[api]'")

from quarantine.core.models import QuarantineRecord
from quarantine.core.replay import ReplayEngine
from quarantine.stores.factory import get_store

app = FastAPI(
    title="quarantine-dq API",
    description="REST API for the quarantine-dq dead letter queue",
    version="0.1.0",
)

_store = None


def _get_store():
    global _store
    if _store is None:
        _store = get_store()
    return _store


class ReplayRequest(ApiModel):
    pipeline_name: str
    failure_type: Optional[str] = None
    since: Optional[datetime] = None
    dry_run: bool = False
    limit: int = 1000


class RecordResponse(ApiModel):
    id: str
    pipeline_name: str
    stage_name: str
    run_id: str
    batch_id: Optional[str]
    failure_type: str
    failure_severity: str
    failure_reason: str
    failure_detail: Dict[str, Any]
    created_at: datetime
    replayed_at: Optional[datetime]
    replay_status: Optional[str]
    resolved_by: Optional[str]
    tags: Dict[str, str]
    record_data: Dict[str, Any]


def _record_to_response(rec: QuarantineRecord) -> Dict[str, Any]:
    return {
        "id": rec.id,
        "pipeline_name": rec.pipeline_name,
        "stage_name": rec.stage_name,
        "run_id": rec.run_id,
        "batch_id": rec.batch_id,
        "failure_type": rec.failure_type,
        "failure_severity": rec.failure_severity,
        "failure_reason": rec.failure_reason,
        "failure_detail": rec.failure_detail,
        "created_at": rec.created_at.isoformat() if rec.created_at else None,
        "replayed_at": rec.replayed_at.isoformat() if rec.replayed_at else None,
        "replay_status": rec.replay_status,
        "resolved_by": rec.resolved_by,
        "tags": rec.tags,
        "record_data": rec.record_data,
    }


@app.get("/records", response_class=JSONResponse)
def list_records(
    pipeline: Optional[str] = Query(None),
    failure_type: Optional[str] = Query(None),
    replay_status: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
):
    store = _get_store()
    since_dt = datetime.fromisoformat(since) if since else None
    records = store.list(
        pipeline_name=pipeline,
        failure_type=failure_type,
        replay_status=replay_status,
        since=since_dt,
        limit=limit,
    )
    return [_record_to_response(r) for r in records]


@app.get("/records/{record_id}", response_class=JSONResponse)
def get_record(record_id: str):
    store = _get_store()
    try:
        rec = store.get(record_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Record {record_id!r} not found")
    return _record_to_response(rec)


@app.post("/records/{record_id}/replay", response_class=JSONResponse)
def replay_one(record_id: str, dry_run: bool = Query(False)):
    store = _get_store()
    try:
        rec = store.get(record_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Record {record_id!r} not found")

    engine = ReplayEngine(store=store)
    success = engine.replay_one(record_id, transform_fn=lambda r: r, dry_run=dry_run)
    return {"record_id": record_id, "success": success, "dry_run": dry_run}


@app.post("/replay", response_class=JSONResponse)
def bulk_replay(req: ReplayRequest):
    store = _get_store()
    engine = ReplayEngine(store=store)
    result = engine.replay(
        pipeline_name=req.pipeline_name,
        transform_fn=lambda r: r,
        failure_type=req.failure_type,
        since=req.since,
        dry_run=req.dry_run,
        limit=req.limit,
    )
    return result.model_dump()


@app.get("/stats", response_class=JSONResponse)
def get_stats(pipeline: Optional[str] = Query(None)):
    store = _get_store()
    return store.stats(pipeline_name=pipeline)


@app.delete("/records/{record_id}", response_class=JSONResponse)
def delete_record(record_id: str):
    store = _get_store()
    try:
        store.get(record_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Record {record_id!r} not found")
    store.delete(record_id)
    return {"deleted": record_id}


@app.get("/health")
def health():
    return {"status": "ok", "service": "quarantine-dq"}
