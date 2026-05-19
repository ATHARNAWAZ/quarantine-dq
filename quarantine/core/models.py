from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class FailureType(str, Enum):
    SCHEMA_VIOLATION = "schema_violation"
    NULL_VIOLATION = "null_violation"
    UNIQUENESS_VIOLATION = "uniqueness_violation"
    RANGE_VIOLATION = "range_violation"
    PATTERN_VIOLATION = "pattern_violation"
    VOLUME_ANOMALY = "volume_anomaly"
    BUSINESS_RULE = "business_rule"
    UNKNOWN = "unknown"


class FailureSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class ReplayStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


class QuarantineRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_name: str
    stage_name: str
    run_id: str
    batch_id: Optional[str] = None
    record_data: Dict[str, Any]
    failure_type: FailureType
    failure_severity: FailureSeverity
    failure_reason: str
    failure_detail: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    replayed_at: Optional[datetime] = None
    replay_status: Optional[str] = None
    resolved_by: Optional[str] = None
    tags: Dict[str, str] = Field(default_factory=dict)

    model_config = {"use_enum_values": True}

    def mark_replayed(self, status: str, resolved_by: Optional[str] = None) -> None:
        self.replayed_at = datetime.utcnow()
        self.replay_status = status
        if resolved_by:
            self.resolved_by = resolved_by

    def to_store_dict(self) -> Dict[str, Any]:
        import json
        d = self.model_dump()
        d["record_data"] = json.dumps(d["record_data"])
        d["failure_detail"] = json.dumps(d["failure_detail"])
        d["tags"] = json.dumps(d["tags"])
        if d["created_at"]:
            d["created_at"] = d["created_at"].isoformat()
        if d["replayed_at"]:
            d["replayed_at"] = d["replayed_at"].isoformat()
        return d

    @classmethod
    def from_store_dict(cls, d: Dict[str, Any]) -> "QuarantineRecord":
        import json
        from dateutil import parser as dateutil_parser
        d = dict(d)
        if isinstance(d.get("record_data"), str):
            d["record_data"] = json.loads(d["record_data"])
        if isinstance(d.get("failure_detail"), str):
            d["failure_detail"] = json.loads(d["failure_detail"])
        if isinstance(d.get("tags"), str):
            d["tags"] = json.loads(d["tags"])
        if isinstance(d.get("created_at"), str):
            d["created_at"] = dateutil_parser.parse(d["created_at"])
        if isinstance(d.get("replayed_at"), str) and d["replayed_at"]:
            d["replayed_at"] = dateutil_parser.parse(d["replayed_at"])
        return cls(**d)


class RuleViolation(BaseModel):
    failure_type: FailureType
    failure_severity: FailureSeverity = FailureSeverity.WARNING
    failure_reason: str
    failure_detail: Dict[str, Any] = Field(default_factory=dict)


class ReplayResult(BaseModel):
    pipeline_name: str
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    dry_run: bool = False
    errors: list = Field(default_factory=list)
