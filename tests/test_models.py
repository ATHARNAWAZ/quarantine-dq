from datetime import datetime

import pytest

from quarantine.core.models import (
    FailureSeverity,
    FailureType,
    QuarantineRecord,
    ReplayResult,
    RuleViolation,
)


def make_record(**kwargs):
    defaults = dict(
        pipeline_name="test_pipeline",
        stage_name="validate",
        run_id="run-001",
        record_data={"order_id": 1, "revenue": 100.0},
        failure_type=FailureType.NULL_VIOLATION,
        failure_severity=FailureSeverity.CRITICAL,
        failure_reason="order_id is null",
    )
    defaults.update(kwargs)
    return QuarantineRecord(**defaults)


def test_record_creation():
    rec = make_record()
    assert rec.id is not None
    assert len(rec.id) == 36  # uuid4
    assert rec.pipeline_name == "test_pipeline"
    assert rec.failure_type == FailureType.NULL_VIOLATION.value


def test_record_defaults():
    rec = make_record()
    assert rec.batch_id is None
    assert rec.replay_status is None
    assert rec.replayed_at is None
    assert rec.tags == {}
    assert rec.failure_detail == {}


def test_record_serialization_round_trip():
    rec = make_record(tags={"env": "prod"}, failure_detail={"field": "order_id"})
    d = rec.to_store_dict()
    assert isinstance(d["record_data"], str)
    assert isinstance(d["tags"], str)

    restored = QuarantineRecord.from_store_dict(d)
    assert restored.id == rec.id
    assert restored.record_data == rec.record_data
    assert restored.tags == rec.tags
    assert restored.failure_detail == rec.failure_detail


def test_mark_replayed():
    rec = make_record()
    rec.mark_replayed("success", resolved_by="bob")
    assert rec.replay_status == "success"
    assert rec.replayed_at is not None
    assert rec.resolved_by == "bob"


def test_replay_result():
    result = ReplayResult(pipeline_name="orders", attempted=10, succeeded=8, failed=2)
    assert result.attempted == 10
    assert result.succeeded == 8


def test_rule_violation():
    v = RuleViolation(
        failure_type=FailureType.SCHEMA_VIOLATION,
        failure_severity=FailureSeverity.WARNING,
        failure_reason="missing column",
    )
    assert v.failure_detail == {}
