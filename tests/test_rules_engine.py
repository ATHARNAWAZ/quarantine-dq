import pytest
from quarantine.rules.engine import RulesEngine
from quarantine.core.models import FailureType, FailureSeverity


def make_engine(**kwargs):
    return RulesEngine(**kwargs)


def test_engine_clean_record():
    engine = make_engine(
        schema={"order_id": int, "revenue": float},
        not_null=["order_id"],
        pk_fields=["order_id"],
    )
    violations = engine.validate({"order_id": 1, "revenue": 9.99})
    assert violations == []


def test_engine_null_violation():
    engine = make_engine(not_null=["order_id"])
    violations = engine.validate({"order_id": None})
    assert len(violations) == 1
    assert violations[0].failure_type == FailureType.NULL_VIOLATION.value


def test_engine_schema_violation():
    engine = make_engine(schema={"order_id": int})
    violations = engine.validate({"order_id": "abc"})
    assert any(v.failure_type == FailureType.SCHEMA_VIOLATION.value for v in violations)


def test_engine_uniqueness_violation():
    engine = make_engine(pk_fields=["order_id"])
    engine.validate({"order_id": 1})
    violations = engine.validate({"order_id": 1})
    assert any(v.failure_type == FailureType.UNIQUENESS_VIOLATION.value for v in violations)


def test_engine_reset_batch():
    engine = make_engine(pk_fields=["order_id"])
    engine.validate({"order_id": 1})
    engine.reset_batch()
    violations = engine.validate({"order_id": 1})
    assert violations == []


def test_engine_worst_severity_critical():
    engine = make_engine(not_null=["order_id"])
    violations = engine.validate({"order_id": None})
    assert engine.worst_severity(violations) == FailureSeverity.CRITICAL


def test_engine_primary_failure_type_priority():
    engine = make_engine(
        schema={"order_id": int},
        not_null=["order_id"],
    )
    violations = engine.validate({"order_id": None})
    ft = engine.primary_failure_type(violations)
    assert ft == FailureType.NULL_VIOLATION


def test_engine_format_reason_single():
    engine = make_engine(not_null=["order_id"])
    violations = engine.validate({"order_id": None})
    reason = engine.format_reason(violations)
    assert "null" in reason.lower()


def test_engine_format_reason_multiple():
    engine = make_engine(
        not_null=["order_id", "customer_id"],
    )
    violations = engine.validate({"order_id": None, "customer_id": None})
    reason = engine.format_reason(violations)
    assert "2 rule violations" in reason
