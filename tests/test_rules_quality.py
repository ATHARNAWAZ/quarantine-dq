import pytest
from quarantine.rules.quality import (
    check_not_null,
    check_pattern,
    check_range,
    check_unique,
    check_volume,
)


def test_not_null_detects_null():
    record = {"order_id": None, "customer_id": "c1"}
    violations = check_not_null(record, ["order_id", "customer_id"])
    assert len(violations) == 1
    assert violations[0].failure_detail["field"] == "order_id"


def test_not_null_detects_absent():
    record = {"customer_id": "c1"}
    violations = check_not_null(record, ["order_id", "customer_id"])
    assert len(violations) == 1


def test_not_null_passes_when_present():
    record = {"order_id": 1, "customer_id": "c1"}
    assert check_not_null(record, ["order_id", "customer_id"]) == []


def test_check_unique_detects_duplicate():
    seen = set()
    r1 = {"order_id": 42}
    r2 = {"order_id": 42}
    assert check_unique(r1, ["order_id"], seen) == []
    violations = check_unique(r2, ["order_id"], seen)
    assert len(violations) == 1


def test_check_unique_compound_key():
    seen = set()
    r1 = {"a": 1, "b": 2}
    r2 = {"a": 1, "b": 3}
    assert check_unique(r1, ["a", "b"], seen) == []
    assert check_unique(r2, ["a", "b"], seen) == []


def test_check_range_below_min():
    record = {"price": -5.0}
    violations = check_range(record, {"price": (0, 1000)})
    assert len(violations) == 1
    assert "below minimum" in violations[0].failure_reason


def test_check_range_above_max():
    record = {"price": 9999.0}
    violations = check_range(record, {"price": (0, 100)})
    assert len(violations) == 1
    assert "exceeds maximum" in violations[0].failure_reason


def test_check_range_within_bounds():
    record = {"price": 50.0}
    assert check_range(record, {"price": (0, 100)}) == []


def test_check_range_none_value_skipped():
    record = {"price": None}
    assert check_range(record, {"price": (0, 100)}) == []


def test_check_pattern_mismatch():
    record = {"email": "not-an-email"}
    violations = check_pattern(record, {"email": r"[^@]+@[^@]+\.[^@]+"})
    assert len(violations) == 1


def test_check_pattern_match():
    record = {"email": "user@example.com"}
    violations = check_pattern(record, {"email": r"[^@]+@[^@]+\.[^@]+"})
    assert violations == []


def test_volume_anomaly_triggers():
    v = check_volume(50, 100, threshold_pct=0.2)
    assert v is not None
    assert "below" in v.failure_reason


def test_volume_anomaly_no_trigger():
    v = check_volume(95, 100, threshold_pct=0.2)
    assert v is None


def test_volume_anomaly_zero_baseline():
    v = check_volume(100, 0, threshold_pct=0.2)
    assert v is None
