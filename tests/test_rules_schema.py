import pytest
from quarantine.rules.schema import (
    check_column_rename,
    check_extra_columns,
    check_missing_columns,
    check_type_mismatch,
)


def test_missing_columns_detects_missing():
    record = {"revenue": 100.0}
    violations = check_missing_columns(record, ["order_id", "revenue"])
    assert len(violations) == 1
    assert violations[0].failure_detail["missing_column"] == "order_id"


def test_missing_columns_all_present():
    record = {"order_id": 1, "revenue": 100.0}
    violations = check_missing_columns(record, ["order_id", "revenue"])
    assert violations == []


def test_extra_columns_strict_detects_extra():
    record = {"order_id": 1, "revenue": 100.0, "unknown_col": "x"}
    violations = check_extra_columns(record, ["order_id", "revenue"], strict=True)
    assert len(violations) == 1
    assert violations[0].failure_detail["extra_column"] == "unknown_col"


def test_extra_columns_non_strict_returns_empty():
    record = {"order_id": 1, "extra": "x"}
    violations = check_extra_columns(record, ["order_id"], strict=False)
    assert violations == []


def test_type_mismatch_detects_wrong_type():
    record = {"order_id": "not-an-int", "revenue": 100.0}
    violations = check_type_mismatch(record, {"order_id": int, "revenue": float})
    assert len(violations) == 1
    assert violations[0].failure_detail["field"] == "order_id"
    assert violations[0].failure_detail["expected_type"] == "int"


def test_type_mismatch_none_is_skipped():
    record = {"order_id": None}
    violations = check_type_mismatch(record, {"order_id": int})
    assert violations == []


def test_type_mismatch_all_correct():
    record = {"order_id": 42, "revenue": 9.99}
    violations = check_type_mismatch(record, {"order_id": int, "revenue": float})
    assert violations == []


def test_column_rename_detects_old_name():
    record = {"order_number": 42}
    violations = check_column_rename(record, {"order_number": "order_id"})
    assert len(violations) == 1
    assert violations[0].failure_detail["old_name"] == "order_number"


def test_column_rename_no_issue_when_new_name_present():
    record = {"order_id": 42}
    violations = check_column_rename(record, {"order_number": "order_id"})
    assert violations == []
