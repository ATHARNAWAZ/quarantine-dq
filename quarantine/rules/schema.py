from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from quarantine.core.models import FailureSeverity, FailureType, RuleViolation


def check_missing_columns(
    record: Dict[str, Any],
    expected_columns: List[str],
) -> List[RuleViolation]:
    violations = []
    for col in expected_columns:
        if col not in record:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.SCHEMA_VIOLATION,
                    failure_severity=FailureSeverity.CRITICAL,
                    failure_reason=f"Required column '{col}' is missing from record",
                    failure_detail={"missing_column": col, "expected_columns": expected_columns},
                )
            )
    return violations


def check_extra_columns(
    record: Dict[str, Any],
    expected_columns: List[str],
    strict: bool = False,
) -> List[RuleViolation]:
    if not strict:
        return []
    violations = []
    expected_set = set(expected_columns)
    for col in record:
        if col not in expected_set:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.SCHEMA_VIOLATION,
                    failure_severity=FailureSeverity.WARNING,
                    failure_reason=f"Unexpected column '{col}' found in record (strict mode)",
                    failure_detail={"extra_column": col, "expected_columns": expected_columns},
                )
            )
    return violations


def check_type_mismatch(
    record: Dict[str, Any],
    schema: Dict[str, type],
) -> List[RuleViolation]:
    violations = []
    for field, expected_type in schema.items():
        if field not in record:
            continue
        value = record[field]
        if value is None:
            continue
        if not isinstance(value, expected_type):
            violations.append(
                RuleViolation(
                    failure_type=FailureType.SCHEMA_VIOLATION,
                    failure_severity=FailureSeverity.CRITICAL,
                    failure_reason=(
                        f"Field '{field}' expected {expected_type.__name__}, "
                        f"received {type(value).__name__}: {value!r}"
                    ),
                    failure_detail={
                        "field": field,
                        "expected_type": expected_type.__name__,
                        "actual_type": type(value).__name__,
                        "actual_value": str(value),
                    },
                )
            )
    return violations


def check_column_rename(
    record: Dict[str, Any],
    column_aliases: Dict[str, str],
) -> List[RuleViolation]:
    """Detects likely column renames: old name present but new name missing."""
    violations = []
    for old_name, new_name in column_aliases.items():
        if old_name in record and new_name not in record:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.SCHEMA_VIOLATION,
                    failure_severity=FailureSeverity.WARNING,
                    failure_reason=(
                        f"Column '{old_name}' found but expected name is '{new_name}' — "
                        f"possible column rename"
                    ),
                    failure_detail={
                        "old_name": old_name,
                        "new_name": new_name,
                        "old_value": record[old_name],
                    },
                )
            )
    return violations
