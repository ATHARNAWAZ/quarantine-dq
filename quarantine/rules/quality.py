from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

from quarantine.core.models import FailureSeverity, FailureType, RuleViolation


def check_not_null(
    record: Dict[str, Any],
    not_null_fields: List[str],
) -> List[RuleViolation]:
    violations = []
    for field in not_null_fields:
        if field in record and record[field] is None:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.NULL_VIOLATION,
                    failure_severity=FailureSeverity.CRITICAL,
                    failure_reason=f"Column '{field}' is null but marked NOT NULL in schema definition",
                    failure_detail={"field": field, "constraint": "NOT NULL"},
                )
            )
        elif field not in record:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.NULL_VIOLATION,
                    failure_severity=FailureSeverity.CRITICAL,
                    failure_reason=f"Required non-null column '{field}' is absent from record",
                    failure_detail={"field": field, "constraint": "NOT NULL"},
                )
            )
    return violations


def check_unique(
    record: Dict[str, Any],
    pk_fields: List[str],
    seen_keys: Set[Tuple],
) -> List[RuleViolation]:
    key_values = tuple(record.get(f) for f in pk_fields)
    key_str = ", ".join(f"{f}={v}" for f, v in zip(pk_fields, key_values))

    if key_values in seen_keys:
        return [
            RuleViolation(
                failure_type=FailureType.UNIQUENESS_VIOLATION,
                failure_severity=FailureSeverity.CRITICAL,
                failure_reason=f"Primary key {key_str!r} already exists in this batch",
                failure_detail={
                    "pk_fields": pk_fields,
                    "key_values": dict(zip(pk_fields, key_values)),
                },
            )
        ]
    seen_keys.add(key_values)
    return []


def check_range(
    record: Dict[str, Any],
    range_constraints: Dict[str, Tuple[Optional[float], Optional[float]]],
) -> List[RuleViolation]:
    violations = []
    for field, (min_val, max_val) in range_constraints.items():
        if field not in record or record[field] is None:
            continue
        value = record[field]
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue

        if min_val is not None and numeric < min_val:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.RANGE_VIOLATION,
                    failure_severity=FailureSeverity.WARNING,
                    failure_reason=f"Field '{field}' value {numeric} is below minimum {min_val}",
                    failure_detail={"field": field, "value": numeric, "min": min_val, "max": max_val},
                )
            )
        elif max_val is not None and numeric > max_val:
            violations.append(
                RuleViolation(
                    failure_type=FailureType.RANGE_VIOLATION,
                    failure_severity=FailureSeverity.WARNING,
                    failure_reason=f"Field '{field}' value {numeric} exceeds maximum {max_val}",
                    failure_detail={"field": field, "value": numeric, "min": min_val, "max": max_val},
                )
            )
    return violations


def check_pattern(
    record: Dict[str, Any],
    pattern_constraints: Dict[str, str],
) -> List[RuleViolation]:
    violations = []
    for field, pattern in pattern_constraints.items():
        if field not in record or record[field] is None:
            continue
        value = str(record[field])
        if not re.fullmatch(pattern, value):
            violations.append(
                RuleViolation(
                    failure_type=FailureType.PATTERN_VIOLATION,
                    failure_severity=FailureSeverity.WARNING,
                    failure_reason=f"Field '{field}' value {value!r} does not match pattern '{pattern}'",
                    failure_detail={"field": field, "value": value, "pattern": pattern},
                )
            )
    return violations


def check_volume(
    current_count: int,
    baseline_count: int,
    threshold_pct: float = 0.2,
) -> Optional[RuleViolation]:
    if baseline_count == 0:
        return None
    deviation = abs(current_count - baseline_count) / baseline_count
    if deviation > threshold_pct:
        direction = "above" if current_count > baseline_count else "below"
        pct_str = f"{deviation * 100:.1f}%"
        return RuleViolation(
            failure_type=FailureType.VOLUME_ANOMALY,
            failure_severity=FailureSeverity.WARNING,
            failure_reason=(
                f"Row count {current_count} is {pct_str} {direction} baseline {baseline_count} "
                f"(threshold: {threshold_pct * 100:.0f}%)"
            ),
            failure_detail={
                "current_count": current_count,
                "baseline_count": baseline_count,
                "deviation_pct": round(deviation * 100, 2),
                "threshold_pct": threshold_pct * 100,
            },
        )
    return None
