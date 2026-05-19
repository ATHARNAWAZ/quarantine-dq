from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from quarantine.core.models import FailureSeverity, FailureType, RuleViolation
from quarantine.rules.business import BusinessRule
from quarantine.rules.quality import (
    check_not_null,
    check_pattern,
    check_range,
    check_unique,
)
from quarantine.rules.schema import (
    check_column_rename,
    check_extra_columns,
    check_missing_columns,
    check_type_mismatch,
)


class RulesEngine:
    def __init__(
        self,
        schema: Optional[Dict[str, type]] = None,
        expected_columns: Optional[List[str]] = None,
        strict_columns: bool = False,
        column_aliases: Optional[Dict[str, str]] = None,
        not_null: Optional[List[str]] = None,
        pk_fields: Optional[List[str]] = None,
        range_constraints: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
        pattern_constraints: Optional[Dict[str, str]] = None,
        rules: Optional[List[BusinessRule]] = None,
    ):
        self.schema = schema or {}
        self.expected_columns = expected_columns or (list(schema.keys()) if schema else [])
        self.strict_columns = strict_columns
        self.column_aliases = column_aliases or {}
        self.not_null = not_null or []
        self.pk_fields = pk_fields or []
        self.range_constraints = range_constraints or {}
        self.pattern_constraints = pattern_constraints or {}
        self.rules = rules or []

        self._seen_keys: Set[Tuple] = set()

    def reset_batch(self) -> None:
        self._seen_keys = set()

    def validate(self, record: Dict[str, Any]) -> List[RuleViolation]:
        violations: List[RuleViolation] = []

        if self.expected_columns:
            violations.extend(check_missing_columns(record, self.expected_columns))
            violations.extend(check_extra_columns(record, self.expected_columns, self.strict_columns))

        if self.schema:
            violations.extend(check_type_mismatch(record, self.schema))

        if self.column_aliases:
            violations.extend(check_column_rename(record, self.column_aliases))

        if self.not_null:
            violations.extend(check_not_null(record, self.not_null))

        if self.pk_fields:
            violations.extend(check_unique(record, self.pk_fields, self._seen_keys))

        if self.range_constraints:
            violations.extend(check_range(record, self.range_constraints))

        if self.pattern_constraints:
            violations.extend(check_pattern(record, self.pattern_constraints))

        for rule in self.rules:
            v = rule.check(record)
            if v:
                violations.append(v)

        return violations

    def worst_severity(self, violations: List[RuleViolation]) -> FailureSeverity:
        if any(v.failure_severity == FailureSeverity.CRITICAL for v in violations):
            return FailureSeverity.CRITICAL
        if any(v.failure_severity == FailureSeverity.WARNING for v in violations):
            return FailureSeverity.WARNING
        return FailureSeverity.INFO

    def primary_failure_type(self, violations: List[RuleViolation]) -> FailureType:
        priority = [
            FailureType.SCHEMA_VIOLATION,
            FailureType.NULL_VIOLATION,
            FailureType.UNIQUENESS_VIOLATION,
            FailureType.BUSINESS_RULE,
            FailureType.RANGE_VIOLATION,
            FailureType.PATTERN_VIOLATION,
            FailureType.VOLUME_ANOMALY,
            FailureType.UNKNOWN,
        ]
        types = {v.failure_type for v in violations}
        for ft in priority:
            if ft in types:
                return ft
        return FailureType.UNKNOWN

    def format_reason(self, violations: List[RuleViolation]) -> str:
        if len(violations) == 1:
            return violations[0].failure_reason
        reasons = "; ".join(v.failure_reason for v in violations)
        return f"{len(violations)} rule violations: {reasons}"
