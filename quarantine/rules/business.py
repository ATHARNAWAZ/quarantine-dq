from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from quarantine.core.models import FailureSeverity, FailureType, RuleViolation


class BusinessRule(ABC):
    """Base class for custom business rules. Subclass and implement validate()."""

    severity: FailureSeverity = FailureSeverity.WARNING

    @abstractmethod
    def validate(self, record: Dict[str, Any]) -> Optional[str]:
        """Return an error message string if the rule is violated, else None."""
        ...

    def check(self, record: Dict[str, Any]) -> Optional[RuleViolation]:
        reason = self.validate(record)
        if reason is None:
            return None
        return RuleViolation(
            failure_type=FailureType.BUSINESS_RULE,
            failure_severity=self.severity,
            failure_reason=reason,
            failure_detail={"rule_class": type(self).__name__},
        )


class PositiveRevenue(BusinessRule):
    """Example built-in rule: revenue must be non-negative."""

    def validate(self, record: Dict[str, Any]) -> Optional[str]:
        revenue = record.get("revenue")
        if revenue is not None and revenue < 0:
            return f"Revenue {revenue} is negative"
        return None


class NonEmptyString(BusinessRule):
    """Validates that a string field is non-empty after stripping whitespace."""

    def __init__(self, field: str):
        self.field = field

    def validate(self, record: Dict[str, Any]) -> Optional[str]:
        value = record.get(self.field)
        if value is not None and isinstance(value, str) and not value.strip():
            return f"Field '{self.field}' is an empty or whitespace-only string"
        return None


class AllowedValues(BusinessRule):
    """Validates that a field's value is within an allowed set."""

    def __init__(self, field: str, allowed: list):
        self.field = field
        self.allowed = set(allowed)

    def validate(self, record: Dict[str, Any]) -> Optional[str]:
        value = record.get(self.field)
        if value is not None and value not in self.allowed:
            return f"Field '{self.field}' value {value!r} is not in allowed values: {sorted(self.allowed)}"
        return None
