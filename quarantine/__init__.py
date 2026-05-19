"""quarantine-dq: Platform-agnostic dead letter queue for data pipelines."""

from quarantine.core.context import QuarantineContext, QuarantineException, quarantine
from quarantine.core.models import (
    FailureSeverity,
    FailureType,
    QuarantineRecord,
    ReplayResult,
    RuleViolation,
)
from quarantine.core.replay import ReplayEngine
from quarantine.rules.business import AllowedValues, BusinessRule, NonEmptyString, PositiveRevenue
from quarantine.rules.engine import RulesEngine
from quarantine.stores.factory import get_store

__version__ = "0.1.0"

__all__ = [
    "quarantine",
    "QuarantineContext",
    "QuarantineException",
    "QuarantineRecord",
    "ReplayEngine",
    "ReplayResult",
    "RulesEngine",
    "BusinessRule",
    "PositiveRevenue",
    "NonEmptyString",
    "AllowedValues",
    "FailureType",
    "FailureSeverity",
    "RuleViolation",
    "get_store",
]
