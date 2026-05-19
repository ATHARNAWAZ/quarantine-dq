import pytest
from quarantine.rules.business import AllowedValues, BusinessRule, NonEmptyString, PositiveRevenue
from quarantine.core.models import FailureType


def test_positive_revenue_passes():
    rule = PositiveRevenue()
    assert rule.check({"revenue": 100.0}) is None


def test_positive_revenue_fails():
    rule = PositiveRevenue()
    v = rule.check({"revenue": -5.0})
    assert v is not None
    assert v.failure_type == FailureType.BUSINESS_RULE.value
    assert "-5.0" in v.failure_reason


def test_positive_revenue_missing_field_passes():
    rule = PositiveRevenue()
    assert rule.check({}) is None


def test_non_empty_string_fails_on_empty():
    rule = NonEmptyString("name")
    v = rule.check({"name": "   "})
    assert v is not None
    assert "name" in v.failure_reason


def test_non_empty_string_passes():
    rule = NonEmptyString("name")
    assert rule.check({"name": "Alice"}) is None


def test_allowed_values_fails():
    rule = AllowedValues("status", ["active", "inactive"])
    v = rule.check({"status": "deleted"})
    assert v is not None
    assert "deleted" in v.failure_reason


def test_allowed_values_passes():
    rule = AllowedValues("status", ["active", "inactive"])
    assert rule.check({"status": "active"}) is None


def test_custom_business_rule():
    class NegativeStock(BusinessRule):
        def validate(self, record):
            if record.get("stock", 0) < 0:
                return f"Stock {record['stock']} is negative"
            return None

    rule = NegativeStock()
    assert rule.check({"stock": 5}) is None
    v = rule.check({"stock": -1})
    assert v is not None
    assert "negative" in v.failure_reason
