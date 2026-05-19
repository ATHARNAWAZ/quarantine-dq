from __future__ import annotations

import functools
import logging
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from rich.console import Console
from rich.table import Table
from rich import box

from quarantine.core.models import (
    FailureSeverity,
    FailureType,
    QuarantineRecord,
    RuleViolation,
)
from quarantine.rules.business import BusinessRule
from quarantine.rules.engine import RulesEngine
from quarantine.stores.factory import get_store

console = Console()
logger = logging.getLogger("quarantine")


class QuarantineException(Exception):
    def __init__(self, record: QuarantineRecord, violations: List[RuleViolation]):
        self.record = record
        self.violations = violations
        super().__init__(record.failure_reason)


class QuarantineContext:
    def __init__(
        self,
        pipeline: str,
        stage: str,
        run_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        schema: Optional[Dict[str, type]] = None,
        expected_columns: Optional[List[str]] = None,
        strict_columns: bool = False,
        column_aliases: Optional[Dict[str, str]] = None,
        not_null: Optional[List[str]] = None,
        pk_fields: Optional[List[str]] = None,
        range_constraints: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
        pattern_constraints: Optional[Dict[str, str]] = None,
        rules: Optional[List[BusinessRule]] = None,
        on_failure: str = "quarantine",
        store_url: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        silent: bool = False,
    ):
        self.pipeline = pipeline
        self.stage = stage
        self.run_id = run_id or str(uuid.uuid4())
        self.batch_id = batch_id
        self.on_failure = on_failure
        self.tags = tags or {}
        self.silent = silent

        self._store = get_store(store_url)
        self._engine = RulesEngine(
            schema=schema,
            expected_columns=expected_columns,
            strict_columns=strict_columns,
            column_aliases=column_aliases,
            not_null=not_null,
            pk_fields=pk_fields,
            range_constraints=range_constraints,
            pattern_constraints=pattern_constraints,
            rules=rules,
        )

        self._processed = 0
        self._quarantined = 0
        self._failure_type_counts: Dict[str, int] = {}

    def __enter__(self) -> "QuarantineContext":
        self._engine.reset_batch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if not self.silent:
            self._print_summary()
        return False

    def process(
        self,
        record: Dict[str, Any],
        transform_fn: Optional[Callable] = None,
    ) -> Optional[Any]:
        self._processed += 1
        violations = self._engine.validate(record)

        if violations:
            self._quarantined += 1
            ft = self._engine.primary_failure_type(violations)
            self._failure_type_counts[ft.value] = self._failure_type_counts.get(ft.value, 0) + 1

            q_record = QuarantineRecord(
                pipeline_name=self.pipeline,
                stage_name=self.stage,
                run_id=self.run_id,
                batch_id=self.batch_id,
                record_data=record,
                failure_type=ft,
                failure_severity=self._engine.worst_severity(violations),
                failure_reason=self._engine.format_reason(violations),
                failure_detail={"violations": [v.model_dump() for v in violations]},
                tags=self.tags,
            )
            self._store.save(q_record)

            if self.on_failure == "raise":
                raise QuarantineException(q_record, violations)
            return None

        if transform_fn is not None:
            return transform_fn(record)
        return record

    def _print_summary(self) -> None:
        if self._processed == 0:
            return
        pct = (self._quarantined / self._processed) * 100 if self._processed > 0 else 0
        status_color = "red" if self._quarantined > 0 else "green"

        table = Table(
            title=f"[bold]Quarantine Summary — {self.pipeline} / {self.stage}[/bold]",
            box=box.ROUNDED,
            show_header=True,
        )
        table.add_column("Metric", style="dim")
        table.add_column("Value", justify="right")
        table.add_row("Records processed", str(self._processed))
        table.add_row(
            "Quarantined",
            f"[{status_color}]{self._quarantined} ({pct:.1f}%)[/{status_color}]",
        )
        for ft, count in sorted(self._failure_type_counts.items()):
            table.add_row(f"  ↳ {ft}", str(count))
        console.print(table)

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "processed": self._processed,
            "quarantined": self._quarantined,
            "failure_types": self._failure_type_counts,
        }


def quarantine(
    pipeline: str,
    stage: str,
    run_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    schema: Optional[Dict[str, type]] = None,
    expected_columns: Optional[List[str]] = None,
    strict_columns: bool = False,
    column_aliases: Optional[Dict[str, str]] = None,
    not_null: Optional[List[str]] = None,
    pk_fields: Optional[List[str]] = None,
    range_constraints: Optional[Dict[str, Tuple[Optional[float], Optional[float]]]] = None,
    pattern_constraints: Optional[Dict[str, str]] = None,
    rules: Optional[List[BusinessRule]] = None,
    on_failure: str = "quarantine",
    store_url: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    silent: bool = False,
):
    """Decorator that wraps a single-record processing function with quarantine semantics."""

    def decorator(fn: Callable) -> Callable:
        ctx = QuarantineContext(
            pipeline=pipeline,
            stage=stage,
            run_id=run_id,
            batch_id=batch_id,
            schema=schema,
            expected_columns=expected_columns,
            strict_columns=strict_columns,
            column_aliases=column_aliases,
            not_null=not_null,
            pk_fields=pk_fields,
            range_constraints=range_constraints,
            pattern_constraints=pattern_constraints,
            rules=rules,
            on_failure=on_failure,
            store_url=store_url,
            tags=tags,
            silent=True,
        )

        @functools.wraps(fn)
        def wrapper(record: Dict[str, Any], *args, **kwargs) -> Optional[Any]:
            violations = ctx._engine.validate(record)
            if violations:
                ctx._quarantined += 1
                ft = ctx._engine.primary_failure_type(violations)
                ctx._failure_type_counts[ft.value] = ctx._failure_type_counts.get(ft.value, 0) + 1
                q_record = QuarantineRecord(
                    pipeline_name=pipeline,
                    stage_name=stage,
                    run_id=ctx.run_id,
                    batch_id=batch_id,
                    record_data=record,
                    failure_type=ft,
                    failure_severity=ctx._engine.worst_severity(violations),
                    failure_reason=ctx._engine.format_reason(violations),
                    failure_detail={"violations": [v.model_dump() for v in violations]},
                    tags=tags or {},
                )
                ctx._store.save(q_record)
                if on_failure == "raise":
                    raise QuarantineException(q_record, violations)
                return None
            ctx._processed += 1
            return fn(record, *args, **kwargs)

        wrapper._quarantine_ctx = ctx
        return wrapper

    return decorator
