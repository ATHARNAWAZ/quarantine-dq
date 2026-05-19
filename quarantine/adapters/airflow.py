from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

from quarantine.core.context import QuarantineContext
from quarantine.rules.business import BusinessRule


class QuarantineOperator:
    """
    Wraps any callable as an Airflow task with quarantine semantics.

    Failed records go to QuarantineStore instead of failing the DAG.
    On completion, pushes XCom with quarantine_count and failure_summary.

    Usage in a DAG:
        op = QuarantineOperator(
            task_id="validate_orders",
            callable=process_order,
            records_callable=fetch_orders,
            pipeline="orders_etl",
            stage="validate",
            not_null=["order_id", "customer_id"],
            dag=dag,
        )
    """

    def __init__(
        self,
        task_id: str,
        callable: Callable,
        records_callable: Callable,
        pipeline: str,
        stage: str,
        run_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        schema: Optional[Dict[str, type]] = None,
        not_null: Optional[List[str]] = None,
        pk_fields: Optional[List[str]] = None,
        range_constraints: Optional[Dict[str, Tuple]] = None,
        pattern_constraints: Optional[Dict[str, str]] = None,
        rules: Optional[List[BusinessRule]] = None,
        on_failure: str = "quarantine",
        store_url: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        dag=None,
        **kwargs,
    ):
        self.task_id = task_id
        self._callable = callable
        self._records_callable = records_callable
        self.pipeline = pipeline
        self.stage = stage
        self._ctx_kwargs = dict(
            pipeline=pipeline,
            stage=stage,
            run_id=run_id,
            batch_id=batch_id,
            schema=schema,
            not_null=not_null,
            pk_fields=pk_fields,
            range_constraints=range_constraints,
            pattern_constraints=pattern_constraints,
            rules=rules,
            on_failure=on_failure,
            store_url=store_url,
            tags=tags,
        )
        self.dag = dag

        # Register with Airflow if available
        try:
            from airflow.models import BaseOperator

            class _WrappedOperator(BaseOperator):
                def __init__(inner_self, **op_kwargs):
                    super().__init__(task_id=task_id, dag=dag, **op_kwargs)

                def execute(inner_self, context):
                    return self._execute_with_quarantine(context)

            self._operator = _WrappedOperator()
        except ImportError:
            self._operator = None

    def _execute_with_quarantine(self, context: Dict[str, Any]) -> Dict[str, Any]:
        records = self._records_callable(context)

        with QuarantineContext(**self._ctx_kwargs) as ctx:
            results = []
            for record in records:
                result = ctx.process(record, self._callable)
                if result is not None:
                    results.append(result)

        summary = {
            "processed": ctx.stats["processed"],
            "quarantine_count": ctx.stats["quarantined"],
            "failure_summary": ctx.stats["failure_types"],
        }

        if context.get("ti"):
            context["ti"].xcom_push(key="quarantine_summary", value=summary)

        return summary

    def execute(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._execute_with_quarantine(context or {})
