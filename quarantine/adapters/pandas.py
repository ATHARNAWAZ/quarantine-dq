from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Tuple

from quarantine.core.models import QuarantineRecord
from quarantine.rules.business import BusinessRule
from quarantine.rules.engine import RulesEngine
from quarantine.stores.factory import get_store


def quarantine_dataframe(
    df,
    pipeline: str,
    stage: str,
    run_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    schema: Optional[Dict[str, type]] = None,
    expected_columns: Optional[List[str]] = None,
    strict_columns: bool = False,
    not_null: Optional[List[str]] = None,
    pk_fields: Optional[List[str]] = None,
    range_constraints: Optional[Dict[str, Tuple]] = None,
    pattern_constraints: Optional[Dict[str, str]] = None,
    rules: Optional[List[BusinessRule]] = None,
    store_url: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Tuple:
    """
    Validate a pandas DataFrame row-by-row.

    Returns (clean_df, quarantined_df).
    - clean_df: all passing rows, unmodified.
    - quarantined_df: failing rows with extra columns:
      __quarantine_id, __failure_type, __failure_reason
    Failing rows are also persisted to the QuarantineStore.
    """
    try:
        import pandas as pd
    except ImportError:
        raise ImportError("pandas is required: uv pip install pandas")

    _run_id = run_id or str(uuid.uuid4())
    store = get_store(store_url)
    engine = RulesEngine(
        schema=schema,
        expected_columns=expected_columns,
        strict_columns=strict_columns,
        not_null=not_null,
        pk_fields=pk_fields,
        range_constraints=range_constraints,
        pattern_constraints=pattern_constraints,
        rules=rules,
    )

    clean_indices = []
    quarantine_rows = []

    for idx, row in df.iterrows():
        record = row.to_dict()
        violations = engine.validate(record)

        if violations:
            ft = engine.primary_failure_type(violations)
            reason = engine.format_reason(violations)
            q_id = str(uuid.uuid4())

            q_record = QuarantineRecord(
                id=q_id,
                pipeline_name=pipeline,
                stage_name=stage,
                run_id=_run_id,
                batch_id=batch_id,
                record_data=record,
                failure_type=ft,
                failure_severity=engine.worst_severity(violations),
                failure_reason=reason,
                failure_detail={"violations": [v.model_dump() for v in violations]},
                tags=tags or {},
            )
            store.save(q_record)

            annotated = dict(record)
            annotated["__quarantine_id"] = q_id
            annotated["__failure_type"] = ft.value
            annotated["__failure_reason"] = reason
            quarantine_rows.append(annotated)
        else:
            clean_indices.append(idx)

    clean_df = df.loc[clean_indices].copy()
    quarantined_df = pd.DataFrame(quarantine_rows) if quarantine_rows else pd.DataFrame()

    return clean_df, quarantined_df
