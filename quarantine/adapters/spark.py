from __future__ import annotations

import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple

from quarantine.core.models import QuarantineRecord
from quarantine.rules.business import BusinessRule
from quarantine.rules.engine import RulesEngine
from quarantine.stores.factory import get_store


def quarantine_rdd(
    rdd,
    pipeline: str,
    stage: str,
    run_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    schema: Optional[Dict[str, type]] = None,
    expected_columns: Optional[List[str]] = None,
    not_null: Optional[List[str]] = None,
    pk_fields: Optional[List[str]] = None,
    range_constraints: Optional[Dict[str, Tuple]] = None,
    pattern_constraints: Optional[Dict[str, str]] = None,
    rules: Optional[List[BusinessRule]] = None,
    store_url: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Tuple:
    """
    Validates a PySpark RDD (of dicts) or DataFrame.

    Returns (clean_rdd, quarantine_count) where:
    - clean_rdd: passing records, ready to continue the pipeline
    - quarantine_count: int, number of records sent to QuarantineStore

    Uses mapPartitions for efficiency — one store connection per partition.
    """
    _run_id = run_id or str(uuid.uuid4())
    _tags = tags or {}

    # Handle Spark DataFrame by converting to RDD of Row dicts
    try:
        from pyspark.sql import DataFrame as SparkDF
        if isinstance(rdd, SparkDF):
            rdd = rdd.rdd.map(lambda row: row.asDict())
    except ImportError:
        pass

    def process_partition(records: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        store = get_store(store_url)
        engine = RulesEngine(
            schema=schema,
            expected_columns=expected_columns,
            not_null=not_null,
            pk_fields=pk_fields,
            range_constraints=range_constraints,
            pattern_constraints=pattern_constraints,
            rules=rules,
        )

        for record in records:
            violations = engine.validate(record)
            if violations:
                ft = engine.primary_failure_type(violations)
                q_record = QuarantineRecord(
                    pipeline_name=pipeline,
                    stage_name=stage,
                    run_id=_run_id,
                    batch_id=batch_id,
                    record_data=record,
                    failure_type=ft,
                    failure_severity=engine.worst_severity(violations),
                    failure_reason=engine.format_reason(violations),
                    failure_detail={"violations": [v.model_dump() for v in violations]},
                    tags=_tags,
                )
                store.save(q_record)
                # Emit a sentinel so we can count quarantined records
                yield {"__quarantined__": True}
            else:
                yield record

    result_rdd = rdd.mapPartitions(process_partition)

    clean_rdd = result_rdd.filter(lambda r: not r.get("__quarantined__", False))
    quarantine_count = result_rdd.filter(lambda r: r.get("__quarantined__", False)).count()

    return clean_rdd, quarantine_count


def quarantine_dataframe(
    df,
    pipeline: str,
    stage: str,
    **kwargs,
) -> Tuple:
    """Convenience wrapper for Spark DataFrames — same signature as quarantine_rdd."""
    clean_rdd, count = quarantine_rdd(df, pipeline, stage, **kwargs)

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            return spark.createDataFrame(clean_rdd), count
    except Exception:
        pass
    return clean_rdd, count
