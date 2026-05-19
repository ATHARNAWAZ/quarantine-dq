"""
Example: PySpark events pipeline with quarantine-dq

Simulates a clickstream/events pipeline that validates event records.
Requires PySpark: uv pip install pyspark

Run:
    uv run python examples/spark_events_pipeline.py
"""

import os
import sys

os.environ.setdefault("QUARANTINE_STORE_URL", "duckdb://./example_quarantine.duckdb")

EVENTS_DATA = [
    {"event_id": "e001", "user_id": "u1001", "event_type": "page_view",  "session_id": "s001", "duration_ms": 2500},
    {"event_id": "e002", "user_id": "u1002", "event_type": "click",      "session_id": "s002", "duration_ms": 150},
    {"event_id": "e003", "user_id": None,    "event_type": "purchase",   "session_id": "s003", "duration_ms": 5000},  # null user
    {"event_id": None,   "user_id": "u1004", "event_type": "page_view",  "session_id": "s004", "duration_ms": 800},   # null event_id
    {"event_id": "e005", "user_id": "u1005", "event_type": "UNKNOWN",    "session_id": "s005", "duration_ms": 100},   # invalid type
    {"event_id": "e006", "user_id": "u1006", "event_type": "click",      "session_id": "s006", "duration_ms": -999},  # negative duration
    {"event_id": "e007", "user_id": "u1007", "event_type": "page_view",  "session_id": "s007", "duration_ms": 3200},
    {"event_id": "e008", "user_id": "u1008", "event_type": "purchase",   "session_id": "s008", "duration_ms": 12000},
]


def main():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("PySpark not installed. Install with: uv pip install pyspark")
        print("Showing example structure only.\n")
        print("quarantine_rdd(rdd, pipeline='events_etl', stage='validate',")
        print("    not_null=['event_id', 'user_id'],")
        print("    pk_fields=['event_id'],")
        print("    range_constraints={'duration_ms': (0, 60000)},")
        print("    rules=[AllowedValues('event_type', ['page_view', 'click', 'purchase'])])")
        return

    from quarantine.adapters.spark import quarantine_rdd
    from quarantine.rules.business import AllowedValues

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("quarantine-events-example")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    rdd = spark.sparkContext.parallelize(EVENTS_DATA)

    clean_rdd, quarantine_count = quarantine_rdd(
        rdd,
        pipeline="events_etl",
        stage="validate_events",
        not_null=["event_id", "user_id"],
        pk_fields=["event_id"],
        range_constraints={"duration_ms": (0, 60000)},
        rules=[AllowedValues("event_type", ["page_view", "click", "purchase"])],
    )

    clean_records = clean_rdd.collect()
    print(f"\nInput records:      {len(EVENTS_DATA)}")
    print(f"Clean records:      {len(clean_records)}")
    print(f"Quarantined:        {quarantine_count}")
    print("\nClean event IDs:", [r.get("event_id") for r in clean_records])
    print("\nTo inspect: uv run quarantine inspect --pipeline events_etl")

    spark.stop()


if __name__ == "__main__":
    main()
