"""
Example: Airflow DAG with QuarantineOperator

Shows how to wrap an Airflow task with quarantine semantics.
Failed records go to the quarantine store; the DAG does NOT fail.

Requires Airflow: uv pip install apache-airflow (note: separate install due to
SQLAlchemy version constraints — see README for details)

This file can also be run standalone (without Airflow installed) to see
the quarantine logic:
    uv run python examples/airflow_dag_example.py
"""

import os

os.environ.setdefault("QUARANTINE_STORE_URL", "duckdb://./example_quarantine.duckdb")

CUSTOMER_RECORDS = [
    {"customer_id": "C001", "name": "Alice Johnson",  "email": "alice@example.com",  "age": 34, "tier": "gold"},
    {"customer_id": "C002", "name": "Bob Smith",      "email": "bob@example.com",    "age": 28, "tier": "silver"},
    {"customer_id": "C003", "name": "Carol White",    "email": "carol@example.com",  "age": -5, "tier": "gold"},    # invalid age
    {"customer_id": None,   "name": "Dave Brown",     "email": "dave@example.com",   "age": 45, "tier": "bronze"},  # null id
    {"customer_id": "C005", "name": "",               "email": "eve@example.com",    "age": 31, "tier": "gold"},    # empty name
    {"customer_id": "C006", "name": "Frank Miller",   "email": "not-an-email",       "age": 52, "tier": "platinum"}, # bad email + invalid tier
    {"customer_id": "C007", "name": "Grace Lee",      "email": "grace@example.com",  "age": 29, "tier": "silver"},
]


def transform_customer(record: dict) -> dict:
    """Enrich a valid customer record."""
    return {**record, "processed": True, "name_upper": record["name"].upper()}


def fetch_customers(context: dict) -> list:
    """Simulates fetching records from a source system."""
    return CUSTOMER_RECORDS


def run_standalone():
    """Run without Airflow — uses QuarantineContext directly."""
    from quarantine.core.context import QuarantineContext
    from quarantine.rules.business import AllowedValues, NonEmptyString

    print("Running airflow_dag_example in standalone mode (no Airflow required)\n")

    with QuarantineContext(
        pipeline="customers_etl",
        stage="validate_customers",
        not_null=["customer_id"],
        range_constraints={"age": (0, 120)},
        pattern_constraints={"email": r"[^@]+@[^@]+\.[^@]+"},
        rules=[
            NonEmptyString("name"),
            AllowedValues("tier", ["bronze", "silver", "gold"]),
        ],
    ) as ctx:
        for record in CUSTOMER_RECORDS:
            result = ctx.process(record, transform_customer)
            if result:
                print(f"  Processed: {result['customer_id']} -> {result['name_upper']}")

    print(f"\nTo review failures: uv run quarantine inspect --pipeline customers_etl")


def build_dag():
    """Build the Airflow DAG (only runs when Airflow is available)."""
    from airflow import DAG
    from airflow.utils.dates import days_ago
    from quarantine.adapters.airflow import QuarantineOperator
    from quarantine.rules.business import AllowedValues, NonEmptyString

    with DAG(
        dag_id="customers_etl",
        start_date=days_ago(1),
        schedule_interval="@daily",
        catchup=False,
        tags=["quarantine-dq", "example"],
    ) as dag:
        validate_customers = QuarantineOperator(
            task_id="validate_customers",
            callable=transform_customer,
            records_callable=fetch_customers,
            pipeline="customers_etl",
            stage="validate_customers",
            not_null=["customer_id"],
            range_constraints={"age": (0, 120)},
            pattern_constraints={"email": r"[^@]+@[^@]+\.[^@]+"},
            rules=[
                NonEmptyString("name"),
                AllowedValues("tier", ["bronze", "silver", "gold"]),
            ],
            dag=dag,
        )

    return dag


if __name__ == "__main__":
    try:
        import airflow  # noqa
        dag = build_dag()
        print(f"DAG '{dag.dag_id}' built successfully.")
    except ImportError:
        run_standalone()
