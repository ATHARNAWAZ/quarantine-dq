"""
Example: pandas orders pipeline with quarantine-dq

Simulates an e-commerce orders ETL pipeline that validates records
and routes bad data to the quarantine store instead of crashing.

Run:
    uv run python examples/pandas_orders_pipeline.py
"""

import os
import sys

import pandas as pd
from rich.console import Console

# Point to a local test DuckDB so the example stays self-contained
os.environ.setdefault("QUARANTINE_STORE_URL", "duckdb://./example_quarantine.duckdb")

from quarantine.adapters.pandas import quarantine_dataframe
from quarantine.core.replay import ReplayEngine
from quarantine.rules.business import AllowedValues, BusinessRule, PositiveRevenue

console = Console()

# --------------------------------------------------------------------------- #
# Sample data — realistic e-commerce orders, with intentional bad rows        #
# --------------------------------------------------------------------------- #

ORDERS_DATA = [
    {"order_id": 1001, "customer_id": "C001", "revenue": 149.99, "status": "confirmed", "currency": "USD"},
    {"order_id": 1002, "customer_id": "C002", "revenue": 89.50,  "status": "pending",   "currency": "USD"},
    {"order_id": 1003, "customer_id": None,   "revenue": 299.00, "status": "confirmed", "currency": "USD"},  # null customer
    {"order_id": 1004, "customer_id": "C004", "revenue": -45.00, "status": "confirmed", "currency": "USD"},  # negative revenue
    {"order_id": 1005, "customer_id": "C005", "revenue": 59.99,  "status": "DELETED",   "currency": "USD"},  # invalid status
    {"order_id": 1006, "customer_id": "C006", "revenue": 199.00, "status": "shipped",   "currency": "USD"},
    {"order_id": None, "customer_id": "C007", "revenue": 79.00,  "status": "pending",   "currency": "USD"},  # null order_id
    {"order_id": 1008, "customer_id": "C008", "revenue": 450.00, "status": "delivered", "currency": "EUR"},  # wrong currency
    {"order_id": 1009, "customer_id": "C009", "revenue": 32.50,  "status": "confirmed", "currency": "USD"},
    {"order_id": 1010, "customer_id": "C010", "revenue": 175.00, "status": "cancelled", "currency": "USD"},
]


def main():
    console.rule("[bold blue]quarantine-dq — Pandas Orders Pipeline Example[/bold blue]")

    df = pd.DataFrame(ORDERS_DATA)
    console.print(f"\n[bold]Input:[/bold] {len(df)} order records\n")

    class USDCurrencyOnly(BusinessRule):
        def validate(self, record):
            if record.get("currency") != "USD":
                return f"Currency '{record.get('currency')}' not supported — only USD accepted"
            return None

    clean_df, quarantined_df = quarantine_dataframe(
        df,
        pipeline="orders_etl",
        stage="validate_orders",
        schema={"order_id": (int, float), "revenue": float},
        not_null=["order_id", "customer_id"],
        pk_fields=["order_id"],
        range_constraints={"revenue": (0, 10000)},
        rules=[
            PositiveRevenue(),
            AllowedValues("status", ["pending", "confirmed", "shipped", "delivered", "cancelled"]),
            USDCurrencyOnly(),
        ],
        tags={"source": "orders_api", "env": "example"},
    )

    console.print(f"\n[green bold]Clean records:[/green bold] {len(clean_df)}")
    console.print(clean_df[["order_id", "customer_id", "revenue", "status"]].to_string(index=False))

    if len(quarantined_df) > 0:
        console.print(f"\n[red bold]Quarantined records:[/red bold] {len(quarantined_df)}")
        cols = [c for c in ["order_id", "customer_id", "__failure_type", "__failure_reason"] if c in quarantined_df.columns]
        console.print(quarantined_df[cols].to_string(index=False))

    console.print("\n[bold]To inspect quarantined records:[/bold]")
    console.print("  uv run quarantine inspect --pipeline orders_etl")
    console.print("\n[bold]To replay after fixes:[/bold]")
    console.print("  uv run quarantine replay --pipeline orders_etl --dry-run")

    console.rule("[bold blue]Done[/bold blue]")


if __name__ == "__main__":
    main()
