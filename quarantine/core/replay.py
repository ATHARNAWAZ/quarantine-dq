from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from quarantine.core.models import QuarantineRecord, ReplayResult
from quarantine.stores.base import QuarantineStore
from quarantine.stores.factory import get_store

console = Console()
logger = logging.getLogger("quarantine.replay")


class ReplayEngine:
    def __init__(self, store: Optional[QuarantineStore] = None, store_url: Optional[str] = None):
        self._store = store or get_store(store_url)

    def replay(
        self,
        pipeline_name: str,
        transform_fn: Callable[[Dict[str, Any]], Any],
        failure_type: Optional[str] = None,
        since: Optional[datetime] = None,
        dry_run: bool = False,
        limit: int = 1000,
    ) -> ReplayResult:
        records = self._store.list(
            pipeline_name=pipeline_name,
            failure_type=failure_type,
            replay_status=None,
            since=since,
            limit=limit,
        )
        # Only replay unresolved records
        records = [r for r in records if r.replay_status is None]

        result = ReplayResult(pipeline_name=pipeline_name, dry_run=dry_run)
        result.attempted = len(records)

        if dry_run:
            console.print(
                f"[yellow][DRY RUN][/yellow] Would replay [bold]{len(records)}[/bold] "
                f"records from pipeline [bold]{pipeline_name}[/bold]"
            )
            for rec in records:
                console.print(
                    f"  • [dim]{rec.id[:8]}…[/dim] [{rec.failure_type}] {rec.failure_reason[:80]}"
                )
            return result

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Replaying {pipeline_name}…", total=len(records)
            )
            for rec in records:
                try:
                    transform_fn(rec.record_data)
                    self._store.mark_replayed(rec.id, "success")
                    result.succeeded += 1
                    logger.info("Replayed record %s successfully", rec.id)
                except Exception as exc:
                    self._store.mark_replayed(rec.id, "failed")
                    result.failed += 1
                    result.errors.append({"record_id": rec.id, "error": str(exc)})
                    logger.warning("Replay failed for record %s: %s", rec.id, exc)
                finally:
                    progress.advance(task)

        console.print(
            f"[bold]Replay complete:[/bold] "
            f"[green]{result.succeeded} succeeded[/green], "
            f"[red]{result.failed} failed[/red] "
            f"(of {result.attempted} attempted)"
        )
        return result

    def replay_one(
        self,
        record_id: str,
        transform_fn: Callable[[Dict[str, Any]], Any],
        dry_run: bool = False,
    ) -> bool:
        rec = self._store.get(record_id)

        if dry_run:
            console.print(
                f"[yellow][DRY RUN][/yellow] Would replay record [bold]{record_id[:8]}…[/bold]"
            )
            return True

        try:
            transform_fn(rec.record_data)
            self._store.mark_replayed(record_id, "success")
            return True
        except Exception as exc:
            self._store.mark_replayed(record_id, "failed")
            logger.warning("Replay failed for record %s: %s", record_id, exc)
            return False
