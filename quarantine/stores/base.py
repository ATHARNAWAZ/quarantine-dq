from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from quarantine.core.models import QuarantineRecord


class QuarantineStore(ABC):
    @abstractmethod
    def save(self, record: QuarantineRecord) -> None: ...

    @abstractmethod
    def get(self, record_id: str) -> QuarantineRecord: ...

    @abstractmethod
    def list(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[QuarantineRecord]: ...

    @abstractmethod
    def mark_replayed(self, record_id: str, status: str) -> None: ...

    @abstractmethod
    def stats(self, pipeline_name: Optional[str] = None) -> Dict[str, Any]: ...

    @abstractmethod
    def delete(self, record_id: str) -> None: ...

    @abstractmethod
    def count(
        self,
        pipeline_name: Optional[str] = None,
        failure_type: Optional[str] = None,
        replay_status: Optional[str] = None,
    ) -> int: ...
