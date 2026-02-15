from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class JobState(str, Enum):
    QUEUED = "queued"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


@dataclass(slots=True)
class JobRecord:
    job_id: str
    zip_name: str
    zip_path: str
    state: JobState
    assigned_worker: str | None
    created_at: str
    queued_at: str
    dispatched_at: str | None
    completed_at: str | None
    failed_at: str | None
    last_error: str | None
    attempt_count: int


@dataclass(slots=True)
class WorkerSnapshot:
    name: str
    reachable: bool
    inbox_zips: list[str]
    outbox_zips: list[str]
    error: str | None = None
