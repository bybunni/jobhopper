from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .models import JobRecord, JobState
from .utils import utc_now_iso


def _row_to_job(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        job_id=row["job_id"],
        zip_name=row["zip_name"],
        zip_path=row["zip_path"],
        state=JobState(row["state"]),
        assigned_worker=row["assigned_worker"],
        created_at=row["created_at"],
        queued_at=row["queued_at"],
        dispatched_at=row["dispatched_at"],
        completed_at=row["completed_at"],
        failed_at=row["failed_at"],
        last_error=row["last_error"],
        attempt_count=int(row["attempt_count"]),
    )


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                zip_name TEXT NOT NULL,
                zip_path TEXT NOT NULL,
                state TEXT NOT NULL,
                assigned_worker TEXT,
                created_at TEXT NOT NULL,
                queued_at TEXT NOT NULL,
                dispatched_at TEXT,
                completed_at TEXT,
                failed_at TEXT,
                last_error TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                worker_name TEXT,
                timestamp TEXT NOT NULL,
                details_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_state (
                worker_name TEXT PRIMARY KEY,
                last_seen_at TEXT NOT NULL,
                status TEXT NOT NULL,
                current_job_id TEXT,
                last_error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_state_queued_at
                ON jobs(state, queued_at);
            CREATE INDEX IF NOT EXISTS idx_job_events_job_timestamp
                ON job_events(job_id, timestamp);
            """
        )
        self.conn.commit()

    def add_event(
        self,
        job_id: str,
        event_type: str,
        details: dict[str, Any] | None = None,
        worker_name: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO job_events(job_id, event_type, worker_name, timestamp, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (job_id, event_type, worker_name, utc_now_iso(), json.dumps(details or {}, sort_keys=True)),
        )
        self.conn.commit()

    def get_job(self, job_id: str) -> JobRecord | None:
        row = self.conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return _row_to_job(row)

    def insert_job_if_missing(self, job_id: str, zip_name: str, zip_path: str) -> bool:
        now = utc_now_iso()
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO jobs(
                job_id, zip_name, zip_path, state, assigned_worker, created_at, queued_at,
                dispatched_at, completed_at, failed_at, last_error, attempt_count
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, NULL, 0)
            """,
            (job_id, zip_name, zip_path, JobState.QUEUED.value, now, now),
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def update_job(
        self,
        job_id: str,
        *,
        state: JobState | None = None,
        zip_name: str | None = None,
        zip_path: str | None = None,
        assigned_worker: str | None = None,
        clear_assigned_worker: bool = False,
        dispatched_at: str | None = None,
        completed_at: str | None = None,
        failed_at: str | None = None,
        last_error: str | None = None,
        attempt_count: int | None = None,
    ) -> None:
        updates: list[str] = []
        values: list[object] = []
        if state is not None:
            updates.append("state = ?")
            values.append(state.value)
        if zip_name is not None:
            updates.append("zip_name = ?")
            values.append(zip_name)
        if zip_path is not None:
            updates.append("zip_path = ?")
            values.append(zip_path)
        if clear_assigned_worker:
            updates.append("assigned_worker = NULL")
        elif assigned_worker is not None:
            updates.append("assigned_worker = ?")
            values.append(assigned_worker)
        if dispatched_at is not None:
            updates.append("dispatched_at = ?")
            values.append(dispatched_at)
        if completed_at is not None:
            updates.append("completed_at = ?")
            values.append(completed_at)
        if failed_at is not None:
            updates.append("failed_at = ?")
            values.append(failed_at)
        if last_error is not None:
            updates.append("last_error = ?")
            values.append(last_error)
        if attempt_count is not None:
            updates.append("attempt_count = ?")
            values.append(attempt_count)
        if not updates:
            return
        values.append(job_id)
        query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"
        self.conn.execute(query, values)
        self.conn.commit()

    def get_running_job_for_worker(self, worker_name: str) -> JobRecord | None:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE state = ? AND assigned_worker = ? ORDER BY queued_at LIMIT 1",
            (JobState.RUNNING.value, worker_name),
        ).fetchone()
        if row is None:
            return None
        return _row_to_job(row)

    def list_jobs_by_state(self, state: JobState) -> list[JobRecord]:
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY queued_at",
            (state.value,),
        ).fetchall()
        return [_row_to_job(row) for row in rows]

    def list_dispatching_jobs(self) -> list[JobRecord]:
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY queued_at",
            (JobState.DISPATCHING.value,),
        ).fetchall()
        return [_row_to_job(row) for row in rows]

    def get_oldest_queued_job(self) -> JobRecord | None:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY queued_at LIMIT 1",
            (JobState.QUEUED.value,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_job(row)

    def update_worker_state(
        self,
        worker_name: str,
        *,
        status: str,
        current_job_id: str | None,
        last_error: str | None,
    ) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """
            INSERT INTO worker_state(worker_name, last_seen_at, status, current_job_id, last_error)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(worker_name) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                status = excluded.status,
                current_job_id = excluded.current_job_id,
                last_error = excluded.last_error
            """,
            (worker_name, now, status, current_job_id, last_error),
        )
        self.conn.commit()

    def summary_counts(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT state, COUNT(*) AS count FROM jobs GROUP BY state").fetchall()
        output = {state.value: 0 for state in JobState}
        for row in rows:
            output[str(row["state"])] = int(row["count"])
        return output

    def list_worker_states(self) -> list[dict[str, str | None]]:
        rows = self.conn.execute(
            """
            SELECT worker_name, last_seen_at, status, current_job_id, last_error
            FROM worker_state
            ORDER BY worker_name
            """
        ).fetchall()
        output: list[dict[str, str | None]] = []
        for row in rows:
            output.append(
                {
                    "worker_name": row["worker_name"],
                    "last_seen_at": row["last_seen_at"],
                    "status": row["status"],
                    "current_job_id": row["current_job_id"],
                    "last_error": row["last_error"],
                }
            )
        return output

    def requeue_job(self, job_id: str, zip_path: str, zip_name: str) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """
            UPDATE jobs
            SET state = ?,
                zip_path = ?,
                zip_name = ?,
                assigned_worker = NULL,
                queued_at = ?,
                dispatched_at = NULL,
                completed_at = NULL,
                failed_at = NULL,
                last_error = NULL
            WHERE job_id = ?
            """,
            (JobState.QUEUED.value, zip_path, zip_name, now, job_id),
        )
        self.conn.commit()
