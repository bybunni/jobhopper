from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

from .app_logging import log_with_fields
from .config import AppConfig, WorkerConfig
from .models import JobRecord, JobState, WorkerSnapshot
from .remote import RemoteError, RemoteExecutor
from .store import Store
from .utils import is_zip_file, parse_job_id_from_zip_name, sha256_file, utc_now_iso


class Dispatcher:
    def __init__(
        self,
        config: AppConfig,
        store: Store,
        remote: RemoteExecutor,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.store = store
        self.remote = remote
        self.logger = logger
        self.empty_since: dict[str, float] = {}
        self.workers_by_name = {worker.name: worker for worker in config.workers}

    def run_forever(self) -> None:
        self.bootstrap()
        while True:
            self.single_cycle()
            time.sleep(self.config.poll.interval_seconds)

    def run_once(self) -> None:
        self.bootstrap()
        self.single_cycle()

    def bootstrap(self) -> None:
        self.ingest_queue()
        snapshots = self.poll_workers()
        self.reconcile_workers(snapshots)
        self._recover_dispatching_jobs()

    def single_cycle(self) -> None:
        self.ingest_queue()
        snapshots = self.poll_workers()
        self.reconcile_workers(snapshots)
        self.dispatch_jobs(snapshots)

    def poll_workers(self) -> dict[str, WorkerSnapshot]:
        snapshots: dict[str, WorkerSnapshot] = {}
        for worker in self.config.workers:
            snapshot = self.remote.snapshot_worker(worker)
            snapshots[worker.name] = snapshot
            if snapshot.reachable:
                log_with_fields(
                    self.logger,
                    logging.INFO,
                    "worker_snapshot",
                    worker=worker.name,
                    inbox_zips=snapshot.inbox_zips,
                    outbox_zips=snapshot.outbox_zips,
                )
            else:
                log_with_fields(
                    self.logger,
                    logging.WARNING,
                    "worker_unreachable",
                    worker=worker.name,
                    error=snapshot.error,
                )
        return snapshots

    def ingest_queue(self) -> None:
        queue_files = sorted(
            [path for path in self.config.paths.queue.iterdir() if is_zip_file(path)],
            key=lambda p: p.stat().st_mtime,
        )
        for zip_path in queue_files:
            job_id = sha256_file(zip_path)
            inserted = self.store.insert_job_if_missing(job_id, zip_path.name, str(zip_path))
            if inserted:
                self.store.add_event(
                    job_id,
                    "queued",
                    {"zip_path": str(zip_path)},
                    worker_name=None,
                )
                log_with_fields(
                    self.logger,
                    logging.INFO,
                    "job_queued",
                    job_id=job_id,
                    zip_path=str(zip_path),
                )
                continue

            existing = self.store.get_job(job_id)
            duplicate_path = self._move_non_destructive(
                zip_path,
                self.config.paths.done,
                preferred_name=f"{zip_path.stem}.duplicate.{zip_path.suffix.lstrip('.')}",
            )
            if existing is not None:
                self.store.add_event(
                    existing.job_id,
                    "duplicate_ignored",
                    {"dropped_zip_path": str(zip_path), "archived_to": str(duplicate_path)},
                    worker_name=None,
                )
                log_with_fields(
                    self.logger,
                    logging.INFO,
                    "duplicate_job_ignored",
                    job_id=existing.job_id,
                    dropped_zip=str(zip_path),
                    archived_to=str(duplicate_path),
                )

    def reconcile_workers(self, snapshots: dict[str, WorkerSnapshot]) -> None:
        for worker in self.config.workers:
            snapshot = snapshots[worker.name]
            running = self.store.get_running_job_for_worker(worker.name)

            if not snapshot.reachable:
                self.store.update_worker_state(
                    worker.name,
                    status="unreachable",
                    current_job_id=running.job_id if running else None,
                    last_error=snapshot.error,
                )
                continue

            if snapshot.inbox_zips:
                self.empty_since.pop(worker.name, None)
                if running is None:
                    self._adopt_running_from_inbox(worker, snapshot.inbox_zips)
                    running = self.store.get_running_job_for_worker(worker.name)
                self.store.update_worker_state(
                    worker.name,
                    status="busy",
                    current_job_id=running.job_id if running else None,
                    last_error=None,
                )
                continue

            now = time.monotonic()
            if worker.name not in self.empty_since:
                self.empty_since[worker.name] = now
            elapsed = now - self.empty_since[worker.name]
            if elapsed < self.config.poll.settle_seconds:
                self.store.update_worker_state(
                    worker.name,
                    status="busy" if running else "free",
                    current_job_id=running.job_id if running else None,
                    last_error=None,
                )
                continue

            if running is not None:
                self._try_complete_running_job(worker, snapshot, running)
                refreshed = self.store.get_running_job_for_worker(worker.name)
                self.store.update_worker_state(
                    worker.name,
                    status="busy" if refreshed else "free",
                    current_job_id=refreshed.job_id if refreshed else None,
                    last_error=None,
                )
            else:
                self.store.update_worker_state(
                    worker.name,
                    status="free",
                    current_job_id=None,
                    last_error=None,
                )

    def dispatch_jobs(self, snapshots: dict[str, WorkerSnapshot]) -> None:
        for worker in self.config.workers:
            snapshot = snapshots[worker.name]
            if not snapshot.reachable:
                continue
            if snapshot.inbox_zips:
                continue
            if worker.name not in self.empty_since:
                continue
            settled = (time.monotonic() - self.empty_since[worker.name]) >= self.config.poll.settle_seconds
            if not settled:
                continue
            if self.store.get_running_job_for_worker(worker.name) is not None:
                continue

            queued = self.store.get_oldest_queued_job()
            if queued is None:
                return
            self._dispatch_single_job(worker, queued)

    def _dispatch_single_job(self, worker: WorkerConfig, job: JobRecord) -> None:
        zip_path = Path(job.zip_path)
        if not zip_path.exists():
            self.store.update_job(
                job.job_id,
                state=JobState.FAILED,
                failed_at=utc_now_iso(),
                last_error=f"source zip missing: {zip_path}",
            )
            self.store.add_event(
                job.job_id,
                "dispatch_failed_missing_zip",
                {"zip_path": str(zip_path)},
                worker_name=worker.name,
            )
            return

        next_attempt = job.attempt_count + 1
        self.store.update_job(
            job.job_id,
            state=JobState.DISPATCHING,
            assigned_worker=worker.name,
            attempt_count=next_attempt,
        )
        self.store.add_event(job.job_id, "dispatch_started", {"attempt": next_attempt}, worker_name=worker.name)
        try:
            remote_zip_path = self.remote.copy_zip_to_worker(worker, zip_path, f"{job.job_id}.zip")
            self.remote.prepare_and_launch(worker, job.job_id, remote_zip_path)
            running_zip = self._move_non_destructive(zip_path, self.config.paths.running)
            self.store.update_job(
                job.job_id,
                state=JobState.RUNNING,
                zip_name=running_zip.name,
                zip_path=str(running_zip),
                assigned_worker=worker.name,
                dispatched_at=utc_now_iso(),
                last_error="",
            )
            self.store.add_event(
                job.job_id,
                "dispatched",
                {"worker": worker.name, "remote_zip_path": remote_zip_path},
                worker_name=worker.name,
            )
            log_with_fields(
                self.logger,
                logging.INFO,
                "job_dispatched",
                job_id=job.job_id,
                worker=worker.name,
                remote_zip_path=remote_zip_path,
            )
        except (RemoteError, OSError) as exc:
            failed_zip = zip_path
            if zip_path.exists():
                failed_zip = self._move_non_destructive(zip_path, self.config.paths.failed)
            self.store.update_job(
                job.job_id,
                state=JobState.FAILED,
                zip_name=failed_zip.name,
                zip_path=str(failed_zip),
                failed_at=utc_now_iso(),
                last_error=str(exc),
            )
            self.store.add_event(
                job.job_id,
                "dispatch_failed",
                {"error": str(exc)},
                worker_name=worker.name,
            )
            log_with_fields(
                self.logger,
                logging.ERROR,
                "job_dispatch_failed",
                job_id=job.job_id,
                worker=worker.name,
                error=str(exc),
            )

    def _try_complete_running_job(
        self,
        worker: WorkerConfig,
        snapshot: WorkerSnapshot,
        running: JobRecord,
    ) -> None:
        artifact_zips = sorted(
            [zip_name for zip_name in snapshot.outbox_zips if zip_name.startswith(running.job_id)]
        )
        if not artifact_zips:
            self.store.add_event(
                running.job_id,
                "completion_waiting_artifacts",
                {"worker_outbox_zips": snapshot.outbox_zips},
                worker_name=worker.name,
            )
            log_with_fields(
                self.logger,
                logging.INFO,
                "job_waiting_for_artifacts",
                job_id=running.job_id,
                worker=worker.name,
            )
            return

        self.remote.fetch_result_zips(worker, artifact_zips, self.config.paths.inbox)
        local_running_zip = Path(running.zip_path)
        done_zip = local_running_zip
        if local_running_zip.exists():
            done_zip = self._move_non_destructive(local_running_zip, self.config.paths.done)

        self.store.update_job(
            running.job_id,
            state=JobState.DONE,
            zip_name=done_zip.name,
            zip_path=str(done_zip),
            completed_at=utc_now_iso(),
            last_error="",
        )
        self.store.add_event(
            running.job_id,
            "completed",
            {"artifacts": artifact_zips, "copied_to": str(self.config.paths.inbox)},
            worker_name=worker.name,
        )
        log_with_fields(
            self.logger,
            logging.INFO,
            "job_completed",
            job_id=running.job_id,
            worker=worker.name,
            artifacts=artifact_zips,
        )

    def _recover_dispatching_jobs(self) -> None:
        dispatching_jobs = self.store.list_dispatching_jobs()
        for job in dispatching_jobs:
            if not job.assigned_worker:
                self.store.update_job(
                    job.job_id,
                    state=JobState.FAILED,
                    failed_at=utc_now_iso(),
                    last_error="dispatcher restarted while dispatching without worker assignment",
                )
                continue
            worker = self.workers_by_name.get(job.assigned_worker)
            if worker is None:
                self.store.update_job(
                    job.job_id,
                    state=JobState.FAILED,
                    failed_at=utc_now_iso(),
                    last_error=f"unknown assigned worker: {job.assigned_worker}",
                )
                continue
            snapshot = self.remote.snapshot_worker(worker)
            expected_name = f"{job.job_id}.zip"
            if expected_name in snapshot.inbox_zips:
                self.store.update_job(job.job_id, state=JobState.RUNNING)
                self.store.add_event(
                    job.job_id,
                    "recovered_dispatching_to_running",
                    {"worker": worker.name},
                    worker_name=worker.name,
                )
            else:
                self.store.update_job(
                    job.job_id,
                    state=JobState.FAILED,
                    failed_at=utc_now_iso(),
                    last_error="dispatcher restarted during dispatch; remote zip not found",
                )
                self.store.add_event(
                    job.job_id,
                    "recovered_dispatching_failed",
                    {"worker": worker.name},
                    worker_name=worker.name,
                )

    def _adopt_running_from_inbox(self, worker: WorkerConfig, inbox_zips: list[str]) -> None:
        for zip_name in sorted(inbox_zips):
            inferred_job_id = parse_job_id_from_zip_name(zip_name)
            if not inferred_job_id:
                continue
            job = self.store.get_job(inferred_job_id)
            if job is None:
                self.store.insert_job_if_missing(
                    inferred_job_id,
                    zip_name=zip_name,
                    zip_path=f"remote://{worker.name}/{worker.inbox_dir}/{zip_name}",
                )
                self.store.update_job(
                    inferred_job_id,
                    state=JobState.RUNNING,
                    assigned_worker=worker.name,
                    dispatched_at=utc_now_iso(),
                )
                self.store.add_event(
                    inferred_job_id,
                    "recovered_remote_running",
                    {"worker": worker.name, "zip_name": zip_name},
                    worker_name=worker.name,
                )
            else:
                self.store.update_job(
                    inferred_job_id,
                    state=JobState.RUNNING,
                    assigned_worker=worker.name,
                    dispatched_at=job.dispatched_at or utc_now_iso(),
                )
                self.store.add_event(
                    inferred_job_id,
                    "adopted_running_from_inbox",
                    {"worker": worker.name, "zip_name": zip_name},
                    worker_name=worker.name,
                )
            log_with_fields(
                self.logger,
                logging.INFO,
                "adopted_running_job",
                worker=worker.name,
                job_id=inferred_job_id,
                zip_name=zip_name,
            )
            return

    def _move_non_destructive(self, src: Path, dst_dir: Path, preferred_name: str | None = None) -> Path:
        dst_dir.mkdir(parents=True, exist_ok=True)
        base_name = preferred_name or src.name
        destination = dst_dir / base_name
        if not destination.exists():
            shutil.move(str(src), str(destination))
            return destination

        stem = destination.stem
        suffix = destination.suffix
        index = 1
        while True:
            candidate = dst_dir / f"{stem}.{index}{suffix}"
            if not candidate.exists():
                shutil.move(str(src), str(candidate))
                return candidate
            index += 1
