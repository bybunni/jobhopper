from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .app_logging import log_with_fields, setup_logger
from .config import AppConfig, ensure_local_paths, load_config
from .dispatcher import Dispatcher
from .remote import RemoteExecutor
from .store import Store
from .utils import utc_now_iso


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jobhopper", description="Lightweight SSH job dispatcher")
    parser.add_argument("--config", required=True, help="Path to jobhopper YAML config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run dispatcher polling loop")
    run_parser.add_argument(
        "--once",
        action="store_true",
        help="Run bootstrap + one polling cycle, then exit",
    )
    subparsers.add_parser("scan", help="Run exactly one polling cycle")
    subparsers.add_parser("status", help="Show queue and worker status")

    requeue = subparsers.add_parser("requeue", help="Manually requeue a job")
    requeue.add_argument("--job-id", required=True, help="Job id to requeue")
    return parser


def _open_runtime(config: AppConfig) -> tuple[Store, Dispatcher]:
    ensure_local_paths(config)
    logger = setup_logger(config.paths.log)
    store = Store(config.paths.db)
    store.init_schema()
    remote = RemoteExecutor(config.dispatch)
    dispatcher = Dispatcher(config=config, store=store, remote=remote, logger=logger)
    return store, dispatcher


def cmd_run(config: AppConfig, *, once: bool = False) -> int:
    store, dispatcher = _open_runtime(config)
    try:
        if once:
            dispatcher.run_once()
            return 0
        dispatcher.run_forever()
    except KeyboardInterrupt:
        log_with_fields(logging.getLogger("jobhopper"), logging.INFO, "shutdown", reason="keyboard_interrupt")
        return 0
    finally:
        store.close()


def cmd_scan(config: AppConfig) -> int:
    store, dispatcher = _open_runtime(config)
    try:
        dispatcher.bootstrap()
        dispatcher.single_cycle()
        return 0
    finally:
        store.close()


def cmd_status(config: AppConfig) -> int:
    ensure_local_paths(config)
    store = Store(config.paths.db)
    try:
        store.init_schema()
        counts = store.summary_counts()
        print("Jobs:")
        for state in ["queued", "dispatching", "running", "done", "failed"]:
            print(f"  {state:12} {counts.get(state, 0)}")

        print("\nWorkers:")
        states = store.list_worker_states()
        if not states:
            print("  (no worker state yet)")
        for row in states:
            error = f" error={row['last_error']}" if row["last_error"] else ""
            print(
                "  "
                f"{row['worker_name']}: status={row['status']} "
                f"job={row['current_job_id']} seen={row['last_seen_at']}{error}"
            )
        return 0
    finally:
        store.close()


def _move_to_queue_non_destructive(src: Path, queue_dir: Path) -> Path:
    queue_dir.mkdir(parents=True, exist_ok=True)
    destination = queue_dir / src.name
    if not destination.exists():
        src.replace(destination)
        return destination

    stem = destination.stem
    suffix = destination.suffix
    index = 1
    while True:
        candidate = queue_dir / f"{stem}.requeue{index}{suffix}"
        if not candidate.exists():
            src.replace(candidate)
            return candidate
        index += 1


def cmd_requeue(config: AppConfig, job_id: str) -> int:
    ensure_local_paths(config)
    store = Store(config.paths.db)
    try:
        store.init_schema()
        job = store.get_job(job_id)
        if job is None:
            print(f"job not found: {job_id}", file=sys.stderr)
            return 2
        if job.state.value not in {"failed", "done"}:
            print(f"job state must be failed or done to requeue, found: {job.state.value}", file=sys.stderr)
            return 2

        source = Path(job.zip_path)
        if not source.exists():
            print(f"job zip not found on disk: {source}", file=sys.stderr)
            return 2

        queued_zip = _move_to_queue_non_destructive(source, config.paths.queue)
        store.requeue_job(job_id, str(queued_zip), queued_zip.name)
        store.add_event(
            job_id,
            "manually_requeued",
            {"queued_zip": str(queued_zip), "at": utc_now_iso()},
            worker_name=None,
        )
        print(f"requeued {job_id} -> {queued_zip}")
        return 0
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)

    if args.command == "run":
        return cmd_run(config, once=bool(args.once))
    if args.command == "scan":
        return cmd_scan(config)
    if args.command == "status":
        return cmd_status(config)
    if args.command == "requeue":
        return cmd_requeue(config, args.job_id)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
