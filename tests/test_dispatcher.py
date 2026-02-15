from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import logging
import unittest

from jobhopper.config import (
    AppConfig,
    DispatchConfig,
    LaunchConfig,
    PathsConfig,
    PollConfig,
    WorkerConfig,
)
from jobhopper.dispatcher import Dispatcher
from jobhopper.models import WorkerSnapshot
from jobhopper.store import Store
from jobhopper.utils import sha256_file


class FakeRemote:
    def __init__(self, worker: WorkerConfig) -> None:
        self.worker = worker
        self.inbox: list[str] = []
        self.outbox: list[str] = []

    def snapshot_worker(self, worker: WorkerConfig) -> WorkerSnapshot:
        return WorkerSnapshot(
            name=worker.name,
            reachable=True,
            inbox_zips=list(self.inbox),
            outbox_zips=list(self.outbox),
            error=None,
        )

    def copy_zip_to_worker(self, worker: WorkerConfig, local_zip: Path, remote_zip_name: str) -> str:
        self.inbox.append(remote_zip_name)
        return f"{worker.inbox_dir}/{remote_zip_name}"

    def prepare_and_launch(self, worker: WorkerConfig, job_id: str, remote_zip_path: str) -> None:
        _ = (worker, job_id, remote_zip_path)

    def fetch_result_zips(self, worker: WorkerConfig, result_zip_names: list[str], local_inbox_dir: Path) -> list[Path]:
        _ = worker
        paths: list[Path] = []
        for zip_name in result_zip_names:
            path = local_inbox_dir / zip_name
            path.write_bytes(b"artifact")
            paths.append(path)
        return paths


class DispatcherLifecycleTest(unittest.TestCase):
    def test_dispatch_and_complete(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            paths = PathsConfig(
                inbox=root / "inbox",
                queue=root / "queue",
                running=root / "running",
                done=root / "done",
                failed=root / "failed",
                db=root / "jobhopper.db",
                log=root / "jobhopper.log",
            )
            for directory in [paths.inbox, paths.queue, paths.running, paths.done, paths.failed]:
                directory.mkdir(parents=True, exist_ok=True)

            worker = WorkerConfig(
                name="worker-a",
                host="example.com",
                user="alice",
                inbox_dir="/tmp/Inbox",
                outbox_dir="/tmp/Outbox",
            )
            config = AppConfig(
                paths=paths,
                poll=PollConfig(interval_seconds=1, settle_seconds=0),
                dispatch=DispatchConfig(
                    ssh_options="",
                    transfer_mode="scp",
                    remote_unpack_dirname="job",
                    launch=LaunchConfig(command_template="echo hi"),
                ),
                workers=[worker],
            )

            input_zip = paths.queue / "model.zip"
            input_zip.write_bytes(b"payload")
            job_id = sha256_file(input_zip)

            store = Store(paths.db)
            store.init_schema()
            remote = FakeRemote(worker)
            logger = logging.getLogger("test_jobhopper")
            logger.handlers.clear()
            logger.addHandler(logging.NullHandler())

            dispatcher = Dispatcher(config, store, remote, logger)
            dispatcher.single_cycle()

            running_job = store.get_job(job_id)
            assert running_job is not None
            self.assertEqual(running_job.state.value, "running")
            self.assertTrue((paths.running / "model.zip").exists())

            remote.inbox.clear()
            remote.outbox = [f"{job_id}.metrics.zip"]
            dispatcher.single_cycle()

            done_job = store.get_job(job_id)
            assert done_job is not None
            self.assertEqual(done_job.state.value, "done")
            self.assertTrue((paths.done / "model.zip").exists())
            self.assertTrue((paths.inbox / f"{job_id}.metrics.zip").exists())
            store.close()


if __name__ == "__main__":
    unittest.main()
