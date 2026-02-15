from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

from .config import DispatchConfig, WorkerConfig
from .models import WorkerSnapshot


class RemoteError(RuntimeError):
    pass


class RemoteExecutor:
    def __init__(self, dispatch_config: DispatchConfig) -> None:
        self.dispatch_config = dispatch_config
        self.ssh_options = shlex.split(dispatch_config.ssh_options)

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(cmd, capture_output=True, text=True, check=False)

    def _ssh(self, worker: WorkerConfig, script: str) -> subprocess.CompletedProcess[str]:
        cmd = ["ssh", *self.ssh_options, worker.address, "bash", "-lc", script]
        return self._run(cmd)

    def _require_ok(self, process: subprocess.CompletedProcess[str], context: str) -> None:
        if process.returncode != 0:
            stderr = process.stderr.strip()
            stdout = process.stdout.strip()
            output = stderr if stderr else stdout
            raise RemoteError(f"{context} failed: {output or 'exit code ' + str(process.returncode)}")

    def ensure_worker_dirs(self, worker: WorkerConfig) -> None:
        script = (
            f"set -euo pipefail; mkdir -p {shlex.quote(worker.inbox_dir)} "
            f"{shlex.quote(worker.outbox_dir)}"
        )
        process = self._ssh(worker, script)
        self._require_ok(process, f"ensure dirs for {worker.name}")

    def list_zip_files(self, worker: WorkerConfig, directory: str) -> list[str]:
        script = (
            "set -euo pipefail; "
            f"d={shlex.quote(directory)}; "
            'if [ ! -d "$d" ]; then exit 0; fi; '
            'shopt -s nullglob; for f in "$d"/*.zip; do basename "$f"; done | sort'
        )
        process = self._ssh(worker, script)
        self._require_ok(process, f"list zips on {worker.name}:{directory}")
        lines = [line.strip() for line in process.stdout.splitlines()]
        return [line for line in lines if line]

    def snapshot_worker(self, worker: WorkerConfig) -> WorkerSnapshot:
        try:
            self.ensure_worker_dirs(worker)
            inbox = self.list_zip_files(worker, worker.inbox_dir)
            outbox = self.list_zip_files(worker, worker.outbox_dir)
            return WorkerSnapshot(
                name=worker.name,
                reachable=True,
                inbox_zips=inbox,
                outbox_zips=outbox,
                error=None,
            )
        except RemoteError as exc:
            return WorkerSnapshot(
                name=worker.name,
                reachable=False,
                inbox_zips=[],
                outbox_zips=[],
                error=str(exc),
            )

    def copy_zip_to_worker(self, worker: WorkerConfig, local_zip: Path, remote_zip_name: str) -> str:
        remote_path = f"{worker.inbox_dir.rstrip('/')}/{remote_zip_name}"
        if self.dispatch_config.transfer_mode == "rsync":
            rsh = "ssh " + " ".join(shlex.quote(item) for item in self.ssh_options)
            cmd = [
                "rsync",
                "-az",
                "-e",
                rsh,
                str(local_zip),
                f"{worker.address}:{remote_path}",
            ]
        else:
            cmd = ["scp", *self.ssh_options, str(local_zip), f"{worker.address}:{remote_path}"]
        process = self._run(cmd)
        self._require_ok(process, f"copy zip to {worker.name}")
        return remote_path

    def prepare_and_launch(
        self,
        worker: WorkerConfig,
        job_id: str,
        remote_zip_path: str,
    ) -> None:
        job_root = f"{worker.inbox_dir.rstrip('/')}/{job_id}"
        extract_dir = f"{job_root.rstrip('/')}/{self.dispatch_config.remote_unpack_dirname}"
        launch_command = self.dispatch_config.launch.command_template.format(
            job_id=job_id,
            worker_inbox=worker.inbox_dir,
            worker_outbox=worker.outbox_dir,
            zip_name=Path(remote_zip_path).name,
            extract_dir=extract_dir,
        )
        launcher_log = f"{worker.outbox_dir.rstrip('/')}/{job_id}.launcher.log"
        script = "\n".join(
            [
                "set -euo pipefail",
                f"mkdir -p {shlex.quote(worker.inbox_dir)} {shlex.quote(worker.outbox_dir)}",
                f"mkdir -p {shlex.quote(job_root)} {shlex.quote(extract_dir)}",
                f"unzip -o {shlex.quote(remote_zip_path)} -d {shlex.quote(extract_dir)} >/dev/null",
                (
                    f"nohup bash -lc {shlex.quote(launch_command)} "
                    f"> {shlex.quote(launcher_log)} 2>&1 < /dev/null &"
                ),
            ]
        )
        process = self._ssh(worker, script)
        self._require_ok(process, f"launch job {job_id} on {worker.name}")

    def fetch_result_zips(
        self,
        worker: WorkerConfig,
        result_zip_names: list[str],
        local_inbox_dir: Path,
    ) -> list[Path]:
        local_inbox_dir.mkdir(parents=True, exist_ok=True)
        copied_paths: list[Path] = []
        for zip_name in result_zip_names:
            local_target = local_inbox_dir / zip_name
            remote_source = f"{worker.address}:{worker.outbox_dir.rstrip('/')}/{zip_name}"
            if self.dispatch_config.transfer_mode == "rsync":
                rsh = "ssh " + " ".join(shlex.quote(item) for item in self.ssh_options)
                cmd = ["rsync", "-az", "-e", rsh, remote_source, str(local_target)]
            else:
                cmd = ["scp", *self.ssh_options, remote_source, str(local_target)]
            process = self._run(cmd)
            self._require_ok(process, f"fetch result zip {zip_name} from {worker.name}")
            copied_paths.append(local_target)
        return copied_paths
