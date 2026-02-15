from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(slots=True)
class PathsConfig:
    inbox: Path
    queue: Path
    running: Path
    done: Path
    failed: Path
    db: Path
    log: Path


@dataclass(slots=True)
class PollConfig:
    interval_seconds: int = 10
    settle_seconds: int = 5


@dataclass(slots=True)
class LaunchConfig:
    command_template: str = "cd {extract_dir} && bash run_job.sh"


@dataclass(slots=True)
class DispatchConfig:
    ssh_options: str = "-o BatchMode=yes -o ConnectTimeout=10"
    transfer_mode: str = "scp"
    remote_unpack_dirname: str = "job"
    launch: LaunchConfig = field(default_factory=LaunchConfig)


@dataclass(slots=True)
class WorkerConfig:
    name: str
    host: str
    user: str
    inbox_dir: str
    outbox_dir: str

    @property
    def address(self) -> str:
        return f"{self.user}@{self.host}"


@dataclass(slots=True)
class AppConfig:
    paths: PathsConfig
    poll: PollConfig
    dispatch: DispatchConfig
    workers: list[WorkerConfig]


def _require(mapping: dict, key: str, section: str) -> object:
    if key not in mapping:
        raise ValueError(f"Missing `{section}.{key}` in config")
    return mapping[key]


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")

    paths_raw = _require(raw, "paths", "root")
    poll_raw = raw.get("poll", {})
    dispatch_raw = raw.get("dispatch", {})
    workers_raw = _require(raw, "workers", "root")

    if not isinstance(paths_raw, dict):
        raise ValueError("`paths` must be a mapping")
    if not isinstance(poll_raw, dict):
        raise ValueError("`poll` must be a mapping")
    if not isinstance(dispatch_raw, dict):
        raise ValueError("`dispatch` must be a mapping")
    if not isinstance(workers_raw, list) or not workers_raw:
        raise ValueError("`workers` must be a non-empty list")

    def to_path(key: str) -> Path:
        value = _require(paths_raw, key, "paths")
        output = Path(str(value)).expanduser()
        if not output.is_absolute():
            output = config_path.parent / output
        return output

    paths = PathsConfig(
        inbox=to_path("inbox"),
        queue=to_path("queue"),
        running=to_path("running"),
        done=to_path("done"),
        failed=to_path("failed"),
        db=to_path("db"),
        log=to_path("log"),
    )

    poll = PollConfig(
        interval_seconds=int(poll_raw.get("interval_seconds", 10)),
        settle_seconds=int(poll_raw.get("settle_seconds", 5)),
    )
    if poll.interval_seconds < 1:
        raise ValueError("`poll.interval_seconds` must be >= 1")
    if poll.settle_seconds < 0:
        raise ValueError("`poll.settle_seconds` must be >= 0")

    launch_raw = dispatch_raw.get("launch", {})
    if not isinstance(launch_raw, dict):
        raise ValueError("`dispatch.launch` must be a mapping")

    dispatch = DispatchConfig(
        ssh_options=str(dispatch_raw.get("ssh_options", "-o BatchMode=yes -o ConnectTimeout=10")),
        transfer_mode=str(dispatch_raw.get("transfer_mode", "scp")).lower(),
        remote_unpack_dirname=str(dispatch_raw.get("remote_unpack_dirname", "job")),
        launch=LaunchConfig(
            command_template=str(launch_raw.get("command_template", "cd {extract_dir} && bash run_job.sh"))
        ),
    )
    if dispatch.transfer_mode not in {"scp", "rsync"}:
        raise ValueError("`dispatch.transfer_mode` must be either `scp` or `rsync`")

    workers: list[WorkerConfig] = []
    seen_names: set[str] = set()
    for idx, item in enumerate(workers_raw):
        if not isinstance(item, dict):
            raise ValueError(f"`workers[{idx}]` must be a mapping")
        worker = WorkerConfig(
            name=str(_require(item, "name", f"workers[{idx}]")),
            host=str(_require(item, "host", f"workers[{idx}]")),
            user=str(_require(item, "user", f"workers[{idx}]")),
            inbox_dir=str(_require(item, "inbox_dir", f"workers[{idx}]")),
            outbox_dir=str(_require(item, "outbox_dir", f"workers[{idx}]")),
        )
        if worker.name in seen_names:
            raise ValueError(f"Duplicate worker name: {worker.name}")
        seen_names.add(worker.name)
        workers.append(worker)

    return AppConfig(paths=paths, poll=poll, dispatch=dispatch, workers=workers)


def ensure_local_paths(config: AppConfig) -> None:
    config.paths.inbox.mkdir(parents=True, exist_ok=True)
    config.paths.queue.mkdir(parents=True, exist_ok=True)
    config.paths.running.mkdir(parents=True, exist_ok=True)
    config.paths.done.mkdir(parents=True, exist_ok=True)
    config.paths.failed.mkdir(parents=True, exist_ok=True)
    config.paths.db.parent.mkdir(parents=True, exist_ok=True)
    config.paths.log.parent.mkdir(parents=True, exist_ok=True)
