from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
import re

JOB_ID_REGEX = re.compile(r"^([a-f0-9]{64})(?:[._-].*)?\.zip$", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def is_zip_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".zip"


def parse_job_id_from_zip_name(zip_name: str) -> str | None:
    match = JOB_ID_REGEX.match(zip_name)
    if not match:
        return None
    return match.group(1).lower()
