#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash run_job.sh <job_id> <worker_inbox> <worker_outbox> <zip_name> <extract_dir>
#
# This script is intended to live inside each job zip and be launched by JobHopper.
# It demonstrates:
#   1) long-running job execution
#   2) writing artifact zips prefixed with job_id into worker Outbox
#   3) moving Inbox zip out of Inbox when done (sentinel clear)

if [ "$#" -ne 5 ]; then
  echo "usage: $0 <job_id> <worker_inbox> <worker_outbox> <zip_name> <extract_dir>" >&2
  exit 2
fi

JOB_ID="$1"
WORKER_INBOX="$2"
WORKER_OUTBOX="$3"
ZIP_NAME="$4"
EXTRACT_DIR="$5"

mkdir -p "$WORKER_OUTBOX"
mkdir -p "$WORKER_INBOX/archive"

LOG_DIR="$EXTRACT_DIR/logs"
ARTIFACT_DIR="$EXTRACT_DIR/artifacts"
mkdir -p "$LOG_DIR" "$ARTIFACT_DIR"

# Replace this block with your actual job command.
# Example: bash run_task.sh --config config.yaml --weights model.safetensors
{
  echo "starting job for $JOB_ID at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sleep 2
  echo "finished job for $JOB_ID at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} | tee "$LOG_DIR/job.log"

# Example artifacts. Replace with your real outputs.
printf '{"job_id":"%s","status":"ok"}\n' "$JOB_ID" > "$ARTIFACT_DIR/metrics.json"

# Create result zip in worker Outbox with required job_id prefix.
python3 - "$EXTRACT_DIR" "$WORKER_OUTBOX/${JOB_ID}.artifacts.zip" <<'PY'
from __future__ import annotations
import pathlib
import sys
import zipfile

root = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for path in root.rglob("*"):
        if path.is_file():
            zf.write(path, path.relative_to(root))
PY

# Move input zip out of Inbox so dispatcher sees the worker as free.
if [ -f "$WORKER_INBOX/$ZIP_NAME" ]; then
  mv "$WORKER_INBOX/$ZIP_NAME" "$WORKER_INBOX/archive/${JOB_ID}.input.zip"
fi

# Optional: remove extracted work dir to free space.
if [ -d "$(dirname "$EXTRACT_DIR")" ]; then
  rm -rf "$(dirname "$EXTRACT_DIR")"
fi
