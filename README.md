# jobhopper

Lightweight, file-backed dispatcher for expensive jobs across SSH workers.

## What it does

- Watches a local queue folder for input `.zip` jobs.
- Assigns jobs to free workers (worker is free when worker Inbox has no `.zip`).
- Copies jobs to workers via `scp` or `rsync`.
- Unzips and launches remote bash command via detached `nohup`.
- Detects completion when worker Inbox is empty and worker Outbox contains zip files prefixed by `job_id`.
- Pulls result zips back to dispatcher Inbox.
- Persists state/events in SQLite so restart recovery is automatic.

## Install

```bash
pip install -e .
```

## Config

Create `jobhopper.yaml`:

```yaml
paths:
  inbox: "./state/inbox"
  queue: "./state/queue"
  running: "./state/running"
  done: "./state/done"
  failed: "./state/failed"
  db: "./state/jobhopper.db"
  log: "./state/jobhopper.log"

poll:
  interval_seconds: 10
  settle_seconds: 5

dispatch:
  ssh_options: "-o BatchMode=yes -o ConnectTimeout=10"
  transfer_mode: "scp"
  remote_unpack_dirname: "job"
  launch:
    command_template: "cd {extract_dir} && bash run_job.sh '{job_id}' '{worker_inbox}' '{worker_outbox}' '{zip_name}' '{extract_dir}'"

workers:
  - name: worker-a
    host: 10.0.0.21
    user: alice
    inbox_dir: /home/alice/Inbox
    outbox_dir: /home/alice/Outbox
  - name: worker-b
    host: 10.0.0.22
    user: alice
    inbox_dir: /home/alice/Inbox
    outbox_dir: /home/alice/Outbox
```

## Usage

```bash
jobhopper --config jobhopper.yaml run
jobhopper --config jobhopper.yaml run --once
jobhopper --config jobhopper.yaml status
jobhopper --config jobhopper.yaml scan
jobhopper --config jobhopper.yaml requeue --job-id <job_id>
```

Drop input zips into `paths.queue`.  
Worker result files must be named with `{job_id}` prefix, e.g. `{job_id}.metrics.zip`.

You can also start from `jobhopper.example.yaml`.

## Worker script templates

Example worker-side script template is included at `examples/run_job.template.sh`.

It shows how to:
- run long jobs inside the unpacked job folder
- write Outbox artifact zip(s) prefixed with `{job_id}`
- move the worker Inbox zip out of Inbox to clear worker busy state

`dispatch.launch.command_template` supports placeholders:
- `{job_id}`
- `{worker_inbox}`
- `{worker_outbox}`
- `{zip_name}`
- `{extract_dir}`
