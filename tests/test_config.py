from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from jobhopper.config import load_config


class ConfigTest(unittest.TestCase):
    def test_load_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "jobhopper.yaml"
            config_path.write_text(
                """
paths:
  inbox: "./inbox"
  queue: "./queue"
  running: "./running"
  done: "./done"
  failed: "./failed"
  db: "./jobhopper.db"
  log: "./jobhopper.log"
workers:
  - name: worker-a
    host: 10.0.0.1
    user: alice
    inbox_dir: /tmp/Inbox
    outbox_dir: /tmp/Outbox
""".strip(),
                encoding="utf-8",
            )
            config = load_config(config_path)
            self.assertEqual(config.poll.interval_seconds, 10)
            self.assertEqual(config.workers[0].name, "worker-a")
            self.assertEqual(config.paths.queue.resolve(), (root / "queue").resolve())


if __name__ == "__main__":
    unittest.main()
