from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from jobhopper.models import JobState
from jobhopper.store import Store


class StoreTest(unittest.TestCase):
    def test_insert_and_requeue(self) -> None:
        with TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "jobhopper.db"
            store = Store(db_path)
            store.init_schema()
            inserted = store.insert_job_if_missing("a" * 64, "job.zip", "/tmp/job.zip")
            self.assertTrue(inserted)
            job = store.get_job("a" * 64)
            assert job is not None
            self.assertEqual(job.state, JobState.QUEUED)

            store.update_job(
                job.job_id,
                state=JobState.FAILED,
                failed_at="2025-01-01T00:00:00+00:00",
                last_error="boom",
            )
            store.requeue_job(job.job_id, "/tmp/job2.zip", "job2.zip")
            updated = store.get_job(job.job_id)
            assert updated is not None
            self.assertEqual(updated.state, JobState.QUEUED)
            self.assertEqual(updated.zip_name, "job2.zip")
            self.assertEqual(updated.zip_path, "/tmp/job2.zip")
            store.close()


if __name__ == "__main__":
    unittest.main()
