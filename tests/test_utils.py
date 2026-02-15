from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from jobhopper.utils import parse_job_id_from_zip_name, sha256_file


class UtilsTest(unittest.TestCase):
    def test_parse_job_id(self) -> None:
        job_id = "a" * 64
        self.assertEqual(parse_job_id_from_zip_name(f"{job_id}.zip"), job_id)
        self.assertEqual(parse_job_id_from_zip_name(f"{job_id}.metrics.zip"), job_id)
        self.assertIsNone(parse_job_id_from_zip_name("not-a-job.zip"))

    def test_sha256_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "a.zip"
            path.write_bytes(b"abc")
            self.assertEqual(
                sha256_file(path),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            )


if __name__ == "__main__":
    unittest.main()
