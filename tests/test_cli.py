import unittest

from jobhopper.cli import build_parser


class CliTest(unittest.TestCase):
    def test_run_once_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--config", "jobhopper.yaml", "run", "--once"])
        self.assertEqual(args.command, "run")
        self.assertTrue(args.once)


if __name__ == "__main__":
    unittest.main()
