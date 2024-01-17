import unittest

from click.testing import CliRunner
from xcube_smos.nckcindex.cli import cli


class CliTest(unittest.TestCase):
    def test_main(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(cli, ["--help"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("Usage: nckcidx [OPTIONS] COMMAND [ARGS]...\n", result.output)
        self.assertIn("Manage NetCDF Kerchunk indexes.\n", result.output)
        self.assertIn("Options:\n", result.output)
        self.assertIn(
            "Commands:\n"
            "  create    Create a NetCDF Kerchunk index.\n"
            "  describe  Describe a NetCDF Kerchunk index.\n"
            "  sync      Synchronize a NetCDF Kerchunk index.\n",
            result.output,
        )

    def test_create_help(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(cli, ["create", "--help"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("Usage: nckcidx create [OPTIONS]\n", result.output)
        self.assertIn("Create a NetCDF Kerchunk index.\n", result.output)
        self.assertIn("Options:\n", result.output)

    def test_sync_help(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(cli, ["sync", "--help"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("Usage: nckcidx sync [OPTIONS]\n", result.output)
        self.assertIn("Synchronize a NetCDF Kerchunk index.\n", result.output)
        self.assertIn("Options:\n", result.output)

    def test_info_help(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(cli, ["describe", "--help"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("Usage: nckcidx describe [OPTIONS]\n", result.output)
        self.assertIn("Describe a NetCDF Kerchunk index.\n", result.output)
        self.assertIn("Options:\n", result.output)
