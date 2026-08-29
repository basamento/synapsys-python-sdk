import os
import unittest
from unittest.mock import patch

from basamento_synapsys import Synapsys, SynapsysConfigError
from basamento_synapsys._config import resolve_config, validate_for_startup


class ConfigTests(unittest.TestCase):
    def test_options_override_environment(self):
        config = resolve_config(
            {"worker_name": "option-worker", "core_url": "http://localhost:8080"},
            {
                "SYNAPSYS_WORKER_NAME": "environment-worker",
                "SYNAPSYS_HEARTBEAT_INTERVAL": "9s",
            },
        )
        self.assertEqual(config.worker_name, "option-worker")
        self.assertEqual(config.heartbeat_interval, 9)

    def test_unknown_option_fails_even_when_disabled(self):
        with self.assertRaisesRegex(SynapsysConfigError, "heratbeat_interval"):
            Synapsys(enabled=False, heratbeat_interval="5s")

    def test_unitless_string_is_rejected(self):
        with self.assertRaisesRegex(SynapsysConfigError, "include a unit"):
            Synapsys(enabled=False, request_timeout="30")

    def test_numeric_duration_means_milliseconds(self):
        config = resolve_config({"connect_timeout": 250, "heartbeat_interval": 5000}, {})
        self.assertEqual(config.connect_timeout, 0.25)
        self.assertEqual(config.heartbeat_interval, 5)

    def test_fractional_heartbeat_is_rejected(self):
        with self.assertRaisesRegex(SynapsysConfigError, "whole number"):
            Synapsys(enabled=False, heartbeat_interval="1500ms")

    def test_disabled_does_not_require_connection_configuration(self):
        with patch.dict(os.environ, {}, clear=True):
            worker = Synapsys(enabled=False)
            original_out = __import__("sys").stdout
            worker.start().stop()
            self.assertIs(__import__("sys").stdout, original_out)
            self.assertFalse(worker.is_started)

    def test_token_is_absent_from_public_config(self):
        worker = Synapsys(
            enabled=False,
            core_token="secret",
            core_url="http://localhost:8080",
            worker_name="worker",
        )
        self.assertNotIn("core_token", worker.config)
        self.assertNotIn("logger", worker.config)

    def test_http_token_warning_is_suppressed_for_loopback(self):
        config = resolve_config(
            {
                "core_url": "http://127.0.0.1:8080",
                "core_token": "secret",
                "worker_name": "worker",
            },
            {},
        )
        self.assertEqual(validate_for_startup(config), ())


if __name__ == "__main__":
    unittest.main()
