import unittest
from unittest.mock import patch

from basamento_synapsys import Synapsys, SynapsysStartupError


class UnreachableClient:
    heartbeat_calls = 0

    def __init__(self, _config):
        pass

    def health(self):
        raise OSError("connection refused")

    def heartbeat(self, _payload):
        type(self).heartbeat_calls += 1
        raise OSError("connection refused")


class ClientTests(unittest.TestCase):
    def test_start_with_core_down_retries_when_not_fail_fast(self):
        UnreachableClient.heartbeat_calls = 0
        worker = Synapsys(
            core_url="http://localhost:9",
            worker_name="worker",
            heartbeat_interval="1s",
            capture_console=False,
        )
        with patch("basamento_synapsys.client.HttpClient", UnreachableClient):
            worker.start()
            try:
                self.assertTrue(worker.is_started)
            finally:
                worker.stop()
        self.assertGreaterEqual(UnreachableClient.heartbeat_calls, 1)

    def test_fail_fast_leaves_worker_unstarted(self):
        worker = Synapsys(
            core_url="http://localhost:9",
            worker_name="worker",
            fail_fast=True,
            capture_console=False,
        )
        with (
            patch("basamento_synapsys.client.HttpClient", UnreachableClient),
            self.assertRaises(SynapsysStartupError),
        ):
            worker.start()
        self.assertFalse(worker.is_started)

    def test_decorator_returns_original_function_and_derives_name(self):
        worker = Synapsys(enabled=False)

        def business_logic():
            return 42

        decorated = worker.progressive(business_logic)
        self.assertIs(decorated, business_logic)
        self.assertEqual(decorated(), 42)
        self.assertEqual(worker.processes[0].name, "business_logic")

    def test_duplicate_names_fail(self):
        worker = Synapsys(enabled=False)
        worker.register_progressive(lambda: None, name="same")
        with self.assertRaisesRegex(Exception, "Duplicate process name"):
            worker.register_endless(lambda: None, name="same")


if __name__ == "__main__":
    unittest.main()
