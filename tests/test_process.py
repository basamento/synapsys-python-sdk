import asyncio
import threading
import unittest

from basamento_synapsys import StopSignal, Synapsys, SynapsysConfigError
from basamento_synapsys._capture import LogCapture
from basamento_synapsys._log import SynapsysLogger
from basamento_synapsys._process import EndlessProcess, ProgressiveProcess


class FakeLogger:
    def __init__(self):
        self.lines = {"debug": [], "info": [], "warning": [], "error": []}

    def debug(self, message):
        self.lines["debug"].append(message)

    def info(self, message):
        self.lines["info"].append(message)

    def warning(self, message):
        self.lines["warning"].append(message)

    def error(self, message):
        self.lines["error"].append(message)


def build(process_class, body, on_stop=None):
    raw_logger = FakeLogger()
    process = process_class(
        name="example",
        body=body,
        on_stop=on_stop,
        logger=SynapsysLogger(raw_logger, "debug"),
    )
    return process, raw_logger


class ProcessTests(unittest.TestCase):
    def test_zero_argument_progressive_function_is_supported(self):
        called = threading.Event()
        process, _ = build(ProgressiveProcess, called.set)
        process.handle_start(101)
        self.assertTrue(called.wait(1))
        self.assertTrue(process.settle(1))
        self.assertEqual(process.current_state, "idle")

    def test_one_argument_function_receives_stop_signal(self):
        received = []

        def body(stop):
            received.append(stop)

        process, _ = build(ProgressiveProcess, body)
        process.handle_start(102)
        self.assertTrue(process.settle(1))
        self.assertIsInstance(received[0], StopSignal)

    def test_invalid_function_signature_fails_during_registration(self):
        worker = Synapsys(enabled=False)

        with self.assertRaisesRegex(SynapsysConfigError, "zero arguments or one"):

            @worker.progressive(name="bad")
            def bad(first, second):
                return first, second

    def test_second_start_does_not_create_duplicate_execution(self):
        release = threading.Event()
        started = threading.Event()
        count = 0

        def body():
            nonlocal count
            count += 1
            started.set()
            release.wait(1)

        process, _ = build(ProgressiveProcess, body)
        process.handle_start(1)
        self.assertTrue(started.wait(1))
        process.handle_start(2)
        release.set()
        process.settle(1)
        self.assertEqual(count, 1)

    def test_body_failure_is_absorbed_and_reported_as_failed(self):
        def body():
            raise RuntimeError("business failure")

        process, logger = build(ProgressiveProcess, body)
        process.handle_start(1)
        process.settle(1)
        self.assertEqual(process.current_state, "failed")
        self.assertTrue(any("business failure" in line for line in logger.lines["error"]))

    def test_endless_stop_is_cooperative(self):
        started = threading.Event()

        def body(stop):
            started.set()
            while not stop.requested:
                stop.wait(1)

        process, _ = build(EndlessProcess, body)
        process.handle_start(1)
        self.assertTrue(started.wait(1))
        process.handle_stop()
        self.assertEqual(process.current_state, "stopping")
        self.assertTrue(process.settle(1))
        self.assertEqual(process.current_state, "idle")

    def test_on_stop_failure_does_not_prevent_stop(self):
        started = threading.Event()

        def body(stop):
            started.set()
            stop.wait()

        def cleanup():
            raise RuntimeError("cleanup failure")

        process, logger = build(EndlessProcess, body, cleanup)
        process.handle_start(1)
        self.assertTrue(started.wait(1))
        process.handle_stop()
        self.assertTrue(process.settle(1))
        self.assertEqual(process.current_state, "idle")
        self.assertTrue(any("cleanup failure" in line for line in logger.lines["error"]))

    def test_async_function_is_cancelled_at_await_point(self):
        started = threading.Event()

        async def body():
            started.set()
            await asyncio.sleep(30)

        process, _ = build(ProgressiveProcess, body)
        process.handle_start(1)
        self.assertTrue(started.wait(1))
        process.handle_stop()
        self.assertTrue(process.settle(1))
        self.assertEqual(process.current_state, "idle")

    def test_token_gating_idle_and_exact_run_id(self):
        process, _ = build(EndlessProcess, lambda: None)
        large_id = 1_788_012_345_678_901_234
        decision = process.accept_command("running", 5, large_id)
        self.assertEqual(decision.command.value, "start")
        process.acknowledge(5)
        self.assertEqual(process.accept_command("stopping", 5, large_id).command.value, "none")
        idle = process.accept_command("idle", 6, large_id)
        self.assertEqual(idle.command.value, "none")
        process.acknowledge(idle.token)
        self.assertEqual(process.to_heartbeat_entry()["runId"], large_id)


class AsyncApplicationLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_body_can_reuse_calling_application_loop(self):
        application_thread = threading.get_ident()
        body_threads = []
        capture = LogCapture()

        async def body():
            body_threads.append(threading.get_ident())
            print("application-loop output")
            await asyncio.sleep(0)

        process, _ = build(ProgressiveProcess, body)
        process._capture = capture
        process.set_asyncio_loop(asyncio.get_running_loop())
        capture.install()
        try:
            process.handle_start(1)
            settled = await asyncio.to_thread(process.settle, 1)
        finally:
            capture.uninstall()

        self.assertTrue(settled)
        self.assertEqual(body_threads, [application_thread])
        self.assertEqual(capture.entries()[0]["message"], "application-loop output")


if __name__ == "__main__":
    unittest.main()
