"""The lifecycle rules shared with every other Synapsys client library.

Deferred starts, token acknowledgement, cancellation versus failure, and the
bounded cooperative shutdown that ends with one final heartbeat.
"""

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from basamento_synapsys import Synapsys
from basamento_synapsys._config import resolve_config
from basamento_synapsys._heartbeat import HeartbeatLoop
from basamento_synapsys._http import HttpResponse
from basamento_synapsys._log import SynapsysLogger
from basamento_synapsys._process import (
    EndlessProcess,
    ProgressiveProcess,
    _Outcome,
)


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

    def has(self, level, fragment):
        return any(fragment in line for line in self.lines[level])


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.payloads = []

    def health(self):
        return HttpResponse(200, {})

    def heartbeat(self, payload):
        self.payloads.append(payload)
        response = self.responses.pop(0) if self.responses else HttpResponse(200, {"processes": []})
        if isinstance(response, BaseException):
            raise response
        return response


def build_loop(processes, client, capture=None):
    raw_logger = FakeLogger()
    logger = SynapsysLogger(raw_logger, "debug")
    config = resolve_config(
        {
            "core_url": "http://localhost:8080",
            "worker_name": "worker",
            "capture_console": capture is not None,
        },
        {},
    )
    loop = HeartbeatLoop(
        config=config,
        processes=processes,
        capture=capture,
        logger=logger,
        client=client,
    )
    return loop, raw_logger


def logger():
    return SynapsysLogger(FakeLogger(), "debug")


def command(name, desired_state, token, run_id):
    return HttpResponse(
        200,
        {
            "processes": [
                {
                    "name": name,
                    "desiredState": desired_state,
                    "desiredToken": token,
                    "runId": run_id,
                }
            ]
        },
    )


class DeferredStartTests(unittest.TestCase):
    def test_start_while_stopping_is_deferred_and_keeps_the_old_run_id(self):
        release = threading.Event()
        bodies = []

        def body(_stop):
            # Deliberately ignores the stop signal, which is the case the deferral
            # rule exists for.
            bodies.append(True)
            release.wait(5)

        process = EndlessProcess(name="stubborn", body=body, on_stop=None, logger=logger())

        self.assertIs(process.handle_start(100), _Outcome.HANDLED)
        self.assertTrue(_wait(lambda: process.current_state == "running"))

        self.assertIs(process.handle_stop(), _Outcome.HANDLED)
        self.assertEqual(process.current_state, "stopping")

        # The redelivered start, while the old run refuses to die.
        self.assertIs(process.handle_start(200), _Outcome.DEFERRED)
        self.assertEqual(len(bodies), 1, "no overlapping execution")
        self.assertEqual(process.run_id, 100, "the active run id is preserved")

        release.set()
        self.assertTrue(process.settle(5))
        self.assertEqual(process.current_state, "idle")

        # Once the old run is terminal, the redelivered start is accepted.
        self.assertIs(process.handle_start(200), _Outcome.HANDLED)
        self.assertTrue(_wait(lambda: len(bodies) == 2))
        self.assertEqual(process.run_id, 200)
        process.handle_stop()
        process.settle(5)

    def test_a_deferred_start_is_not_acknowledged_and_is_applied_later(self):
        release = threading.Event()

        def body(_stop):
            release.wait(5)

        process = EndlessProcess(name="stubborn", body=body, on_stop=None, logger=logger())
        client = FakeClient(
            [
                command("stubborn", "running", 1, 111),
                command("stubborn", "stopping", 2, 111),
                command("stubborn", "running", 3, 222),
                command("stubborn", "running", 3, 222),
            ]
        )
        loop, _ = build_loop([process], client)

        loop.tick()
        self.assertTrue(_wait(lambda: process.current_state == "running"))
        self.assertEqual(process.ack_token, 1)

        loop.tick()
        self.assertEqual(process.ack_token, 2)
        self.assertEqual(process.current_state, "stopping")

        loop.tick()  # start while stopping: deferred
        self.assertEqual(process.ack_token, 2, "the start must not be acknowledged")
        self.assertEqual(process.run_id, 111, "the old run id must not be replaced")

        release.set()
        self.assertTrue(process.settle(5))

        loop.tick()  # Core redelivers the same command
        self.assertEqual(process.ack_token, 3)
        self.assertEqual(process.run_id, 222)
        process.handle_stop()
        process.settle(5)

    def test_stop_for_an_inactive_process_is_a_handled_no_op(self):
        process = EndlessProcess(name="p", body=lambda _s: None, on_stop=None, logger=logger())
        self.assertIs(process.handle_stop(), _Outcome.HANDLED)
        self.assertEqual(process.current_state, "idle")
        self.assertIs(process.handle_stop(), _Outcome.HANDLED)

    def test_duplicate_start_does_not_reassign_the_active_run(self):
        started = threading.Event()
        bodies = []

        def body(stop):
            bodies.append(True)
            started.set()
            stop.wait(5)

        process = EndlessProcess(name="p", body=body, on_stop=None, logger=logger())
        self.assertIs(process.handle_start(7), _Outcome.HANDLED)
        self.assertTrue(started.wait(5))
        self.assertIs(process.handle_start(8), _Outcome.HANDLED)
        self.assertEqual(len(bodies), 1)
        self.assertEqual(process.run_id, 7)
        process.handle_stop()
        process.settle(5)

    def test_progressive_start_while_in_flight_is_deferred(self):
        release = threading.Event()
        bodies = []

        def body(_stop):
            bodies.append(True)
            release.wait(5)

        process = ProgressiveProcess(name="p", body=body, on_stop=None, logger=logger())
        self.assertIs(process.handle_start(1), _Outcome.HANDLED)
        self.assertTrue(_wait(lambda: len(bodies) == 1))
        process.handle_stop()
        self.assertIs(process.handle_start(2), _Outcome.DEFERRED)

        release.set()
        self.assertTrue(process.settle(5))
        release.clear()
        release.set()
        self.assertIs(process.handle_start(2), _Outcome.HANDLED)
        self.assertTrue(process.settle(5))
        self.assertEqual(len(bodies), 2)


class FailureTests(unittest.TestCase):
    def test_a_cooperative_stop_is_not_a_failure(self):
        raw = FakeLogger()

        def body(stop):
            stop.wait(5)
            stop.raise_if_requested()

        process = ProgressiveProcess(
            name="p", body=body, on_stop=None, logger=SynapsysLogger(raw, "debug")
        )
        process.handle_start(1)
        self.assertTrue(_wait(lambda: process.current_state == "running"))
        process.handle_stop()
        self.assertTrue(process.settle(5))

        self.assertEqual(process.current_state, "idle")
        self.assertEqual(raw.lines["error"], [])

    def test_a_failing_body_ends_failed_and_is_reported(self):
        raw = FakeLogger()

        def body():
            raise RuntimeError("business exploded")

        process = ProgressiveProcess(
            name="p", body=body, on_stop=None, logger=SynapsysLogger(raw, "debug")
        )
        process.handle_start(1)
        self.assertTrue(process.settle(5))

        self.assertEqual(process.current_state, "failed")
        self.assertTrue(raw.has("error", "Progressive process 'p' failed"))

    def test_a_failed_process_can_be_started_again(self):
        attempts = []

        def body():
            attempts.append(True)
            if len(attempts) == 1:
                raise RuntimeError("first attempt fails")

        process = ProgressiveProcess(name="p", body=body, on_stop=None, logger=logger())
        process.handle_start(1)
        self.assertTrue(process.settle(5))
        self.assertEqual(process.current_state, "failed")

        self.assertIs(process.handle_start(2), _Outcome.HANDLED)
        self.assertTrue(process.settle(5))
        self.assertEqual(process.current_state, "idle")
        self.assertEqual(len(attempts), 2)

    def test_a_throwing_on_stop_still_stops_the_process(self):
        raw = FakeLogger()

        def on_stop():
            raise RuntimeError("cleanup exploded")

        process = EndlessProcess(
            name="p",
            body=lambda stop: stop.wait(5),
            on_stop=on_stop,
            logger=SynapsysLogger(raw, "debug"),
        )
        process.handle_start(1)
        self.assertTrue(_wait(lambda: process.current_state == "running"))
        self.assertIs(process.handle_stop(), _Outcome.HANDLED)
        self.assertTrue(process.settle(5))

        self.assertEqual(process.current_state, "idle")
        self.assertTrue(raw.has("error", "on_stop() failed for process 'p'"))


class MalformedResponseTests(unittest.TestCase):
    def test_an_empty_2xx_body_is_a_protocol_failure(self):
        client = FakeClient([HttpResponse(200, None)])
        loop, raw = build_loop([], client)
        loop.tick()
        self.assertTrue(raw.has("warning", "malformed heartbeat response"))

    def test_a_body_without_processes_is_a_protocol_failure(self):
        client = FakeClient([HttpResponse(200, {"unexpected": True})])
        loop, raw = build_loop([], client)
        loop.tick()
        self.assertTrue(raw.has("warning", "malformed heartbeat response"))

    def test_a_malformed_response_neither_acknowledges_nor_drops_logs(self):
        from basamento_synapsys._capture import LogCapture, _Scope

        capture = LogCapture()
        scope = _Scope(capture, "p", 42)
        capture._enqueue(scope, "stdout", "first")
        capture._enqueue(scope, "stdout", "second")

        process = ProgressiveProcess(name="p", body=lambda: None, on_stop=None, logger=logger())
        client = FakeClient([HttpResponse(200, None), command("p", "running", 4, 41)])
        loop, _ = build_loop([process], client, capture)

        loop.tick()
        self.assertEqual(process.ack_token, 0)
        self.assertEqual(capture.size, 2, "drained logs are requeued")

        loop.tick()
        self.assertEqual(process.ack_token, 4)
        messages = [entry["message"] for entry in client.payloads[1]["logs"]]
        self.assertEqual(messages[:2], ["first", "second"], "requeued in original order")
        process.settle(5)


class _ShutdownCoreHandler(BaseHTTPRequestHandler):
    beats = []
    lock = threading.Lock()
    response_delay = 0.0
    next_command = None

    def log_message(self, _format, *args):
        return

    def do_GET(self):
        self.send_response(200 if self.path == "/api/health" else 404)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        with self.lock:
            type(self).beats.append(payload)
            command_now = type(self).next_command
            delay = type(self).response_delay
        if delay:
            time.sleep(delay)
        body = {"processes": [command_now] if command_now else []}
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        _ShutdownCoreHandler.beats = []
        _ShutdownCoreHandler.response_delay = 0.0
        _ShutdownCoreHandler.next_command = None
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _ShutdownCoreHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def _worker(self, **options):
        settings = {
            "core_url": self.url,
            "core_token": "token",
            "worker_name": "worker",
            "heartbeat_interval": "1s",
            "capture_console": False,
        }
        settings.update(options)
        worker = Synapsys(**settings)
        worker._logger = SynapsysLogger(self.raw_logger, "debug")
        return worker

    @property
    def raw_logger(self):
        if not hasattr(self, "_raw"):
            self._raw = FakeLogger()
        return self._raw

    def test_shutdown_sends_one_final_heartbeat_with_final_state(self):
        worker = self._worker()
        worker.register_endless(lambda stop: stop.wait(30), name="poller")
        worker.start()
        _ShutdownCoreHandler.next_command = {
            "name": "poller",
            "desiredState": "running",
            "desiredToken": 1,
            "runId": 900,
        }
        self.assertTrue(_wait(lambda: worker.processes[0].current_state == "running"))

        before = len(_ShutdownCoreHandler.beats)
        worker.stop()

        self.assertGreater(len(_ShutdownCoreHandler.beats), before)
        final = _ShutdownCoreHandler.beats[-1]
        self.assertEqual(final["processes"][0]["currentState"], "idle")
        self.assertTrue(self.raw_logger.has("info", "Worker stopped"))

    def test_the_final_response_never_starts_new_work(self):
        started = []
        worker = self._worker()
        worker.register_endless(lambda stop: (started.append(True), stop.wait(30)), name="poller")
        worker.start()
        self.assertTrue(_wait(lambda: len(_ShutdownCoreHandler.beats) > 0))

        _ShutdownCoreHandler.next_command = {
            "name": "poller",
            "desiredState": "running",
            "desiredToken": 5,
            "runId": 555,
        }
        worker.stop()
        time.sleep(0.2)

        self.assertEqual(started, [], "no process was started by the final response")
        self.assertEqual(worker.processes[0].ack_token, 0)

    def test_a_failing_final_heartbeat_does_not_fail_shutdown(self):
        worker = self._worker()
        worker.register_endless(lambda stop: stop.wait(30), name="poller")
        worker.start()
        self.assertTrue(_wait(lambda: len(_ShutdownCoreHandler.beats) > 0))

        self.server.shutdown()  # Core disappears mid-shutdown
        worker.stop()
        self.assertTrue(self.raw_logger.has("info", "Worker stopped"))

    def test_shutdown_is_idempotent_and_sends_one_final_beat(self):
        worker = self._worker()
        worker.register_endless(lambda stop: stop.wait(30), name="poller")
        worker.start()
        self.assertTrue(_wait(lambda: len(_ShutdownCoreHandler.beats) > 0))

        before = len(_ShutdownCoreHandler.beats)
        worker.stop()
        worker.stop()
        worker.stop()
        self.assertEqual(len(_ShutdownCoreHandler.beats) - before, 1)

    def test_concurrent_stop_calls_do_not_tear_down_twice(self):
        worker = self._worker()
        worker.register_endless(lambda stop: stop.wait(30), name="poller")
        worker.start()
        self.assertTrue(_wait(lambda: len(_ShutdownCoreHandler.beats) > 0))

        before = len(_ShutdownCoreHandler.beats)
        threads = [threading.Thread(target=worker.stop) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertLessEqual(len(_ShutdownCoreHandler.beats) - before, 1)

    def test_business_code_that_ignores_cancellation_is_reported_not_killed(self):
        release = threading.Event()
        worker = self._worker(shutdown_timeout="300ms")
        worker.register_endless(lambda: release.wait(20), name="stubborn")
        worker.start()
        _ShutdownCoreHandler.next_command = {
            "name": "stubborn",
            "desiredState": "running",
            "desiredToken": 1,
            "runId": 42,
        }
        self.assertTrue(_wait(lambda: worker.processes[0].current_state == "running"))

        started = time.monotonic()
        worker.stop()
        elapsed = time.monotonic() - started
        release.set()

        self.assertLess(elapsed, 5, f"shutdown waited {elapsed:.1f}s")
        self.assertTrue(self.raw_logger.has("warning", "did not stop before the shutdown timeout"))

    def test_stop_before_start_is_a_no_op(self):
        worker = self._worker()
        worker.stop()
        self.assertEqual(_ShutdownCoreHandler.beats, [])

    def test_a_cleanup_hook_that_throws_does_not_break_shutdown(self):
        def on_stop():
            raise RuntimeError("cleanup exploded")

        worker = self._worker()
        worker.register_endless(lambda stop: stop.wait(30), name="poller", on_stop=on_stop)
        worker.start()
        _ShutdownCoreHandler.next_command = {
            "name": "poller",
            "desiredState": "running",
            "desiredToken": 1,
            "runId": 7,
        }
        self.assertTrue(_wait(lambda: worker.processes[0].current_state == "running"))

        worker.stop()
        self.assertTrue(self.raw_logger.has("info", "Worker stopped"))


def _wait(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


if __name__ == "__main__":
    unittest.main()
