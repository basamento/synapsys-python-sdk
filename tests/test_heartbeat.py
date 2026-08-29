import unittest

from basamento_synapsys._capture import LogCapture, _Scope
from basamento_synapsys._config import resolve_config
from basamento_synapsys._heartbeat import HeartbeatLoop
from basamento_synapsys._http import HttpResponse
from basamento_synapsys._log import SynapsysLogger
from basamento_synapsys._process import ProgressiveProcess


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


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.payloads = []

    def health(self):
        return HttpResponse(200, {})

    def heartbeat(self, payload):
        self.payloads.append(payload)
        response = self.responses.pop(0)
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


class HeartbeatTests(unittest.TestCase):
    def test_failed_heartbeat_requeues_drained_logs(self):
        capture = LogCapture()
        scope = _Scope(capture, "one", 42)
        capture._enqueue(scope, "stdout", "first")
        client = FakeClient([OSError("network down"), HttpResponse(200, {"processes": []})])
        loop, _ = build_loop([], client, capture)

        loop.tick()
        self.assertEqual(capture.size, 1)
        loop.tick()
        self.assertEqual(capture.size, 0)
        self.assertEqual(client.payloads[1]["logs"][0]["message"], "first")

    def test_one_command_failure_does_not_block_its_sibling(self):
        raw_logger = FakeLogger()
        logger = SynapsysLogger(raw_logger, "debug")
        started = []
        broken = ProgressiveProcess(name="broken", body=lambda: None, on_stop=None, logger=logger)
        healthy = ProgressiveProcess(
            name="healthy", body=lambda: started.append(True), on_stop=None, logger=logger
        )

        def fail_start(_run_id):
            raise RuntimeError("handler failed")

        broken.handle_start = fail_start
        client = FakeClient(
            [
                HttpResponse(
                    200,
                    {
                        "processes": [
                            {
                                "name": "broken",
                                "desiredState": "running",
                                "desiredToken": 1,
                                "runId": 1,
                            },
                            {
                                "name": "healthy",
                                "desiredState": "running",
                                "desiredToken": 1,
                                "runId": 2,
                            },
                        ]
                    },
                )
            ]
        )
        loop, _ = build_loop([broken, healthy], client)
        loop.tick()
        healthy.settle(1)

        self.assertEqual(broken.current_state, "failed")
        self.assertEqual(broken.ack_token, 0)
        self.assertEqual(healthy.ack_token, 1)
        self.assertEqual(started, [True])

    def test_unknown_response_fields_are_ignored(self):
        process = ProgressiveProcess(
            name="known",
            body=lambda: None,
            on_stop=None,
            logger=SynapsysLogger(FakeLogger(), "debug"),
        )
        client = FakeClient(
            [
                HttpResponse(
                    200,
                    {
                        "future": True,
                        "processes": [
                            {
                                "name": "known",
                                "desiredState": "idle",
                                "desiredToken": 1,
                                "runId": None,
                                "anotherFutureField": 123,
                            }
                        ],
                    },
                )
            ]
        )
        loop, _ = build_loop([process], client)
        loop.tick()
        self.assertEqual(process.ack_token, 1)


if __name__ == "__main__":
    unittest.main()
