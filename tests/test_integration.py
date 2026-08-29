import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from basamento_synapsys import StopSignal, Synapsys


class FakeCoreHandler(BaseHTTPRequestHandler):
    payloads = []
    lock = threading.Lock()
    run_id = 1_788_012_345_678_901_234

    def log_message(self, _format, *args):
        return

    def do_GET(self):
        if self.path == "/api/health":
            self.send_response(200)
            self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        with self.lock:
            self.payloads.append((self.headers.get("Authorization"), payload))
            number = len(self.payloads)
        response = {
            "futureCoreField": {"ignored": True},
            "processes": [
                {
                    "name": "monthly-report",
                    "desiredState": "running" if number == 1 else "idle",
                    "desiredToken": 1,
                    "runId": self.run_id,
                    "futureProcessField": "ignored",
                }
            ],
        }
        encoded = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        FakeCoreHandler.payloads = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeCoreHandler)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(2)

    def test_real_http_heartbeat_command_execution_and_logs(self):
        ran = threading.Event()
        worker = Synapsys(
            core_url=f"http://127.0.0.1:{self.server.server_port}",
            core_token="test-token",
            worker_name="integration-worker",
            heartbeat_interval="1s",
            connect_timeout="1s",
            request_timeout="1s",
        )

        @worker.progressive(name="monthly-report")
        def report(stop: StopSignal):
            print("report generated")
            ran.set()

        worker.start()
        try:
            self.assertTrue(ran.wait(2))
            # Send captured output without waiting for the next scheduled beat.
            worker._heartbeat.tick()
        finally:
            worker.stop()

        self.assertGreaterEqual(len(FakeCoreHandler.payloads), 2)
        authorization, first = FakeCoreHandler.payloads[0]
        self.assertEqual(authorization, "Bearer test-token")
        self.assertEqual(first["framework"], "python")
        self.assertEqual(first["workerName"], "integration-worker")
        self.assertEqual(first["processes"][0]["currentState"], "idle")

        second = FakeCoreHandler.payloads[1][1]
        self.assertEqual(second["processes"][0]["runId"], FakeCoreHandler.run_id)
        self.assertEqual(second["processes"][0]["ackToken"], 1)
        self.assertEqual(second["logs"][0]["message"], "report generated")
        self.assertEqual(second["logs"][0]["runId"], FakeCoreHandler.run_id)


if __name__ == "__main__":
    unittest.main()
