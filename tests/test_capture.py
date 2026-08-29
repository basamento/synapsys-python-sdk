import io
import sys
import threading
import unittest

from basamento_synapsys._capture import LogCapture


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.original_out = sys.stdout
        self.original_err = sys.stderr
        self.fake_out = io.StringIO()
        self.fake_err = io.StringIO()
        sys.stdout = self.fake_out
        sys.stderr = self.fake_err
        self.capture = LogCapture()
        self.capture.install()

    def tearDown(self):
        self.capture.uninstall()
        sys.stdout = self.original_out
        sys.stderr = self.original_err

    def test_application_output_is_teed_and_scoped(self):
        print("outside")
        with self.capture.scope("worker", 99):
            print("inside")
        entries = self.capture.entries()
        self.assertEqual(self.fake_out.getvalue(), "outside\ninside\n")
        self.assertEqual([entry["message"] for entry in entries], ["inside"])
        self.assertEqual(entries[0]["runId"], 99)

    def test_stderr_and_stdout_have_correct_metadata(self):
        with self.capture.scope("worker", 4):
            print("normal")
            print("bad", file=sys.stderr)
        entries = self.capture.entries()
        self.assertEqual(
            [(entry["source"], entry["level"]) for entry in entries],
            [("stdout", "info"), ("stderr", "error")],
        )
        self.assertEqual([entry["sequence"] for entry in entries], [1, 2])

    def test_partial_and_blank_lines(self):
        with self.capture.scope("worker", 5):
            sys.stdout.write("partial")
            sys.stdout.write(" line\n\n   \n")
        self.assertEqual([entry["message"] for entry in self.capture.entries()], ["partial line"])

    def test_null_run_id_is_not_captured(self):
        with self.capture.scope("worker", None):
            print("orphan")
        self.assertEqual(self.capture.size, 0)

    def test_context_is_isolated_between_threads(self):
        def body(name, run_id):
            with self.capture.scope(name, run_id):
                print(name)

        threads = [
            threading.Thread(target=body, args=("alpha", 1)),
            threading.Thread(target=body, args=("beta", 2)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(
            sorted((entry["processName"], entry["runId"]) for entry in self.capture.entries()),
            [("alpha", 1), ("beta", 2)],
        )

    def test_byte_budget_and_requeue(self):
        with self.capture.scope("worker", 8):
            for _ in range(5):
                print("日本語日本語日本語日")
        batch = self.capture.drain(100, 70)
        self.assertEqual(len(batch), 2)
        self.capture.requeue_front(batch)
        self.assertEqual(self.capture.size, 5)

    def test_uninstall_restores_streams(self):
        out_proxy = sys.stdout
        self.capture.uninstall()
        self.assertIsNot(sys.stdout, out_proxy)
        self.assertIs(sys.stdout, self.fake_out)


if __name__ == "__main__":
    unittest.main()
