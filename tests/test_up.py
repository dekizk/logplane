"""`logplane up`, and how `doctor` reports whether a coordinator is running."""
import fcntl
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from logplane import cli as logplane_cli
from logplane import up as launcher
from logplane.local_config import initialize_local_config
from logplane.pm_lock import lock_path, pm_status
from tests.test_cli import FakeObserver

SRC = Path(__file__).resolve().parents[1] / "src"


class PmStatusTests(unittest.TestCase):
    def test_reports_each_state_without_creating_the_lock(self):
        url = "http://127.0.0.1:1"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pm.lock"
            self.assertEqual("not_found", pm_status(url, path=path)["status"])
            self.assertFalse(path.exists())

            path.write_text("4242")
            self.assertEqual("not_running", pm_status(url, path=path)["status"])

            with path.open("r+") as held:
                fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.assertEqual({"status": "running", "detail": "pid 4242"},
                                 pm_status(url, path=path))
            # Checking never leaves the lock held, so a PM can still start.
            with path.open("r+") as again:
                fcntl.flock(again, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_remote_bus_is_unknown(self):
        self.assertEqual("unknown", pm_status("http://example.com:8765")["status"])

    def test_lock_path_matches_the_pm(self):
        from logplane import pm_agent

        self.assertEqual(pm_agent.LOCK_PATH, lock_path(pm_agent.BUS_URL))


class DoctorCoordinatorTests(unittest.TestCase):
    def doctor(self, status, extra=()):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("logplane.pm_lock.pm_status", return_value=status):
            code = logplane_cli.main(["doctor", *extra], stdout=stdout, stderr=stderr,
                                     client_factory=FakeObserver,
                                     local_config_path="/nonexistent/logplane.local.json")
        self.assertEqual(0, code, stderr.getvalue())
        return stdout.getvalue()

    def test_running_and_stopped_coordinators_are_reported(self):
        self.assertIn("Coordinator: running (pid 7)",
                      self.doctor({"status": "running", "detail": "pid 7"}))
        stopped = self.doctor({"status": "not_running", "detail": "no PM"})
        self.assertIn("Coordinator: not running", stopped)
        self.assertIn("logplane up", stopped)
        value = json.loads(self.doctor({"status": "not_running", "detail": "no PM"}, ["--json"]))
        self.assertEqual("not_running", value["pm"]["status"])


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@unittest.skipIf(os.name != "posix", "uses POSIX signals")
class UpTests(unittest.TestCase):
    def test_readiness_requires_this_childs_marker_not_just_a_living_child(self):
        for headers in ({}, {"X-Logplane-Startup-Id": "another-launch"}):
            with self.subTest(headers=headers):
                child = Mock()
                child.poll.side_effect = [None, None, 2]
                with patch.object(launcher.httpx, "get", return_value=httpx.Response(200, headers=headers)), \
                     patch.object(launcher.time, "sleep"):
                    self.assertFalse(launcher._wait_for_bus(
                        "http://unused", child, startup_id="this-launch", timeout=20))

        child = Mock()
        for exit_code, expected in ((None, True), (2, False)):
            with self.subTest(exit_code=exit_code):
                child.poll.side_effect = [None, exit_code]
                with patch.object(launcher.httpx, "get", return_value=httpx.Response(
                        200, headers={"X-Logplane-Startup-Id": "this-launch"})):
                    self.assertEqual(expected, launcher._wait_for_bus(
                        "http://unused", child, startup_id="this-launch", timeout=20))

    def test_listener_arriving_after_preflight_never_starts_the_pm(self):
        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            child = Mock()
            # The new bus stays alive while another process answers health,
            # then exits after losing the bind race.
            child.poll.side_effect = [None, None, 2, 2, 2]
            with patch.object(launcher, "pm_status", return_value={"status": "not_running"}), \
                 patch.object(launcher, "_something_answers", return_value=False), \
                 patch.object(launcher.signal, "signal"), \
                 patch.object(launcher.subprocess, "Popen", return_value=child) as spawn, \
                 patch.object(launcher.httpx, "get", return_value=httpx.Response(200)), \
                 patch.object(launcher.time, "sleep"):
                out, err = io.StringIO(), io.StringIO()
                code = launcher.run_up(config, snapshot=False, quiet=False, stdout=out, stderr=err)
            self.assertEqual(2, code)
            self.assertEqual(1, spawn.call_count, "must not spawn the coordinator")
            self.assertIn("did not start", err.getvalue())
            self.assertNotIn("logplane is up", out.getvalue())
            self.assertTrue(spawn.call_args.kwargs["env"]["LOGPLANE_STARTUP_ID"])

    def test_up_refuses_a_port_something_else_answers_on(self):
        import http.server
        import threading

        class Health(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok": true}')

            def log_message(self, *args):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = initialize_local_config(root)
            value = json.loads(config.read_text())
            server = http.server.HTTPServer(("127.0.0.1", 0), Health)
            value["bus"]["port"] = server.server_address[1]
            config.write_text(json.dumps(value))
            url = f"http://127.0.0.1:{value['bus']['port']}"
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                environment = {k: v for k, v in os.environ.items() if not k.startswith("LOGPLANE_")}
                environment["PYTHONPATH"] = str(SRC)
                result = subprocess.run(
                    [sys.executable, "-m", "logplane.cli", "up", "--config", str(config)],
                    cwd=root, env=environment, capture_output=True, text=True, timeout=60)
            finally:
                server.shutdown()
            self.assertEqual(2, result.returncode, result.stdout + result.stderr)
            self.assertIn("already listening", result.stderr)
            self.assertNotIn("logplane is up", result.stdout)
            self.assertNotEqual("running", pm_status(url)["status"])
            self.assertFalse((root / ".logplane" / "logs" / "pm.log").exists())

    def test_up_runs_both_and_stops_both(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = initialize_local_config(root)
            value = json.loads(config.read_text())
            value["bus"]["port"] = free_port()
            config.write_text(json.dumps(value))
            url = f"http://127.0.0.1:{value['bus']['port']}"
            environment = {k: v for k, v in os.environ.items() if not k.startswith("LOGPLANE_")}
            environment["PYTHONPATH"] = str(SRC)
            up = subprocess.Popen(
                [sys.executable, "-m", "logplane.cli", "up", "--config", str(config)],
                cwd=root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                deadline = time.monotonic() + 30
                while pm_status(url)["status"] != "running" and time.monotonic() < deadline:
                    self.assertIsNone(up.poll(), up.stderr.read() if up.poll() is not None else "")
                    time.sleep(0.2)
                self.assertEqual("running", pm_status(url)["status"])

                second = subprocess.run(
                    [sys.executable, "-m", "logplane.cli", "up", "--config", str(config)],
                    cwd=root, env=environment, capture_output=True, text=True, timeout=30)
                self.assertEqual(2, second.returncode)
                self.assertIn("already running", second.stderr)

                up.send_signal(signal.SIGINT)
                out, err = up.communicate(timeout=30)
            finally:
                if up.poll() is None:
                    up.kill()
                    up.communicate()
            self.assertEqual(0, up.returncode, err)
            self.assertIn(f"logplane is up: bus {url}", out)
            self.assertIn("Stopping the coordinator, then the bus", out)
            self.assertEqual("not_running", pm_status(url)["status"])
            self.assertTrue((root / ".logplane" / "logs" / "pm.log").exists())


if __name__ == "__main__":
    unittest.main()
