"""Credential-free coverage of the OpenClaw pilot adapter."""

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from examples.openclaw.openclaw_executor import OpenClawExecutor, build_prompt, parse_result
from examples.openclaw.live_smoke import verify_trial
from logplane.executors import AssignmentContext, Completed, PermanentFailure, RetryableFailure
from tests.test_pm_agent import assigned, created, event, registered


def assignment():
    return AssignmentContext(correlation_id="trial", task_id=1,
                             assignment_id="task:1:attempt:1", assignment_event_id=3,
                             attempt=1, goal="Return 42", assignee="openclaw",
                             worker_instance_id="worker-1")


FAKE = '''import json, pathlib, sys, time
mode = sys.argv[1]
args = sys.argv[2:]
assert args[:2] == ["agent", "exec"]
assert "--json" in args and "--config" in args and "--cwd" in args
assert "--deliver" not in args and "--session-id" not in args
prompt = pathlib.Path(args[args.index("--message-file") + 1]).read_text()
assert "task:1:attempt:1" in prompt
pathlib.Path("started").touch()
if mode == "sleep": time.sleep(30)
if mode == "overflow":
    sys.stdout.write("x" * 70000)
    sys.stdout.flush()
    time.sleep(30)
else:
    print(json.dumps({"ok": True, "status": "ok", "final": json.dumps({
        "status": "completed", "summary": "done", "result": {"value": 42}}),
        "toolSummary": {"calls": 0}, "usage": {"total": 12}}))
'''


class OpenClawExampleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"tools": {"deny": ["*"]}, "plugins": {"enabled": False}}))
        self.fake = self.root / "fake.py"
        self.fake.write_text(FAKE)

    def tearDown(self):
        self.temp.cleanup()

    def executor(self, mode="ok", **kwargs):
        return OpenClawExecutor(command=(sys.executable, str(self.fake), mode),
                                config=self.config, working_directory=self.root,
                                model="test/model", **kwargs)

    def test_successful_bounded_process_and_report(self):
        executor = self.executor()
        try:
            outcome = executor.execute(assignment())
            self.assertIsInstance(outcome, Completed)
            self.assertEqual(outcome.result["value"], 42)
            self.assertEqual(executor.last_report[1]["usage"]["total"], 12)
        finally:
            executor.close()

    def test_timeout(self):
        executor = self.executor("sleep", timeout=0.2)
        outcome = executor.execute(assignment())
        self.assertIsInstance(outcome, RetryableFailure)
        self.assertEqual(outcome.code, "openclaw_timeout")

    def test_output_limit(self):
        executor = self.executor("overflow", timeout=3)
        outcome = executor.execute(assignment())
        self.assertIsInstance(outcome, PermanentFailure)
        self.assertEqual(outcome.code, "openclaw_output_too_large")

    def test_cancel_and_close(self):
        executor = self.executor("sleep", timeout=5)
        outcomes = []
        thread = threading.Thread(target=lambda: outcomes.append(executor.execute(assignment())))
        thread.start()
        try:
            deadline = time.monotonic() + 2
            while not (self.root / "started").exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            executor.cancel(assignment().assignment_id)
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcomes[0].code, "openclaw_cancelled")
        finally:
            executor.close()
            thread.join(3)
        self.assertIsInstance(executor.execute(assignment()), PermanentFailure)
        self.assertEqual(executor._active, {})

    def test_cleanup_errors_do_not_replace_the_outcome(self):
        from unittest import mock
        import subprocess
        executor = self.executor()
        real_wait = subprocess.Popen.wait
        calls = {"wait": 0}

        def stubborn_wait(process, timeout=None):
            calls["wait"] += 1
            if calls["wait"] > 1:  # the first reap succeeds; later waits stall
                raise subprocess.TimeoutExpired(process.args, timeout)
            return real_wait(process, timeout=timeout)

        try:
            with mock.patch("examples.openclaw.openclaw_executor.os.killpg",
                            side_effect=PermissionError), \
                 mock.patch.object(subprocess.Popen, "wait", stubborn_wait):
                outcome = executor.execute(assignment())
            self.assertIsInstance(outcome, Completed)
            self.assertEqual(outcome.result["value"], 42)
        finally:
            executor.close()

    def test_cancel_before_start_does_not_poison_future_assignment(self):
        executor = self.executor()
        executor.cancel(assignment().assignment_id)
        self.assertIsInstance(executor.execute(assignment()), Completed)

    def test_rejects_tool_capable_config_and_invalid_timeout(self):
        with self.assertRaises(ValueError):
            self.executor(timeout=float("nan"))
        self.config.write_text('{"tools":{"deny":[]},"plugins":{"enabled":false}}')
        with self.assertRaisesRegex(ValueError, "tools.deny"):
            self.executor()

    def test_requires_successful_outer_and_inner_outcome(self):
        for code, envelope in (
            (0, {}), (0, []), (0, {"ok": True, "status": "ok", "final": "not json"}),
            (0, {"ok": False, "status": "error", "final": '{}'}),
            (1, {"ok": True, "status": "ok", "final": '{}'}),
        ):
            with self.subTest(code=code, envelope=envelope):
                outcome, _ = parse_result(code, json.dumps(envelope).encode())
                self.assertIsInstance(outcome, PermanentFailure)
        outcome, _ = parse_result(2, b'{"ok":false,"status":"timeout"}')
        self.assertIsInstance(outcome, RetryableFailure)

    def test_all_structured_outcome_kinds(self):
        for data in (
            {"status": "completed", "summary": "done", "result": {}},
            {"status": "blocked", "reason": "need input"},
            {"status": "retryable_failure", "code": "temporary", "reason": "retry"},
            {"status": "permanent_failure", "code": "permanent", "reason": "stop"},
        ):
            with self.subTest(status=data["status"]):
                outcome, _ = parse_result(0, json.dumps({"ok": True, "status": "ok", "final": json.dumps(data)}).encode())
                self.assertNotEqual(getattr(outcome, "code", None), "openclaw_invalid_output")

    def test_prompt_includes_decision_and_dependency_contract(self):
        prompt = build_prompt(assignment())
        self.assertIn("authoritative human answers", prompt)
        self.assertIn('"dependencies": []', prompt)
        self.assertIn('"assignment_id": "task:1:attempt:1"', prompt)

    def test_smoke_accepts_missing_optional_tool_count_without_claiming_zero(self):
        events = [registered(), created(), assigned(3, 1), event(4, "task.completed", "alice", {
            "task_id": 1, "assignment_id": "task:1:attempt:1", "worker_instance_id": "alice-1",
            "summary": "done", "result": {"value": 42},
        }, caused_by=3)]
        self.assertTrue(verify_trial(events, 1, {}))
        self.assertTrue(verify_trial(events, 1, {"toolSummary": {"calls": 0}}))
        self.assertFalse(verify_trial(events, 1, {"toolSummary": {"calls": 1}}))
        events[-1]["payload"]["result"] = {"value": 0}
        self.assertFalse(verify_trial(events, 1, {}))


if __name__ == "__main__":
    unittest.main()
