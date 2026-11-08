"""Credential-free coverage of the Claude Code worker example."""

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from examples.claude_code.claude_code_executor import ClaudeCodeExecutor, telemetry_usage
from examples.claude_code.run_worker import parse_args
from logplane.executors import AssignmentContext, Blocked, Completed, PermanentFailure, RetryableFailure

REPO = Path(__file__).resolve().parents[1]

FAKE_CLAUDE = r'''
import json, os, pathlib, sys, time
mode = os.environ.get("FAKE_CLAUDE_MODE", "structured")
stdin = sys.stdin.read()
log = os.environ.get("FAKE_CLAUDE_LOG")
if log:
    pathlib.Path(log).write_text(json.dumps({
        "argv": sys.argv[1:], "stdin": stdin, "has_key": "ANTHROPIC_API_KEY" in os.environ}))
pathlib.Path("started").touch()
usage = {"input_tokens": 100, "output_tokens": 20,
         "cache_creation_input_tokens": 5, "cache_read_input_tokens": 0}
base = {"type": "result", "subtype": "success", "is_error": False, "num_turns": 2,
        "total_cost_usd": 0.0123, "usage": usage, "modelUsage": {"claude-test": {}},
        "permission_denials": []}
outcome = {"status": "completed", "summary": "done", "result_json": json.dumps({"value": 42})}
code = 0
if mode == "structured":
    base.update(result="Done.", structured_output=outcome)
elif mode == "blocked":
    base.update(result="", structured_output={"status": "blocked", "reason": "Which branch?"})
elif mode == "text":
    base.update(result=json.dumps({"status": "completed", "summary": "done", "result": {"value": 42}}))
elif mode == "bad_result":
    base.update(result="", structured_output={**outcome, "result_json": "[1, 2]"})
elif mode == "auth_error":
    base.update(is_error=True, result="Invalid API key · Please run /login")
    code = 1
elif mode in ("error_max_turns", "error_max_structured_output_retries", "error_max_budget_usd"):
    base.update(subtype=mode, is_error=True)
    code = 1
elif mode == "garbage":
    print("not json")
    sys.exit(0)
elif mode == "sleep":
    time.sleep(30)
elif mode == "overflow":
    sys.stdout.write("x" * 300000)
    sys.stdout.flush()
    time.sleep(30)
print(json.dumps(base))
sys.exit(code)
'''


def assignment():
    return AssignmentContext(correlation_id="trial", task_id=1,
                             assignment_id="task:1:attempt:1", assignment_event_id=3,
                             attempt=1, goal="Return 42", assignee="claude",
                             worker_instance_id="worker-1")


class RecordingSink:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def _call(self, name, assignment, **fields):
        if self.fail:
            raise RuntimeError("bus down")
        self.calls.append((name, fields))
        return {"id": len(self.calls)}

    def model_started(self, assignment, **fields):
        return self._call("model_started", assignment, **fields)

    def model_completed(self, assignment, **fields):
        return self._call("model_completed", assignment, **fields)

    def model_failed(self, assignment, **fields):
        return self._call("model_failed", assignment, **fields)


class ClaudeCodeExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fake = self.root / "fake_claude"
        self.fake.write_text(f"#!{sys.executable}\n{FAKE_CLAUDE}", encoding="utf-8")
        self.fake.chmod(0o755)
        self.log = self.root / "invocation.json"

    def tearDown(self):
        self.temp.cleanup()

    def executor(self, mode="structured", auth="login", **kwargs):
        environment = {**os.environ, "FAKE_CLAUDE_MODE": mode, "FAKE_CLAUDE_LOG": str(self.log),
                       "ANTHROPIC_API_KEY": "test-key-not-real"}
        return ClaudeCodeExecutor(auth=auth, working_directory=self.root, command=(str(self.fake),),
                                  environment=environment, **kwargs)

    def run_one(self, mode="structured", **kwargs):
        executor = self.executor(mode, **kwargs)
        try:
            return executor.execute(assignment()), executor
        finally:
            executor.close()

    def invocation(self):
        return json.loads(self.log.read_text())

    def test_login_mode_uses_the_login_and_user_settings_only(self):
        outcome, executor = self.run_one(auth="login")
        self.assertIsInstance(outcome, Completed)
        self.assertEqual({"value": 42}, dict(outcome.result))
        seen = self.invocation()
        self.assertNotIn("--bare", seen["argv"])
        self.assertIn("--setting-sources", seen["argv"])
        self.assertEqual("user", seen["argv"][seen["argv"].index("--setting-sources") + 1])
        self.assertFalse(seen["has_key"], "an exported API key must not reach a login-mode run")
        report = executor.last_report[1]
        self.assertEqual("structured_output", report["outcome_source"])
        self.assertEqual("login", report["auth"])
        self.assertEqual(0.0123, report["total_cost_usd"])

    def test_api_key_mode_runs_bare_with_the_key(self):
        outcome, _ = self.run_one(auth="api-key")
        self.assertIsInstance(outcome, Completed)
        seen = self.invocation()
        self.assertEqual("--bare", seen["argv"][0])
        self.assertNotIn("--setting-sources", seen["argv"])
        self.assertTrue(seen["has_key"])

    def test_api_key_mode_requires_a_key(self):
        with self.assertRaisesRegex(ValueError, "ANTHROPIC_API_KEY"):
            ClaudeCodeExecutor(auth="api-key", working_directory=self.root, environment={})
        with self.assertRaises(ValueError):
            ClaudeCodeExecutor(auth="password", working_directory=self.root, environment={})

    def test_locked_down_by_default_and_prompt_goes_on_stdin(self):
        self.run_one()
        argv = self.invocation()["argv"]
        self.assertEqual("", argv[argv.index("--tools") + 1])
        self.assertEqual("dontAsk", argv[argv.index("--permission-mode") + 1])
        self.assertEqual("mcp__*", argv[argv.index("--disallowedTools") + 1])
        for flag in ("--strict-mcp-config", "--disable-slash-commands", "--no-session-persistence",
                     "--json-schema"):
            self.assertIn(flag, argv)
        self.assertFalse(any("task:1:attempt:1" in item for item in argv))
        self.assertIn('"assignment_id": "task:1:attempt:1"', self.invocation()["stdin"])
        self.assertIn("Tools are disabled", self.invocation()["stdin"])

    def test_read_only_tools_option(self):
        self.run_one(read_only_tools=True)
        argv = self.invocation()["argv"]
        self.assertEqual("Read,Grep,Glob", argv[argv.index("--tools") + 1])
        self.assertIn("cannot edit files", self.invocation()["stdin"])

    def test_model_effort_and_limits_are_passed(self):
        self.run_one(model="sonnet", effort="high", max_turns=6, max_budget_usd=1.25)
        argv = self.invocation()["argv"]
        for flag, value in (("--model", "sonnet"), ("--effort", "high"),
                            ("--max-turns", "6"), ("--max-budget-usd", "1.25")):
            self.assertEqual(value, argv[argv.index(flag) + 1])

    def test_text_fallback_and_blocked(self):
        outcome, executor = self.run_one("text")
        self.assertIsInstance(outcome, Completed)
        self.assertEqual("result_text", executor.last_report[1]["outcome_source"])
        outcome, _ = self.run_one("blocked")
        self.assertIsInstance(outcome, Blocked)
        self.assertEqual("Which branch?", outcome.reason)

    def test_failures_map_to_clear_codes(self):
        for mode, code in (("auth_error", "claude_failed"),
                           ("error_max_turns", "claude_max_turns"),
                           ("error_max_structured_output_retries", "claude_no_valid_outcome"),
                           ("error_max_budget_usd", "claude_budget_exceeded"),
                           ("bad_result", "claude_invalid_outcome"),
                           ("garbage", "claude_invalid_output")):
            with self.subTest(mode=mode):
                outcome, _ = self.run_one(mode)
                self.assertIsInstance(outcome, PermanentFailure)
                self.assertEqual(code, outcome.code)
        outcome, _ = self.run_one("auth_error")
        self.assertIn("Invalid API key", outcome.reason)

    def test_timeout_output_limit_and_cancel(self):
        outcome, _ = self.run_one("sleep", timeout=0.3)
        self.assertEqual("claude_timeout", outcome.code)
        outcome, _ = self.run_one("overflow", timeout=5)
        self.assertEqual("claude_output_too_large", outcome.code)
        executor = self.executor("sleep", timeout=10)
        outcomes = []
        thread = threading.Thread(target=lambda: outcomes.append(executor.execute(assignment())))
        thread.start()
        try:
            deadline = time.monotonic() + 3
            while not (self.root / "started").exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            executor.cancel("task:1:attempt:1")
            thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertIsInstance(outcomes[0], RetryableFailure)
            self.assertEqual("claude_cancelled", outcomes[0].code)
        finally:
            executor.close()
            thread.join(3)

    def test_usage_is_reported_for_budget_accounting(self):
        sink = RecordingSink()
        self.run_one(telemetry_sink=sink)
        self.assertEqual(["model_started", "model_completed"], [name for name, _ in sink.calls])
        usage = sink.calls[1][1]["usage"]
        self.assertEqual(125, usage["total_tokens"])
        self.assertEqual(0.0123, usage["cost_usd"])
        self.assertEqual("claude-test", sink.calls[1][1]["model"])
        sink = RecordingSink()
        self.run_one("error_max_turns", telemetry_sink=sink)
        self.assertEqual("model_failed", sink.calls[1][0])
        self.assertFalse(sink.calls[1][1]["retryable"])

    def test_telemetry_failure_does_not_change_the_outcome(self):
        outcome, _ = self.run_one(telemetry_sink=RecordingSink(fail=True))
        self.assertIsInstance(outcome, Completed)

    def test_usage_without_counts_omits_tokens(self):
        self.assertEqual({}, telemetry_usage({}))
        self.assertEqual({"cost_usd": 0.5, "cost_status": "estimated"},
                         telemetry_usage({"total_cost_usd": 0.5, "usage": {"input_tokens": 1}}))

    def test_worker_requires_an_explicit_auth_choice(self):
        with self.assertRaises(SystemExit):
            parse_args(["--working-directory", str(self.root)])
        args = parse_args(["--auth", "login", "--working-directory", str(self.root)])
        self.assertEqual("login", args.auth)

    def test_live_smoke_with_stand_in_reaches_budget_accounting(self):
        evidence = self.root / "evidence"
        environment = {**os.environ, "FAKE_CLAUDE_MODE": "structured"}
        environment.pop("ANTHROPIC_API_KEY", None)
        done = subprocess.run(
            [sys.executable, "-m", "examples.claude_code.live_smoke", "--yes", "--auth", "login",
             "--claude-command", str(self.fake), "--output", str(evidence)],
            cwd=REPO, env=environment, capture_output=True, text=True, timeout=90)
        self.assertEqual(0, done.returncode, done.stdout[-2000:] + done.stderr[-2000:])
        summary = json.loads((evidence / "summary.json").read_text())
        self.assertTrue(all(summary["checks"].values()), summary["checks"])
        self.assertEqual("login", summary["claude_report"]["auth"])


if __name__ == "__main__":
    unittest.main()
