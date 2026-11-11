"""Credential-free coverage of the Codex worker example."""

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from examples.codex.codex_executor import CodexExecutor, parse_events, strict_outcome_schema
from logplane.executors import AssignmentContext, Completed, PermanentFailure, RetryableFailure

FAKE_CODEX = r'''
import json, os, pathlib, sys, time
args = sys.argv[1:]
if args[:2] == ["login", "status"]:
    status = os.environ.get("FAKE_LOGIN", "Logged in using ChatGPT")
    sys.stderr.write(status + "\n")
    sys.exit(0 if status.startswith("Logged in") else 1)
assert args[0] == "exec" and args[-1] == "-", args
log = os.environ.get("FAKE_CODEX_LOG")
stdin = sys.stdin.read()
schema = json.loads(pathlib.Path(args[args.index("--output-schema") + 1]).read_text())
if log:
    pathlib.Path(log).write_text(json.dumps({
        "argv": args, "stdin": stdin, "schema": schema,
        "auth_names": sorted(n for n in os.environ if n.startswith("OPENAI_") or n == "CODEX_API_KEY")}))
for name, value in (("FAKE_PID", str(os.getpid())),):
    if os.environ.get(name):
        pathlib.Path(os.environ[name]).write_text(value)
mode = os.environ.get("FAKE_CODEX_MODE", "ok")
def emit(event):
    print(json.dumps(event), flush=True)
emit({"type": "thread.started", "thread_id": "t1"})
emit({"type": "turn.started"})
if mode == "sleep":
    time.sleep(30)
if mode == "write":
    emit({"type": "item.completed", "item": {"id": "i0", "type": "file_change", "changes": []}})
if mode == "fail":
    emit({"type": "turn.failed", "error": {"message": "usage limit reached"}})
    sys.exit(1)
emit({"type": "item.completed", "item": {"id": "i1", "type": "command_execution",
                                         "command": "rg foo", "status": "completed"}})
outcome = {"status": "completed", "summary": "done", "reason": None, "code": None,
           "result_json": json.dumps({"value": 42})}
if mode == "garbled":
    outcome = "not json at all"
emit({"type": "item.completed", "item": {"id": "i2", "type": "agent_message",
                                         "text": outcome if isinstance(outcome, str) else json.dumps(outcome)}})
emit({"type": "turn.completed", "usage": {"input_tokens": 1000, "cached_input_tokens": 400,
                                          "output_tokens": 50, "reasoning_output_tokens": 20}})
'''


def assignment():
    return AssignmentContext(correlation_id="trial", task_id=1, assignment_id="task:1:attempt:1",
                             assignment_event_id=3, attempt=1, goal="Return 42",
                             assignee="codex", worker_instance_id="worker-1")


class RecordingSink:
    def __init__(self):
        self.calls = []

    def model_started(self, assignment, **fields):
        self.calls.append(("model_started", fields))

    def model_completed(self, assignment, **fields):
        self.calls.append(("model_completed", fields))

    def model_failed(self, assignment, **fields):
        self.calls.append(("model_failed", fields))


class CodexExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fake = self.root / "fake_codex"
        self.fake.write_text(f"#!{sys.executable}\n{FAKE_CODEX}", encoding="utf-8")
        self.fake.chmod(0o755)
        self.log = self.root / "invocation.json"
        self.work = self.root / "work"
        self.work.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def executor(self, mode="ok", **kwargs):
        environment = {**os.environ, "FAKE_CODEX_MODE": mode, "FAKE_CODEX_LOG": str(self.log),
                       "OPENAI_API_KEY": "sk-test-not-real", "CODEX_API_KEY": "x",
                       "OPENAI_BASE_URL": "http://gateway", **kwargs.pop("extra_env", {})}
        return CodexExecutor(working_directory=self.work, command=(str(self.fake),),
                             environment=environment, **kwargs)

    def run_one(self, mode="ok", **kwargs):
        executor = self.executor(mode, **kwargs)
        try:
            return executor.execute(assignment()), executor
        finally:
            executor.close()

    def test_read_only_clean_run_with_chatgpt_login(self):
        outcome, executor = self.run_one(model="gpt-test", effort="high")
        self.assertIsInstance(outcome, Completed)
        self.assertEqual({"value": 42}, dict(outcome.result))
        seen = json.loads(self.log.read_text())
        argv = seen["argv"]
        self.assertEqual("read-only", argv[argv.index("--sandbox") + 1])
        for flag in ("--json", "--ephemeral", "--ignore-user-config", "--ignore-rules",
                     "--skip-git-repo-check"):
            self.assertIn(flag, argv)
        self.assertEqual("gpt-test", argv[argv.index("--model") + 1])
        self.assertIn('model_reasoning_effort="high"', argv)
        self.assertEqual([], seen["auth_names"], "API keys and endpoints must not reach Codex")
        self.assertIn('"assignment_id": "task:1:attempt:1"', seen["stdin"])
        self.assertFalse(any("task:1:attempt:1" in a for a in argv))
        self.assertEqual("chatgpt", executor.auth_method)
        self.assertIn("OPENAI_API_KEY", executor.removed_variables)
        report = executor.last_report[1]
        self.assertEqual(1000, report["usage"]["input_tokens"])
        self.assertEqual({"agent_message": 1, "command_execution": 1}, report["items"])

    def test_refuses_anything_but_a_chatgpt_login(self):
        for status in ("Not logged in", "API key configured - sk-…", "Logged in using access token"):
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.executor(extra_env={"FAKE_LOGIN": status})

    def test_strict_schema(self):
        schema = strict_outcome_schema({"type": "object", "properties": {"x": {"type": "string"}},
                                        "required": ["x"], "additionalProperties": False})
        self.assertEqual(sorted(schema["properties"]), schema["required"])
        self.assertFalse(schema["additionalProperties"])
        self.run_one(result_schema={"type": "object", "properties": {}, "required": [],
                                    "additionalProperties": False})
        self.assertIn("result", json.loads(self.log.read_text())["schema"]["properties"])

    def test_actions_beyond_reading_discard_the_run(self):
        outcome, _ = self.run_one("write")
        self.assertIsInstance(outcome, PermanentFailure)
        self.assertEqual("codex_unexpected_actions", outcome.code)

    def test_failures(self):
        outcome, _ = self.run_one("fail")
        self.assertEqual("codex_failed", outcome.code)
        self.assertIn("usage limit", outcome.reason)
        outcome, _ = self.run_one("garbled")
        self.assertEqual("codex_invalid_outcome", outcome.code)
        self.assertEqual("codex_invalid_output", parse_events(0, b"not json\n")[0].code)
        self.assertEqual("codex_no_outcome", parse_events(0, b'{"type":"turn.completed","usage":{}}\n')[0].code)

    def test_timeout_and_cancel(self):
        outcome, _ = self.run_one("sleep", timeout=0.5)
        self.assertIsInstance(outcome, RetryableFailure)
        self.assertEqual("codex_timeout", outcome.code)
        pid_file = self.root / "pid"
        executor = self.executor("sleep", timeout=10, extra_env={"FAKE_PID": str(pid_file)})
        outcomes = []
        thread = threading.Thread(target=lambda: outcomes.append(executor.execute(assignment())))
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while not pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            pid = int(pid_file.read_text())
            executor.cancel("task:1:attempt:1")
            thread.join(5)
            self.assertEqual("codex_cancelled", outcomes[0].code)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            executor.close()
            thread.join(3)

    def test_usage_reaches_telemetry(self):
        sink = RecordingSink()
        self.run_one(telemetry_sink=sink)
        self.assertEqual(["model_started", "model_completed"], [name for name, _ in sink.calls])
        self.assertEqual(1050, sink.calls[1][1]["usage"]["total_tokens"])


if __name__ == "__main__":
    unittest.main()
