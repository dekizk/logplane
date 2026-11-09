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

def settings(path):
    try:
        return json.loads(pathlib.Path(path).read_text())
    except (OSError, ValueError):
        return {}

# A small model of Claude Code: settings `env` blocks apply unless --bare, and
# --setting-sources limits which settings files load.
argv = sys.argv[1:]
sources = (argv[argv.index("--setting-sources") + 1].split(",") if "--setting-sources" in argv
           else ["user", "project", "local"])
effective = dict(os.environ)
if "--bare" not in argv:
    if "user" in sources:
        effective.update(settings(os.path.join(os.environ.get("CLAUDE_CONFIG_DIR", "/nonexistent"),
                                               "settings.json")).get("env", {}))
    if "project" in sources:
        effective.update(settings(os.path.join(os.getcwd(), ".claude", "settings.json")).get("env", {}))
third_party = any(effective.get(name) for name in (
    "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY"))
AUTH_NAMES = sorted(n for n in os.environ if n.startswith(("ANTHROPIC_", "CLAUDE_CODE_USE_"))
                    or n.startswith("CLAUDE_CODE_OAUTH"))

if argv == ["auth", "status"]:
    method = os.environ.get("FAKE_AUTH_METHOD") or (
        "third_party" if third_party else "api_key" if effective.get("ANTHROPIC_API_KEY")
        else "claude.ai")
    auth_log = os.environ.get("FAKE_AUTH_LOG")
    if auth_log:
        pathlib.Path(auth_log).write_text(json.dumps({
            "auth_names": AUTH_NAMES, "cwd": os.getcwd(), "cwd_entries": os.listdir(".")}))
    print(json.dumps({"loggedIn": method != "none", "authMethod": method}))
    sys.exit(0 if method != "none" else 1)

mode = os.environ.get("FAKE_CLAUDE_MODE", "structured")
stdin = sys.stdin.read()
log = os.environ.get("FAKE_CLAUDE_LOG")
if log:
    pathlib.Path(log).write_text(json.dumps({
        "argv": argv, "stdin": stdin, "has_key": "ANTHROPIC_API_KEY" in os.environ,
        "auth_names": AUTH_NAMES}))
for name, value in (("FAKE_STARTED", "started"), ("FAKE_PID", str(os.getpid()))):
    if os.environ.get(name):
        pathlib.Path(os.environ[name]).write_text(value)

key_source = "none" if third_party else (
    "ANTHROPIC_API_KEY" if effective.get("ANTHROPIC_API_KEY") else "none")
key_source = os.environ.get("FAKE_KEY_SOURCE", key_source)
granted = argv[argv.index("--tools") + 1] if "--tools" in argv else "Bash,Edit,Read"
tools = os.environ.get("FAKE_TOOLS", granted)
init = {"type": "system", "subtype": "init", "apiKeySource": key_source,
        "tools": [t for t in tools.split(",") if t] + ["StructuredOutput"],
        "model": "claude-test", "mcp_servers": [], "permissionMode": "dontAsk",
        "claude_code_version": "9.9.9", "cwd": os.getcwd()}
if not os.environ.get("FAKE_NO_INIT"):
    print(json.dumps(init))

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
    for _ in range(10):
        sys.stdout.write("x" * (1024 * 1024))
    sys.stdout.flush()
    time.sleep(30)
print(json.dumps({"type": "assistant", "message": {"content": []}}))
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
        self.auth_log = self.root / "auth.json"
        self.config = self.root / "claude-config"  # stands in for ~/.claude
        self.config.mkdir()
        self.work = self.root / "work"
        self.work.mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def environment(self, mode="structured", **extra):
        environment = {**os.environ, "FAKE_CLAUDE_MODE": mode, "FAKE_CLAUDE_LOG": str(self.log),
                       "FAKE_AUTH_LOG": str(self.auth_log), "CLAUDE_CONFIG_DIR": str(self.config),
                       "ANTHROPIC_API_KEY": "test-key-not-real"}
        environment.update(extra)
        return environment

    def executor(self, mode="structured", auth="login", environment=None, **kwargs):
        return ClaudeCodeExecutor(auth=auth, working_directory=self.work, command=(str(self.fake),),
                                  environment=environment or self.environment(mode), **kwargs)

    def run_one(self, mode="structured", **kwargs):
        executor = self.executor(mode, **kwargs)
        try:
            return executor.execute(assignment()), executor
        finally:
            executor.close()

    def invocation(self):
        return json.loads(self.log.read_text())

    def test_login_mode_uses_the_login_in_safe_mode_with_user_settings_only(self):
        outcome, executor = self.run_one(auth="login")
        self.assertIsInstance(outcome, Completed)
        self.assertEqual({"value": 42}, dict(outcome.result))
        seen = self.invocation()
        self.assertNotIn("--bare", seen["argv"])
        self.assertIn("--safe-mode", seen["argv"])
        self.assertEqual("user", seen["argv"][seen["argv"].index("--setting-sources") + 1])
        self.assertFalse(seen["has_key"], "an exported API key must not reach a login-mode run")
        report = executor.last_report[1]
        self.assertEqual("structured_output", report["outcome_source"])
        self.assertEqual("login", report["auth"])
        self.assertEqual("claude.ai", report["auth_method"])
        self.assertTrue(report["safe_mode"])
        self.assertEqual(0.0123, report["total_cost_usd"])

    def test_login_mode_removes_every_auth_and_provider_switch(self):
        environment = self.environment(
            ANTHROPIC_AUTH_TOKEN="t", ANTHROPIC_BASE_URL="http://gateway", ANTHROPIC_PROFILE="p",
            CLAUDE_CODE_USE_BEDROCK="1", CLAUDE_CODE_USE_VERTEX="1",
            CLAUDE_CODE_OAUTH_TOKEN="login-token")
        outcome, executor = self.run_one(environment=environment)
        self.assertIsInstance(outcome, Completed)
        # Only the Claude login's own token survives, in the check and in the run.
        self.assertEqual(["CLAUDE_CODE_OAUTH_TOKEN"], json.loads(self.auth_log.read_text())["auth_names"],
                         "auth status must be checked with the same clean environment")
        self.assertEqual(["CLAUDE_CODE_OAUTH_TOKEN"], self.invocation()["auth_names"])
        self.assertIn("CLAUDE_CODE_USE_BEDROCK", executor.removed_variables)
        self.assertIn("ANTHROPIC_AUTH_TOKEN", executor.last_report[1]["removed_variables"])
        self.assertNotIn("CLAUDE_CODE_OAUTH_TOKEN", executor.removed_variables)

    def test_login_mode_refuses_anything_but_a_claude_login(self):
        for method in ("api_key", "api_key_helper", "third_party", "none"):
            with self.subTest(method=method):
                with self.assertRaises(ValueError):
                    self.executor(environment=self.environment(FAKE_AUTH_METHOD=method))
        executor = self.executor(environment=self.environment(FAKE_AUTH_METHOD="oauth_token"))
        self.assertEqual("oauth_token", executor.auth_method)

    def test_user_customizations_are_an_explicit_opt_in(self):
        self.run_one(load_user_config=True)
        self.assertNotIn("--safe-mode", self.invocation()["argv"])
        with self.assertRaises(ValueError):
            self.executor(auth="api-key", load_user_config=True)

    def test_api_key_mode_runs_bare_with_the_key(self):
        outcome, executor = self.run_one(auth="api-key")
        self.assertIsInstance(outcome, Completed)
        seen = self.invocation()
        self.assertEqual("--bare", seen["argv"][0])
        self.assertNotIn("--setting-sources", seen["argv"])
        self.assertTrue(seen["has_key"])
        self.assertEqual("ANTHROPIC_API_KEY", executor.last_report[1]["api_key_source"])

    def test_api_key_mode_removes_provider_and_token_overrides(self):
        environment = self.environment(
            CLAUDE_CODE_USE_BEDROCK="1", AWS_BEARER_TOKEN_BEDROCK="b", ANTHROPIC_AUTH_TOKEN="t",
            ANTHROPIC_BASE_URL="http://gateway", CLAUDE_CODE_OAUTH_TOKEN="login-token")
        outcome, executor = self.run_one(auth="api-key", environment=environment)
        self.assertIsInstance(outcome, Completed)
        self.assertEqual(["ANTHROPIC_API_KEY"], self.invocation()["auth_names"])
        for name in ("CLAUDE_CODE_USE_BEDROCK", "AWS_BEARER_TOKEN_BEDROCK", "ANTHROPIC_AUTH_TOKEN",
                     "ANTHROPIC_BASE_URL", "CLAUDE_CODE_OAUTH_TOKEN"):
            self.assertIn(name, executor.removed_variables)
        self.assertNotIn("ANTHROPIC_API_KEY", executor.removed_variables)

    def test_each_run_must_report_the_expected_credential_source(self):
        for auth, wrong in (("login", "apiKeyHelper"), ("login", "ANTHROPIC_API_KEY"),
                            ("api-key", "none"), ("api-key", "apiKeyHelper")):
            with self.subTest(auth=auth, wrong=wrong):
                outcome, executor = self.run_one(
                    auth=auth, environment=self.environment(FAKE_KEY_SOURCE=wrong))
                self.assertIsInstance(outcome, PermanentFailure)
                self.assertEqual("claude_wrong_auth", outcome.code)
                self.assertEqual(wrong, executor.last_report[1]["api_key_source"])
        outcome, _ = self.run_one(environment=self.environment(FAKE_NO_INIT="1"))
        self.assertEqual("claude_unverified_run", outcome.code)

    def test_runs_with_unexpected_tools_are_discarded(self):
        for tools in ("Read,Bash", "mcp__files__read", "Write"):
            with self.subTest(tools=tools):
                outcome, _ = self.run_one(environment=self.environment(FAKE_TOOLS=tools))
                self.assertEqual("claude_unexpected_tools", outcome.code)
        outcome, executor = self.run_one(read_only_tools=True)
        self.assertIsInstance(outcome, Completed)
        self.assertEqual(["Glob", "Grep", "Read", "StructuredOutput"], executor.last_report[1]["tools"])

    def test_auth_check_sees_the_same_settings_as_the_worker(self):
        # Project settings in the worker's folder are ignored by worker runs
        # (--setting-sources user), so they must not decide the check either.
        (self.work / ".claude").mkdir()
        (self.work / ".claude" / "settings.json").write_text(
            json.dumps({"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}}))
        outcome, executor = self.run_one()
        self.assertIsInstance(outcome, Completed)
        self.assertEqual("claude.ai", executor.auth_method)
        self.assertEqual("none", executor.last_report[1]["api_key_source"])
        checked = json.loads(self.auth_log.read_text())
        self.assertNotEqual(str(self.work), checked["cwd"])
        self.assertEqual([], checked["cwd_entries"], "the check must run in an empty folder")
        # The launcher's own folder must not decide it either.
        launcher = self.root / "launcher"
        (launcher / ".claude").mkdir(parents=True)
        (launcher / ".claude" / "settings.json").write_text(
            json.dumps({"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}}))
        previous = os.getcwd()
        os.chdir(launcher)
        try:
            self.executor().close()
        finally:
            os.chdir(previous)
        # User settings do apply to worker runs, so a provider set there is refused.
        (self.config / "settings.json").write_text(
            json.dumps({"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}}))
        with self.assertRaisesRegex(ValueError, "third_party"):
            self.executor()

    def test_api_key_mode_requires_a_key(self):
        with self.assertRaisesRegex(ValueError, "ANTHROPIC_API_KEY"):
            ClaudeCodeExecutor(auth="api-key", working_directory=self.work, environment={})
        with self.assertRaises(ValueError):
            ClaudeCodeExecutor(auth="password", working_directory=self.work, environment={})

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

    def test_read_only_login_refuses_user_rules_that_widen_reads(self):
        for permissions in ({"additionalDirectories": ["/elsewhere"]},
                            {"allow": ["Read(//Users/me/**)"]}, {"allow": ["Grep"]}):
            with self.subTest(permissions=permissions):
                (self.config / "settings.json").write_text(json.dumps({"permissions": permissions}))
                with self.assertRaisesRegex(ValueError, "other folders"):
                    self.executor(read_only_tools=True)
        (self.config / "settings.json").write_text(json.dumps({"permissions": {"allow": ["WebSearch"]}}))
        self.executor(read_only_tools=True).close()
        # api-key mode never loads user settings, so the same rules don't matter there.
        (self.config / "settings.json").write_text(json.dumps({"permissions": {"allow": ["Read"]}}))
        self.executor(auth="api-key", read_only_tools=True).close()

    def test_read_only_refuses_symlinks_that_leave_the_working_directory(self):
        outside = self.root / "outside"
        outside.mkdir()
        (self.work / "inside.txt").write_text("ok")
        (self.work / "local-link").symlink_to(self.work / "inside.txt")
        self.executor(read_only_tools=True).close()
        (self.work / "nested").mkdir()
        (self.work / "nested" / "escape").symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "nested/escape"):
            self.executor(read_only_tools=True)

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

    def test_timeout_and_output_limit(self):
        outcome, _ = self.run_one("sleep", timeout=0.3)
        self.assertEqual("claude_timeout", outcome.code)
        outcome, _ = self.run_one("overflow", timeout=5)
        self.assertEqual("claude_output_too_large", outcome.code)

    def test_cancel_stops_a_running_claude_process(self):
        started, pid_file = self.root / "cancel-started", self.root / "cancel-pid"
        executor = self.executor(environment=self.environment(
            "sleep", FAKE_STARTED=str(started), FAKE_PID=str(pid_file)), timeout=10)
        outcomes = []
        thread = threading.Thread(target=lambda: outcomes.append(executor.execute(assignment())))
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while not pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(started.exists(), "the stand-in never started")
            pid = int(pid_file.read_text())
            os.kill(pid, 0)  # running now
            executor.cancel("task:1:attempt:1")
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertIsInstance(outcomes[0], RetryableFailure)
            self.assertEqual("claude_cancelled", outcomes[0].code)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
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
            parse_args(["--working-directory", str(self.work)])
        args = parse_args(["--auth", "login", "--working-directory", str(self.work)])
        self.assertEqual("login", args.auth)
        self.assertFalse(args.load_user_config)

    def test_live_smoke_with_stand_in_reaches_budget_accounting(self):
        evidence = self.root / "evidence"
        environment = {**os.environ, "FAKE_CLAUDE_MODE": "structured",
                       "CLAUDE_CONFIG_DIR": str(self.config)}
        done = subprocess.run(
            [sys.executable, "-m", "examples.claude_code.live_smoke", "--yes", "--auth", "login",
             "--claude-command", str(self.fake), "--output", str(evidence)],
            cwd=REPO, env=environment, capture_output=True, text=True, timeout=90)
        self.assertEqual(0, done.returncode, done.stdout[-2000:] + done.stderr[-2000:])
        summary = json.loads((evidence / "summary.json").read_text())
        self.assertTrue(all(summary["checks"].values()), summary["checks"])
        self.assertEqual("login", summary["claude_report"]["auth"])
        self.assertEqual("claude.ai", summary["claude_report"]["auth_method"])


if __name__ == "__main__":
    unittest.main()
