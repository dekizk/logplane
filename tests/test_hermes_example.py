import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, patch

from examples.hermes.hermes_executor import HermesExecutor
from examples.hermes import code_review, live_smoke, run_worker
from logplane.artifacts import ArtifactStore
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    PermanentFailure,
    RetryableFailure,
)


def assignment(**overrides):
    values = {
        "correlation_id": "hermes-trial",
        "task_id": 7,
        "assignment_id": "task:7:attempt:1",
        "assignment_event_id": 22,
        "attempt": 1,
        "goal": "Summarize the supplied integration note",
        "context": {"text": "Hermes is an interchangeable executor."},
        "required_capabilities": ("hermes",),
        "max_retries": 1,
        "retryable_failures": 0,
        "assignee": "hermes",
        "worker_instance_id": "hermes-1",
    }
    values.update(overrides)
    return AssignmentContext(**values)


FAKE_HERMES = r'''import json
import os
import subprocess
import sys
import time
from pathlib import Path

capture = os.environ.get("FAKE_HERMES_CAPTURE")
if capture:
    Path(capture).write_text(
        json.dumps({
            "argv": sys.argv[1:],
            "cwd": os.getcwd(),
            "source": os.environ.get("HERMES_SESSION_SOURCE"),
            "safe": os.environ.get("HERMES_SAFE_MODE"),
            "ignore_rules": os.environ.get("HERMES_IGNORE_RULES"),
        }),
        encoding="utf-8",
    )
started = os.environ.get("FAKE_HERMES_STARTED")
if started:
    Path(started).write_text("started", encoding="utf-8")
sleep_seconds = float(os.environ.get("FAKE_HERMES_SLEEP", "0"))
if sleep_seconds:
    time.sleep(sleep_seconds)
if "--usage-file" in sys.argv:
    usage_path = Path(sys.argv[sys.argv.index("--usage-file") + 1])
    usage_path.write_text(
        os.environ.get(
            "FAKE_HERMES_USAGE",
            json.dumps({"total_tokens": 12, "model": "fake", "unknown": "drop"}),
        ),
        encoding="utf-8",
    )
sys.stdout.write(os.environ.get("FAKE_HERMES_STDOUT", ""))
sys.stderr.write(os.environ.get("FAKE_HERMES_STDERR", ""))
raise SystemExit(int(os.environ.get("FAKE_HERMES_EXIT", "0")))
'''


class HermesExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.fake = self.root / "fake_hermes.py"
        self.fake.write_text(FAKE_HERMES, encoding="utf-8")

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_executor(self, output, **overrides):
        environment = {
            "FAKE_HERMES_STDOUT": json.dumps(output),
        }
        environment.update(overrides.pop("environment", {}))
        timeout = overrides.pop("timeout", 2)
        return HermesExecutor(
            working_directory=self.root,
            model="fake-model",
            provider="fake-provider",
            toolsets=("clarify",),
            command=(sys.executable, str(self.fake)),
            timeout=timeout,
            environment=environment,
            **overrides,
        )

    def test_all_logplane_outcomes_are_supported(self):
        cases = (
            (
                {"status": "completed", "summary": "done", "result": {"value": 1}},
                Completed,
            ),
            ({"status": "blocked", "reason": "approval"}, Blocked),
            (
                {
                    "status": "retryable_failure",
                    "code": "temporary",
                    "reason": "try again",
                },
                RetryableFailure,
            ),
            (
                {
                    "status": "permanent_failure",
                    "code": "invalid",
                    "reason": "cannot run",
                },
                PermanentFailure,
            ),
        )
        for payload, expected_type in cases:
            with self.subTest(status=payload["status"]):
                result = self.make_executor(payload).execute(assignment())
                self.assertIsInstance(result, expected_type)

    def test_invocation_is_isolated_bounded_and_reports_filtered_usage(self):
        capture = self.root / "capture.json"
        usages = []
        executor = self.make_executor(
            {"status": "completed", "summary": "done", "result": {}},
            environment={"FAKE_HERMES_CAPTURE": str(capture)},
            usage_callback=lambda assignment_id, usage: usages.append(
                (assignment_id, dict(usage))
            ),
        )
        result = executor.execute(assignment())
        self.assertIsInstance(result, Completed)

        recorded = json.loads(capture.read_text(encoding="utf-8"))
        args = recorded["argv"]
        prompt = args[args.index("-z") + 1]
        self.assertIn('"assignment_id": "task:7:attempt:1"', prompt)
        self.assertIn("logplane owns assignment", prompt)
        self.assertIn("decisions array contains authoritative human responses", prompt)
        self.assertEqual("clarify", args[args.index("--toolsets") + 1])
        self.assertIn("--safe-mode", args)
        self.assertNotIn("--reasoning", args)
        self.assertEqual(self.root.resolve(), Path(recorded["cwd"]).resolve())
        self.assertEqual("tool", recorded["source"])
        self.assertEqual("1", recorded["safe"])
        self.assertEqual("1", recorded["ignore_rules"])
        self.assertEqual("task:7:attempt:1", usages[0][0])
        self.assertEqual(12, usages[0][1]["total_tokens"])
        self.assertNotIn("unknown", usages[0][1])

    def test_model_telemetry_is_emitted_without_controlling_the_outcome(self):
        telemetry = Mock()
        telemetry.model_started.return_value = {"id": 88}
        executor = self.make_executor(
            {"status": "completed", "summary": "done", "result": {}},
            telemetry_sink=telemetry,
        )

        result = executor.execute(assignment())

        self.assertIsInstance(result, Completed)
        started = telemetry.model_started.call_args.kwargs
        self.assertEqual("task:7:attempt:1:model:1", started["invocation_id"])
        self.assertEqual("fake-provider", started["provider"])
        self.assertIn("ASSIGNMENT_JSON", started["input_content"])
        completed = telemetry.model_completed.call_args.kwargs
        self.assertEqual(88, completed["caused_by"])
        self.assertEqual(12, completed["usage"]["total_tokens"])

        telemetry.model_started.side_effect = RuntimeError("telemetry offline")
        telemetry.model_completed.side_effect = RuntimeError("telemetry offline")
        unaffected = executor.execute(assignment())
        self.assertIsInstance(unaffected, Completed)

    def test_prompt_marks_human_decisions_as_authoritative(self):
        prompt = HermesExecutor.build_prompt(
            assignment(
                attempt=2,
                assignment_id="task:7:attempt:2",
                context={"release_target": None},
                decisions=(
                    {
                        "event_id": 31,
                        "actor": "human",
                        "assignment_id": "task:7:attempt:1",
                        "decision_id": "decision:task:7:attempt:1",
                        "decision": {"release_target": "staging"},
                    },
                ),
            )
        )
        self.assertIn('"release_target": null', prompt)
        self.assertIn('"release_target": "staging"', prompt)
        self.assertIn("A later decision supersedes conflicting", prompt)
        self.assertIn("Do not block again for information already supplied", prompt)
        self.assertIn("dependencies array", prompt)
        self.assertIn("declared depends_on order", prompt)

    def test_process_errors_timeout_and_invalid_output_are_typed(self):
        failed = self.make_executor(
            {"status": "completed", "summary": "unused"},
            environment={"FAKE_HERMES_EXIT": "1", "FAKE_HERMES_STDERR": "offline"},
        ).execute(assignment())
        self.assertIsInstance(failed, RetryableFailure)
        self.assertEqual("hermes_process_failed", failed.code)

        timed_out = self.make_executor(
            {"status": "completed", "summary": "late"},
            timeout=0.03,
            environment={"FAKE_HERMES_SLEEP": "1"},
        ).execute(assignment())
        self.assertIsInstance(timed_out, RetryableFailure)
        self.assertEqual("hermes_timeout", timed_out.code)

        invalid = self.make_executor(
            {},
            environment={"FAKE_HERMES_STDOUT": "not-json"},
        ).execute(assignment())
        self.assertIsInstance(invalid, PermanentFailure)
        self.assertEqual("invalid_hermes_output", invalid.code)

        oversized = self.make_executor(
            {},
            max_output_bytes=64,
            environment={"FAKE_HERMES_STDOUT": "x" * 1000},
        ).execute(assignment())
        self.assertIsInstance(oversized, PermanentFailure)
        self.assertEqual("hermes_output_too_large", oversized.code)

        missing = HermesExecutor(
            working_directory=self.root,
            model="fake-model",
            provider="fake-provider",
            command=(str(self.root / "missing-hermes"),),
        ).execute(assignment())
        self.assertIsInstance(missing, PermanentFailure)
        self.assertEqual("hermes_start_failed", missing.code)

    def test_cancellation_terminates_the_active_process(self):
        started = self.root / "started"
        executor = self.make_executor(
            {"status": "completed", "summary": "late"},
            timeout=5,
            environment={
                "FAKE_HERMES_SLEEP": "5",
                "FAKE_HERMES_STARTED": str(started),
            },
        )
        results = []
        thread = threading.Thread(
            target=lambda: results.append(executor.execute(assignment()))
        )
        thread.start()
        deadline = time.time() + 2
        while time.time() < deadline and not started.exists():
            time.sleep(0.005)
        self.assertTrue(started.exists())
        executor.cancel("task:7:attempt:1")
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(results[0], RetryableFailure)
        self.assertEqual("hermes_cancelled", results[0].code)

    def test_reasoning_is_forwarded_in_safe_mode(self):
        capture = self.root / "reasoning.json"
        executor = self.make_executor(
            {"status": "completed", "summary": "done", "result": {}},
            reasoning="high",
            environment={"FAKE_HERMES_CAPTURE": str(capture)},
        )
        self.assertIsInstance(executor.execute(assignment()), Completed)
        args = json.loads(capture.read_text(encoding="utf-8"))["argv"]
        self.assertEqual(1, args.count("--reasoning"))
        self.assertEqual("high", args[args.index("--reasoning") + 1])
        self.assertIn("--safe-mode", args)

    def test_invalid_reasoning_is_rejected(self):
        for value in ("", "HIGH", "invalid", False, 3):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "reasoning"):
                self.make_executor({}, reasoning=value)

    def test_configuration_requires_explicit_bounded_authority(self):
        with self.assertRaisesRegex(ValueError, "safe_mode requires"):
            HermesExecutor(
                working_directory=self.root,
                model=None,
                provider=None,
            )
        for toolset in ("all", "*", "todo", "Todo", "delegation", "cronjob"):
            with self.subTest(toolset=toolset):
                with self.assertRaisesRegex(ValueError, "not allowed"):
                    HermesExecutor(
                        working_directory=self.root,
                        model="fake-model",
                        provider="fake-provider",
                        toolsets=(toolset,),
                    )


class HermesWorkerWiringTests(unittest.TestCase):
    def test_reasoning_cli_default_and_validation(self):
        base = ["--working-directory", "/tmp", "--model", "fake", "--provider", "fake"]
        self.assertIsNone(run_worker.parse_args(base).reasoning)
        self.assertEqual("none", run_worker.parse_args(base + ["--reasoning", "none"]).reasoning)
        with patch("sys.stderr"), self.assertRaises(SystemExit) as error:
            run_worker.parse_args(base + ["--reasoning", "invalid"])
        self.assertEqual(2, error.exception.code)

    @patch("examples.hermes.run_worker.WorkerRuntime")
    @patch("examples.hermes.run_worker.BusClient")
    @patch("examples.hermes.run_worker.HermesExecutor")
    def test_worker_uses_public_runtime_contract(
        self,
        executor_class,
        client_class,
        runtime_class,
    ):
        executor = executor_class.return_value
        bus_client = client_class.return_value
        runtime = runtime_class.return_value
        with tempfile.TemporaryDirectory() as temp_dir:
            run_worker.main(
                [
                    "--working-directory",
                    temp_dir,
                    "--model",
                    "fake-model",
                    "--provider",
                    "fake-provider",
                    "--reasoning",
                    "high",
                    "--capability",
                    "research",
                ]
            )
        executor_class.assert_called_once()
        self.assertEqual("high", executor_class.call_args.kwargs["reasoning"])
        client_class.assert_called_once_with(
            "http://127.0.0.1:8765",
            actor="hermes",
        )
        runtime_class.assert_called_once_with(
            bus_client,
            name="hermes",
            executor=executor,
            instance_id=ANY,
            capacity=1,
            capabilities=["research"],
            heartbeat_seconds=5.0,
        )
        runtime.run.assert_called_once_with()


class HermesLiveSmokeTests(unittest.TestCase):
    def test_artifact_capture_verification_requires_explicit_complete_refs(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ArtifactStore(Path(directory))
            input_ref = store.put_text("prompt", kind="model_input")
            output_ref = store.put_text("result", kind="model_output")
            events = [
                {"payload": {"artifacts": [input_ref]}},
                {"payload": {"artifacts": [output_ref]}},
            ]

            self.assertEqual(
                [input_ref, output_ref],
                live_smoke.verify_artifact_capture(events, store, required=True),
            )
            self.assertEqual(
                [],
                live_smoke.verify_artifact_capture(
                    [{"payload": {"artifacts": []}}],
                    None,
                    required=False,
                ),
            )
            with self.assertRaisesRegex(RuntimeError, "without explicit opt-in"):
                live_smoke.verify_artifact_capture(events, store, required=False)
            with self.assertRaisesRegex(RuntimeError, "input and output"):
                live_smoke.verify_artifact_capture(events[:1], store, required=True)


FAKE_REVIEWER = """import json, os, sys, time
from pathlib import Path
args = sys.argv[1:]
prompt = args[args.index("-z") + 1]
payload = json.loads(prompt[prompt.index("ASSIGNMENT_JSON") + len("ASSIGNMENT_JSON"):])
context = payload.get("context", {})
Path(args[args.index("--usage-file") + 1]).write_text(json.dumps(
    {"input_tokens": 3000, "output_tokens": 400, "total_tokens": 3400,
     "estimated_cost_usd": 0.01, "model": "fake"}))
time.sleep(float(os.environ.get("FAKE_SLEEP", "3")))
if context.get("stage") == "review":
    findings = [{"file": context["file"], "line": context["start_line"], "severity": "high",
                 "title": "Problem", "detail": "Concrete."}]
else:
    findings = [f for d in payload.get("dependencies", [])
                for f in d.get("result", {}).get("findings", [])][:10]
print(json.dumps({"status": "completed", "summary": context.get("stage", "?"),
                  "result": {"findings": findings}}))
"""


class FilerExecutorTests(unittest.TestCase):
    def test_files_chosen_severities_once(self):
        with tempfile.TemporaryDirectory() as directory:
            workdir = Path(directory)
            executor = code_review.FilerExecutor(workdir)
            findings = [{"file": "a.py", "line": 1, "severity": "high", "title": "A", "detail": "x"},
                        {"file": "b.py", "line": 2, "severity": "low", "title": "B", "detail": "y"}]

            def run(attempt, decision=None):
                decisions = () if decision is None else (
                    {"event_id": 9, "actor": "human", "assignment_id": f"task:7:attempt:{attempt}",
                     "decision_id": "decision:task:7", "decision": decision},)
                return executor.execute(assignment(
                    attempt=attempt, assignment_id=f"task:7:attempt:{attempt}",
                    context={"stage": "file"}, decisions=decisions,
                    dependencies=({"task_id": 6, "completion_event_id": 60, "summary": "m",
                                   "result": {"findings": findings}},)))

            blocked = run(1)
            self.assertIsInstance(blocked, Blocked)
            self.assertIn("2 findings (1 high, 0 medium, 1 low)", blocked.reason)
            first = run(2, "medium")
            self.assertEqual({"filed": 1, "skipped": 0}, {k: first.result[k] for k in ("filed", "skipped")})
            again = run(3, "medium")
            self.assertEqual({"filed": 0, "skipped": 1}, {k: again.result[k] for k in ("filed", "skipped")})
            self.assertIn("**high** `a.py:1` A", (workdir / code_review.REPORT).read_text())

    def test_selection_skips_hidden_ignored_dependency_and_secret_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "app.py").write_text("x = 1\n")
            (root / "private_config.py").write_text("token = 'x'\n")
            (root / "secrets.py").write_text("")
            for folder in (".venv", "node_modules", "build"):
                (root / folder).mkdir()
                (root / folder / "vendor.py").write_text("y = 2\n")
            names = [p.name for p in code_review.select_files(root, "*.py")]
            self.assertEqual(["app.py", "private_config.py"], names)

            subprocess.run(["git", "init", "-q", str(root)], check=True)
            (root / ".gitignore").write_text("private_config.py\n")
            names = [p.name for p in code_review.select_files(root, "*.py")]
            self.assertEqual(["app.py"], names)

    def test_chunks_interleave_files_largest_first(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "big.py").write_text("x = 1\n" * 40)
            (root / "small.py").write_text("y = 2\n" * 5)
            chunks = code_review.chunk_files(root, "*.py", 100, 3)
            self.assertEqual(["big.py", "small.py", "big.py"], [c["file"] for c in chunks])
            self.assertEqual((1, 16), (chunks[0]["start_line"], chunks[0]["end_line"]))
            self.assertEqual((17, 32), (chunks[2]["start_line"], chunks[2]["end_line"]))


@unittest.skipIf(os.name != "posix", "the example uses POSIX process signals")
class HermesCodeReviewLiveTests(unittest.TestCase):
    def test_review_survives_a_crash_and_a_cap_and_files_once(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        fake = root / "fake_hermes"
        fake.write_text(f"#!{sys.executable}\n" + FAKE_REVIEWER, encoding="utf-8")
        fake.chmod(0o755)
        source = root / "src"
        source.mkdir()
        for name in ("alpha", "beta", "gamma"):
            (source / f"{name}.py").write_text(f"# {name}\n" + "pass\n" * 60)
        out = io.StringIO()
        with patch.dict(os.environ, {"FAKE_SLEEP": "3"}):
            args = code_review.parse_args([
                "--provider", "fake", "--model", "fake", "--hermes-command", str(fake),
                "--path", str(source), "--chunk-bytes", "100", "--max-chunks", "9",
                "--workers", "2", "--yes", "--pace", "0.25", "--lease", "3",
                "--dir", str(root / "run"), "--no-keep-running"])
            code = code_review.run(args, out=out, stdin=io.StringIO())
        text = out.getvalue()
        self.assertEqual(0, code, text)
        self.assertIn("What will be sent", text)
        self.assertIn("alpha.py lines 1-", text)
        self.assertIn("was killed (kill -9)", text)
        self.assertIn("New work stopped after", text)
        self.assertIn("(2 batch merges, then a final merge)", text)
        self.assertIn("Filed 9 finding(s), skipped 0", text)
        self.assertIn("1 lease(s) expired", text)
        self.assertTrue((root / "run" / code_review.REPORT).exists())


if __name__ == "__main__":
    unittest.main()
