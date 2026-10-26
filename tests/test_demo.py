"""The scripted demos: stage logic in isolation, then each scenario live."""

import io
import json
import os
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

from logplane import demo
from logplane.executors import AssignmentContext, Blocked, Completed


def assignment(task_id, *, attempt=1, context=None, dependencies=(), decisions=()):
    return AssignmentContext(
        correlation_id="flow-1",
        task_id=task_id,
        assignment_id=f"task:{task_id}:attempt:{attempt}",
        assignment_event_id=100 + attempt,
        attempt=attempt,
        goal=f"stage {task_id}",
        context=context or {},
        dependencies=tuple(dependencies),
        decisions=tuple(decisions),
        assignee="lead",
        worker_instance_id="lead-1",
    )


def decision(task_id, value, attempt=1):
    return {"event_id": 90, "actor": "human", "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "decision_id": f"decision:task:{task_id}:attempt:{attempt}", "decision": value}


def dependency(task_id, outcome):
    return {"task_id": task_id, "completion_event_id": 50 + task_id,
            "summary": outcome.summary, "result": demo.plain(outcome.result)}


class ScenarioStageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.workdir = Path(self.directory.name)
        self.executor = demo.ScenarioExecutor("lead", self.workdir)

    def tearDown(self):
        self.directory.cleanup()

    def run_stage(self, task_id, stage, *, dependencies=(), decisions=(), attempt=1, **context):
        return self.executor.execute(assignment(
            task_id, attempt=attempt, dependencies=dependencies, decisions=decisions,
            context={"stage": stage, "work_seconds": 0, **context}))

    def approval(self, attempt=1, answer=None):
        return self.run_stage(
            2, "approval", attempt=attempt,
            decisions=[decision(2, answer)] if answer is not None else [],
            tool="issue_refund", arguments={"order": 4411}, call="issue_refund(order=4411)")

    def test_approval_blocks_then_executes_the_tool_at_most_once(self):
        blocked = self.approval()
        self.assertIsInstance(blocked, Blocked)
        self.assertIn("issue_refund(order=4411)", blocked.reason)
        first = self.approval(attempt=2, answer="Approve")
        self.assertEqual("executed", first.result["outcome"])
        retried = self.approval(attempt=3, answer="approve")
        self.assertIn("not repeated", retried.summary)
        entries = demo.read_inbox(self.workdir / demo.REFUND_LEDGER)
        self.assertEqual(1, len(entries))
        self.assertEqual("human", entries[0]["approved_by"])

    def test_denied_or_invalid_answers_never_execute_the_tool(self):
        denied = self.approval(attempt=2, answer="deny")
        self.assertEqual("denied", denied.result["outcome"])
        again = self.approval(attempt=2, answer="maybe")
        self.assertIsInstance(again, Blocked)
        self.assertIn('"maybe" is not one of: approve, deny', again.reason)
        self.assertEqual([], demo.read_inbox(self.workdir / demo.REFUND_LEDGER))

    def test_reply_reflects_the_approval_outcome(self):
        for outcome, phrase in (("executed", "refund is on its way"),
                                ("denied", "teammate will follow up")):
            approved = Completed(summary="s", result={"tool": "issue_refund", "outcome": outcome})
            drafted = Completed(summary="d", result={})
            reply = self.run_stage(4, "reply", dependencies=[dependency(2, approved),
                                                             dependency(3, drafted)])
            self.assertIn(phrase, reply.summary)

    def test_model_usage_is_reported_through_telemetry_without_affecting_outcomes(self):
        calls = []

        class Sink:
            def model_started(self, assignment, **fields):
                calls.append(("started", fields))

            def model_completed(self, assignment, **fields):
                calls.append(("completed", fields))

        self.executor.telemetry = Sink()
        self.run_stage(1, "work", model_usage=demo.usage(100, 20, 0.5))
        self.assertEqual(["started", "completed"], [name for name, _ in calls])
        self.assertEqual({"input_tokens": 100, "output_tokens": 20, "cost_usd": 0.5},
                         calls[1][1]["usage"])
        self.assertEqual("task:1:attempt:1:model", calls[1][1]["invocation_id"])

        class BrokenSink:
            def model_started(self, assignment, **fields):
                raise RuntimeError("telemetry is down")

            model_completed = model_started

        self.executor.telemetry = BrokenSink()
        outcome = self.run_stage(1, "work", model_usage=demo.usage(1, 1, 0.1))
        self.assertIsInstance(outcome, Completed)
        calls.clear()
        self.executor.telemetry = Sink()
        self.run_stage(1, "work")
        self.assertEqual([], calls)  # no model_usage, no model call reported

    def test_tail_lines_show_cost_and_policy(self):
        usage_line = demo.describe_event({"id": 9, "topic": "workflow.usage_recorded",
                                          "actor": "w", "payload": {"task_id": 3,
                                          "cost_usd": 0.4, "tokens": 10300}})
        self.assertIn("task 3: $0.40, 10,300 tokens", usage_line)
        small = demo.describe_event({"id": 9, "topic": "workflow.usage_recorded", "actor": "w",
                                     "payload": {"task_id": 3, "cost_usd": 0.012}})
        self.assertIn("$0.012", small)
        cap = demo.describe_event({"id": 4, "topic": "workflow.policy_set", "actor": "human",
                                   "payload": {"max_total_cost_usd": 2.0}})
        self.assertIn("cost cap $2.00 by human", cap)

    def test_audit_pipeline_merges_triages_and_reports_through_real_results(self):
        reviews = [self.run_stage(2 + index, "review", contract=contract)
                   for index, contract in enumerate(demo.REVIEW_FINDINGS)]
        merged = self.run_stage(5, "dedupe", dependencies=[
            dependency(2 + index, review) for index, review in enumerate(reviews)])
        self.assertIsInstance(merged, Completed)
        findings = merged.result["findings"]
        self.assertEqual(3, len(findings))
        oracle = next(item for item in findings if item["root_cause"] == "stale-oracle-price")
        self.assertEqual(("PriceOracle.sol", "Router.sol"), tuple(oracle["locations"]))

        blocked = self.run_stage(6, "triage", dependencies=[dependency(5, merged)])
        self.assertIsInstance(blocked, Blocked)
        self.assertIn(oracle["id"], blocked.reason)
        triaged = self.run_stage(6, "triage", attempt=2, dependencies=[dependency(5, merged)],
                                 decisions=[decision(6, "High")])
        severities = {item["id"]: item["severity"] for item in triaged.result["findings"]}
        self.assertEqual("High", severities[oracle["id"]])

        report = self.run_stage(7, "report", dependencies=[dependency(6, triaged)])
        self.assertIsInstance(report, Completed)
        self.assertIn(oracle["id"], (self.workdir / demo.REPORT_FILE).read_text())

    def test_severity_answers_are_normalized_or_asked_again(self):
        self.assertEqual("Medium", demo.normalize_choice("  medium ", demo.SEVERITIES))
        self.assertIsNone(demo.normalize_choice("banana", demo.SEVERITIES))
        merged = Completed(summary="m", result={"findings": demo.dedupe_findings([
            {"contract": c, "findings": f} for c, f in demo.REVIEW_FINDINGS.items()])})
        upstream = [dependency(5, merged)]
        triaged = self.run_stage(6, "triage", attempt=2, dependencies=upstream,
                                 decisions=[decision(6, "medium")])
        self.assertEqual({"High", "Medium"},
                         {item["severity"] for item in triaged.result["findings"]})
        again = self.run_stage(6, "triage", attempt=2, dependencies=upstream,
                               decisions=[decision(6, "banana")])
        self.assertIsInstance(again, Blocked)
        self.assertIn('"banana" is not one of: High, Medium', again.reason)

    def test_submission_is_idempotent_across_attempts(self):
        findings = {"findings": [{"id": f"AUD-{n}", "severity": "High", "title": f"t{n}"}
                                 for n in (1, 2)]}
        upstream = [{"task_id": 7, "completion_event_id": 70, "summary": "r", "result": findings}]
        first = self.run_stage(8, "submit", dependencies=upstream)
        second = self.run_stage(8, "submit", dependencies=upstream, attempt=2)
        self.assertEqual(["AUD-1", "AUD-2"], list(first.result["submitted"]))
        self.assertEqual([], list(second.result["submitted"]))
        self.assertEqual(["AUD-1", "AUD-2"], list(second.result["skipped"]))
        entries = demo.read_inbox(self.workdir / demo.INBOX_FILE)
        self.assertEqual(2, len({entry["effect_id"] for entry in entries}))
        self.assertEqual(2, len(entries))

    def test_receiver_accepts_each_effect_key_once_under_concurrency(self):
        path = self.workdir / demo.REFUND_LEDGER
        barrier = threading.Barrier(8)
        accepted = []

        def deliver(attempt):
            barrier.wait()
            accepted.append(demo.deliver_effect(path, "effect-1", {"attempt": attempt}))

        threads = [threading.Thread(target=deliver, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(1, accepted.count(True))
        self.assertEqual(1, len(demo.read_inbox(path)))

    def test_overlapping_approved_attempts_refund_once(self):
        # A lease can expire while the old worker is still running, so two
        # attempts of the same task may execute at the same time.
        executors = [demo.ScenarioExecutor(name, self.workdir) for name in ("alice", "bob")]
        barrier = threading.Barrier(2)
        outcomes = []

        def run(executor, attempt):
            barrier.wait()
            outcomes.append(executor.execute(assignment(
                2, attempt=attempt, decisions=[decision(2, "approve")],
                context={"stage": "approval", "work_seconds": 0, "tool": "issue_refund",
                         "call": "issue_refund(order=4411)", "arguments": {"order": 4411}})))

        threads = [threading.Thread(target=run, args=(executor, n))
                   for n, executor in enumerate(executors, start=2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(1, len(demo.read_inbox(self.workdir / demo.REFUND_LEDGER)))
        self.assertEqual(1, sum("not repeated" in outcome.summary for outcome in outcomes))

    def test_unknown_stage_is_an_error(self):
        with self.assertRaises(ValueError):
            self.run_stage(1, "nonsense")


class ReadingPauseTests(unittest.TestCase):
    def harness(self, directory, read_pauses):
        return demo.Harness(Path(directory), out=io.StringIO(), read_pauses=read_pauses)

    def test_narration_waits_only_when_asked(self):
        with tempfile.TemporaryDirectory() as directory, \
                unittest.mock.patch("logplane.demo.time.sleep") as sleep:
            self.harness(directory, False).say("one two three four five six seven eight")
            sleep.assert_not_called()
        with tempfile.TemporaryDirectory() as directory, \
                unittest.mock.patch("logplane.demo.time.sleep") as sleep:
            pausing = self.harness(directory, True)
            pausing.say("one two three four five six seven eight nine ten eleven twelve")
            pausing.say("short")
            pausing.say(" ".join(["word"] * 200))
            self.assertEqual([3.0, 1.5, 8.0], [call.args[0] for call in sleep.call_args_list])


class NarrationWrapTests(unittest.TestCase):
    def test_narration_wraps_at_word_boundaries(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, \
                unittest.mock.patch("logplane.demo.shutil.get_terminal_size",
                                    return_value=os.terminal_size((50, 24))):
            demo.Harness(Path(directory), out=out).say(
                "The log hasn't been told that alice died, and nothing guesses. "
                "Her lease simply stops being renewed.")
        lines = [line for line in out.getvalue().splitlines() if line]
        self.assertGreater(len(lines), 1)
        self.assertTrue(lines[0].startswith("▸ "))
        self.assertTrue(all(len(line) <= 49 for line in lines))
        self.assertIn("renewed.", lines[-1])

    def test_recap_bullets_wrap_at_word_boundaries(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, \
                unittest.mock.patch("logplane.demo.shutil.get_terminal_size",
                                    return_value=os.terminal_size((50, 24))):
            demo.Harness(Path(directory), out=out).recap([
                "An approved refund is keyed by assignment.effect_id, so a retry after a "
                "crash cannot refund twice."])
        lines = [line for line in out.getvalue().splitlines() if line.strip()]
        bullet = lines[1:]
        self.assertTrue(bullet[0].startswith("  • "))
        self.assertTrue(all(line.startswith("    ") for line in bullet[1:]))
        self.assertTrue(all(len(line) <= 49 for line in bullet))
        self.assertIn("twice.", bullet[-1])


class DemoCommandTests(unittest.TestCase):
    def test_shared_session_always_cleans_up(self):
        for error, expected in ((None, 0), (demo.DemoError("scenario failed"), 1),
                                (KeyboardInterrupt(), 130), (RuntimeError("unexpected"), None)):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory, \
                 unittest.mock.patch.object(demo.Harness, "stop_all") as stop, \
                 unittest.mock.patch.object(demo, "_free_port", return_value=12345):
                out = io.StringIO()

                def scenario(harness):
                    # Non-interactive scripted demonstrations retain automatic
                    # answers; those are separate from external-send consent.
                    self.assertEqual("yes", harness.ask("Simulated approval?", default="yes"))
                    if error is not None:
                        raise error

                def run():
                    return demo.run_session(scenario, prefix="test-", label="test", out=out,
                                            stdin=io.StringIO(), directory=directory,
                                            keep_running=False)

                if expected is None:
                    with self.assertRaisesRegex(RuntimeError, "unexpected"):
                        run()
                else:
                    self.assertEqual(expected, run())
                    self.assertIn("History is kept", out.getvalue())
                stop.assert_called_once_with()

    def test_listing_and_unknown_scenarios(self):
        out = io.StringIO()
        self.assertEqual(0, demo.run(None, out=out))
        for name in demo.SCENARIOS:
            self.assertIn(name, out.getvalue())
        self.assertEqual(2, demo.run("nope", out=io.StringIO()))

    def test_refuses_a_non_empty_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "keep.txt").write_text("mine")
            out = io.StringIO()
            self.assertEqual(2, demo.run("recovery", out=out, directory=directory))
            self.assertIn("new or empty", out.getvalue())


@unittest.skipIf(os.name != "posix", "demo scenarios use POSIX process signals")
class LiveScenarioTests(unittest.TestCase):
    """Each scenario end to end with real processes, at a fast pace."""

    def play(self, name):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        out = io.StringIO()
        code = demo.run(name, out=out, pace=0.25, assume_yes=True,
                        directory=directory.name, keep_running=False)
        if code != 0:
            # The temporary directory is removed after the test, so keep the
            # process logs in the failure message for CI.
            logs = sorted((Path(directory.name) / "logs").glob("*.log"))
            tails = "".join(f"\n--- {log.name} (last 40 lines)\n" + "\n".join(
                log.read_text(errors="replace").splitlines()[-40:]) for log in logs)
            self.fail(out.getvalue() + tails)
        self.assertIn("What just happened", out.getvalue())
        return Path(directory.name), out.getvalue()

    def test_recovery(self):
        _, output = self.play("recovery")
        self.assertIn("task 2 → bob (attempt 2)", output)

    def test_approval(self):
        workdir, output = self.play("approval")
        self.assertIn("Executed issue_refund(order=4411, amount_usd=120.00) after human approval",
                      output)
        self.assertEqual(1, len(demo.read_inbox(workdir / demo.REFUND_LEDGER)))
        self.assertIn("usage recorded", output)

    def test_budget(self):
        _, output = self.play("budget")
        self.assertIn("cost_usd budget is exhausted", output)
        self.assertIn("cost cap $5.00 by human", output)
        self.assertIn("All ten are done: 103,000 tokens and $4.00", output)

    def test_audit_triage(self):
        workdir, output = self.play("audit-triage")
        entries = demo.read_inbox(workdir / demo.INBOX_FILE)
        self.assertEqual(3, len(entries))
        self.assertEqual(3, len({entry["effect_id"] for entry in entries}))
        self.assertEqual({1, 2}, {entry["attempt"] for entry in entries})
        self.assertIn("skipped 1 already submitted", output)


if __name__ == "__main__":
    unittest.main()
