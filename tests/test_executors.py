import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from logplane import executors
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    InProcessExecutor,
    PermanentFailure,
    RetryableFailure,
    SubprocessExecutor,
)


def assignment(**overrides):
    values = {
        "correlation_id": "workflow-one",
        "task_id": 4,
        "assignment_id": "task:4:attempt:2",
        "assignment_event_id": 19,
        "attempt": 2,
        "goal": "Run the integration suite",
        "context": {"repository": "agent-bus", "flags": ["fast"]},
        "decisions": (
            {
                "event_id": 17,
                "actor": "human",
                "assignment_id": "task:4:attempt:1",
                "decision_id": "decision:task:4:attempt:1",
                "decision": {"database": "SQLite"},
            },
        ),
        "required_capabilities": ("python", "testing"),
        "max_retries": 3,
        "retryable_failures": 1,
        "assignee": "alice",
        "worker_instance_id": "alice-1",
        "ownership_mode": "canary",
        "ownership_owner": "agent-bus",
        "external_origin": {"system": "legacy", "task_ref": "work-4"},
    }
    values.update(overrides)
    return AssignmentContext(**values)


OUTCOMES = (
    Completed("done", {"commit": "abc"}),
    Blocked("approval required"),
    RetryableFailure("rate_limited", "try later"),
    PermanentFailure("invalid_goal", "cannot execute"),
)


def outcome_payload(outcome):
    if isinstance(outcome, Completed):
        return {"status": "completed", "summary": outcome.summary, "result": dict(outcome.result)}
    if isinstance(outcome, Blocked):
        return {"status": "blocked", "reason": outcome.reason}
    status = (
        "retryable_failure"
        if isinstance(outcome, RetryableFailure)
        else "permanent_failure"
    )
    return {"status": status, "code": outcome.code, "reason": outcome.reason}


class ExecutorConformanceMixin:
    def make_executor(self, outcome):
        raise NotImplementedError

    def test_all_standard_outcomes_conform(self):
        for expected in OUTCOMES:
            with self.subTest(outcome=expected.__class__.__name__):
                executor = self.make_executor(expected)
                actual = executor.execute(assignment())
                self.assertEqual(expected, actual)
                close = getattr(executor, "close", None)
                if callable(close):
                    close()


class InProcessConformanceTests(ExecutorConformanceMixin, unittest.TestCase):
    def make_executor(self, outcome):
        return InProcessExecutor(lambda received: outcome)

    def test_object_run_method_is_supported(self):
        class Agent:
            def run(self, received):
                return Completed(f"finished {received.task_id}")

        result = InProcessExecutor(Agent()).execute(assignment())
        self.assertEqual("finished 4", result.summary)

    def test_untyped_return_is_rejected(self):
        with self.assertRaisesRegex(TypeError, "executor must return"):
            InProcessExecutor(lambda received: {"status": "completed"}).execute(
                assignment()
            )

    def test_cooperative_cancel_and_close_are_forwarded(self):
        class Agent:
            def __init__(self):
                self.cancelled = []
                self.closed = False

            def run(self, received):
                return Completed("done")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

            def close(self):
                self.closed = True

        agent = Agent()
        executor = InProcessExecutor(agent)
        executor.cancel("task:4:attempt:2")
        executor.close()
        self.assertEqual(["task:4:attempt:2"], agent.cancelled)
        self.assertTrue(agent.closed)


class SubprocessConformanceTests(ExecutorConformanceMixin, unittest.TestCase):
    def test_cancelling_an_inactive_assignment_leaves_no_tombstone(self):
        executor = SubprocessExecutor([sys.executable, "-c", "pass"])
        executor.cancel("task:99:attempt:1")
        self.assertEqual(set(), executor._cancelled)
        self.assertEqual(set(), executor._active_calls)

    def make_executor(self, outcome):
        encoded = json.dumps(outcome_payload(outcome))
        code = (
            "import json,sys; assignment=json.load(sys.stdin); "
            f"print({encoded!r})"
        )
        return SubprocessExecutor([sys.executable, "-c", code])

    def test_exit_75_is_retryable(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import sys; sys.exit(75)"]
        )
        self.assertIsInstance(executor.execute(assignment()), RetryableFailure)

    def test_invalid_or_oversized_output_is_permanent(self):
        invalid = SubprocessExecutor(
            [sys.executable, "-c", "print('not-json')"]
        )
        self.assertEqual(
            "invalid_executor_output",
            invalid.execute(assignment()).code,
        )

        oversized = SubprocessExecutor(
            [sys.executable, "-c", "print('x' * 1000)"],
            max_protocol_bytes=800,
        )
        self.assertEqual(
            "protocol_output_too_large",
            oversized.execute(assignment(context={})).code,
        )

    def test_timeout_is_retryable(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import time; time.sleep(1)"],
            timeout=0.02,
        )
        self.assertEqual("subprocess_timeout", executor.execute(assignment()).code)

    def test_cancel_terminates_a_running_subprocess(self):
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import time; time.sleep(10)"],
            timeout=20,
        )
        results = []
        thread = threading.Thread(
            target=lambda: results.append(executor.execute(assignment()))
        )
        thread.start()
        deadline = time.time() + 1
        while time.time() < deadline:
            with executor._lock:
                if executor._processes:
                    break
            time.sleep(0.005)
        executor.cancel("task:4:attempt:2")
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual("subprocess_cancelled", results[0].code)

    def test_process_creation_failure_is_reported_as_start_failure(self):
        executor = SubprocessExecutor(["missing-agent"])
        with patch("logplane.executors.subprocess.Popen", side_effect=OSError("missing")):
            result = executor.execute(assignment())
        self.assertIsInstance(result, PermanentFailure)
        self.assertEqual("subprocess_start_failed", result.code)

    def test_post_start_os_error_is_retryable_io_failure(self):
        class EmptyPipe:
            def read(self, size):
                return b""

            def close(self):
                pass

        class FailingStdin:
            def write(self, value):
                raise OSError("write failed")

            def close(self):
                pass

        class StartedProcess:
            pid = 4_000_001  # never signalled for real: killpg is patched below

            def __init__(self):
                self.stdin = FailingStdin()
                self.stdout = EmptyPipe()
                self.stderr = EmptyPipe()
                self.returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = -15

            def wait(self, timeout=None):
                return self.returncode

            def kill(self):
                self.returncode = -9

        executor = SubprocessExecutor(["agent"])
        with (
            patch("logplane.executors.subprocess.Popen", return_value=StartedProcess()),
            patch("logplane.executors.os.killpg", side_effect=ProcessLookupError) as killpg,
        ):
            result = executor.execute(assignment())
        if os.name == "posix":
            self.assertTrue(killpg.called)
            self.assertEqual({4_000_001}, {call.args[0] for call in killpg.call_args_list})
        self.assertIsInstance(result, RetryableFailure)
        self.assertEqual("subprocess_io_failed", result.code)
        self.assertIn("write failed", result.reason)
        self.assertEqual({}, executor._processes)
        self.assertEqual(set(), executor._cancelled)


# The command prints one outcome, after starting a child that appends to a
# file for as long as it lives. A test knows the child is gone when the file
# stops growing, which works even where exited processes linger as zombies.
CHILD = "import sys,time\nwhile True:\n    open(sys.argv[1],'a').write('x'); time.sleep(0.02)\n"
PARENT = (
    "import json,os,subprocess,sys,time\n"
    "sys.stdin.read()\n"
    "options = json.loads(sys.argv[2])\n"
    "child = subprocess.Popen([sys.executable, '-c', {child!r}, sys.argv[1]],\n"
    "                         stdout=None if options.get('inherit') else subprocess.DEVNULL)\n"
    "while not os.path.exists(sys.argv[1]) or not os.path.getsize(sys.argv[1]):\n"
    "    time.sleep(0.01)\n"
    "if options.get('ignore_term'):\n"
    "    import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "if options.get('hang'):\n"
    "    time.sleep(60)\n"
    "print(json.dumps({{'status': 'completed', 'summary': 'own group: ' + str(os.getpgrp() == os.getpid())}}))\n"
).format(child=CHILD)


@unittest.skipIf(os.name != "posix", "process groups are a POSIX mechanism")
class SubprocessProcessGroupTests(unittest.TestCase):
    """An attempt that ends takes the processes it started with it."""

    def executor(self, marker, **options):
        timeout = options.pop("timeout", 20)
        return SubprocessExecutor(
            [sys.executable, "-c", PARENT, str(marker), json.dumps(options)], timeout=timeout
        )

    def wait_for_child(self, marker):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if marker.exists() and marker.stat().st_size:
                return
            time.sleep(0.01)
        self.fail("the command never started its child")

    def assert_child_stopped(self, marker):
        size = marker.stat().st_size
        time.sleep(0.3)  # fifteen of the child's write intervals
        self.assertEqual(size, marker.stat().st_size, "the command's child is still running")

    def test_a_timeout_stops_the_commands_children(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, "alive")
            result = self.executor(marker, hang=True, timeout=1.0).execute(assignment())
            self.assertEqual("subprocess_timeout", result.code)
            self.assertTrue(marker.stat().st_size)
            self.assert_child_stopped(marker)

    def test_cancellation_stops_the_commands_children(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, "alive")
            executor = self.executor(marker, hang=True)
            results = []
            thread = threading.Thread(target=lambda: results.append(executor.execute(assignment())))
            thread.start()
            self.wait_for_child(marker)
            executor.cancel("task:4:attempt:2")
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual("subprocess_cancelled", results[0].code)
            self.assert_child_stopped(marker)

    def test_close_stops_the_commands_children(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, "alive")
            executor = self.executor(marker, hang=True)
            thread = threading.Thread(target=lambda: executor.execute(assignment()))
            thread.start()
            self.wait_for_child(marker)
            executor.close()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assert_child_stopped(marker)

    def test_a_command_that_ignores_sigterm_is_killed_with_its_children(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, "alive")
            executor = self.executor(marker, hang=True, ignore_term=True)
            results = []
            thread = threading.Thread(target=lambda: results.append(executor.execute(assignment())))
            thread.start()
            self.wait_for_child(marker)
            time.sleep(0.2)  # let the command install its SIGTERM handler
            started = time.monotonic()
            executor.cancel("task:4:attempt:2")
            thread.join(6)
            self.assertFalse(thread.is_alive())
            self.assertLess(time.monotonic() - started, 5)
            self.assert_child_stopped(marker)

    def test_a_child_left_running_after_a_normal_exit_is_stopped(self):
        # The child inherits the command's stdout, so before this it also kept
        # the output pipe open after the command had exited.
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory, "alive")
            started = time.monotonic()
            result = self.executor(marker, inherit=True).execute(assignment())
            self.assertIsInstance(result, Completed)
            self.assertEqual("own group: True", result.summary)
            self.assertLess(time.monotonic() - started, 8)
            self.assert_child_stopped(marker)

    def test_stopping_twice_or_after_exit_is_harmless(self):
        process = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
        process.wait(5)
        executors._stop_process_group(process)
        executors._stop_process_group(process)
        self.assertEqual(0, process.returncode)

    def test_a_process_that_owns_cleanup_keeps_commands_in_its_own_group(self):
        # The adapter-check probe is stopped as one process group by its
        # parent. A command given its own session would escape that, so there
        # the command stays in the group and only it is signalled directly.
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(executors, "_commands_get_own_group", True):
            executors.share_process_group_with_commands()
            marker = Path(directory, "alive")
            with patch("logplane.executors.os.killpg") as killpg:
                result = self.executor(marker).execute(assignment())
            self.assertIsInstance(result, Completed)
            self.assertEqual("own group: False", result.summary)
            killpg.assert_not_called()  # it would have signalled this test run
            # Nothing here stops the child: that is the group owner's job.
            size = marker.stat().st_size
            time.sleep(0.2)
            self.assertGreater(marker.stat().st_size, size)
            subprocess.run(["pkill", "-f", str(marker)], check=False)

    def test_the_timeout_also_covers_delivering_the_assignment(self):
        # More input than a pipe holds, and a command that never reads it.
        # The write used to block before the timeout had started.
        executor = SubprocessExecutor(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            timeout=0.5, max_protocol_bytes=1024 * 1024,
        )
        results = []
        thread = threading.Thread(target=lambda: results.append(
            executor.execute(assignment(context={"blob": "x" * 256_000}))), daemon=True)
        started = time.monotonic()
        thread.start()
        thread.join(8)
        try:
            self.assertFalse(thread.is_alive(), "blocked writing to a command that never reads")
            self.assertEqual("subprocess_timeout", results[0].code)
            self.assertLess(time.monotonic() - started, 6)
        finally:
            executor.close()

    def test_a_large_assignment_still_arrives_whole(self):
        code = ("import json,sys; request=sys.stdin.read(); "
                "print(json.dumps({'status':'completed','summary':str(len(request))}))")
        executor = SubprocessExecutor([sys.executable, "-c", code], timeout=20,
                                      max_protocol_bytes=1024 * 1024)
        result = executor.execute(assignment(context={"blob": "x" * 256_000}))
        self.assertIsInstance(result, Completed)
        self.assertGreater(int(result.summary), 256_000)

    def test_a_command_closed_before_it_started_is_stopped_without_deadlock(self):
        with tempfile.TemporaryDirectory() as directory:
            executor = self.executor(Path(directory, "alive"), hang=True)
            started = threading.Event()
            spawn = subprocess.Popen

            def spawn_then_close(*args, **kwargs):
                process = spawn(*args, **kwargs)
                executor.close()  # ownership is lost while the command starts
                started.set()
                return process

            results = []
            with patch("logplane.executors.subprocess.Popen", side_effect=spawn_then_close):
                thread = threading.Thread(
                    target=lambda: results.append(executor.execute(assignment())), daemon=True)
                thread.start()
                thread.join(8)
            self.assertFalse(thread.is_alive())
            self.assertEqual("subprocess_cancelled", results[0].code)
            self.assertEqual(set(), executor._own_groups)


class AssignmentContextTests(unittest.TestCase):
    def test_deadline_is_validated_and_serialized(self):
        value = assignment(deadline_at=2_000_000_000.0)
        self.assertEqual(2_000_000_000.0, value.to_dict()["deadline_at"])
        with self.assertRaisesRegex(ValueError, "deadline_at"):
            assignment(deadline_at=float("inf"))

    def test_workflow_policy_identity_is_validated_and_serialized(self):
        value = assignment(workflow_policy_event_id=42)
        self.assertEqual(42, value.to_dict()["workflow_policy_event_id"])
        with self.assertRaisesRegex(ValueError, "workflow_policy_event_id"):
            assignment(workflow_policy_event_id=0)

    def test_fairness_evidence_is_validated_and_serialized(self):
        value = assignment(
            fairness_policy="workflow_round_robin_v1",
            previous_assignment_event_id=41,
        )
        self.assertEqual(
            {
                "policy": "workflow_round_robin_v1",
                "previous_assignment_event_id": 41,
            },
            value.to_dict()["fairness"],
        )
        with self.assertRaisesRegex(ValueError, "fairness_policy"):
            assignment(fairness_policy="random")
        with self.assertRaisesRegex(ValueError, "requires fairness_policy"):
            assignment(previous_assignment_event_id=41)

    def test_assignment_event_rejects_partial_fairness_evidence(self):
        source = assignment()
        event = {
            "id": source.assignment_event_id,
            "correlation_id": source.correlation_id,
            "payload": {
                "task_id": source.task_id,
                "assignment_id": source.assignment_id,
                "attempt": source.attempt,
                "goal": source.goal,
                "assignee": source.assignee,
                "worker_instance_id": source.worker_instance_id,
                "fairness": {"policy": "workflow_round_robin_v1"},
            },
        }
        with self.assertRaisesRegex(ValueError, "must contain"):
            AssignmentContext.from_event(event)

    def test_v04_positional_constructor_order_remains_compatible(self):
        value = AssignmentContext(
            "workflow-one",
            4,
            "task:4:attempt:2",
            19,
            2,
            "Run the integration suite",
            {"repository": "agent-bus"},
            ("python",),
            3,
            1,
            "alice",
            "alice-1",
            "canary",
            "agent-bus",
            {"system": "legacy", "task_ref": "work-4"},
        )
        self.assertEqual((), value.decisions)
        self.assertEqual("work-4", value.external_origin["task_ref"])

    def test_context_is_immutable_and_serializes_for_adapters(self):
        value = assignment()
        with self.assertRaises(TypeError):
            value.context["repository"] = "other"
        with self.assertRaises(TypeError):
            value.external_origin["task_ref"] = "other"
        with self.assertRaises(TypeError):
            value.decisions[0]["decision"]["database"] = "Postgres"

        serialized = value.to_dict()
        self.assertEqual(["fast"], serialized["context"]["flags"])
        self.assertEqual("SQLite", serialized["decisions"][0]["decision"]["database"])
        self.assertEqual("canary", serialized["ownership"]["mode"])

    def test_assignment_event_contract_is_parsed(self):
        source = assignment()
        event = {
            "id": source.assignment_event_id,
            "correlation_id": source.correlation_id,
            "payload": {
                "task_id": source.task_id,
                "assignment_id": source.assignment_id,
                "attempt": source.attempt,
                "goal": source.goal,
                "context": source.to_dict()["context"],
                "decisions": source.to_dict()["decisions"],
                "required_capabilities": list(source.required_capabilities),
                "retry_policy": {"max_retries": source.max_retries},
                "retryable_failures": source.retryable_failures,
                "assignee": source.assignee,
                "worker_instance_id": source.worker_instance_id,
                "ownership": source.to_dict()["ownership"],
                "external_origin": source.to_dict()["external_origin"],
            },
        }
        self.assertEqual(source, AssignmentContext.from_event(event))

    def test_historical_assignment_without_decisions_remains_compatible(self):
        source = assignment(decisions=())
        event = {
            "id": source.assignment_event_id,
            "correlation_id": source.correlation_id,
            "payload": {
                "task_id": source.task_id,
                "assignment_id": source.assignment_id,
                "attempt": source.attempt,
                "goal": source.goal,
                "context": source.to_dict()["context"],
                "required_capabilities": list(source.required_capabilities),
                "retry_policy": {"max_retries": source.max_retries},
                "retryable_failures": source.retryable_failures,
                "assignee": source.assignee,
                "worker_instance_id": source.worker_instance_id,
                "ownership": source.to_dict()["ownership"],
            },
        }
        self.assertEqual((), AssignmentContext.from_event(event).decisions)

    def test_resolved_dependencies_are_immutable_and_required_for_refs(self):
        source = assignment()
        event = {
            "id": source.assignment_event_id,
            "correlation_id": source.correlation_id,
            "payload": {
                "task_id": source.task_id,
                "assignment_id": source.assignment_id,
                "attempt": source.attempt,
                "goal": source.goal,
                "assignee": source.assignee,
                "worker_instance_id": source.worker_instance_id,
                "dependency_refs": [
                    {"task_id": 1, "completion_event_id": 18},
                ],
            },
        }
        with self.assertRaisesRegex(ValueError, "must be resolved"):
            AssignmentContext.from_event(event)

        event["payload"]["dependencies"] = [
            {
                "task_id": 1,
                "completion_event_id": 18,
                "summary": "prepared input",
                "result": {"value": 42},
            }
        ]
        parsed = AssignmentContext.from_event(event)
        self.assertEqual(42, parsed.dependencies[0]["result"]["value"])
        with self.assertRaises(TypeError):
            parsed.dependencies[0]["result"]["value"] = 7
        self.assertEqual(42, parsed.to_dict()["dependencies"][0]["result"]["value"])


if __name__ == "__main__":
    unittest.main()
