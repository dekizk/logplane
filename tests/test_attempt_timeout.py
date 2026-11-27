"""Per-attempt timeout in the worker runtime. No real bus, agents, or model calls."""

import threading
import time
from unittest.mock import patch

import httpx

from logplane import runtime as runtime_module
from logplane.executors import Completed, RetryableFailure
from logplane.runtime import ERROR_EXIT_CODE, WorkerRuntime
from tests.test_runtime import FakeBus, RuntimeTestCase, assigned_event


class CooperativeExecutor:
    """Blocks until cancelled, like an agent stuck on a call that never answers."""

    def __init__(self, after_cancel=None):
        self.cancelled = []
        self.executed = []
        self._released = threading.Event()
        self._after_cancel = after_cancel or RetryableFailure("cancelled", "told to stop")

    def execute(self, assignment):
        self.executed.append(assignment.assignment_id)
        if assignment.task_id != 1:
            return Completed(f"task {assignment.task_id} done")
        self._released.wait(5)
        return self._after_cancel

    def cancel(self, assignment_id):
        self.cancelled.append(assignment_id)
        self._released.set()


def published(bus, topic):
    return [event for event in bus.published if event["topic"] == topic]


class AttemptTimeoutTests(RuntimeTestCase):
    def runtime(self, bus, executor, **kwargs):
        self.messages = []
        return WorkerRuntime(
            bus, name="alice", instance_id="alice-1", executor=executor,
            publish_retry_seconds=0.005, log=self.messages.append, **kwargs,
        )

    def test_off_by_default_and_invalid_values_are_rejected(self):
        class Slow:
            def execute(self, assignment):
                time.sleep(0.15)
                return Completed("took its time")

        bus = FakeBus()
        runtime = self.runtime(bus, Slow())
        self.assertIsNone(runtime.attempt_timeout_seconds)
        runtime.run(iter([assigned_event()]))
        self.assertEqual(1, len(published(bus, "task.completed")))
        self.assertEqual({}, runtime._attempt_timers)

        for invalid in (0, -1, float("nan"), float("inf"), True, "60"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ValueError, "^attempt_timeout_seconds must be a positive finite duration"
            ):
                self.runtime(FakeBus(), Slow(), attempt_timeout_seconds=invalid)

    def test_a_hung_attempt_is_cancelled_and_reported_as_a_retryable_timeout(self):
        bus = FakeBus()
        executor = CooperativeExecutor()
        runtime = self.runtime(bus, executor, attempt_timeout_seconds=0.05)
        started = time.monotonic()
        runtime.run(iter([assigned_event(), assigned_event(task_id=2, event_id=11)]))
        self.assertLess(time.monotonic() - started, 3)

        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        failure, = published(bus, "task.attempt_failed")
        self.assertEqual("attempt_timeout", failure["payload"]["failure_code"])
        self.assertTrue(failure["payload"]["retryable"])
        self.assertEqual(1, failure["payload"]["task_id"])
        self.assertEqual("task:1:attempt:1", failure["payload"]["assignment_id"])
        self.assertIn("0.05-second limit", failure["payload"]["reason"])
        self.assertNotIn("retry_after_seconds", failure["payload"])
        # The worker itself is fine: it went on to the next assignment.
        self.assertEqual([2], [e["payload"]["task_id"] for e in published(bus, "task.completed")])
        self.assertEqual(0, runtime.exit_code)
        self.assertEqual({}, runtime._attempt_timers)
        self.assertEqual(set(), runtime._timed_out)
        self.assertTrue(any("attempt timeout (0.05s) reached" in line for line in self.messages))

    def test_whatever_a_cancelled_executor_returns_the_attempt_is_a_timeout(self):
        # The limit passed and the executor was told to stop. A result it
        # hands back after that is not treated as finishing in time.
        bus = FakeBus()
        executor = CooperativeExecutor(after_cancel=Completed("finished anyway"))
        self.runtime(bus, executor, attempt_timeout_seconds=0.05).run(iter([assigned_event()]))
        self.assertEqual([], published(bus, "task.completed"))
        failure, = published(bus, "task.attempt_failed")
        self.assertEqual("attempt_timeout", failure["payload"]["failure_code"])

    def test_an_attempt_inside_the_limit_is_untouched(self):
        class Quick:
            cancelled = []

            def execute(self, assignment):
                return Completed("done")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        bus = FakeBus()
        executor = Quick()
        runtime = self.runtime(bus, executor, attempt_timeout_seconds=30)
        runtime.run(iter([assigned_event()]))
        self.assertEqual(1, len(published(bus, "task.completed")))
        self.assertEqual([], published(bus, "task.attempt_failed"))
        self.assertEqual([], executor.cancelled)
        self.assertEqual({}, runtime._attempt_timers)  # the timer was cancelled, not left to fire

    def test_a_revoked_attempt_never_reports_a_timeout(self):
        bus = FakeBus()
        executor = CooperativeExecutor()
        runtime = self.runtime(bus, executor, attempt_timeout_seconds=0.2)

        def events():
            yield assigned_event()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not executor.executed:
                time.sleep(0.005)
            yield {"id": 11, "topic": "task.assignment_expired",
                   "payload": {"assignment_id": "task:1:attempt:1",
                               "worker_instance_id": "alice-1"}}
            self.assertEqual({}, runtime._attempt_timers)
            time.sleep(0.3)  # past the limit

        runtime.run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)  # by the revocation
        self.assertEqual([], published(bus, "task.attempt_failed"))
        self.assertFalse(any("attempt timeout" in line for line in self.messages))

    def test_an_executor_that_ignores_cancellation_is_reported_for_and_the_worker_stops(self):
        release = threading.Event()
        cancelled = []

        class Stuck:
            def execute(self, assignment):
                release.wait(5)
                return Completed("far too late")

            def cancel(self, assignment_id):
                cancelled.append(assignment_id)  # and carries on regardless

        bus = FakeBus()
        runtime = self.runtime(bus, Stuck(), capacity=1, attempt_timeout_seconds=0.05)

        def events():
            yield assigned_event()
            self.assertTrue(runtime.stop_event.wait(3))
            # The task was released while the executor was still stuck.
            failure, = published(bus, "task.attempt_failed")
            self.assertEqual("attempt_timeout", failure["payload"]["failure_code"])
            self.assertTrue(failure["payload"]["retryable"])
            self.assertEqual(0, runtime.active_count)
            release.set()
            yield assigned_event(task_id=2, event_id=11)  # not accepted any more

        with patch.object(runtime_module, "ATTEMPT_CANCEL_GRACE_SECONDS", 0.05):
            try:
                runtime.run(events())
            finally:
                release.set()
        self.assertEqual(["task:1:attempt:1"], cancelled)
        self.assertEqual(1, len(published(bus, "task.attempt_failed")))
        self.assertEqual([], published(bus, "task.completed"))  # the late result is discarded
        self.assertEqual([1], [e["payload"]["task_id"] for e in published(bus, "task.started")])
        self.assertFalse(runtime.stopped_by_disconnect)
        self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)
        self.assertTrue(any("did not stop within" in line for line in self.messages))

    def test_an_abandoned_attempt_is_still_reported_if_the_executor_returns_meanwhile(self):
        # The worker has given up on the executor and is reporting for it, but
        # the bus is briefly unreachable. The executor returning in that
        # window must not make the worker drop the report.
        tried = threading.Event()

        class ReturnsLate:
            def execute(self, assignment):
                tried.wait(5)
                return Completed("far too late")

        class Bus(FakeBus):
            attempts = 0

            def publish(self, topic, payload, **kwargs):
                if topic == "task.attempt_failed":
                    Bus.attempts += 1
                    if Bus.attempts == 1:
                        tried.set()
                        raise httpx.ConnectError("blip")
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        self.messages = []
        runtime = WorkerRuntime(
            bus, name="alice", instance_id="alice-1", executor=ReturnsLate(),
            publish_retry_seconds=0.2, attempt_timeout_seconds=0.05, log=self.messages.append,
        )

        def events():
            yield assigned_event()
            self.assertTrue(runtime.stop_event.wait(3))

        with patch.object(runtime_module, "ATTEMPT_CANCEL_GRACE_SECONDS", 0.05):
            runtime.run(events())
        failure, = published(bus, "task.attempt_failed")
        self.assertEqual("attempt_timeout", failure["payload"]["failure_code"])
        self.assertEqual([], published(bus, "task.completed"))
        self.assertEqual(set(), runtime._abandoned)
        self.assertEqual(0, runtime.active_count)
        self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)

    def test_a_retiring_worker_stops_even_if_its_report_never_gets_through(self):
        # Heartbeats keep succeeding while the failure report is never
        # acknowledged. The worker must not stay registered, renewing its
        # lease and taking work, for as long as that lasts.
        release = threading.Event()
        counts = {"reports": 0, "heartbeats_after": 0}
        abandoned = threading.Event()

        class Stuck:
            def execute(self, assignment):
                release.wait(5)
                return Completed("far too late")

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.attempt_failed":
                    abandoned.set()
                    counts["reports"] += 1
                    raise httpx.ConnectError("never acknowledged")
                if topic == "agent.heartbeat" and abandoned.is_set():
                    counts["heartbeats_after"] += 1
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        self.messages = []
        runtime = WorkerRuntime(
            bus, name="alice", instance_id="alice-1", executor=Stuck(), capacity=2,
            heartbeat_seconds=0.02, publish_retry_seconds=0.02,
            attempt_timeout_seconds=0.05, log=self.messages.append,
        )

        def events():
            yield assigned_event()
            self.assertTrue(abandoned.wait(3))
            # A second slot is free, but a retiring worker takes nothing new.
            yield assigned_event(task_id=2, event_id=11)
            started = time.monotonic()
            self.assertTrue(runtime.stop_event.wait(3))
            self.assertLess(time.monotonic() - started, 2)
            release.set()

        with (
            patch.object(runtime_module, "ATTEMPT_CANCEL_GRACE_SECONDS", 0.05),
            patch.object(runtime_module, "ATTEMPT_REPORT_SECONDS", 0.2),
        ):
            try:
                runtime.run(events())
            finally:
                release.set()
        self.assertGreaterEqual(counts["reports"], 2)   # it did retry...
        self.assertLess(counts["reports"], 30)          # ...for a bounded time
        self.assertLessEqual(counts["heartbeats_after"], 1)  # at most one already in flight
        self.assertEqual([1], [e["payload"]["task_id"] for e in published(bus, "task.started")])
        self.assertEqual([], published(bus, "task.attempt_failed"))
        self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)
        self.assertTrue(any("could not report the timeout" in line for line in self.messages))
        # It still says it has stopped, so the coordinator need not wait.
        self.assertEqual(1, len(published(bus, "agent.stopped")))

    def test_an_executor_that_stops_within_the_grace_keeps_the_worker_running(self):
        release = threading.Event()

        class Slowish:
            def execute(self, assignment):
                if assignment.task_id == 1:
                    release.wait(5)
                    time.sleep(0.05)  # some clean-up after being told to stop
                    return RetryableFailure("cancelled", "stopped")
                return Completed("done")

            def cancel(self, assignment_id):
                release.set()

        bus = FakeBus()
        runtime = self.runtime(bus, Slowish(), attempt_timeout_seconds=0.05)
        with patch.object(runtime_module, "ATTEMPT_CANCEL_GRACE_SECONDS", 2.0):
            runtime.run(iter([assigned_event(), assigned_event(task_id=2, event_id=11)]))
        self.assertEqual(1, len(published(bus, "task.attempt_failed")))
        self.assertEqual([2], [e["payload"]["task_id"] for e in published(bus, "task.completed")])
        self.assertEqual(0, runtime.exit_code)
        self.assertFalse(any("did not stop within" in line for line in self.messages))
