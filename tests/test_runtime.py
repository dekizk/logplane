import threading
import time
import unittest
from unittest.mock import patch

import httpx

from logplane.executors import Blocked, Completed, PermanentFailure, RetryableFailure
from logplane.runtime import RUNTIME_TOPICS, WorkerRuntime


def assigned_event(
    task_id=1,
    attempt=1,
    *,
    event_id=10,
    instance="alice-1",
    dependency_refs=(),
    deadline_at=None,
    workflow_policy_event_id=None,
    agent_policy_event_id=None,
):
    event = {
        "id": event_id,
        "ts": 100.0,
        "topic": "task.assigned",
        "actor": "pm",
        "correlation_id": "workflow-one",
        "payload": {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "attempt": attempt,
            "assignee": "alice",
            "worker_instance_id": instance,
            "goal": f"task {task_id}",
            "context": {"source": "test"},
            "required_capabilities": ["python"],
            "retry_policy": {"max_retries": 2},
            "retryable_failures": attempt - 1,
            "ownership": {"mode": "controlled", "owner": "agent-bus"},
            "dependency_refs": list(dependency_refs),
        },
    }
    if deadline_at is not None:
        event["payload"]["deadline_at"] = deadline_at
    if workflow_policy_event_id is not None:
        event["payload"][
            "workflow_policy_event_id"
        ] = workflow_policy_event_id
    if agent_policy_event_id is not None:
        event["payload"]["agent_policy_event_id"] = agent_policy_event_id
    return event


class FakeBus:
    actor = "alice"

    def __init__(self, events=(), stored_events=()):
        self.events = list(events)
        self.published = []
        self.subscribe_calls = []
        self._next_id = 100
        self._lock = threading.Lock()
        self.stored_events = {event["id"]: event for event in stored_events}

    def publish(self, topic, payload, **kwargs):
        with self._lock:
            event = {
                "id": self._next_id,
                "ts": time.time(),
                "topic": topic,
                "actor": self.actor,
                "correlation_id": "workflow-one",
                "payload": payload,
                **kwargs,
            }
            self._next_id += 1
            self.published.append(event)
            return event

    def subscribe(self, **kwargs):
        self.subscribe_calls.append(kwargs)
        return iter(self.events)

    def get_event(self, event_id):
        return self.stored_events[event_id]

    def query_all(self, *, after_id=0, topics=None):
        return [
            event for event in sorted(self.stored_events.values(), key=lambda row: row["id"])
            if event["id"] > after_id and (topics is None or event["topic"] in topics)
        ]


class StaticExecutor:
    def __init__(self, outcome):
        self.outcome = outcome

    def execute(self, assignment):
        return self.outcome


class RuntimeTestCase(unittest.TestCase):
    def setUp(self):
        # These unit fixtures deliberately omit the full task/PM history.
        # Admission is tested separately against persisted, validated events.
        self.admission_patch = patch.object(
            WorkerRuntime, "_assignment_is_accepted", return_value=True
        )
        self.admission_patch.start()
        self.addCleanup(self.admission_patch.stop)


class RuntimeOutcomeTests(RuntimeTestCase):
    def run_outcome(self, outcome, **runtime_options):
        fake_bus = FakeBus([assigned_event()])
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=StaticExecutor(outcome),
            capabilities=["python"],
            heartbeat_seconds=100,
            log=lambda message: None,
            **runtime_options,
        )
        runtime.run()
        return fake_bus

    def test_outcomes_translate_to_lifecycle_events(self):
        cases = (
            (Completed("done", {"value": 1}), "task.completed"),
            (Blocked("approval"), "task.blocked"),
            (RetryableFailure("temporary", "retry"), "task.attempt_failed"),
            (PermanentFailure("invalid", "stop"), "task.attempt_failed"),
        )
        for outcome, final_topic in cases:
            with self.subTest(outcome=outcome.__class__.__name__):
                bus = self.run_outcome(outcome)
                topics = [event["topic"] for event in bus.published]
                self.assertEqual(
                    ["agent.registered", "task.started", final_topic],
                    topics,
                )
                final = bus.published[-1]
                self.assertEqual(
                    "task:1:attempt:1",
                    final["payload"]["assignment_id"],
                )
                if isinstance(outcome, RetryableFailure):
                    self.assertTrue(final["payload"]["retryable"])
                if isinstance(outcome, PermanentFailure):
                    self.assertFalse(final["payload"]["retryable"])

    def test_unexpected_exception_policy_is_explicit(self):
        class Broken:
            def execute(self, assignment):
                raise RuntimeError("boom")

        for retryable in (False, True):
            with self.subTest(retryable=retryable):
                bus = FakeBus([assigned_event()])
                runtime = WorkerRuntime(
                    bus,
                    name="alice",
                    instance_id="alice-1",
                    executor=Broken(),
                    heartbeat_seconds=100,
                    unexpected_exceptions_retryable=retryable,
                    log=lambda message: None,
                )
                runtime.run()
                self.assertEqual(
                    retryable,
                    bus.published[-1]["payload"]["retryable"],
                )
                self.assertEqual(
                    "executor_exception",
                    bus.published[-1]["payload"]["failure_code"],
                )

    def test_subscription_starts_at_registration_and_uses_runtime_topics(self):
        bus = self.run_outcome(Completed("done"))
        call = bus.subscribe_calls[0]
        self.assertEqual(list(RUNTIME_TOPICS), call["topics"])
        self.assertEqual(100, call["from_id"])
        self.assertIsInstance(call["stop_event"], threading.Event)

    def test_nonretryable_bus_rejection_stops_the_worker(self):
        class RejectingBus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.started":
                    request = httpx.Request("POST", "http://bus/events")
                    response = httpx.Response(422, request=request)
                    raise httpx.HTTPStatusError(
                        "invalid lifecycle event",
                        request=request,
                        response=response,
                    )
                return super().publish(topic, payload, **kwargs)

        class CountingExecutor:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("done")

        executor = CountingExecutor()
        runtime = WorkerRuntime(
            RejectingBus([assigned_event()]),
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run()
        self.assertTrue(runtime.stop_event.is_set())
        self.assertEqual(0, executor.calls)

    def test_dependency_references_are_resolved_for_the_executor(self):
        completion = {
            "id": 9,
            "topic": "task.completed",
            "correlation_id": "workflow-one",
            "payload": {
                "task_id": 1,
                "summary": "prepared data",
                "result": {"value": 42},
            },
        }
        assignment_event = assigned_event(
            task_id=2,
            dependency_refs=({"task_id": 1, "completion_event_id": 9},),
        )

        class CapturingExecutor:
            received = None

            def execute(self, assignment):
                self.received = assignment
                return Completed("done")

        executor = CapturingExecutor()
        fake_bus = FakeBus([assignment_event], stored_events=[completion])
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()

        self.assertEqual(42, executor.received.dependencies[0]["result"]["value"])
        self.assertNotIn("dependencies", assignment_event["payload"])
        self.assertEqual("task.completed", fake_bus.published[-1]["topic"])

    def test_transient_dependency_lookup_recovers_before_worker_stop(self):
        completion = {
            "id": 9,
            "topic": "task.completed",
            "correlation_id": "workflow-one",
            "payload": {
                "task_id": 1,
                "summary": "prepared data",
                "result": {"value": 42},
            },
        }

        class FlakyBus(FakeBus):
            lookup_calls = 0

            def get_event(self, event_id):
                self.lookup_calls += 1
                if self.lookup_calls < 3:
                    request = httpx.Request("GET", f"http://bus/events/{event_id}")
                    raise httpx.ConnectError("temporary disconnect", request=request)
                return super().get_event(event_id)

        class CapturingExecutor:
            received = None

            def execute(self, assignment):
                self.received = assignment
                return Completed("done")

        executor = CapturingExecutor()
        fake_bus = FlakyBus(
            [
                assigned_event(
                    task_id=2,
                    dependency_refs=(
                        {"task_id": 1, "completion_event_id": 9},
                    ),
                )
            ],
            stored_events=[completion],
        )
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            publish_retry_seconds=0.001,
            log=lambda message: None,
        ).run()

        self.assertEqual(3, fake_bus.lookup_calls)
        self.assertEqual(42, executor.received.dependencies[0]["result"]["value"])
        self.assertEqual("task.completed", fake_bus.published[-1]["topic"])

    def test_exhausted_transient_dependency_lookup_stops_worker(self):
        class OfflineBus(FakeBus):
            lookup_calls = 0

            def get_event(self, event_id):
                self.lookup_calls += 1
                request = httpx.Request("GET", f"http://bus/events/{event_id}")
                raise httpx.ConnectError("offline", request=request)

        class MustNotRun:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("unexpected")

        executor = MustNotRun()
        fake_bus = OfflineBus(
            [
                assigned_event(
                    task_id=2,
                    dependency_refs=(
                        {"task_id": 1, "completion_event_id": 9},
                    ),
                )
            ]
        )
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            publish_retry_seconds=0.001,
            log=lambda message: None,
        )
        runtime.run()

        self.assertEqual(3, fake_bus.lookup_calls)
        self.assertEqual(0, executor.calls)
        self.assertTrue(runtime.stop_event.is_set())
        self.assertEqual(["agent.registered"], [event["topic"] for event in fake_bus.published])

    def test_dependency_lookup_4xx_stops_without_retry(self):
        class MissingEventBus(FakeBus):
            lookup_calls = 0

            def get_event(self, event_id):
                self.lookup_calls += 1
                request = httpx.Request("GET", f"http://bus/events/{event_id}")
                response = httpx.Response(404, request=request)
                raise httpx.HTTPStatusError(
                    "event not found",
                    request=request,
                    response=response,
                )

        fake_bus = MissingEventBus(
            [
                assigned_event(
                    task_id=2,
                    dependency_refs=(
                        {"task_id": 1, "completion_event_id": 9},
                    ),
                )
            ]
        )
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=StaticExecutor(Completed("unexpected")),
            heartbeat_seconds=100,
            publish_retry_seconds=0.001,
            log=lambda message: None,
        )
        runtime.run()

        self.assertEqual(1, fake_bus.lookup_calls)
        self.assertTrue(runtime.stop_event.is_set())

    def test_dependency_fetch_attempts_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "dependency_fetch_attempts"):
            WorkerRuntime(
                FakeBus(),
                name="alice",
                executor=StaticExecutor(Completed("done")),
                dependency_fetch_attempts=0,
            )

    def test_aggregate_dependency_input_is_bounded_before_executor_runs(self):
        completions = []
        refs = []
        for task_id in range(1, 4):
            event_id = 20 + task_id
            refs.append({"task_id": task_id, "completion_event_id": event_id})
            completions.append(
                {
                    "id": event_id,
                    "topic": "task.completed",
                    "correlation_id": "workflow-one",
                    "payload": {
                        "task_id": task_id,
                        "summary": "large",
                        "result": {"value": "x" * 12_000},
                    },
                }
            )

        class MustNotRun:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("unexpected")

        executor = MustNotRun()
        fake_bus = FakeBus(
            [assigned_event(task_id=4, dependency_refs=refs)],
            stored_events=completions,
        )
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()

        self.assertEqual(0, executor.calls)
        failure = fake_bus.published[-1]
        self.assertEqual("task.attempt_failed", failure["topic"])
        self.assertEqual(
            "dependency_input_too_large",
            failure["payload"]["failure_code"],
        )
        self.assertFalse(failure["payload"]["retryable"])


class RuntimeOwnershipTests(RuntimeTestCase):
    def enable_real_admission(self, fake_bus):
        self.admission_patch.stop()
        history = [
            {"id": 1, "ts": 100.0, "topic": "task.created", "actor": "human",
             "correlation_id": "workflow-one", "payload": {"task_id": 1, "title": "task 1"}},
            {"id": 2, "ts": 100.0, "topic": "agent.registered", "actor": "alice",
             "payload": {"name": "alice", "instance_id": "alice-1", "capacity": 1}},
        ]
        for event in fake_bus.events:
            event.setdefault("ts", 100.0)
            event.setdefault("actor", "human")
            history.append(event)
        fake_bus.stored_events = {event["id"]: event for event in history}

    def test_agent_policy_change_fences_stale_assignment_before_executor(self):
        class RecordingExecutor:
            def __init__(self):
                self.assignments = []

            def execute(self, assignment):
                self.assignments.append(
                    (assignment.assignment_id, assignment.agent_policy_event_id)
                )
                return Completed("done")

        policy = {
            "id": 21,
            "topic": "agent.policy_set",
            "payload": {
                "agent_name": "alice",
                "source": "operator",
                "reason": "one slot",
                "max_active_assignments": 1,
            },
        }
        executor = RecordingExecutor()
        fake_bus = FakeBus(
            [
                policy,
                assigned_event(
                    event_id=22, attempt=1, agent_policy_event_id=20
                ),
                assigned_event(
                    event_id=23, attempt=2, agent_policy_event_id=21
                ),
            ]
        )
        self.enable_real_admission(fake_bus)
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()

        self.assertIn("agent.policy_set", RUNTIME_TOPICS)
        self.assertEqual([("task:1:attempt:2", 21)], executor.assignments)

    def test_policy_change_fences_stale_assignment_before_executor(self):
        class RecordingExecutor:
            def __init__(self):
                self.assignments = []
                self.policy_event_ids = []

            def execute(self, assignment):
                self.assignments.append(assignment.assignment_id)
                self.policy_event_ids.append(assignment.workflow_policy_event_id)
                return Completed("done")

        policy = {
            "id": 11,
            "topic": "workflow.policy_set",
            "correlation_id": "workflow-one",
            "payload": {
                "source": "operator",
                "reason": "lower concurrency",
                "max_active_assignments": 1,
            },
        }
        stale = assigned_event(
            event_id=12,
            attempt=1,
            workflow_policy_event_id=10,
        )
        current = assigned_event(
            event_id=13,
            attempt=2,
            workflow_policy_event_id=11,
        )
        executor = RecordingExecutor()
        fake_bus = FakeBus([policy, stale, current])
        self.enable_real_admission(fake_bus)
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()

        self.assertIn("workflow.policy_set", RUNTIME_TOPICS)
        self.assertEqual(["task:1:attempt:2"], executor.assignments)
        self.assertEqual([11], executor.policy_event_ids)
        self.assertEqual(
            1,
            len(
                [
                    event
                    for event in fake_bus.published
                    if event["topic"] == "task.started"
                ]
            ),
        )

    def test_already_expired_assignment_never_starts_or_executes(self):
        class CountingExecutor:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("unexpected")

        executor = CountingExecutor()
        bus = FakeBus([assigned_event(deadline_at=time.time() - 1)])
        WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run()

        self.assertEqual(0, executor.calls)
        self.assertEqual(["agent.registered"], [event["topic"] for event in bus.published])

    def test_deadline_during_started_publish_is_rechecked_before_execution(self):
        publishing_started = threading.Event()
        release_publish = threading.Event()

        class BlockingStartBus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.started":
                    publishing_started.set()
                    release_publish.wait(2)
                return super().publish(topic, payload, **kwargs)

        class CountingExecutor:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("unexpected")

        executor = CountingExecutor()
        bus = BlockingStartBus(
            [assigned_event(deadline_at=time.time() + 0.05)]
        )
        run_thread = threading.Thread(
            target=WorkerRuntime(
                bus,
                name="alice",
                instance_id="alice-1",
                executor=executor,
                heartbeat_seconds=100,
                log=lambda message: None,
            ).run
        )
        run_thread.start()
        self.assertTrue(publishing_started.wait(1))
        time.sleep(0.08)
        release_publish.set()
        run_thread.join(2)
        self.assertFalse(run_thread.is_alive())
        self.assertEqual(0, executor.calls)

    def test_replayed_assignment_is_not_executed_after_cleanup(self):
        completed = threading.Event()

        class CompletionBus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                event = super().publish(topic, payload, **kwargs)
                if topic == "task.completed":
                    completed.set()
                return event

        class CountingExecutor:
            calls = 0

            def execute(self, assignment):
                self.calls += 1
                return Completed("done")

        delivery = assigned_event()

        def events():
            yield delivery
            self.assertTrue(completed.wait(1))
            yield delivery

        executor = CountingExecutor()
        bus = CompletionBus()
        WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run(events())

        self.assertEqual(1, executor.calls)
        self.assertEqual(
            1,
            len([event for event in bus.published if event["topic"] == "task.started"]),
        )

    def test_ownership_loss_during_outcome_publish_does_not_cancel_executor(self):
        publishing = threading.Event()
        release_publish = threading.Event()

        class BlockingBus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.completed":
                    publishing.set()
                    release_publish.wait(2)
                return super().publish(topic, payload, **kwargs)

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                return Completed("done")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        expiry = {
            "id": 11,
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
        }

        def events():
            yield assigned_event()
            self.assertTrue(publishing.wait(1))
            yield expiry
            release_publish.set()

        executor = CancellableExecutor()
        WorkerRuntime(
            BlockingBus(),
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run(events())
        self.assertEqual([], executor.cancelled)

    def test_cancel_and_close_hooks_never_overlap(self):
        began = threading.Event()
        cancel_entered = threading.Event()
        allow_cancel_finish = threading.Event()
        execute_release = threading.Event()
        close_entered = threading.Event()

        class CoordinatedExecutor:
            def __init__(self):
                self.in_cancel = False
                self.overlapped = False

            def execute(self, assignment):
                began.set()
                execute_release.wait(2)
                return Completed("late")

            def cancel(self, assignment_id):
                self.in_cancel = True
                cancel_entered.set()
                allow_cancel_finish.wait(2)
                self.in_cancel = False
                execute_release.set()

            def close(self):
                self.overlapped = self.in_cancel
                close_entered.set()

        executor = CoordinatedExecutor()
        runtime = WorkerRuntime(
            FakeBus(),
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )

        def events():
            yield assigned_event()
            while not runtime.stop_event.wait(0.01):
                pass

        run_thread = threading.Thread(target=runtime.run, args=(events(),))
        run_thread.start()
        self.assertTrue(began.wait(1))
        stop_thread = threading.Thread(target=runtime.stop)
        stop_thread.start()
        self.assertTrue(cancel_entered.wait(1))
        shutdown_thread = threading.Thread(target=runtime.shutdown)
        shutdown_thread.start()
        self.assertFalse(close_entered.wait(0.05))
        allow_cancel_finish.set()
        stop_thread.join(2)
        shutdown_thread.join(2)
        run_thread.join(2)
        self.assertTrue(close_entered.is_set())
        self.assertFalse(executor.overlapped)

    def test_persisted_deadline_locally_revokes_without_waiting_for_pm(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)
                release.set()

        executor = CancellableExecutor()
        fake_bus = FakeBus(
            [assigned_event(deadline_at=time.time() + 0.05)]
        )
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run()
        self.assertTrue(began.is_set())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn(
            "task.completed",
            [event["topic"] for event in fake_bus.published],
        )
        self.assertEqual({}, runtime._deadline_timers)

    def test_cancel_request_revokes_running_task_and_suppresses_result(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        cancel_request = {
            "id": 11,
            "topic": "task.cancel_requested",
            "payload": {"task_id": 1, "reason": "stop now"},
        }

        def events():
            yield assigned_event()
            self.assertTrue(began.wait(1))
            yield cancel_request
            release.set()

        executor = CancellableExecutor()
        fake_bus = FakeBus()
        runtime = WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn(
            "task.completed",
            [event["topic"] for event in fake_bus.published],
        )
        self.assertEqual({}, runtime._task_by_assignment)

    def test_deadline_terminal_event_revokes_running_assignment(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        deadline = {
            "id": 11,
            "topic": "task.deadline_exceeded",
            "payload": {
                "task_id": 1,
                "deadline_at": 100.0,
                "last_assignment_id": "task:1:attempt:1",
                "attempts": 1,
            },
        }

        def events():
            yield assigned_event()
            self.assertTrue(began.wait(1))
            yield deadline
            release.set()

        executor = CancellableExecutor()
        fake_bus = FakeBus()
        WorkerRuntime(
            fake_bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        ).run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn(
            "task.completed",
            [event["topic"] for event in fake_bus.published],
        )

    def test_capacity_bounds_concurrent_execution(self):
        release = threading.Event()
        two_running = threading.Event()

        class MeasuringExecutor:
            def __init__(self):
                self.lock = threading.Lock()
                self.current = 0
                self.maximum = 0

            def execute(self, assignment):
                with self.lock:
                    self.current += 1
                    self.maximum = max(self.maximum, self.current)
                    if self.current == 2:
                        two_running.set()
                release.wait(2)
                with self.lock:
                    self.current -= 1
                return Completed("done")

        executor = MeasuringExecutor()
        bus = FakeBus(
            [
                assigned_event(1, event_id=10),
                assigned_event(2, event_id=11),
                assigned_event(3, event_id=12),
            ]
        )
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            capacity=2,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        thread = threading.Thread(target=runtime.run)
        thread.start()
        self.assertTrue(two_running.wait(1))
        self.assertEqual(2, executor.maximum)
        release.set()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(3, len([e for e in bus.published if e["topic"] == "task.completed"]))

    def test_expiry_cancels_adapter_and_suppresses_late_result(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("too late")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        assignment = assigned_event()
        expiry = {
            "id": 11,
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
        }

        def events():
            yield assignment
            self.assertTrue(began.wait(1))
            yield expiry
            release.set()

        executor = CancellableExecutor()
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run(events())
        self.assertEqual(["task:1:attempt:1"], executor.cancelled)
        self.assertNotIn("task.completed", [event["topic"] for event in bus.published])

    def test_expiry_of_queued_work_does_not_cancel_adapter(self):
        began = threading.Event()
        release = threading.Event()

        class CancellableExecutor:
            def __init__(self):
                self.cancelled = []

            def execute(self, assignment):
                began.set()
                release.wait(2)
                return Completed("done")

            def cancel(self, assignment_id):
                self.cancelled.append(assignment_id)

        expiry = {
            "id": 12,
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": 2,
                "assignment_id": "task:2:attempt:1",
                "worker_instance_id": "alice-1",
            },
        }

        def events():
            yield assigned_event(1, event_id=10)
            self.assertTrue(began.wait(1))
            yield assigned_event(2, event_id=11)
            yield expiry
            release.set()

        executor = CancellableExecutor()
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=executor,
            capacity=1,
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.run(events())
        self.assertEqual([], executor.cancelled)
        started_task_ids = [
            event["payload"]["task_id"]
            for event in bus.published
            if event["topic"] == "task.started"
        ]
        self.assertEqual([1], started_task_ids)

    def test_replacement_instance_stops_old_runtime(self):
        bus = FakeBus()
        runtime = WorkerRuntime(
            bus,
            name="alice",
            instance_id="alice-1",
            executor=StaticExecutor(Completed("done")),
            heartbeat_seconds=100,
            log=lambda message: None,
        )
        runtime.process_event(
            {
                "topic": "agent.registered",
                "payload": {"name": "alice", "instance_id": "alice-2"},
            }
        )
        self.assertTrue(runtime.stop_event.is_set())


if __name__ == "__main__":
    unittest.main()
