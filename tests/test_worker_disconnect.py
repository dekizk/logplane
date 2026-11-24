"""Worker-local disconnect policy. No real bus, agents, or model calls."""

import os
import threading
from unittest.mock import patch

import httpx

from logplane.executors import Completed
from logplane.runtime import DISCONNECT_EXIT_CODE, ERROR_EXIT_CODE, WorkerRuntime
from tests.test_runtime import FakeBus, RuntimeTestCase, StaticExecutor, assigned_event


class DisconnectTests(RuntimeTestCase):
    def runtime(self, bus=None, executor=None, **kwargs):
        return WorkerRuntime(
            bus or FakeBus(), name="alice", instance_id="alice-1",
            executor=executor or StaticExecutor(Completed("done")),
            publish_retry_seconds=0.005, **kwargs,
        )

    def test_default_opt_in_and_invalid_configuration(self):
        key = "LOGPLANE_WORKER_DISCONNECT_TIMEOUT_SECONDS"
        with patch.dict(os.environ):
            os.environ.pop(key, None)
            worker = self.runtime()
            worker._write_failed()
            self.assertIsNone(worker._disconnect_timer)
            os.environ[key] = "120"
            self.assertEqual(self.runtime().disconnect_timeout_seconds, 120)
            self.assertEqual(self.runtime(disconnect_timeout_seconds=90).disconnect_timeout_seconds, 90)
            for invalid in ("-1", "0", "nan", "inf", "no"):
                os.environ[key] = invalid
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    self.runtime()

    def test_short_outage_recovers_without_a_second_execution_or_new_outcome(self):
        # Advance outage time explicitly: the worker policy is 60s, independent
        # of the coordinator's 20s lease. The same completion is retried at 30s.
        clock = [0]
        timers = []
        writes = []
        executions = []

        class Timer:
            def __init__(self, delay, callback):
                self.due, self.callback = clock[0] + delay, callback
                timers.append(self)

            def start(self):
                pass

            def cancel(self):
                pass

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.completed":
                    writes.append((payload, kwargs["idempotency_key"]))
                    if len(writes) == 1:
                        raise httpx.ConnectError("offline")
                    clock[0] = 30
                    for timer in timers:
                        if timer.due <= clock[0]:
                            timer.callback()
                return super().publish(topic, payload, **kwargs)

        class Executor:
            def execute(self, assignment):
                executions.append(assignment.assignment_id)
                return Completed("done")

        bus = Bus([assigned_event()])
        runtime = self.runtime(bus, Executor(), disconnect_timeout_seconds=60)
        with patch("logplane.runtime.threading.Timer", Timer):
            runtime.run()
        self.assertEqual([60], [timer.due for timer in timers])
        self.assertEqual(["task:1:attempt:1"], executions)
        self.assertEqual(2, len(writes))
        self.assertEqual(writes[0], writes[1])
        self.assertEqual(1, sum(e["topic"] == "task.completed" for e in bus.published))

    def test_recovery_disarms_old_callback_and_repeated_failures_do_not_extend(self):
        runtime = self.runtime(disconnect_timeout_seconds=300)
        runtime._write_failed()
        old = runtime._disconnect_timer
        runtime._write_failed()
        self.assertIs(old, runtime._disconnect_timer)
        runtime._write_confirmed()
        runtime._write_failed()
        try:
            runtime._disconnect_elapsed(old)
            self.assertFalse(runtime.stop_event.is_set())
        finally:
            runtime._write_confirmed()

    def test_prolonged_outage_stops_publication_and_cancels_only_running_work(self):
        executing = threading.Event()
        pending = threading.Event()
        cancelled = []
        release = threading.Event()
        messages = []

        class Executor:
            def execute(self, assignment):
                if assignment.task_id == 1:
                    executing.set()
                    release.wait(3)
                return Completed("private result")

            def cancel(self, assignment_id):
                cancelled.append(assignment_id)
                release.set()

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.completed":
                    pending.set()
                    raise httpx.ConnectError("offline")
                if topic == "agent.heartbeat" and pending.is_set():
                    raise httpx.ConnectError("offline")
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        runtime = self.runtime(bus, Executor(), capacity=2, heartbeat_seconds=0.01,
                               disconnect_timeout_seconds=0.1, log=messages.append)

        def events():
            yield assigned_event()
            self.assertTrue(executing.wait(2))
            yield assigned_event(task_id=2, event_id=11)
            self.assertTrue(pending.wait(2))
            self.assertTrue(runtime.stop_event.wait(2))
            yield assigned_event(task_id=3, event_id=12)

        try:
            runtime.run(events())
        finally:
            release.set()
        self.assertEqual(["task:1:attempt:1"], cancelled)
        report, = [line for line in messages if "disconnect timeout" in line]
        self.assertIn("'task:1:attempt:1': 'executing'", report)
        self.assertIn("'task:2:attempt:1': 'publishing_outcome'", report)
        self.assertNotIn("private result", report)
        self.assertEqual([1, 2], sorted(e["payload"]["task_id"] for e in bus.published
                                        if e["topic"] == "task.started"))
        self.assertFalse(any(e["topic"] in ("task.completed", "task.attempt_failed")
                             for e in bus.published))

    def test_heartbeat_failures_alone_stop_an_idle_worker(self):
        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "agent.heartbeat":
                    raise httpx.ConnectError("offline")
                return super().publish(topic, payload, **kwargs)

        runtime = self.runtime(Bus(), heartbeat_seconds=0.005, disconnect_timeout_seconds=0.03)
        def events():
            self.assertTrue(runtime.stop_event.wait(2))
            yield assigned_event()
        runtime.run(events())
        self.assertEqual(0, runtime.active_count)

    def test_observed_revocation_discards_pending_outcome_before_timeout(self):
        tried = threading.Event()
        keys = []

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.completed":
                    keys.append(kwargs["idempotency_key"])
                    tried.set()
                    raise httpx.ConnectError("offline")
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        runtime = self.runtime(bus, disconnect_timeout_seconds=60)
        def events():
            yield assigned_event()
            self.assertTrue(tried.wait(2))
            yield {"id": 11, "topic": "task.assignment_expired",
                   "payload": {"assignment_id": "task:1:attempt:1",
                               "worker_instance_id": "alice-1"}}
            self.assertEqual(0, runtime.active_count)
            self.assertFalse(runtime.stop_event.is_set())
        runtime.run(events())
        self.assertTrue(keys)
        self.assertFalse(any(e["topic"] == "task.completed" for e in bus.published))


class DisconnectReportingTests(RuntimeTestCase):
    """How a caller or supervisor learns why the worker stopped."""

    def runtime(self, bus=None, **kwargs):
        return WorkerRuntime(
            bus or FakeBus(), name="alice", instance_id="alice-1",
            executor=StaticExecutor(Completed("done")), **kwargs,
        )

    def test_a_disconnect_stop_is_reported_with_a_restartable_exit_code(self):
        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "agent.heartbeat":
                    raise httpx.ConnectError("offline")
                return super().publish(topic, payload, **kwargs)

        runtime = self.runtime(Bus(), heartbeat_seconds=0.005, disconnect_timeout_seconds=0.03,
                               log=lambda message: None)
        self.assertFalse(runtime.stopped_by_disconnect)
        self.assertEqual(0, runtime.exit_code)

        def events():
            self.assertTrue(runtime.stop_event.wait(2))
            yield assigned_event()

        runtime.run(events())
        self.assertTrue(runtime.stopped_by_disconnect)
        self.assertEqual(DISCONNECT_EXIT_CODE, runtime.exit_code)
        self.assertEqual(75, DISCONNECT_EXIT_CODE)

    def test_an_ordinary_stop_reports_success(self):
        runtime = self.runtime(disconnect_timeout_seconds=60, log=lambda message: None)
        runtime.run(iter(()))
        self.assertFalse(runtime.stopped_by_disconnect)
        self.assertEqual(0, runtime.exit_code)

    def test_a_timeout_shorter_than_two_heartbeat_intervals_is_rejected(self):
        # The timer starts at the first failed write. Shorter than two
        # heartbeat intervals, it would run out before a retry was even sent.
        # (A floor only: a retry answered after the timer fires is too late.)
        with self.assertRaisesRegex(
            ValueError,
            r"^disconnect_timeout_seconds must be at least twice the heartbeat interval \(5s\)",
        ):
            self.runtime(disconnect_timeout_seconds=9.9)
        self.assertEqual(10, self.runtime(disconnect_timeout_seconds=10).disconnect_timeout_seconds)
        self.assertEqual(
            2, self.runtime(heartbeat_seconds=1, disconnect_timeout_seconds=2).disconnect_timeout_seconds
        )
        with self.assertRaisesRegex(ValueError, r"heartbeat interval \(30s\)"):
            self.runtime(heartbeat_seconds=30, disconnect_timeout_seconds=45)

    def test_an_unreachable_bus_during_admission_is_a_restartable_stop(self):
        # The worker refuses to run an assignment it cannot verify and stops
        # at once, timeout or no timeout. That is a bus failure, not success.
        class Bus(FakeBus):
            def query_all(self, *args, **kwargs):
                raise httpx.ConnectError("offline")

        self.admission_patch.stop()  # exercise the real admission read
        self.addCleanup(self.admission_patch.start)
        for options in ({}, {"disconnect_timeout_seconds": 60}):
            with self.subTest(options=options):
                bus = Bus()
                executed = []

                class Executor:
                    def execute(self, assignment):
                        executed.append(assignment.assignment_id)
                        return Completed("done")

                runtime = WorkerRuntime(bus, name="alice", instance_id="alice-1",
                                        executor=Executor(), log=lambda message: None,
                                        **options)
                runtime.run(iter([assigned_event()]))
                self.assertEqual([], executed)
                self.assertTrue(runtime.stopped_by_disconnect)
                self.assertEqual(DISCONNECT_EXIT_CODE, runtime.exit_code)

    def test_other_self_inflicted_stops_exit_with_a_plain_failure(self):
        request = httpx.Request("POST", "http://bus/events")

        def rejected(status):
            return httpx.HTTPStatusError(
                "rejected", request=request, response=httpx.Response(status, request=request)
            )

        class RejectsStart(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.started":
                    raise rejected(409)
                return super().publish(topic, payload, **kwargs)

        runtime = self.runtime(RejectsStart(), log=lambda message: None)
        runtime.run(iter([assigned_event()]))
        self.assertFalse(runtime.stopped_by_disconnect)
        self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)

        # An assignment that cannot be resolved safely: malformed, or the bus
        # refuses the lookup. Neither is an outage.
        self.admission_patch.stop()
        self.addCleanup(self.admission_patch.start)
        for failure in (ValueError("coordination history is not strictly ordered"), rejected(403)):
            with self.subTest(failure=failure):
                class Bus(FakeBus):
                    def query_all(self, *args, **kwargs):
                        raise failure

                runtime = self.runtime(Bus(), log=lambda message: None)
                runtime.run(iter([assigned_event()]))
                self.assertFalse(runtime.stopped_by_disconnect)
                self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)
        self.assertEqual(1, ERROR_EXIT_CODE)

    def test_a_refused_heartbeat_stops_the_worker_as_a_failure_not_an_outage(self):
        # The bus is reachable and has said no (a bad token, say). Waiting out
        # the disconnect timeout and exiting 75 would invite a restart loop;
        # with no timeout the worker used to log the refusal forever.
        request = httpx.Request("POST", "http://bus/events")
        for timeout in (0.2, None):
            for status in (401, 403):
                with self.subTest(timeout=timeout, status=status):
                    running = threading.Event()
                    cancelled = []
                    messages = []

                    class Bus(FakeBus):
                        def publish(self, topic, payload, **kwargs):
                            if topic == "agent.heartbeat" and running.is_set():
                                raise httpx.HTTPStatusError(
                                    "refused", request=request,
                                    response=httpx.Response(status, request=request),
                                )
                            return super().publish(topic, payload, **kwargs)

                    class Executor:
                        def execute(self, assignment):
                            running.set()
                            released.wait(3)
                            return Completed("never reported")

                        def cancel(self, assignment_id):
                            cancelled.append(assignment_id)
                            released.set()

                    released = threading.Event()
                    bus = Bus()
                    runtime = WorkerRuntime(
                        bus, name="alice", instance_id="alice-1", executor=Executor(),
                        heartbeat_seconds=0.01, disconnect_timeout_seconds=timeout,
                        log=messages.append,
                    )

                    def events():
                        yield assigned_event()
                        self.assertTrue(runtime.stop_event.wait(2))
                        yield assigned_event(task_id=2, event_id=11)

                    try:
                        runtime.run(events())
                    finally:
                        released.set()
                    refusals = [line for line in messages if "bus rejected agent.heartbeat" in line]
                    self.assertEqual(1, len(refusals), messages)
                    self.assertIn(f"HTTP {status}", refusals[0])
                    self.assertEqual(["task:1:attempt:1"], cancelled)
                    self.assertFalse(runtime.stopped_by_disconnect)
                    self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)
                    self.assertFalse(any(e["topic"] == "task.completed" for e in bus.published))
                    self.assertEqual([1], [e["payload"]["task_id"] for e in bus.published
                                           if e["topic"] == "task.started"])

    def test_a_heartbeat_the_bus_fails_to_serve_is_still_an_outage(self):
        request = httpx.Request("POST", "http://bus/events")

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "agent.heartbeat":
                    raise httpx.HTTPStatusError(
                        "unavailable", request=request,
                        response=httpx.Response(503, request=request),
                    )
                return super().publish(topic, payload, **kwargs)

        runtime = self.runtime(Bus(), heartbeat_seconds=0.005, disconnect_timeout_seconds=0.03,
                               log=lambda message: None)

        def events():
            self.assertTrue(runtime.stop_event.wait(2))
            yield assigned_event()

        runtime.run(events())
        self.assertTrue(runtime.stopped_by_disconnect)
        self.assertEqual(DISCONNECT_EXIT_CODE, runtime.exit_code)

    def test_a_requested_stop_and_a_replacement_instance_exit_zero(self):
        runtime = self.runtime(log=lambda message: None)
        runtime.run(iter([{"id": 5, "topic": "agent.registered",
                           "payload": {"name": "alice", "instance_id": "alice-2"}}]))
        self.assertTrue(runtime.stop_event.is_set())
        self.assertEqual(0, runtime.exit_code)

    def test_errors_name_the_setting_the_operator_actually_used(self):
        key = "LOGPLANE_WORKER_DISCONNECT_TIMEOUT_SECONDS"
        with patch.dict(os.environ):
            for invalid in ("-5", "0", "nan", "inf", "soon", "3"):
                os.environ[key] = invalid
                with self.subTest(source="environment", invalid=invalid):
                    with self.assertRaisesRegex(ValueError, f"^{key} must be "):
                        self.runtime()
            # An explicit argument wins over the environment and is the one named.
            for invalid in (-5, 0, float("nan"), float("inf"), True, "60", 3):
                with self.subTest(source="argument", invalid=invalid):
                    with self.assertRaisesRegex(ValueError, "^disconnect_timeout_seconds must be "):
                        self.runtime(disconnect_timeout_seconds=invalid)

