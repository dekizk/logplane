"""A worker that stops in an orderly way hands its work back at once.

The worker announces the stop with agent.stopped; the coordinator then expires
what it held without waiting out the lease, and without charging the task when
the stop was requested. No real bus, agents, or model calls.
"""

import os
import signal
import threading
import time
import unittest
from unittest.mock import patch

import httpx

from logplane.event_validation import EventValidationError, validate_event
from logplane.executors import Completed, RetryableFailure
from logplane.operations import explain_task, worker_views
from logplane.pm_agent import plan_next_emission
from logplane.projection import replay_events
from logplane.runtime import ERROR_EXIT_CODE, WorkerRuntime
from logplane import runtime as runtime_module
from tests.test_pm_agent import assigned, created, event, registered
from tests.test_runtime import FakeBus, RuntimeTestCase, assigned_event

LEASE = 20.0
NOW = 101.0  # one second after every fixture event: all leases are fresh


def stopped(event_id, *, name="alice", instance="alice-1", requested=True, actor=None):
    return event(event_id, "agent.stopped", actor or name,
                 {"name": name, "instance_id": instance, "requested": requested})


def started(event_id, attempt, *, task_id=1, worker="alice", instance="alice-1"):
    return event(event_id, "task.started", worker,
                 {"task_id": task_id, "assignment_id": f"task:{task_id}:attempt:{attempt}",
                  "worker_instance_id": instance})


def heartbeat(event_id, *, name="alice", instance="alice-1", ts=100.5):
    return event(event_id, "agent.heartbeat", name,
                 {"name": name, "instance_id": instance}, ts=ts)


def with_lease_policy(created_event, max_expiries=3):
    created_event["payload"]["lease_policy"] = {"max_expiries": max_expiries}
    return created_event


def plan(events, **options):
    return plan_next_emission(replay_events(events), now=NOW, lease_seconds=LEASE, **options)


def record(events, planned):
    """Append a planned PM effect the way the bus would."""
    return events + [dict(planned, id=events[-1]["id"] + 1, actor="pm", ts=NOW,
                          schema_version=2, correlation_id=None)]


class StopEventTests(unittest.TestCase):
    def test_shape_is_validated(self):
        validate_event("agent.stopped", "alice",
                       {"name": "alice", "instance_id": "alice-1", "requested": True}, None, None, 2)
        validate_event("agent.stopped", "alice",
                       {"name": "alice", "instance_id": "alice-1"}, None, None, 2)
        for payload in ({"name": "alice"}, {"instance_id": "alice-1"},
                        {"name": "bob", "instance_id": "alice-1"},
                        {"name": "alice", "instance_id": "alice-1", "requested": "yes"}):
            with self.subTest(payload=payload), self.assertRaises(EventValidationError):
                validate_event("agent.stopped", "alice", payload, None, None, 2)
        expiry = {"task_id": 1, "assignment_id": "task:1:attempt:1", "assignee": "alice",
                  "worker_instance_id": "alice-1", "reason": "worker stopped"}
        validate_event("task.assignment_expired", "pm", {**expiry, "released": True}, None, None, 2)
        with self.assertRaises(EventValidationError):
            validate_event("task.assignment_expired", "pm", {**expiry, "released": 1}, None, None, 2)

    def test_a_stopped_worker_holds_no_lease_and_a_heartbeat_does_not_revive_it(self):
        events = [registered(), stopped(2)]
        state = replay_events(events)
        self.assertEqual(2, state.workers["alice"].stopped_event_id)
        self.assertTrue(state.workers["alice"].stop_requested)
        self.assertEqual([], state.active_workers(NOW, LEASE))
        state = replay_events(events + [heartbeat(3), stopped(4, requested=False)])
        self.assertEqual([], state.active_workers(NOW, LEASE))
        self.assertEqual(2, state.workers["alice"].stopped_event_id)  # the first stop stands
        self.assertTrue(state.workers["alice"].stop_requested)
        view, = worker_views(state, now=NOW, lease_seconds=LEASE)
        self.assertEqual("stopped", view["status"])

    def test_only_the_registered_instance_can_stop_itself(self):
        for label, impostor in (
            ("a replaced process", stopped(2, instance="alice-0")),
            ("another actor", stopped(2, actor="mallory")),
            ("an unknown worker", stopped(2, name="carol", instance="carol-1")),
        ):
            with self.subTest(label):
                state = replay_events([registered(), impostor])
                self.assertIsNone(state.workers["alice"].stopped_event_id)
                self.assertEqual(1, len(state.active_workers(NOW, LEASE)))

    def test_registering_again_brings_the_name_back(self):
        state = replay_events([registered(), stopped(2), registered(3, instance="alice-2")])
        worker = state.workers["alice"]
        self.assertEqual(("alice-2", None, False),
                         (worker.instance_id, worker.stopped_event_id, worker.stop_requested))
        self.assertEqual([worker], state.active_workers(NOW, LEASE))


class HandBackTests(unittest.TestCase):
    def running(self, *, lease_policy=True, begun=True):
        task = created(3)
        if lease_policy:
            with_lease_policy(task)
        events = [registered(), registered(2, name="bob", instance="bob-1"), task,
                  assigned(4, 1)]
        return events + ([started(5, 1)] if begun else [])

    def test_a_requested_stop_hands_work_back_at_once_and_free_of_charge(self):
        for lease_policy in (True, False):
            with self.subTest(lease_policy=lease_policy):
                events = self.running(lease_policy=lease_policy)
                self.assertIsNone(plan(events))  # alice's lease is fresh: nothing to do
                events.append(stopped(6))
                explained = explain_task(replay_events(events), 1, now=NOW, lease_seconds=LEASE)
                self.assertEqual("active_worker_stopped", explained["code"])

                # Not held back by the start-up and reconnect grace: the worker
                # said it stopped, so there is no outage to allow for.
                planned = plan(events, hold_lease_expiry=True)
                self.assertEqual("task.assignment_expired", planned["topic"])
                self.assertEqual("worker stopped", planned["payload"]["reason"])
                self.assertIs(True, planned["payload"]["released"])
                self.assertEqual("expire:task:1:attempt:1", planned["idempotency_key"])

                events = record(events, planned)
                task = replay_events(events).tasks[1]
                self.assertEqual(("open", 0, 0),
                                 (task.status, task.retryable_failures, task.lease_expiries))
                again = plan(events)
                self.assertEqual(("task.assigned", 2, "bob"),
                                 (again["topic"], again["payload"]["attempt"],
                                  again["payload"]["assignee"]))

    def test_a_stopped_worker_is_not_given_new_work(self):
        events = [registered(), with_lease_policy(created(2)), stopped(3)]
        self.assertIsNone(plan(events))
        explained = explain_task(replay_events(events), 1, now=NOW, lease_seconds=LEASE)
        self.assertEqual("no_active_workers", explained["code"])
        # ...until a new process registers under the name.
        planned = plan(events + [registered(4, instance="alice-2")])
        self.assertEqual(("task.assigned", "alice-2"),
                         (planned["topic"], planned["payload"]["worker_instance_id"]))

    def test_a_worker_that_stopped_itself_over_a_failure_counts_as_a_lost_worker(self):
        events = self.running() + [stopped(6, requested=False)]
        planned = plan(events)
        self.assertEqual("worker stopped", planned["payload"]["reason"])
        self.assertNotIn("released", planned["payload"])
        task = replay_events(record(events, planned)).tasks[1]
        self.assertEqual(("open", 0, 1), (task.status, task.retryable_failures, task.lease_expiries))

        legacy = self.running(lease_policy=False) + [stopped(6, requested=False)]
        task = replay_events(record(legacy, plan(legacy))).tasks[1]
        self.assertEqual((1, 0), (task.retryable_failures, task.lease_expiries))

    def test_an_attempt_that_never_started_is_never_charged(self):
        # It cannot be why the worker stopped, whatever the reason was.
        events = self.running(begun=False) + [stopped(5, requested=False)]
        planned = plan(events)
        self.assertIs(True, planned["payload"]["released"])
        task = replay_events(record(events, planned)).tasks[1]
        self.assertEqual((0, 0), (task.retryable_failures, task.lease_expiries))

    def test_an_expiry_without_the_flag_is_charged_as_before(self):
        events = self.running() + [event(6, "task.assignment_expired", "pm", {
            "task_id": 1, "assignment_id": "task:1:attempt:1", "assignee": "alice",
            "worker_instance_id": "alice-1", "reason": "worker lease expired"})]
        self.assertEqual(1, replay_events(events).tasks[1].lease_expiries)


def topics(bus):
    return [item["topic"] for item in bus.published]


def stops(bus):
    return [item for item in bus.published if item["topic"] == "agent.stopped"]


class AnnouncementTests(RuntimeTestCase):
    def runtime(self, bus, executor=None, **kwargs):
        self.messages = []

        class Quick:
            def execute(self, assignment):
                return Completed("done")

        return WorkerRuntime(bus, name="alice", instance_id="alice-1",
                             executor=executor or Quick(), publish_retry_seconds=0.005,
                             log=self.messages.append, **kwargs)

    def test_a_requested_stop_is_announced_after_the_executor_has_returned(self):
        order = []
        running = threading.Event()

        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "agent.stopped":
                    order.append("announced")
                return super().publish(topic, payload, **kwargs)

        class Executor:
            def __init__(self):
                self.released = threading.Event()

            def execute(self, assignment):
                running.set()
                self.released.wait(5)
                time.sleep(0.05)  # cleaning up after being told to stop
                order.append("executor returned")
                return RetryableFailure("cancelled", "stopped")

            def cancel(self, assignment_id):
                self.released.set()

        bus = Bus()
        runtime = self.runtime(bus, Executor())

        def events():
            yield assigned_event()
            self.assertTrue(running.wait(2))
            runtime.stop()
            yield assigned_event(task_id=2, event_id=11)

        runtime.run(events())
        self.assertEqual(["executor returned", "announced"], order)
        announcement, = stops(bus)
        self.assertEqual({"name": "alice", "instance_id": "alice-1", "requested": True},
                         announcement["payload"])
        self.assertEqual("agent.stopped", topics(bus)[-1])
        self.assertEqual(0, runtime.exit_code)

    def test_running_out_of_a_finite_event_list_is_not_a_stop(self):
        bus = FakeBus()
        self.runtime(bus).run(iter([assigned_event()]))
        self.assertEqual(["agent.registered", "task.started", "task.completed"], topics(bus))

    def test_a_replaced_worker_does_not_announce_what_the_coordinator_already_knows(self):
        bus = FakeBus()
        runtime = self.runtime(bus)
        runtime.run(iter([{"id": 5, "topic": "agent.registered",
                           "payload": {"name": "alice", "instance_id": "alice-2"}},
                          assigned_event()]))
        self.assertTrue(runtime.stop_event.is_set())
        self.assertEqual([], stops(bus))
        self.assertEqual(0, runtime.exit_code)

    def test_a_worker_that_stops_itself_over_a_failure_says_it_was_not_asked_to(self):
        request = httpx.Request("POST", "http://bus/events")

        class RejectsStart(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "task.started":
                    raise httpx.HTTPStatusError(
                        "rejected", request=request,
                        response=httpx.Response(409, request=request))
                return super().publish(topic, payload, **kwargs)

        bus = RejectsStart()
        runtime = self.runtime(bus)
        runtime.run(iter([assigned_event()]))
        self.assertEqual(ERROR_EXIT_CODE, runtime.exit_code)
        announcement, = stops(bus)
        self.assertIs(False, announcement["payload"]["requested"])

    def test_no_announcement_is_attempted_when_the_bus_could_not_be_reached(self):
        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic in ("agent.heartbeat", "agent.stopped"):
                    raise httpx.ConnectError("offline")
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        runtime = self.runtime(bus, heartbeat_seconds=0.005, disconnect_timeout_seconds=0.03)

        def events():
            self.assertTrue(runtime.stop_event.wait(2))
            yield assigned_event()

        runtime.run(events())
        self.assertTrue(runtime.stopped_by_disconnect)
        self.assertFalse(any("announce" in line for line in self.messages))

    def test_an_announcement_that_fails_does_not_get_in_the_way_of_stopping(self):
        class Bus(FakeBus):
            def publish(self, topic, payload, **kwargs):
                if topic == "agent.stopped":
                    raise httpx.ConnectError("gone")
                return super().publish(topic, payload, **kwargs)

        bus = Bus()
        runtime = self.runtime(bus)

        def events():
            runtime.stop()
            yield assigned_event()

        runtime.run(events())
        self.assertEqual([], stops(bus))
        self.assertTrue(any("could not announce the stop (ConnectError)" in line
                            for line in self.messages))
        self.assertEqual(0, runtime.exit_code)

    def test_an_abandoned_attempt_is_announced_while_its_executor_is_still_stuck(self):
        release = threading.Event()

        class Stuck:
            def execute(self, assignment):
                release.wait(5)
                return Completed("far too late")

        bus = FakeBus()
        runtime = self.runtime(bus, Stuck(), attempt_timeout_seconds=0.05)

        def events():
            yield assigned_event()
            self.assertTrue(runtime.stop_event.wait(3))
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not stops(bus):
                time.sleep(0.005)
            # Announced before the executor let go, and after the failure.
            self.assertEqual(["task.attempt_failed", "agent.stopped"], topics(bus)[-2:])
            self.assertIs(False, stops(bus)[0]["payload"]["requested"])
            release.set()

        with patch.object(runtime_module, "ATTEMPT_CANCEL_GRACE_SECONDS", 0.05):
            try:
                runtime.run(events())
            finally:
                release.set()
        self.assertEqual(1, len(stops(bus)))  # shutdown did not announce a second time


@unittest.skipIf(os.name != "posix", "uses POSIX signals")
class SigtermTests(RuntimeTestCase):
    def setUp(self):
        super().setUp()
        if threading.current_thread() is not threading.main_thread():
            self.skipTest("signal handlers can only be set from the main thread")
        self.previous = signal.getsignal(signal.SIGTERM)
        self.addCleanup(signal.signal, signal.SIGTERM, self.previous)

    def runtime(self, bus):
        class Quick:
            def execute(self, assignment):
                return Completed("done")

        return WorkerRuntime(bus, name="alice", instance_id="alice-1", executor=Quick(),
                             log=lambda message: None)

    def test_sigterm_stops_the_worker_the_way_ctrl_c_does(self):
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        bus = FakeBus()
        runtime = self.runtime(bus)

        def events():
            yield assigned_event()
            # Never send it unless run() has taken the signal over: with the
            # default action it would end the test run.
            self.assertIsNot(signal.SIG_DFL, signal.getsignal(signal.SIGTERM))
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(5)
            self.fail("SIGTERM did not interrupt the worker")

        with self.assertRaises(KeyboardInterrupt):
            runtime.run(events())
        self.assertTrue(runtime.stop_event.is_set())
        announcement, = stops(bus)
        self.assertIs(True, announcement["payload"]["requested"])
        self.assertIs(signal.SIG_DFL, signal.getsignal(signal.SIGTERM))  # put back

    def test_an_existing_sigterm_handler_is_left_alone(self):
        def mine(signum, frame):
            pass

        signal.signal(signal.SIGTERM, mine)
        bus = FakeBus()
        runtime = self.runtime(bus)

        def events():
            self.assertIs(mine, signal.getsignal(signal.SIGTERM))
            yield assigned_event()

        runtime.run(events())
        self.assertIs(mine, signal.getsignal(signal.SIGTERM))


if __name__ == "__main__":
    unittest.main()
