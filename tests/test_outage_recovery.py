"""Lost workers have their own allowance, and a coordinator that was not
watching the log does not read old heartbeats as dead workers."""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import httpx

from logplane import bus
from logplane.client import BusClient
from logplane.event_validation import EventValidationError, validate_event
from logplane.operations import explain_task, task_view
from logplane.pm_agent import (
    MIN_OBSERVATION_GAP_SECONDS, OrderedProjectionCursor, plan_next_emission, reconcile,
)
from logplane.projection import PMState, replay_events
from tests.test_pm_agent import (
    FakeBus, assigned, attempt_failed, completed, created, event, expired, registered,
)

LONG_LEASE = 100_000


def with_lease_policy(created_event, max_expiries):
    created_event["payload"]["lease_policy"] = {"max_expiries": max_expiries}
    return created_event


def apply_plan(events, planned, *, ts=101.0):
    """Record a planned PM effect the way the bus would."""
    return events + [dict(planned, id=events[-1]["id"] + 1, actor="pm", ts=ts,
                          schema_version=2, correlation_id=None)]


def plan(events, now=101.0):
    return plan_next_emission(replay_events(events), now=now, lease_seconds=LONG_LEASE)


class LeasePolicyContractTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_new_tasks_get_a_materialized_lease_policy(self):
        made = bus.append_event("task.created", "human", {"title": "t"}, idempotency_key="a")
        self.assertEqual({"max_expiries": bus.DEFAULT_MAX_LEASE_EXPIRIES},
                         made["payload"]["lease_policy"])
        self.assertEqual(3, bus.DEFAULT_MAX_LEASE_EXPIRIES)
        # Omitting it on a retry of the same request is still the same request.
        again = bus.append_event("task.created", "human", {"title": "t"}, idempotency_key="a")
        self.assertEqual(made["id"], again["id"])
        explicit = bus.append_event("task.created", "human",
                                    {"title": "t", "lease_policy": {"max_expiries": 0}})
        self.assertEqual({"max_expiries": 0}, explicit["payload"]["lease_policy"])

    def test_lease_policy_shape_is_validated(self):
        for bad in ({}, {"max_expiries": -1}, {"max_expiries": "3"}, {"max_expiries": True},
                    {"max_expiries": 1, "extra": 1}, 3, None):
            with self.subTest(bad=bad), self.assertRaises(EventValidationError):
                validate_event("task.created", "human", {"title": "t", "lease_policy": bad},
                               None, None, 2)


class LeaseAllowanceTests(unittest.TestCase):
    def events(self, *, max_retries=0, max_expiries=1):
        return [registered(), with_lease_policy(created(max_retries=max_retries), max_expiries),
                assigned(3, 1), expired(4, 1)]

    def test_a_lost_worker_does_not_use_up_a_retry(self):
        events = self.events(max_retries=0, max_expiries=1)
        task = replay_events(events).tasks[1]
        self.assertEqual(("open", 0, 1), (task.status, task.retryable_failures, task.lease_expiries))
        planned = plan(events)
        self.assertEqual(("task.assigned", 2), (planned["topic"], planned["payload"]["attempt"]))

    def test_the_allowance_runs_out_and_fails_the_task_with_its_own_reason(self):
        events = self.events(max_retries=5, max_expiries=1) + [assigned(5, 2), expired(6, 2)]
        planned = plan(events)
        self.assertEqual(("task.failed", "lease_expiry_limit_exhausted"),
                         (planned["topic"], planned["payload"]["reason_code"]))
        self.assertIn("lost its worker 2 times", planned["payload"]["reason"])
        explained = explain_task(replay_events(events), 1, now=101.0, lease_seconds=LONG_LEASE)
        self.assertEqual("retry_exhaustion_reconciliation_pending", explained["code"])
        self.assertIn("lost its worker", explained["summary"])
        failed = apply_plan(events, planned)
        task = replay_events(failed).tasks[1]
        self.assertEqual(("failed", "lease_expiry_limit_exhausted"),
                         (task.status, task.terminal_failure_code))
        self.assertIsNone(plan(failed))

    def test_a_zero_allowance_fails_on_the_first_lost_worker(self):
        planned = plan(self.events(max_retries=5, max_expiries=0))
        self.assertEqual("lease_expiry_limit_exhausted", planned["payload"]["reason_code"])

    def test_real_failures_still_use_the_retry_budget(self):
        events = [registered(), with_lease_policy(created(max_retries=0), 3), assigned(3, 1),
                  attempt_failed(4, 1, retryable=True)]
        task = replay_events(events).tasks[1]
        self.assertEqual((1, 0), (task.retryable_failures, task.lease_expiries))
        self.assertEqual("retry_budget_exhausted", plan(events)["payload"]["reason_code"])

    def test_tasks_without_a_lease_policy_keep_the_old_rule(self):
        events = [registered(), created(max_retries=0), assigned(3, 1), expired(4, 1)]
        task = replay_events(events).tasks[1]
        self.assertEqual((1, 0, None),
                         (task.retryable_failures, task.lease_expiries, task.max_lease_expiries))
        self.assertEqual("retry_budget_exhausted", plan(events)["payload"]["reason_code"])

    def test_a_human_retry_reopens_a_task_that_lost_too_many_workers(self):
        events = self.events(max_retries=0, max_expiries=0)
        failed = apply_plan(events, plan(events))
        retry = event(failed[-1]["id"] + 1, "task.retry_requested", "human",
                      {"task_id": 1, "reason": "worker fixed", "additional_retries": 1},
                      caused_by=failed[-1]["id"])
        reopened = failed + [retry]
        task = replay_events(reopened).tasks[1]
        self.assertEqual(("open", 1, 1, 1),
                         (task.status, task.lease_expiries, task.retry_grant, task.retry_grant_base))
        self.assertEqual("task.assigned", plan(reopened)["topic"])


class RetryGrantTests(unittest.TestCase):
    """A human retry grants N further assignments in total, whatever ends each."""

    def failed_task(self, *, policy=True):
        first = created(max_retries=0)
        if policy:
            with_lease_policy(first, 0)
        events = [registered(), first, assigned(3, 1), attempt_failed(4, 1, retryable=True)]
        return apply_plan(events, plan(events))

    def grant(self, events, additional):
        return events + [event(events[-1]["id"] + 1, "task.retry_requested", "human",
                               {"task_id": 1, "reason": "try again", "additional_retries": additional},
                               caused_by=events[-1]["id"])]

    def run_attempts(self, events, endings):
        """Assign and end attempts in turn; return the attempts assigned and the last plan."""
        attempts = []
        for ending in endings:
            planned = plan(events)
            if planned["topic"] != "task.assigned":
                return attempts, planned, events
            attempt = planned["payload"]["attempt"]
            attempts.append(attempt)
            next_id = events[-1]["id"] + 1
            events = events + [assigned(next_id, attempt), ending(next_id + 1, attempt)]
        return attempts, plan(events), events

    def retryable(self, event_id, attempt):
        return attempt_failed(event_id, attempt, retryable=True)

    def test_a_grant_of_two_allows_exactly_two_assignments_however_they_end(self):
        for label, endings in (
            ("failure then lost worker", (self.retryable, expired, self.retryable)),
            ("lost worker then failure", (expired, self.retryable, expired)),
            ("two failures", (self.retryable, self.retryable, self.retryable)),
            ("two lost workers", (expired, expired, expired)),
        ):
            with self.subTest(label):
                attempts, final, events = self.run_attempts(
                    self.grant(self.failed_task(), 2), endings)
                self.assertEqual([2, 3], attempts)
                self.assertEqual(("task.failed", "retry_budget_exhausted"),
                                 (final["topic"], final["payload"]["reason_code"]))
                self.assertIn("2 further attempt(s) granted", final["payload"]["reason"])
                task = replay_events(apply_plan(events, final)).tasks[1]
                self.assertEqual("failed", task.status)

    def test_a_grant_of_one_allows_one_assignment(self):
        attempts, final, _ = self.run_attempts(self.grant(self.failed_task(), 1),
                                               (expired, expired))
        self.assertEqual([2], attempts)
        self.assertEqual("task.failed", final["topic"])

    def test_a_task_without_a_lease_policy_behaves_as_it_did_before(self):
        attempts, final, events = self.run_attempts(
            self.grant(self.failed_task(policy=False), 2), (expired, self.retryable, expired))
        self.assertEqual([2, 3], attempts)
        self.assertEqual("retry_budget_exhausted", final["payload"]["reason_code"])
        # The recorded numbers are the ones earlier releases wrote.
        self.assertEqual((3, 2), (final["payload"]["retryable_failures"],
                                  final["payload"]["max_retries"]))

    def test_a_second_grant_starts_a_fresh_count(self):
        _, final, events = self.run_attempts(self.grant(self.failed_task(), 1), (expired, expired))
        again = self.grant(apply_plan(events, final), 2)
        attempts, final, _ = self.run_attempts(again, (expired, self.retryable, expired))
        self.assertEqual([3, 4], attempts)
        self.assertEqual("task.failed", final["topic"])

    def test_a_granted_attempt_can_still_complete(self):
        attempts, final, events = self.run_attempts(self.grant(self.failed_task(), 2),
                                                    (expired, completed))
        self.assertEqual([2, 3], attempts)
        self.assertIsNone(final)
        self.assertEqual("completed", replay_events(events).tasks[1].status)

    def test_the_views_count_what_is_left_of_a_grant(self):
        events = self.grant(self.failed_task(), 2)
        view = task_view(replay_events(events), 1, now=101.0, lease_seconds=LONG_LEASE)
        self.assertEqual(({"attempts": 2, "used": 0}, 1, False),
                         (view["retry_grant"], view["retry_policy"]["remaining"],
                          view["lease_policy"]["in_force"]))
        _, _, events = self.run_attempts(events, (expired,))
        view = task_view(replay_events(events), 1, now=101.0, lease_seconds=LONG_LEASE)
        self.assertEqual(({"attempts": 2, "used": 1}, 0),
                         (view["retry_grant"], view["retry_policy"]["remaining"]))
        fresh = [registered(), with_lease_policy(created(max_retries=1), 3)]
        view = task_view(replay_events(fresh), 1, now=101.0, lease_seconds=LONG_LEASE)
        self.assertEqual((None, True), (view["retry_grant"], view["lease_policy"]["in_force"]))

    def test_a_malformed_lease_policy_in_history_does_not_poison_replay(self):
        bad = created(max_retries=0)
        bad["payload"]["lease_policy"] = {"max_expiries": "many"}
        state = replay_events([registered(), bad, created(3, task_id=2)])
        self.assertEqual([2], sorted(state.tasks))


class LogBus(FakeBus):
    def __init__(self, starting_id, ts):
        super().__init__(starting_id, ts=ts)
        self.unread = []

    def heartbeat(self, ts, name="alice"):
        self.unread.append(event(self.next_id, "agent.heartbeat", name,
                                 {"name": name, "instance_id": f"{name}-1"}, ts=ts))
        self.next_id += 1

    def query_all(self, after_id=0, topics=None):
        log = sorted(self.unread + self.events, key=lambda item: item["id"])
        return [item for item in log if item["id"] > after_id]


class LeaseGraceTests(unittest.TestCase):
    """alice last heartbeated at 100 and holds task 1; the lease is 20 seconds."""

    def start(self):
        cursor = OrderedProjectionCursor(PMState())
        cursor.consume([registered(), created(max_retries=2), assigned(3, 1)])
        return cursor, LogBus(starting_id=4, ts=0.0)

    def run_at(self, cursor, log, now, lease=20):
        log.ts = now
        cursor.catch_up(log)
        emitted = reconcile(cursor.state, log, now=now, lease_seconds=lease, cursor=cursor)
        return [item["topic"] for item in emitted]

    def test_the_planner_can_hold_expiry_for_a_stale_heartbeat_only(self):
        state = replay_events([registered(), created(max_retries=2), assigned(3, 1)])
        self.assertEqual("task.assignment_expired",
                         plan_next_emission(state, now=200.0, lease_seconds=20)["topic"])
        self.assertIsNone(plan_next_emission(state, now=200.0, lease_seconds=20,
                                             hold_lease_expiry=True))
        # A replaced worker is evidence in the log, not silence: it still expires.
        replaced = replay_events([registered(), created(max_retries=2), assigned(3, 1),
                                  registered(4, instance="alice-2", ts=199.0)])
        held = plan_next_emission(replaced, now=200.0, lease_seconds=20, hold_lease_expiry=True)
        self.assertEqual(("task.assignment_expired", "worker process was replaced"),
                         (held["topic"], held["payload"]["reason"]))

    def test_after_a_gap_workers_get_one_lease_to_be_heard_from(self):
        cursor, log = self.start()
        self.assertEqual([], self.run_at(cursor, log, 105.0))
        # The laptop slept for a minute: the coordinator saw nothing, sent or not.
        self.assertEqual([], self.run_at(cursor, log, 165.0))
        self.assertEqual(185.0, cursor.lease_grace_until)
        log.heartbeat(167.0)
        for now in (170.0, 175.0, 180.0, 185.0, 186.0):
            with self.subTest(now=now):
                self.assertEqual([], self.run_at(cursor, log, now))
        self.assertEqual("assigned", cursor.state.tasks[1].status)

    def test_a_worker_that_stays_silent_is_expired_once_the_grace_ends(self):
        cursor, log = self.start()
        self.run_at(cursor, log, 105.0)
        self.assertEqual([], self.run_at(cursor, log, 165.0))
        # Keepalive-paced looks (15 seconds apart) must not renew the grace.
        self.assertEqual([], self.run_at(cursor, log, 180.0))
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 195.0))

    def test_startup_begins_with_a_grace_period(self):
        cursor, log = self.start()
        cursor.begin_lease_grace(165.0, 20)
        self.assertEqual([], self.run_at(cursor, log, 166.0))
        self.assertEqual([], self.run_at(cursor, log, 181.0))
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 196.0))

    def test_without_a_gap_a_stale_lease_expires_as_before(self):
        cursor, log = self.start()
        for now in (105.0, 110.0, 115.0, 120.0):
            self.assertEqual([], self.run_at(cursor, log, now))
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 125.0))

    def test_a_short_lease_is_not_held_forever_by_keepalive_paced_looks(self):
        self.assertGreater(MIN_OBSERVATION_GAP_SECONDS, 15)
        cursor, log = self.start()
        self.assertEqual([], self.run_at(cursor, log, 102.0, lease=5))
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 117.0, lease=5))

    def test_a_suspension_in_the_middle_of_a_pass_still_starts_the_grace(self):
        cursor = OrderedProjectionCursor(PMState())
        cursor.consume([registered(), registered(2, name="bob", instance="bob-1"),
                        created(3, max_retries=2), assigned(4, 1), created(5, task_id=2)])
        log = LogBus(starting_id=6, ts=105.0)
        cursor.observe(100.0, 20)
        # The first step of the pass reads 105 and assigns task 2; the machine is
        # then suspended, and the next step of the same pass reads 165.
        readings = iter([105.0] + [165.0] * 20)
        with redirect_stdout(io.StringIO()):
            emitted = reconcile(cursor.state, log, lease_seconds=20, cursor=cursor,
                                clock=lambda: next(readings))
        self.assertEqual(["task.assigned"], [item["topic"] for item in emitted])
        self.assertEqual(185.0, cursor.lease_grace_until)
        self.assertEqual("assigned", cursor.state.tasks[1].status)

    def test_a_suspension_during_the_final_read_before_expiry_drops_the_plan(self):
        cursor, log = self.start()
        clock = {"now": 121.0}
        cursor.observe(119.0, 20)

        class SlowRead(LogBus):
            def query_all(self, after_id=0, topics=None):
                # Nothing new to read, but the machine sleeps while reading.
                clock["now"] = 181.0
                return super().query_all(after_id, topics)

        slow = SlowRead(starting_id=4, ts=181.0)
        with redirect_stdout(io.StringIO()):
            emitted = reconcile(cursor.state, slow, lease_seconds=20, cursor=cursor,
                                clock=lambda: clock["now"])
        self.assertEqual([], emitted)   # 21s stale at 121, but not expired at 181
        self.assertEqual(201.0, cursor.lease_grace_until)
        self.assertEqual("assigned", cursor.state.tasks[1].status)
        # Still silent once the grace ends: now it is expired.
        clock["now"] = 190.0
        self.assertEqual([], reconcile(cursor.state, log, lease_seconds=20, cursor=cursor,
                                       clock=lambda: clock["now"]))
        clock["now"] = 201.0
        log.ts = 201.0
        topics = [item["topic"] for item in reconcile(
            cursor.state, log, lease_seconds=20, cursor=cursor, clock=lambda: clock["now"])]
        self.assertIn("task.assignment_expired", topics)

    def test_an_ordinary_empty_read_before_expiry_still_expires(self):
        cursor, log = self.start()
        clock = {"now": 119.0}
        reconcile(cursor.state, log, lease_seconds=20, cursor=cursor, clock=lambda: clock["now"])
        clock["now"] = 121.0
        log.ts = 121.0
        topics = [item["topic"] for item in reconcile(
            cursor.state, log, lease_seconds=20, cursor=cursor, clock=lambda: clock["now"])]
        self.assertIn("task.assignment_expired", topics)

    def test_looks_that_each_follow_a_long_break_keep_renewing_the_grace(self):
        # A documented limit, not a guarantee: with every look more than a lease
        # after the last, a silent worker is never judged.
        cursor, log = self.start()
        for now in (110.0, 151.0, 192.0, 233.0):
            with self.subTest(now=now), redirect_stdout(io.StringIO()):
                cursor.reconnected(now, 20)
                self.assertEqual([], self.run_at(cursor, log, now))
        self.assertEqual("assigned", cursor.state.tasks[1].status)

    def test_a_reconnect_starts_the_grace_however_short_the_outage(self):
        # Heartbeat at 100; the stream drops and is back within the lease.
        cursor, log = self.start()
        self.assertEqual([], self.run_at(cursor, log, 104.0))
        self.assertTrue(cursor.reconnected(119.0, 20))
        self.assertEqual([], self.run_at(cursor, log, 119.0))
        self.assertEqual([], self.run_at(cursor, log, 125.0))   # 25s old, but held
        self.assertEqual([], self.run_at(cursor, log, 138.0))
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 139.0))

    def test_without_the_reconnect_signal_the_same_timing_expires(self):
        cursor, log = self.start()
        self.run_at(cursor, log, 104.0)
        self.run_at(cursor, log, 119.0)
        self.assertIn("task.assignment_expired", self.run_at(cursor, log, 125.0))

    def test_a_stream_that_keeps_dropping_cannot_hold_expiry_forever(self):
        cursor = OrderedProjectionCursor(PMState())
        self.assertTrue(cursor.reconnected(100.0, 20))
        self.assertEqual(120.0, cursor.lease_grace_until)
        for now in (105.0, 119.0, 120.0, 139.9):   # in force, then cooling down
            with self.subTest(now=now):
                self.assertFalse(cursor.reconnected(now, 20))
                self.assertEqual(120.0, cursor.lease_grace_until)
        self.assertTrue(cursor.reconnected(140.0, 20))
        self.assertEqual(160.0, cursor.lease_grace_until)
        # A grace begun at start-up does not use up the reconnect's.
        cursor = OrderedProjectionCursor(PMState())
        cursor.begin_lease_grace(100.0, 20)
        self.assertTrue(cursor.reconnected(112.0, 20))
        self.assertEqual(132.0, cursor.lease_grace_until)
        self.assertFalse(cursor.reconnected(118.0, 20))
        # Flapping every 5 seconds from 100 to 300: expiry is possible half the time.
        cursor = OrderedProjectionCursor(PMState())
        open_seconds = 0
        for now in range(100, 300, 5):
            cursor.reconnected(float(now), 20)
            open_seconds += 5 if now >= cursor.lease_grace_until else 0
        self.assertGreaterEqual(open_seconds, 80)

    def test_observe_reports_only_gaps_longer_than_a_lease(self):
        cursor = OrderedProjectionCursor(PMState())
        self.assertIsNone(cursor.observe(100.0, 60))
        self.assertIsNone(cursor.observe(160.0, 60))
        self.assertEqual(61.0, cursor.observe(221.0, 60))
        self.assertEqual(281.0, cursor.lease_grace_until)
        self.assertIsNone(cursor.observe(200.0, 60))  # a clock stepping back is not a gap



class SubscribeReconnectTests(unittest.TestCase):
    def subscribe(self, streams, **options):
        calls, attempts = [], iter(streams)

        def stream_once(topics, from_id, on_idle, correlation_id, stop_event):
            for step in next(attempts):
                if step == "idle":
                    on_idle()
                elif step == "drop":
                    raise httpx.ConnectError("bus went away")
                else:
                    yield {"id": step, "topic": "x", "payload": {}}

        with tempfile.TemporaryDirectory() as directory:
            client = BusClient("http://127.0.0.1:8765", actor="pm", offset_dir=Path(directory))
            with patch.object(client, "_stream_once", stream_once), \
                    patch("logplane.client.time.sleep"), redirect_stdout(io.StringIO()):
                events = client.subscribe(on_reconnect=lambda: calls.append("reconnect"),
                                          **options)
                for _ in range(sum(1 for stream in streams for step in stream
                                   if isinstance(step, int))):
                    calls.append(next(events)["id"])
        return calls

    def test_on_reconnect_runs_before_the_first_event_of_each_later_stream(self):
        self.assertEqual([1, 2, "reconnect", 3, 4, "reconnect", 5],
                         self.subscribe([[1, 2, "drop"], [3, 4, "drop"], [5]]))

    def test_the_first_stream_is_not_a_reconnect(self):
        self.assertEqual([1], self.subscribe([[1]]))

    def test_on_reconnect_runs_before_a_keepalive_that_arrives_first(self):
        calls = []
        result = self.subscribe([[1, "drop"], ["idle", "idle", 2]],
                                on_idle=lambda: calls.append("idle"))
        self.assertEqual([1, "reconnect", 2], result)
        self.assertEqual(["idle", "idle"], calls)

    def test_an_empty_stream_followed_by_another_counts_once(self):
        self.assertEqual([1, "reconnect", 2], self.subscribe([[1, "drop"], [], [2]]))


if __name__ == "__main__":
    unittest.main()
