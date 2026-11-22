"""A retryable failure can ask for a wait before the next attempt."""

import json
import os
import time
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from examples.claude_code.claude_code_executor import (
    USAGE_LIMIT_DEFAULT_WAIT, parse_result, usage_limit_wait,
)
from examples.codex.codex_executor import codex_usage_limit_wait, parse_events
from logplane import HttpAgentConfig
from logplane.event_validation import EventValidationError, validate_event
from logplane.executors import PermanentFailure, RetryableFailure, outcome_from_dict
from logplane.limits import MAX_RETRY_AFTER_SECONDS
from logplane.operations import explain_task, task_view
from logplane.pm_agent import OrderedProjectionCursor, plan_next_emission, reconcile
from logplane.projection import PMState, replay_events
from tests.test_pm_agent import FakeBus, assigned, attempt_failed, created, event, registered
from tests.test_runtime import RuntimeOutcomeTests
from tests.test_v09_integration_kit import assignment


def delayed_failure(event_id, attempt, delay, *, ts=100.0, retryable=True):
    failure = attempt_failed(event_id, attempt, retryable=retryable)
    failure["ts"] = ts
    if delay is not None:
        failure["payload"]["retry_after_seconds"] = delay
    return failure


def history(delay, *, max_retries=2):
    return [registered(), created(max_retries=max_retries), assigned(3, 1),
            delayed_failure(4, 1, delay)]


class ValidationTests(unittest.TestCase):
    def validate(self, **changes):
        payload = {"task_id": 1, "assignment_id": "task:1:attempt:1", "worker_instance_id": "w",
                   "failure_code": "limit", "reason": "wait", "retryable": True}
        payload.update(changes)
        validate_event("task.attempt_failed", "alice", payload, 3, None, 2)

    def test_delay_is_optional_positive_bounded_and_only_for_retryable_failures(self):
        self.validate()
        self.validate(retry_after_seconds=1)
        self.validate(retry_after_seconds=2400.5)
        self.validate(retry_after_seconds=MAX_RETRY_AFTER_SECONDS)
        for bad in (0, -1, "60", True, None, float("inf"), MAX_RETRY_AFTER_SECONDS + 1):
            with self.subTest(bad=bad), self.assertRaises(EventValidationError):
                self.validate(retry_after_seconds=bad)
        with self.assertRaisesRegex(EventValidationError, "only to a retryable failure"):
            self.validate(retryable=False, retry_after_seconds=60)


def plan(state, now):
    # A long lease keeps the one registered worker healthy across the wait.
    return plan_next_emission(state, now=now, lease_seconds=100_000)


class ProjectionTests(unittest.TestCase):
    def test_the_task_is_not_reassigned_until_the_delay_has_passed(self):
        state = replay_events(history(600))
        task = state.tasks[1]
        self.assertEqual(("open", 700.0, 1), (task.status, task.retry_not_before,
                                               task.retryable_failures))
        self.assertIsNone(plan(state, 101.0))
        self.assertIsNone(plan(state, 699.9))
        planned = plan(state, 700.0)
        self.assertEqual(("task.assigned", 2),
                         (planned["topic"], planned["payload"]["attempt"]))

    def test_replay_refuses_an_assignment_stamped_before_the_delay_has_passed(self):
        early = assigned(5, 2)
        early["ts"] = 699.9
        state = replay_events(history(600) + [early])
        task = state.tasks[1]
        # Recorded, but not ownership: the task still waits, and the attempt
        # number is consumed so the next plan gets a fresh assignment id.
        self.assertEqual(("open", 700.0, None, 2),
                         (task.status, task.retry_not_before, task.assignment_id,
                          task.last_assignment_attempt))
        self.assertIsNone(plan(state, 699.95))
        planned = plan(state, 700.0)
        self.assertEqual(("task.assigned", 3, "task:1:attempt:3"),
                         (planned["topic"], planned["payload"]["attempt"],
                          planned["payload"]["assignment_id"]))
        on_time = assigned(6, 3)
        on_time["ts"] = 700.0
        state = replay_events(history(600) + [early, on_time])
        self.assertEqual(("assigned", "task:1:attempt:3", None),
                         (state.tasks[1].status, state.tasks[1].assignment_id,
                          state.tasks[1].retry_not_before))

    def test_without_a_delay_the_retry_is_immediate_as_before(self):
        state = replay_events(history(None))
        self.assertIsNone(state.tasks[1].retry_not_before)
        self.assertEqual("task.assigned", plan(state, 101.0)["topic"])

    def test_an_exhausted_retry_budget_fails_at_once_despite_a_delay(self):
        state = replay_events(history(600, max_retries=0))
        planned = plan(state, 101.0)
        self.assertEqual(("task.failed", "retry_budget_exhausted"),
                         (planned["topic"], planned["payload"]["reason_code"]))

    def test_the_next_assignment_clears_the_delay(self):
        events = history(600)
        second = assigned(5, 2)
        second["ts"] = 700.0
        state = replay_events(events + [second])
        self.assertEqual(("assigned", None),
                         (state.tasks[1].status, state.tasks[1].retry_not_before))
        # A later failure without a delay retries at once.
        state = replay_events(events + [second, delayed_failure(6, 2, None, ts=710.0)])
        self.assertEqual("task.assigned", plan(state, 711.0)["topic"])

    def test_a_malformed_or_oversized_delay_in_history_is_ignored_not_fatal(self):
        for bad in ("soon", -5, True, MAX_RETRY_AFTER_SECONDS * 2):
            with self.subTest(bad=bad):
                state = replay_events(history(bad))
                self.assertEqual(("open", None, 1),
                                 (state.tasks[1].status, state.tasks[1].retry_not_before,
                                  state.tasks[1].retryable_failures))

    def test_a_deadline_still_ends_a_waiting_task(self):
        events = [registered(), created(max_retries=2, deadline_at=400.0), assigned(3, 1),
                  delayed_failure(4, 1, 600)]
        events[2]["payload"]["deadline_at"] = 400.0
        state = replay_events(events)
        self.assertIsNone(plan(state, 399.0))
        self.assertEqual("task.deadline_exceeded", plan(state, 400.0)["topic"])

    def test_the_wait_is_explained(self):
        state = replay_events(history(600))
        explained = explain_task(state, 1, now=160.0, lease_seconds=20)
        self.assertEqual("retry_delay_pending", explained["code"])
        self.assertIn("about 9 minutes", explained["summary"])
        self.assertIn("tool_timeout", explained["summary"])
        self.assertEqual(540.0, explained["details"]["seconds_remaining"])
        self.assertEqual([4], explained["event_ids"])
        self.assertEqual(700.0, task_view(state, 1, now=160.0, lease_seconds=20)["retry_not_before"])
        after = explain_task(state, 1, now=701.0, lease_seconds=20)
        self.assertNotEqual("retry_delay_pending", after["code"])


class ClockBus(FakeBus):
    """A bus that stamps events with its own clock (``ts``), not the PM's."""

    def __init__(self, starting_id, ts):
        super().__init__(starting_id, ts=ts)
        self.unread = []

    def heartbeat(self, ts):
        self.unread.append(event(self.next_id, "agent.heartbeat", "bob",
                                 {"name": "bob", "instance_id": "bob-1"}, ts=ts))
        self.next_id += 1

    def query_all(self, after_id=0, topics=None):
        log = sorted(self.unread + self.events, key=lambda item: item["id"])
        return [item for item in log if item["id"] > after_id]


class FastCoordinatorClockTests(unittest.TestCase):
    """The PM's clock runs ahead of the bus's; the retry is due at bus time 700."""

    def events(self):
        return [registered(), registered(2, name="bob", instance="bob-1"),
                created(3, max_retries=2), assigned(4, 1), delayed_failure(5, 1, 600),
                created(6, task_id=2)]

    def task_one_assignments(self, bus):
        return [item["payload"]["attempt"] for item in bus.events
                if item["topic"] == "task.assigned" and item["payload"]["task_id"] == 1]

    def test_one_early_refusal_then_the_task_is_held_until_the_bus_clock_catches_up(self):
        cursor = OrderedProjectionCursor(PMState())
        cursor.consume(self.events())
        state, bus = cursor.state, ClockBus(starting_id=7, ts=699.9)
        options = dict(lease_seconds=100_000, cursor=cursor)

        reconcile(state, bus, now=700.5, **options)
        self.assertEqual([2], self.task_one_assignments(bus))  # refused once, not in a loop
        self.assertEqual(("open", "assigned"), (state.tasks[1].status, state.tasks[2].status))
        self.assertEqual({1: 700.0}, cursor.retry_holds)

        reconcile(state, bus, now=700.6, **options)
        bus.heartbeat(699.95)
        cursor.catch_up(bus)
        reconcile(state, bus, now=700.7, **options)
        self.assertEqual([2], self.task_one_assignments(bus))

        bus.ts = 700.0
        bus.heartbeat(700.0)
        cursor.catch_up(bus)
        reconcile(state, bus, now=700.8, **options)
        self.assertEqual([2, 3], self.task_one_assignments(bus))
        self.assertEqual(("assigned", "task:1:attempt:3"),
                         (state.tasks[1].status, state.tasks[1].assignment_id))
        self.assertEqual({}, cursor.retry_holds)

    def test_without_a_cursor_reconcile_still_stops_after_one_refusal(self):
        state = replay_events(self.events())
        bus = FakeBus(starting_id=7, ts=699.9)
        emitted = reconcile(state, bus, now=700.5, lease_seconds=100_000)
        self.assertEqual([2], self.task_one_assignments(bus))
        self.assertEqual("assigned", state.tasks[2].status)
        self.assertLess(len(emitted), 5)

    def test_matching_clocks_assign_on_time_with_no_refusal(self):
        cursor = OrderedProjectionCursor(PMState())
        cursor.consume(self.events())
        bus = ClockBus(starting_id=7, ts=700.0)
        reconcile(cursor.state, bus, now=700.0, lease_seconds=100_000, cursor=cursor)
        self.assertEqual([2], self.task_one_assignments(bus))
        self.assertEqual("assigned", cursor.state.tasks[1].status)
        self.assertEqual({}, cursor.retry_holds)


class OutcomeTests(unittest.TestCase):
    def test_retryable_failure_validates_its_delay(self):
        self.assertIsNone(RetryableFailure("c", "r").retry_after_seconds)
        self.assertEqual(90, RetryableFailure("c", "r", retry_after_seconds=90).retry_after_seconds)
        for bad in (0, -1, "60", True, float("nan"), MAX_RETRY_AFTER_SECONDS + 1):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                RetryableFailure("c", "r", retry_after_seconds=bad)

    def test_cli_and_http_agents_can_report_a_delay(self):
        outcome = outcome_from_dict({"status": "retryable_failure", "code": "limit",
                                     "reason": "wait", "retry_after_seconds": 120})
        self.assertEqual(120, outcome.retry_after_seconds)
        self.assertIsNone(outcome_from_dict({"status": "retryable_failure", "code": "c",
                                             "reason": "r"}).retry_after_seconds)

    def test_http_adapter_honours_retry_after_in_seconds(self):
        def adapter(status, headers):
            return HttpAgentConfig("http://127.0.0.1:9000/execute").build(client=httpx.Client(
                transport=httpx.MockTransport(lambda request: httpx.Response(status, headers=headers))))

        for status, headers, expected in (
            (429, {"Retry-After": "120"}, 120.0),
            (503, {"Retry-After": "7"}, 7.0),
            (503, {}, None),
            (429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, None),
            (429, {"Retry-After": "-3"}, None),
            (429, {"Retry-After": str(MAX_RETRY_AFTER_SECONDS * 3)}, float(MAX_RETRY_AFTER_SECONDS)),
        ):
            with self.subTest(status=status, headers=headers):
                outcome = adapter(status, headers).execute(assignment())
                self.assertIsInstance(outcome, RetryableFailure)
                self.assertEqual(expected, outcome.retry_after_seconds)


class RuntimeTests(RuntimeOutcomeTests):
    def failure_payload(self, outcome):
        bus = self.run_outcome(outcome)
        return next(e["payload"] for e in bus.published if e["topic"] == "task.attempt_failed")

    def test_the_worker_publishes_the_delay_only_when_one_is_given(self):
        payload = self.failure_payload(RetryableFailure("limit", "wait", retry_after_seconds=300))
        self.assertEqual((True, 300), (payload["retryable"], payload["retry_after_seconds"]))
        self.assertNotIn("retry_after_seconds", self.failure_payload(RetryableFailure("t", "r")))
        self.assertNotIn("retry_after_seconds", self.failure_payload(PermanentFailure("p", "r")))


BRISBANE = ZoneInfo("Australia/Brisbane")
NOW = datetime(2026, 11, 22, 14, 10, tzinfo=BRISBANE).timestamp()


class UsageLimitTests(unittest.TestCase):
    def test_claude_limit_messages_give_the_wait_until_reset(self):
        for message, expected in (
            ("You've hit your 5-hour limit · resets 2:50pm (Australia/Brisbane)", 40 * 60 + 60),
            ("You've hit your 5-hour limit · resets 2am (Australia/Brisbane)", (11 * 60 + 50) * 60 + 60),
            ("You've hit your weekly limit · resets Nov 25, 9am (Australia/Brisbane)",
             (2 * 24 * 60 + 18 * 60 + 50) * 60 + 60),
            ("Claude usage limit reached. Your limit will reset at 3pm (UTC).",
             (10 * 60 + 50) * 60 + 60),
            ("You've hit your limit", USAGE_LIMIT_DEFAULT_WAIT),
            ("You've hit your limit · resets 25:99pm", USAGE_LIMIT_DEFAULT_WAIT),
        ):
            with self.subTest(message=message):
                self.assertEqual(expected, usage_limit_wait(message, NOW))
        for message in ("Invalid API key · Please run /login", "", "context limit exceeded",
                        "resets 2:50pm"):
            with self.subTest(message=message):
                self.assertIsNone(usage_limit_wait(message, NOW))

    def test_a_reset_time_that_has_just_passed_means_tomorrow(self):
        wait = usage_limit_wait("You've hit your limit · resets 2:10pm (Australia/Brisbane)", NOW)
        self.assertEqual(24 * 3600 + 60, wait)

    def test_the_wait_is_elapsed_time_across_a_daylight_saving_change(self):
        new_york = ZoneInfo("America/New_York")
        message = "You've hit your limit · resets 3:30am (America/New_York)"
        for day, hours in (((2026, 3, 8), 2), ((2026, 11, 1), 4), ((2026, 6, 1), 3)):
            with self.subTest(day=day):
                now = datetime(*day, 0, 30, tzinfo=new_york).timestamp()
                self.assertEqual(hours * 3600 + 60, usage_limit_wait(message, now))

    def test_a_message_without_a_zone_uses_the_hosts_rules_for_the_reset_date(self):
        previous = os.environ.get("TZ")
        os.environ["TZ"] = "America/New_York"
        time.tzset()
        try:
            new_york = ZoneInfo("America/New_York")
            message = "You've hit your limit · resets 3:30am"
            for day, hours in (((2026, 3, 8), 2), ((2026, 11, 1), 4), ((2026, 6, 1), 3)):
                with self.subTest(day=day):
                    now = datetime(*day, 0, 30, tzinfo=new_york).timestamp()
                    self.assertEqual(hours * 3600 + 60, usage_limit_wait(message, now))
        finally:
            if previous is None:
                del os.environ["TZ"]
            else:
                os.environ["TZ"] = previous
            time.tzset()

    def test_claude_code_reports_a_limit_as_a_retryable_failure_with_a_wait(self):
        def stream(result):
            init = {"type": "system", "subtype": "init", "apiKeySource": "none", "tools": []}
            final = {"type": "result", "subtype": "success", "is_error": True, "result": result}
            return (json.dumps(init) + "\n" + json.dumps(final) + "\n").encode()

        outcome, report = parse_result(
            1, stream("You've hit your 5-hour limit · resets 2:50pm (Australia/Brisbane)"),
            expected_key_source="none")
        self.assertIsInstance(outcome, RetryableFailure)
        self.assertEqual("claude_usage_limit", outcome.code)
        self.assertTrue(0 < outcome.retry_after_seconds <= 24 * 3600 + 60)
        self.assertEqual(round(outcome.retry_after_seconds), report["usage_limit_wait_seconds"])
        other, _ = parse_result(1, stream("Invalid API key"), expected_key_source="none")
        self.assertEqual("claude_failed", other.code)
        self.assertIsInstance(other, PermanentFailure)

    def test_codex_limit_messages(self):
        self.assertEqual(3 * 3600 + 12 * 60 + 60, codex_usage_limit_wait(
            "You've hit your usage limit. Upgrade to Pro or try again in 3 hours 12 minutes.", NOW))
        self.assertEqual(2 * 86400 + 5 * 60 + 60, codex_usage_limit_wait(
            "You've hit your usage limit. Try again in 2 days 5 minutes.", NOW))
        self.assertEqual(USAGE_LIMIT_DEFAULT_WAIT, codex_usage_limit_wait("usage limit reached", NOW))
        self.assertIsNone(codex_usage_limit_wait("stream disconnected", NOW))

        def events(message):
            return (json.dumps({"type": "turn.failed", "error": {"message": message}}) + "\n").encode()

        outcome, report = parse_events(1, events("You've hit your usage limit. Try again in 45 minutes."))
        self.assertIsInstance(outcome, RetryableFailure)
        self.assertEqual(("codex_usage_limit", 45 * 60 + 60.0),
                         (outcome.code, outcome.retry_after_seconds))
        self.assertEqual(45 * 60 + 60, report["usage_limit_wait_seconds"])
        refused, _ = parse_events(1, events("This content was flagged for possible cybersecurity risk."))
        self.assertEqual("codex_safety_refusal", refused.code)
        plain, _ = parse_events(1, events("model overloaded"))
        self.assertEqual("codex_failed", plain.code)

    def test_codex_classifies_from_stderr_when_no_event_was_printed(self):
        # Parser checks with made-up output, not messages seen from the provider.
        limited, _ = parse_events(1, b"", "Error: You've hit your usage limit. Try again in 45 minutes.")
        self.assertIsInstance(limited, RetryableFailure)
        self.assertEqual(("codex_usage_limit", 45 * 60 + 60.0),
                         (limited.code, limited.retry_after_seconds))
        refused, _ = parse_events(
            1, b"", "This content was flagged for possible cybersecurity risk; usage limit unaffected.")
        self.assertIsInstance(refused, PermanentFailure)
        self.assertEqual("codex_safety_refusal", refused.code)
        other, _ = parse_events(1, b"", "Error: unknown configuration key")
        self.assertEqual("codex_failed", other.code)


if __name__ == "__main__":
    unittest.main()
