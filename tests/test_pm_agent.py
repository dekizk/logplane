import fcntl
import os
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from logplane import bus
from logplane import pm_agent
from logplane.pm_agent import PMState, apply_event, plan_next_emission, reconcile
from logplane.topics import COORDINATION_TOPICS, INTEGRATION_TOPICS, KNOWN_TOPICS, TELEMETRY_TOPICS


def event(
    event_id,
    topic,
    actor,
    payload,
    *,
    ts=100.0,
    caused_by=None,
    schema_version=2,
    correlation_id=None,
):
    return {
        "id": event_id,
        "ts": ts,
        "topic": topic,
        "actor": actor,
        "payload": payload,
        "caused_by": caused_by,
        "correlation_id": correlation_id,
        "schema_version": schema_version,
        "idempotency_key": None,
    }


class FakeBus:
    def __init__(self, starting_id, ts=101.0):
        self.next_id = starting_id
        self.ts = ts
        self.events = []
        self.by_key = {}

    def publish(self, topic, payload, caused_by=None, idempotency_key=None):
        if idempotency_key in self.by_key:
            return self.by_key[idempotency_key]
        sent = event(
            self.next_id,
            topic,
            "pm",
            payload,
            ts=self.ts,
            caused_by=caused_by,
        )
        sent["idempotency_key"] = idempotency_key
        self.next_id += 1
        self.events.append(sent)
        self.by_key[idempotency_key] = sent
        return sent


def registered(event_id=1, name="alice", instance="alice-1", ts=100.0):
    return event(
        event_id,
        "agent.registered",
        name,
        {
            "name": name,
            "instance_id": instance,
            "capacity": 1,
            "capabilities": [],
        },
        ts=ts,
    )


MISSING = object()


def created(
    event_id=2,
    task_id=1,
    ts=100.0,
    correlation_id=None,
    max_retries=MISSING,
    depends_on=(),
    priority=MISSING,
    not_before=MISSING,
    deadline_at=MISSING,
):
    payload = {"task_id": task_id, "title": "demo"}
    if depends_on:
        payload["depends_on"] = list(depends_on)
    if max_retries is not MISSING:
        payload["retry_policy"] = {"max_retries": max_retries}
    if priority is not MISSING:
        payload["priority"] = priority
    if not_before is not MISSING:
        payload["not_before"] = not_before
    if deadline_at is not MISSING:
        payload["deadline_at"] = deadline_at
    return event(
        event_id,
        "task.created",
        "human",
        payload,
        ts=ts,
        correlation_id=correlation_id,
    )


def assigned(event_id, attempt, *, task_id=1, worker="alice", instance="alice-1"):
    return event(
        event_id,
        "task.assigned",
        "pm",
        {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "attempt": attempt,
            "assignee": worker,
            "worker_instance_id": instance,
        },
    )


def expired(event_id, attempt, *, task_id=1, worker="alice", instance="alice-1"):
    return event(
        event_id,
        "task.assignment_expired",
        "pm",
        {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "assignee": worker,
            "worker_instance_id": instance,
            "reason": "worker lease expired",
        },
    )


def attempt_failed(
    event_id,
    attempt,
    *,
    retryable,
    task_id=1,
    worker="alice",
    instance="alice-1",
):
    return event(
        event_id,
        "task.attempt_failed",
        worker,
        {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "worker_instance_id": instance,
            "failure_code": "tool_timeout" if retryable else "invalid_input",
            "reason": "executor failed",
            "retryable": retryable,
        },
    )


def completed(event_id, attempt, *, task_id=1, worker="alice", instance="alice-1"):
    return event(
        event_id,
        "task.completed",
        worker,
        {
            "task_id": task_id,
            "assignment_id": f"task:{task_id}:attempt:{attempt}",
            "worker_instance_id": instance,
            "summary": "done",
            "result": {"task": task_id},
        },
    )


class PMLockTests(unittest.TestCase):
    def test_contended_lock_is_not_truncated(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pm.lock"
            lock_path.write_text("existing-owner")

            with lock_path.open("r+") as held_lock:
                fcntl.flock(held_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(pm_agent, "LOCK_PATH", lock_path):
                    with self.assertRaisesRegex(SystemExit, "another PM"):
                        with pm_agent.single_pm_lock():
                            self.fail("contended lock should not be acquired")

            self.assertEqual("existing-owner", lock_path.read_text())

    @unittest.skipUnless(hasattr(os, "O_NOFOLLOW"), "O_NOFOLLOW is required")
    def test_lock_refuses_symlink_without_modifying_target(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            target.write_text("do-not-truncate")
            lock_path = Path(directory) / "pm.lock"
            lock_path.symlink_to(target)

            with patch.object(pm_agent, "LOCK_PATH", lock_path):
                with self.assertRaisesRegex(SystemExit, "cannot safely open"):
                    with pm_agent.single_pm_lock():
                        self.fail("symlink lock should not be acquired")

            self.assertEqual("do-not-truncate", target.read_text())

    def test_lock_refuses_a_file_replaced_while_locking(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pm.lock"
            real_flock = fcntl.flock

            def flock_then_replace(fd, operation):
                real_flock(fd, operation)
                lock_path.unlink()
                lock_path.write_text("someone else's lock")

            with patch.object(pm_agent, "LOCK_PATH", lock_path), \
                    patch.object(pm_agent.fcntl, "flock", side_effect=flock_then_replace):
                with self.assertRaisesRegex(SystemExit, "replaced while locking"):
                    with pm_agent.single_pm_lock():
                        self.fail("a replaced lock file must not count as held")

    def test_new_lock_is_user_only(self):
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "pm.lock"

            with patch.object(pm_agent, "LOCK_PATH", lock_path):
                with pm_agent.single_pm_lock():
                    mode = lock_path.stat().st_mode & 0o777

            self.assertEqual(0o600, mode)


class ReconciliationTests(unittest.TestCase):
    def test_ready_tasks_are_ordered_by_priority_then_creation_event(self):
        state = PMState()
        worker = registered(event_id=1)
        worker["payload"]["capacity"] = 3
        for item in (
            worker,
            created(event_id=2, task_id=50, priority="low"),
            created(event_id=3, task_id=20, priority="urgent"),
            created(event_id=4, task_id=10, priority="urgent"),
        ):
            self.assertTrue(apply_event(state, item))

        emitted = reconcile(state, FakeBus(starting_id=5), now=101.0)

        assignments = [
            item["payload"]["task_id"]
            for item in emitted
            if item["topic"] == "task.assigned"
        ]
        self.assertEqual([20, 10, 50], assignments)

    def test_future_urgent_task_does_not_block_eligible_normal_work(self):
        state = PMState()
        for item in (
            registered(event_id=1),
            created(
                event_id=2,
                task_id=1,
                priority="urgent",
                not_before=200.0,
            ),
            created(event_id=3, task_id=2, priority="normal"),
        ):
            self.assertTrue(apply_event(state, item))

        planned = plan_next_emission(state, now=101.0)
        self.assertEqual(2, planned["payload"]["task_id"])

        delayed_only = PMState()
        for item in (
            registered(event_id=1),
            created(
                event_id=2,
                task_id=1,
                priority="urgent",
                not_before=200.0,
            ),
        ):
            self.assertTrue(apply_event(delayed_only, item))
        self.assertIsNone(
            plan_next_emission(delayed_only, now=199.999, lease_seconds=200)
        )
        self.assertEqual(
            1,
            plan_next_emission(
                delayed_only,
                now=200.0,
                lease_seconds=200,
            )["payload"]["task_id"],
        )

    def test_historical_tasks_replay_as_immediate_normal_priority(self):
        state = PMState()
        self.assertTrue(apply_event(state, created(event_id=1)))

        task = state.tasks[1]
        self.assertEqual("normal", task.priority)
        self.assertIsNone(task.not_before)

    def test_waiting_cancellation_is_terminal_and_crash_safe(self):
        history = [
            created(event_id=1, task_id=1, correlation_id="workflow-controls"),
            event(
                2,
                "task.cancel_requested",
                "human",
                {"task_id": 1, "reason": "superseded"},
                correlation_id="workflow-controls",
            ),
        ]
        state = PMState()
        for item in history:
            self.assertTrue(apply_event(state, item))

        planned = plan_next_emission(state, now=101.0)
        self.assertEqual("task.cancelled", planned["topic"])
        self.assertEqual(0, planned["payload"]["attempts"])
        self.assertIsNone(planned["payload"]["last_assignment_id"])
        terminal = event(
            3,
            planned["topic"],
            "pm",
            planned["payload"],
            caused_by=planned["caused_by"],
        )
        self.assertTrue(apply_event(state, terminal))
        self.assertEqual("cancelled", state.tasks[1].status)
        self.assertEqual(0, state.tasks[1].retryable_failures)
        self.assertIsNone(plan_next_emission(state, now=101.0))

        replayed = PMState()
        for item in history + [terminal]:
            self.assertTrue(apply_event(replayed, item))
        self.assertIsNone(plan_next_emission(replayed, now=101.0))

    def test_event_order_decides_completion_cancellation_race(self):
        completed_first = PMState()
        for item in (created(event_id=1), assigned(2, 1), completed(3, 1)):
            self.assertTrue(apply_event(completed_first, item))
        self.assertFalse(
            apply_event(
                completed_first,
                event(
                    4,
                    "task.cancel_requested",
                    "human",
                    {"task_id": 1, "reason": "too late"},
                ),
            )
        )
        self.assertEqual("completed", completed_first.tasks[1].status)

        cancelled_first = PMState()
        for item in (created(event_id=1), assigned(2, 1)):
            self.assertTrue(apply_event(cancelled_first, item))
        self.assertTrue(
            apply_event(
                cancelled_first,
                event(
                    3,
                    "task.cancel_requested",
                    "human",
                    {"task_id": 1, "reason": "stop now"},
                ),
            )
        )
        self.assertFalse(apply_event(cancelled_first, completed(4, 1)))
        planned = plan_next_emission(cancelled_first, now=101.0)
        self.assertEqual("task.cancelled", planned["topic"])
        self.assertEqual("task:1:attempt:1", planned["payload"]["last_assignment_id"])

    def test_deadline_preempts_late_outcome_without_consuming_retry_budget(self):
        state = PMState()
        self.assertTrue(apply_event(state, registered(event_id=1)))
        self.assertTrue(
            apply_event(
                state,
                created(event_id=2, deadline_at=105.0),
            )
        )
        assignment = plan_next_emission(state, now=101.0)
        self.assertEqual(105.0, assignment["payload"]["deadline_at"])
        self.assertTrue(
            apply_event(
                state,
                event(3, "task.assigned", "pm", assignment["payload"], ts=101.0),
            )
        )
        late_completion = completed(4, 1)
        late_completion["ts"] = 105.0
        self.assertFalse(apply_event(state, late_completion))

        terminal = plan_next_emission(state, now=105.0)
        self.assertEqual("task.deadline_exceeded", terminal["topic"])
        self.assertEqual("task:1:attempt:1", terminal["payload"]["last_assignment_id"])
        self.assertTrue(
            apply_event(
                state,
                event(
                    5,
                    terminal["topic"],
                    "pm",
                    terminal["payload"],
                    ts=105.0,
                    caused_by=terminal["caused_by"],
                ),
            )
        )
        self.assertEqual("deadline_exceeded", state.tasks[1].status)
        self.assertEqual(0, state.tasks[1].retryable_failures)

    def test_cancelled_dependency_cascades_without_assignment(self):
        state = PMState()
        workflow = "workflow-controls"
        for item in (
            created(event_id=1, task_id=1, correlation_id=workflow),
            created(
                event_id=2,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
            event(
                3,
                "task.cancel_requested",
                "human",
                {"task_id": 1, "reason": "stop root"},
                correlation_id=workflow,
            ),
        ):
            self.assertTrue(apply_event(state, item))

        emitted = reconcile(state, FakeBus(starting_id=4), now=101.0)
        self.assertEqual(
            ["task.cancelled", "task.dependency_failed"],
            [item["topic"] for item in emitted],
        )
        self.assertEqual("dependency_failed", state.tasks[2].status)

    def test_deadline_exceeded_dependency_cascades_without_assignment(self):
        state = PMState()
        workflow = "workflow-deadline-dag"
        for item in (
            created(
                event_id=1,
                task_id=1,
                correlation_id=workflow,
                deadline_at=100.0,
            ),
            created(
                event_id=2,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
        ):
            self.assertTrue(apply_event(state, item))

        emitted = reconcile(
            state,
            FakeBus(starting_id=3, ts=101.0),
            now=101.0,
        )
        self.assertEqual(
            ["task.deadline_exceeded", "task.dependency_failed"],
            [item["topic"] for item in emitted],
        )
        self.assertEqual("deadline_exceeded", state.tasks[1].status)
        self.assertEqual("dependency_failed", state.tasks[2].status)

        replayed = PMState()
        for item in (
            created(
                event_id=1,
                task_id=1,
                correlation_id=workflow,
                deadline_at=100.0,
            ),
            created(
                event_id=2,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
            *emitted,
        ):
            self.assertTrue(apply_event(replayed, item))
        self.assertIsNone(plan_next_emission(replayed, now=101.0))

    def test_blocked_and_retry_pending_tasks_can_be_cancelled(self):
        blocked = PMState()
        for item in (
            created(event_id=1),
            assigned(2, 1),
            event(
                3,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "approval needed",
                },
            ),
            event(
                4,
                "task.cancel_requested",
                "human",
                {"task_id": 1, "reason": "do not wait"},
            ),
        ):
            self.assertTrue(apply_event(blocked, item))
        self.assertEqual(
            "task.cancelled",
            plan_next_emission(blocked, now=101.0)["topic"],
        )

        retry_pending = PMState()
        for item in (
            created(event_id=1, max_retries=3),
            assigned(2, 1),
            attempt_failed(3, 1, retryable=True),
            event(
                4,
                "task.cancel_requested",
                "human",
                {"task_id": 1, "reason": "stop retries"},
            ),
        ):
            self.assertTrue(apply_event(retry_pending, item))
        planned = plan_next_emission(retry_pending, now=101.0)
        self.assertEqual("task.cancelled", planned["topic"])
        self.assertEqual(1, planned["payload"]["attempts"])
        self.assertEqual(1, retry_pending.tasks[1].retryable_failures)

    def test_chain_waits_for_completion_then_assigns_by_reference(self):
        state = PMState()
        workflow = "workflow-dag"
        for item in (
            registered(),
            created(event_id=2, task_id=1, correlation_id=workflow),
            created(
                event_id=3,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
        ):
            self.assertTrue(apply_event(state, item))

        root_assignment = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual(1, root_assignment["payload"]["task_id"])
        self.assertEqual([], root_assignment["payload"]["dependency_refs"])
        self.assertTrue(
            apply_event(
                state,
                event(4, "task.assigned", "pm", root_assignment["payload"]),
            )
        )
        self.assertIsNone(plan_next_emission(state, now=101.0, lease_seconds=20))
        self.assertTrue(apply_event(state, completed(5, 1, task_id=1)))

        child_assignment = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual(2, child_assignment["payload"]["task_id"])
        self.assertEqual(
            [{"task_id": 1, "completion_event_id": 5}],
            child_assignment["payload"]["dependency_refs"],
        )
        self.assertEqual(5, child_assignment["caused_by"])

    def test_fan_in_and_fan_out_are_replay_derived(self):
        state = PMState()
        workflow = "workflow-dag"
        registration = registered()
        registration["payload"]["capacity"] = 3
        for item in (
            registration,
            created(event_id=2, task_id=1, correlation_id=workflow),
            created(event_id=3, task_id=2, correlation_id=workflow),
            created(
                event_id=4,
                task_id=3,
                correlation_id=workflow,
                depends_on=(1, 2),
            ),
            created(
                event_id=5,
                task_id=4,
                correlation_id=workflow,
                depends_on=(1,),
            ),
        ):
            self.assertTrue(apply_event(state, item))

        for event_id, task_id in ((6, 1), (8, 2)):
            planned = plan_next_emission(state, now=101.0, lease_seconds=20)
            self.assertEqual(task_id, planned["payload"]["task_id"])
            self.assertTrue(
                apply_event(state, event(event_id, "task.assigned", "pm", planned["payload"]))
            )
            self.assertTrue(
                apply_event(state, completed(event_id + 1, 1, task_id=task_id))
            )

        fan_in = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual(3, fan_in["payload"]["task_id"])
        self.assertEqual(
            [
                {"task_id": 1, "completion_event_id": 7},
                {"task_id": 2, "completion_event_id": 9},
            ],
            fan_in["payload"]["dependency_refs"],
        )
        self.assertTrue(
            apply_event(state, event(10, "task.assigned", "pm", fan_in["payload"]))
        )
        fan_out = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual(4, fan_out["payload"]["task_id"])

    def test_terminal_dependency_failure_cascades_without_assignment(self):
        state = PMState()
        workflow = "workflow-dag"
        for item in (
            registered(),
            created(event_id=2, task_id=1, correlation_id=workflow, max_retries=0),
            created(
                event_id=3,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
            created(
                event_id=4,
                task_id=3,
                correlation_id=workflow,
                depends_on=(2,),
            ),
            assigned(5, 1, task_id=1),
            attempt_failed(6, 1, task_id=1, retryable=False),
        ):
            self.assertTrue(apply_event(state, item))

        emitted = reconcile(
            state,
            FakeBus(starting_id=7),
            now=101.0,
            lease_seconds=20,
        )
        self.assertEqual(
            ["task.failed", "task.dependency_failed", "task.dependency_failed"],
            [item["topic"] for item in emitted],
        )
        self.assertEqual("dependency_failed", state.tasks[2].status)
        self.assertEqual("dependency_failed", state.tasks[3].status)
        self.assertFalse(
            any(item["topic"] == "task.assigned" for item in emitted)
        )

    def test_restart_reconciles_ready_dependency_once(self):
        workflow = "workflow-dag"
        history = [
            registered(),
            created(event_id=2, task_id=1, correlation_id=workflow),
            created(
                event_id=3,
                task_id=2,
                correlation_id=workflow,
                depends_on=(1,),
            ),
            assigned(4, 1, task_id=1),
            completed(5, 1, task_id=1),
        ]
        state = PMState()
        for item in history:
            self.assertTrue(apply_event(state, item))
        first_bus = FakeBus(starting_id=6)
        emitted = reconcile(state, first_bus, now=101.0, lease_seconds=20)
        self.assertEqual(["task.assigned"], [item["topic"] for item in emitted])

        replayed = PMState()
        for item in history + first_bus.events:
            self.assertTrue(apply_event(replayed, item))
        self.assertEqual(
            [],
            reconcile(
                replayed,
                FakeBus(starting_id=7),
                now=101.0,
                lease_seconds=20,
            ),
        )

    def test_reducer_rejects_forward_and_cross_workflow_dependencies(self):
        state = PMState()
        self.assertFalse(
            apply_event(
                state,
                created(
                    task_id=2,
                    correlation_id="workflow-one",
                    depends_on=(1,),
                ),
            )
        )
        self.assertTrue(
            apply_event(state, created(task_id=1, correlation_id="workflow-one"))
        )
        self.assertFalse(
            apply_event(
                state,
                created(
                    event_id=2,
                    task_id=2,
                    correlation_id="workflow-two",
                    depends_on=(1,),
                ),
            )
        )

    def test_task_projection_retains_workflow_correlation(self):
        state = PMState()
        self.assertTrue(
            apply_event(
                state,
                created(correlation_id="workflow-one"),
            )
        )
        self.assertEqual("workflow-one", state.tasks[1].correlation_id)

    def test_task_projection_retains_persisted_retry_policy(self):
        state = PMState()
        self.assertTrue(apply_event(state, created(max_retries=4)))
        self.assertEqual(4, state.tasks[1].max_retries)

        legacy = PMState()
        self.assertTrue(apply_event(legacy, created()))
        self.assertIsNone(legacy.tasks[1].max_retries)

    def test_assignment_carries_executor_context_and_ownership(self):
        state = PMState()
        self.assertTrue(apply_event(state, registered()))
        self.assertTrue(
            apply_event(
                state,
                event(
                    2,
                    "task.created",
                    "bridge",
                    {
                        "task_id": 1,
                        "title": "Run imported task",
                        "context": {"repository": "agent-bus"},
                        "required_capabilities": [],
                        "retry_policy": {"max_retries": 2},
                        "external_origin": {
                            "system": "legacy",
                            "task_ref": "work-1",
                        },
                        "ownership": {
                            "mode": "canary",
                            "owner": "agent-bus",
                        },
                    },
                    correlation_id="workflow-one",
                ),
            )
        )

        payload = plan_next_emission(
            state,
            now=101.0,
            lease_seconds=20,
        )["payload"]
        self.assertEqual({"repository": "agent-bus"}, payload["context"])
        self.assertEqual({"max_retries": 2}, payload["retry_policy"])
        self.assertEqual(0, payload["retryable_failures"])
        self.assertEqual(
            {"mode": "canary", "owner": "agent-bus"},
            payload["ownership"],
        )
        self.assertEqual("work-1", payload["external_origin"]["task_ref"])

    def test_restart_reconciles_assignment_missing_after_replay(self):
        history = [registered(), created()]
        state = PMState()
        for item in history:
            self.assertTrue(apply_event(state, item))

        bus = FakeBus(starting_id=3)
        emitted = reconcile(state, bus, now=101.0, lease_seconds=20)

        self.assertEqual(["task.assigned"], [item["topic"] for item in emitted])
        self.assertEqual("task:1:attempt:1", emitted[0]["payload"]["assignment_id"])

        restarted = PMState()
        for item in history + bus.events:
            apply_event(restarted, item)
        second_bus = FakeBus(starting_id=4)
        self.assertEqual([], reconcile(restarted, second_bus, now=102.0, lease_seconds=20))

    def test_restart_reconciles_missing_human_decision_request(self):
        log = [
            registered(),
            created(),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "need approval",
                },
                caused_by=3,
            ),
        ]
        state = PMState()
        for item in log:
            apply_event(state, item)

        bus = FakeBus(starting_id=5)
        emitted = reconcile(state, bus, now=101.0, lease_seconds=20)

        self.assertEqual(["decision.needed"], [item["topic"] for item in emitted])
        self.assertEqual("decision:task:1:attempt:1", emitted[0]["payload"]["decision_id"])
        self.assertEqual("need approval", emitted[0]["payload"]["reason"])

    def test_stale_worker_cannot_complete_reassigned_attempt(self):
        state = PMState()
        base = [
            registered(name="alice", instance="alice-1"),
            registered(event_id=2, name="bob", instance="bob-1"),
            created(event_id=3),
            event(
                4,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=3,
            ),
            event(
                5,
                "task.assignment_expired",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                    "reason": "lease expired",
                },
                caused_by=4,
            ),
            event(
                6,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "attempt": 2,
                    "assignee": "bob",
                    "worker_instance_id": "bob-1",
                },
                caused_by=5,
            ),
        ]
        for item in base:
            self.assertTrue(apply_event(state, item))

        stale = event(
            7,
            "task.completed",
            "alice",
            {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
            caused_by=4,
        )
        self.assertFalse(apply_event(state, stale))
        self.assertEqual("assigned", state.tasks[1].status)
        self.assertEqual("bob", state.tasks[1].assignee)

    def test_retry_boundary_and_crash_window_emit_one_terminal_failure(self):
        history = [
            registered(),
            created(max_retries=1),
            assigned(3, 1),
            expired(4, 1),
        ]
        state = PMState()
        for item in history:
            self.assertTrue(apply_event(state, item))

        retry = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assigned", retry["topic"])
        self.assertEqual(2, retry["payload"]["attempt"])
        second_assignment = event(5, "task.assigned", "pm", retry["payload"])
        self.assertTrue(apply_event(state, second_assignment))
        second_expiry = expired(6, 2)
        self.assertTrue(apply_event(state, second_expiry))

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.failed", planned["topic"])
        self.assertEqual("retry_budget_exhausted", planned["payload"]["reason_code"])
        self.assertEqual(2, planned["payload"]["retryable_failures"])
        self.assertEqual(1, planned["payload"]["max_retries"])
        self.assertEqual(6, planned["caused_by"])

        # This models a PM restart after the final expiry was committed but
        # before its terminal effect was appended.
        restarted = PMState()
        crash_history = history + [second_assignment, second_expiry]
        for item in crash_history:
            self.assertTrue(apply_event(restarted, item))
        bus_after_restart = FakeBus(starting_id=7)
        emitted = reconcile(
            restarted,
            bus_after_restart,
            now=101.0,
            lease_seconds=20,
        )
        self.assertEqual(["task.failed"], [item["topic"] for item in emitted])

        replayed = PMState()
        for item in crash_history + bus_after_restart.events:
            self.assertTrue(apply_event(replayed, item))
        self.assertEqual("failed", replayed.tasks[1].status)
        self.assertEqual(
            [],
            reconcile(
                replayed,
                FakeBus(starting_id=8),
                now=101.0,
                lease_seconds=20,
            ),
        )

    def test_permanent_worker_failure_is_terminal_without_retry(self):
        state = PMState()
        for item in (
            registered(),
            created(max_retries=5),
            assigned(3, 1),
            attempt_failed(4, 1, retryable=False),
        ):
            self.assertTrue(apply_event(state, item))

        emitted = reconcile(
            state,
            FakeBus(starting_id=5),
            now=101.0,
            lease_seconds=20,
        )
        self.assertEqual(["task.failed"], [item["topic"] for item in emitted])
        self.assertEqual("invalid_input", emitted[0]["payload"]["reason_code"])
        self.assertEqual(0, state.tasks[1].retryable_failures)
        self.assertEqual("failed", state.tasks[1].status)

        late = event(
            6,
            "task.completed",
            "alice",
            {
                "task_id": 1,
                "assignment_id": "task:1:attempt:1",
                "worker_instance_id": "alice-1",
            },
        )
        self.assertFalse(apply_event(state, late))
        self.assertEqual("failed", state.tasks[1].status)

        retry_request = event(
            7,
            "task.retry_requested",
            "human",
            {
                "task_id": 1,
                "additional_retries": 1,
                "reason": "corrected the invalid input",
            },
            caused_by=emitted[0]["id"],
        )
        self.assertTrue(apply_event(state, retry_request))
        self.assertEqual(0, state.tasks[1].max_retries)
        retry_assignment = plan_next_emission(
            state,
            now=101.0,
            lease_seconds=20,
        )
        self.assertEqual(2, retry_assignment["payload"]["attempt"])
        self.assertTrue(
            apply_event(
                state,
                event(8, "task.assigned", "pm", retry_assignment["payload"]),
            )
        )
        self.assertTrue(apply_event(state, attempt_failed(9, 2, retryable=True)))
        self.assertEqual(
            "task.failed",
            plan_next_emission(state, now=101.0, lease_seconds=20)["topic"],
        )

    def test_human_retry_extends_budget_without_resetting_attempt(self):
        state = PMState()
        for item in (
            registered(),
            created(max_retries=0),
            assigned(3, 1),
            attempt_failed(4, 1, retryable=True),
        ):
            self.assertTrue(apply_event(state, item))

        terminal_bus = FakeBus(starting_id=5)
        terminal = reconcile(
            state,
            terminal_bus,
            now=101.0,
            lease_seconds=20,
        )
        self.assertEqual(["task.failed"], [item["topic"] for item in terminal])
        retry_request = event(
            6,
            "task.retry_requested",
            "human",
            {
                "task_id": 1,
                "additional_retries": 1,
                "reason": "transient service is healthy again",
            },
            caused_by=terminal[0]["id"],
        )
        self.assertTrue(apply_event(state, retry_request))
        self.assertEqual(1, state.tasks[1].retryable_failures)
        self.assertEqual(1, state.tasks[1].max_retries)

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assigned", planned["topic"])
        self.assertEqual(2, planned["payload"]["attempt"])
        self.assertEqual("task:1:attempt:2", planned["payload"]["assignment_id"])
        self.assertTrue(
            apply_event(
                state,
                event(7, "task.assigned", "pm", planned["payload"]),
            )
        )
        self.assertTrue(apply_event(state, attempt_failed(8, 2, retryable=True)))
        self.assertEqual(
            "task.failed",
            plan_next_emission(state, now=101.0, lease_seconds=20)["topic"],
        )

    def test_multiple_human_decisions_are_carried_forward(self):
        first_decision = {
            "event_id": 6,
            "actor": "human",
            "assignment_id": "task:1:attempt:1",
            "decision_id": "decision:task:1:attempt:1",
            "decision": {"database": "SQLite"},
        }
        log = [
            registered(),
            created(max_retries=0),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "choose database",
                },
                caused_by=3,
            ),
            event(
                5,
                "decision.needed",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "reason": "choose database",
                },
                caused_by=4,
            ),
            event(
                6,
                "decision.made",
                "human",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": {"database": "SQLite"},
                },
                caused_by=5,
            ),
            event(
                7,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "attempt": 2,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                    "decisions": [first_decision],
                },
                caused_by=6,
            ),
            event(
                8,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "worker_instance_id": "alice-1",
                    "reason": "choose region",
                },
                caused_by=7,
            ),
            event(
                9,
                "decision.needed",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "decision_id": "decision:task:1:attempt:2",
                    "reason": "choose region",
                },
                caused_by=8,
            ),
            event(
                10,
                "decision.made",
                "operator",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:2",
                    "decision_id": "decision:task:1:attempt:2",
                    "decision": {"region": "ap-southeast-2"},
                },
                caused_by=9,
            ),
        ]
        state = PMState()
        for item in log:
            self.assertTrue(apply_event(state, item))

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task:1:attempt:3", planned["payload"]["assignment_id"])
        self.assertEqual(
            [first_decision, {
                "event_id": 10,
                "actor": "operator",
                "assignment_id": "task:1:attempt:2",
                "decision_id": "decision:task:1:attempt:2",
                "decision": {"region": "ap-southeast-2"},
            }],
            planned["payload"]["decisions"],
        )

    def test_legacy_task_remains_unbounded(self):
        state = PMState()
        for item in (
            registered(),
            created(),
            assigned(3, 1),
            expired(4, 1),
        ):
            self.assertTrue(apply_event(state, item))

        self.assertIsNone(state.tasks[1].max_retries)
        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assigned", planned["topic"])
        self.assertEqual(2, planned["payload"]["attempt"])

    def test_human_decision_creates_a_new_attempt(self):
        state = PMState()
        log = [
            registered(),
            created(max_retries=0),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            event(
                4,
                "task.blocked",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "reason": "choose backend",
                },
                caused_by=3,
            ),
            event(
                5,
                "decision.needed",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "reason": "choose backend",
                },
                caused_by=4,
            ),
            event(
                6,
                "decision.made",
                "human",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": "SQLite",
                },
                caused_by=5,
            ),
        ]
        for item in log:
            self.assertTrue(apply_event(state, item))

        self.assertEqual(0, state.tasks[1].retryable_failures)
        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assigned", planned["topic"])
        self.assertEqual(2, planned["payload"]["attempt"])
        self.assertEqual("task:1:attempt:2", planned["payload"]["assignment_id"])
        self.assertEqual(
            [
                {
                    "event_id": 6,
                    "actor": "human",
                    "assignment_id": "task:1:attempt:1",
                    "decision_id": "decision:task:1:attempt:1",
                    "decision": "SQLite",
                }
            ],
            planned["payload"]["decisions"],
        )

        restarted = PMState()
        for item in log:
            self.assertTrue(apply_event(restarted, item))
        self.assertEqual(
            planned,
            plan_next_emission(restarted, now=101.0, lease_seconds=20),
        )

    def test_worker_replacement_expires_old_attempt(self):
        state = PMState()
        for item in (
            registered(),
            created(),
            event(
                3,
                "task.assigned",
                "pm",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "attempt": 1,
                    "assignee": "alice",
                    "worker_instance_id": "alice-1",
                },
                caused_by=2,
            ),
            registered(event_id=4, instance="alice-2", ts=101.0),
        ):
            apply_event(state, item)

        planned = plan_next_emission(state, now=101.0, lease_seconds=20)
        self.assertEqual("task.assignment_expired", planned["topic"])
        self.assertEqual("worker process was replaced", planned["payload"]["reason"])

    def test_lease_is_not_expired_by_a_heartbeat_the_pm_has_not_read_yet(self):
        state = PMState()
        cursor = pm_agent.OrderedProjectionCursor(state)
        cursor.consume([
            registered(ts=100.0),
            created(ts=100.0),
            event(3, "task.assigned", "pm", {
                "task_id": 1, "assignment_id": "task:1:attempt:1", "attempt": 1,
                "assignee": "alice", "worker_instance_id": "alice-1"}, caused_by=2),
        ])
        unread = [event(4, "agent.heartbeat", "alice",
                        {"name": "alice", "instance_id": "alice-1"}, ts=125.0)]

        class LaggingBus(FakeBus):
            def query_all(self, after_id=0, topics=None):
                log = sorted(unread + self.events, key=lambda item: item["id"])
                return [item for item in log if item["id"] > after_id]

        fake = LaggingBus(starting_id=5)
        reconcile(state, fake, now=130.0, lease_seconds=20, cursor=cursor,
                  default_workflow_max_active_assignments=None)
        self.assertNotIn("task.assignment_expired", [item["topic"] for item in fake.events])
        self.assertEqual(4, cursor.last_event_id)

        unread.clear()
        reconcile(state, fake, now=150.0, lease_seconds=20, cursor=cursor,
                  default_workflow_max_active_assignments=None)
        self.assertIn("task.assignment_expired", [item["topic"] for item in fake.events])

    def test_malformed_event_does_not_poison_replay(self):
        state = PMState()
        malformed = event(1, "agent.registered", "alice", {})
        self.assertFalse(apply_event(state, malformed))
        self.assertEqual({}, state.workers)


class PMMainTests(unittest.TestCase):
    def test_pm_replays_and_subscribes_only_to_coordination_topics(self):
        class MainBus:
            def __init__(self):
                self.query_calls = []
                self.subscribe_calls = []

            def query_all(self, **kwargs):
                self.query_calls.append(kwargs)
                return []

            def subscribe(self, **kwargs):
                self.subscribe_calls.append(kwargs)
                return iter(())

        fake_bus = MainBus()
        with (
            patch("logplane.pm_agent.single_pm_lock", return_value=nullcontext()),
            patch("logplane.pm_agent.BusClient", return_value=fake_bus),
        ):
            pm_agent.main()

        expected = list(pm_agent.PM_TOPICS)
        self.assertEqual(
            [{"after_id": 0, "topics": expected}],
            fake_bus.query_calls,
        )
        self.assertEqual(expected, fake_bus.subscribe_calls[0]["topics"])
        self.assertEqual(set(COORDINATION_TOPICS), set(pm_agent.PM_TOPICS))
        self.assertLessEqual(set(COORDINATION_TOPICS), set(KNOWN_TOPICS))
        self.assertTrue(set(INTEGRATION_TOPICS).isdisjoint(pm_agent.PM_TOPICS))
        self.assertTrue(set(TELEMETRY_TOPICS).isdisjoint(pm_agent.PM_TOPICS))


if __name__ == "__main__":
    unittest.main()
