import tempfile
import unittest
from pathlib import Path

from logplane import bus
from logplane.operations import explain_task, task_view, workflow_view
from logplane.pm_agent import OrderedProjectionCursor, PM_TOPICS, PMState, reconcile
from logplane.projection import replay_events


class DirectPMBus:
    def __init__(self, before_publish=None):
        self.before_publish = before_publish

    def publish(self, topic, payload, **kwargs):
        if self.before_publish is not None:
            self.before_publish(topic, payload, kwargs)
        return bus.append_event(topic, "pm", payload, **kwargs)

    def query_all(self, *, after_id=0, topics=None):
        return bus.fetch_after(after_id, topics)


class WorkflowFairnessTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()
        self.worker = bus.append_event(
            "agent.registered",
            "alice",
            {
                "name": "alice",
                "instance_id": "alice-1",
                "capacity": 1,
                "capabilities": [],
            },
        )

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_round_robin_between_workflows_and_priority_within_each(self):
        a_low = self._create("flow-a", "A low", priority="low")
        a_urgent = self._create("flow-a", "A urgent", priority="urgent")
        b_low = self._create("flow-b", "B low", priority="low")
        b_high = self._create("flow-b", "B high", priority="high")
        state, cursor = self._projection()

        observed = []
        for _ in range(4):
            emitted = reconcile(
                state,
                DirectPMBus(),
                now=self.worker["ts"],
                cursor=cursor,
            )
            assignment = next(
                item for item in emitted if item["topic"] == "task.assigned"
            )
            observed.append(assignment["payload"]["task_id"])
            self.assertEqual(
                "workflow_round_robin_v1",
                assignment["payload"]["fairness"]["policy"],
            )
            self._complete(state, cursor, assignment["payload"]["task_id"])

        self.assertEqual(
            [
                a_urgent["payload"]["task_id"],
                b_high["payload"]["task_id"],
                a_low["payload"]["task_id"],
                b_low["payload"]["task_id"],
            ],
            observed,
        )

    def test_newly_eligible_workflow_gets_next_turn(self):
        first = self._create("flow-a", "A first")
        second = self._create("flow-a", "A second")
        delayed = self._create(
            "flow-b",
            "B delayed",
            not_before=self.worker["ts"] + 10,
        )
        state, cursor = self._projection()

        first_emitted = reconcile(
            state,
            DirectPMBus(),
            now=self.worker["ts"],
            cursor=cursor,
        )
        self.assertEqual(
            first["payload"]["task_id"],
            first_emitted[-1]["payload"]["task_id"],
        )
        self._complete(state, cursor, first["payload"]["task_id"])

        next_emitted = reconcile(
            state,
            DirectPMBus(),
            now=self.worker["ts"] + 11,
            cursor=cursor,
        )
        self.assertEqual(
            delayed["payload"]["task_id"],
            next_emitted[-1]["payload"]["task_id"],
        )
        self.assertNotEqual(
            second["payload"]["task_id"],
            next_emitted[-1]["payload"]["task_id"],
        )

    def test_crossed_assignment_cursor_is_fenced_and_reissued(self):
        planned_task = self._create("flow-a", "planned")
        crossing_task = self._create("flow-b", "crossing")
        bus.append_event(
            "agent.registered",
            "bob",
            {
                "name": "bob",
                "instance_id": "bob-1",
                "capacity": 1,
                "capabilities": [],
            },
        )
        state, cursor = self._projection()
        injected = False

        def cross(topic, payload, kwargs):
            nonlocal injected
            if topic != "task.assigned" or injected:
                return
            injected = True
            crossing_id = crossing_task["payload"]["task_id"]
            bus.append_event(
                "task.assigned",
                "pm",
                {
                    **payload,
                    "task_id": crossing_id,
                    "assignment_id": f"task:{crossing_id}:attempt:1",
                    "title": "crossing",
                    "goal": "crossing",
                },
                correlation_id="flow-b",
                idempotency_key=f"assign:task:{crossing_id}:attempt:1",
            )

        emitted = reconcile(
            state,
            DirectPMBus(cross),
            now=self.worker["ts"],
            cursor=cursor,
        )
        planned_id = planned_task["payload"]["task_id"]
        planned_assignments = [
            item
            for item in emitted
            if item["topic"] == "task.assigned"
            and item["payload"]["task_id"] == planned_id
        ]
        self.assertEqual(2, len(planned_assignments))
        self.assertEqual(1, planned_assignments[0]["payload"]["attempt"])
        self.assertEqual(2, planned_assignments[1]["payload"]["attempt"])
        self.assertEqual(
            planned_assignments[0]["payload"]["fairness"],
            {
                "policy": "workflow_round_robin_v1",
                "previous_assignment_event_id": None,
            },
        )
        crossing_assignment = next(
            item
            for item in bus.fetch_after(0, ["task.assigned"])
            if item["payload"]["task_id"]
            == crossing_task["payload"]["task_id"]
        )
        self.assertEqual(
            crossing_assignment["id"],
            planned_assignments[1]["payload"]["fairness"][
                "previous_assignment_event_id"
            ],
        )
        self.assertEqual("task:1:attempt:2", state.tasks[planned_id].assignment_id)

        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(
            state.last_scheduling_assignment_event_id,
            replayed.last_scheduling_assignment_event_id,
        )
        self.assertEqual(
            state.last_scheduled_workflow_key,
            replayed.last_scheduled_workflow_key,
        )
        self.assertEqual(
            state.tasks[planned_id].assignment_id,
            replayed.tasks[planned_id].assignment_id,
        )

    def test_explanations_and_views_expose_fairness_evidence(self):
        selected = self._create("flow-a", "selected")
        waiting = self._create("flow-b", "waiting", priority="urgent")
        state, cursor = self._projection()

        explanation = explain_task(
            state,
            waiting["payload"]["task_id"],
            now=self.worker["ts"],
            lease_seconds=20,
        )
        self.assertEqual("ready_after_fair_workflow", explanation["code"])
        self.assertEqual(
            selected["payload"]["task_id"],
            explanation["details"]["selected_task_id"],
        )

        emitted = reconcile(
            state,
            DirectPMBus(),
            now=self.worker["ts"],
            cursor=cursor,
        )
        assignment = emitted[-1]
        task = task_view(
            state,
            assignment["payload"]["task_id"],
            now=self.worker["ts"],
            lease_seconds=20,
        )
        workflow = workflow_view(
            state,
            "flow-a",
            now=self.worker["ts"],
            lease_seconds=20,
        )
        self.assertEqual(
            "workflow_round_robin_v1",
            task["assignment_fairness"]["policy"],
        )
        self.assertEqual(
            assignment["id"],
            workflow["scheduling"]["last_assignment_event_id"],
        )

    def test_legacy_assignment_without_fairness_seeds_replay_cursor(self):
        created = self._create("legacy-flow", "legacy")
        state, _ = self._projection()
        task_id = created["payload"]["task_id"]
        legacy = bus.append_event(
            "task.assigned",
            "pm",
            {
                "task_id": task_id,
                "assignment_id": f"task:{task_id}:attempt:1",
                "attempt": 1,
                "assignee": "alice",
                "worker_instance_id": "alice-1",
                "title": "legacy",
                "goal": "legacy",
                "context": {},
                "decisions": [],
                "dependency_refs": [],
                "required_capabilities": [],
                "retry_policy": {"max_retries": None},
                "retryable_failures": 0,
                "ownership": {"mode": "controlled", "owner": "agent-bus"},
            },
            correlation_id="legacy-flow",
        )
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(legacy["id"], replayed.last_scheduling_assignment_event_id)
        self.assertIsNone(replayed.tasks[task_id].assignment_fairness_policy)

    def test_bus_rejects_malformed_fairness_evidence(self):
        created = self._create("flow", "task")
        state, _ = self._projection()
        task = state.tasks[created["payload"]["task_id"]]
        worker = state.workers["alice"]
        planned = reconcile(
            state,
            DirectPMBus(),
            now=self.worker["ts"],
        )[0]
        base = dict(planned["payload"])
        del task, worker
        for malformed in (
            {"policy": "random", "previous_assignment_event_id": None},
            {"policy": "workflow_round_robin_v1"},
            {"policy": "workflow_round_robin_v1", "previous_assignment_event_id": 0},
        ):
            with self.subTest(fairness=malformed):
                with self.assertRaises(bus.EventValidationError):
                    bus.validate_event(
                        "task.assigned",
                        "pm",
                        {**base, "fairness": malformed},
                        planned["caused_by"],
                        "flow",
                        2,
                    )

    def _create(self, correlation_id, title, **scheduling):
        return bus.append_event(
            "task.created",
            "human",
            {"title": title, **scheduling},
            correlation_id=correlation_id,
        )

    def _projection(self):
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        return state, cursor

    def _complete(self, state, cursor, task_id):
        task = state.tasks[task_id]
        bus.append_event(
            "task.completed",
            "alice",
            {
                "task_id": task_id,
                "assignment_id": task.assignment_id,
                "worker_instance_id": task.worker_instance_id,
                "summary": "done",
                "result": {},
            },
        )
        cursor.catch_up(DirectPMBus())


if __name__ == "__main__":
    unittest.main()
