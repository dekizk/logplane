import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from logplane import cli as logplane_cli
from logplane import bus
from logplane.client import BusClient
from logplane.executors import AssignmentContext
from logplane.local_config import LocalConfig, initialize_local_config
from logplane.operations import explain_task, worker_views
from logplane.pm_agent import (
    OrderedProjectionCursor,
    PM_TOPICS,
    PMState,
    _default_workflow_max_active_assignments,
    _optional_positive_env,
    reconcile,
)
from logplane.projection import replay_events
from logplane.telemetry import BusTelemetrySink, ProducerIdentity


class DirectPMBus:
    actor = "pm"

    def __init__(self, before_publish=None):
        self.before_publish = before_publish

    def publish(self, topic, payload, **kwargs):
        if self.before_publish is not None:
            self.before_publish(topic, payload, kwargs)
        return bus.append_event(topic, "pm", payload, **kwargs)

    def query_all(self, *, after_id=0, topics=None):
        return bus.fetch_after(after_id, topics)


class DirectWorkerBus:
    actor = "alice"

    @staticmethod
    def publish(topic, payload, **kwargs):
        return bus.append_event(topic, "alice", payload, **kwargs)


class Phase3CAccountingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_db_path = bus.DB_PATH
        bus.DB_PATH = Path(self.temp_dir.name) / "events.db"
        bus.init_db()

    def tearDown(self):
        bus.DB_PATH = self.original_db_path
        self.temp_dir.cleanup()

    def test_defaults_materialize_before_reserved_assignment(self):
        worker = self._register(capacity=4)
        self._create("default-budget", "task")
        state, cursor = self._projection()

        emitted = reconcile(
            state,
            DirectPMBus(),
            now=worker["ts"],
            cursor=cursor,
            default_workflow_max_active_assignments=3,
            default_agent_max_active_assignments=2,
            default_workflow_max_total_tokens=100,
            default_workflow_max_total_cost_usd=1.0,
            default_workflow_max_attempts=5,
            default_workflow_max_wall_clock_seconds=300.0,
            default_workflow_reserve_tokens_per_assignment=40,
            default_workflow_reserve_cost_usd_per_assignment=0.25,
        )

        self.assertEqual(
            ["workflow.policy_set", "agent.policy_set", "task.assigned"],
            [item["topic"] for item in emitted],
        )
        workflow_policy, agent_policy, assignment = emitted
        self.assertEqual(
            workflow_policy["id"],
            assignment["payload"]["budget_reservation"]["policy_event_id"],
        )
        self.assertEqual(
            {"policy_event_id": workflow_policy["id"], "tokens": 40, "cost_usd": 0.25},
            assignment["payload"]["budget_reservation"],
        )
        self.assertEqual(
            agent_policy["id"], assignment["payload"]["agent_policy_event_id"]
        )
        restarted = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        control = restarted.workflows["default-budget"]
        self.assertEqual(100, control.max_total_tokens)
        self.assertEqual(1.0, control.max_total_cost_usd)
        self.assertEqual(5, control.max_attempts)
        self.assertEqual(300.0, control.max_wall_clock_seconds)

    def test_reported_usage_releases_reserve_and_missing_usage_keeps_it(self):
        worker = self._register(capacity=1)
        self._create("budget-flow", "first")
        second = self._create("budget-flow", "second")
        third = self._create("budget-flow", "third")
        self._budget_policy("budget-flow", tokens=100, reserve_tokens=60)
        state, cursor = self._projection()

        first_assignment = reconcile(
            state, DirectPMBus(), now=worker["ts"], cursor=cursor
        )[-1]
        self._report_usage(first_assignment, tokens=40, cost=0.2)
        self._complete(first_assignment)
        cursor.catch_up(DirectPMBus())
        usage = state.workflow_budget_usage("budget-flow")
        self.assertEqual(40, usage["tokens"])
        self.assertAlmostEqual(0.2, usage["cost_usd"])

        second_assignment = reconcile(
            state, DirectPMBus(), now=worker["ts"], cursor=cursor
        )[-1]
        self.assertEqual(second["payload"]["task_id"], second_assignment["payload"]["task_id"])
        self._complete(second_assignment)
        cursor.catch_up(DirectPMBus())

        usage = state.workflow_budget_usage("budget-flow")
        self.assertEqual(100, usage["tokens"])
        self.assertEqual(1, usage["missing_token_reports"])
        self.assertEqual(
            [], reconcile(state, DirectPMBus(), now=worker["ts"], cursor=cursor)
        )
        explanation = explain_task(
            state,
            third["payload"]["task_id"],
            now=worker["ts"],
            lease_seconds=20,
        )
        self.assertEqual("workflow_budget_limit", explanation["code"])
        self.assertEqual("tokens", explanation["details"]["resource"])

        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(
            state.workflow_budget_usage("budget-flow"),
            replayed.workflow_budget_usage("budget-flow"),
        )

    def test_actual_usage_above_reserve_is_authoritative_and_policy_can_extend(self):
        worker = self._register(capacity=1)
        self._create("overrun-flow", "first")
        second = self._create("overrun-flow", "second")
        self._budget_policy("overrun-flow", tokens=100, reserve_tokens=40)
        state, cursor = self._projection()
        assignment = reconcile(
            state, DirectPMBus(), now=worker["ts"], cursor=cursor
        )[-1]
        self._report_usage(assignment, tokens=80, cost=0.2)
        self._complete(assignment)
        cursor.catch_up(DirectPMBus())

        self.assertEqual(80, state.workflow_budget_usage("overrun-flow")["tokens"])
        self.assertEqual(
            [], reconcile(state, DirectPMBus(), now=worker["ts"], cursor=cursor)
        )
        extended = self._budget_policy(
            "overrun-flow", tokens=160, reserve_tokens=40, reason="extend"
        )
        cursor.catch_up(DirectPMBus())
        resumed = reconcile(
            state, DirectPMBus(), now=worker["ts"], cursor=cursor
        )[-1]
        self.assertEqual(second["payload"]["task_id"], resumed["payload"]["task_id"])
        self.assertEqual(
            extended["id"], resumed["payload"]["budget_reservation"]["policy_event_id"]
        )

    def test_attempt_and_wall_clock_budgets_block_admission(self):
        worker = self._register(capacity=1)
        self._create("attempt-flow", "first")
        second = self._create("attempt-flow", "second")
        bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": "one attempt",
                "max_attempts": 1,
            },
            correlation_id="attempt-flow",
        )
        state, cursor = self._projection()
        assignment = reconcile(
            state, DirectPMBus(), now=worker["ts"], cursor=cursor
        )[-1]
        self._complete(assignment)
        cursor.catch_up(DirectPMBus())
        explanation = explain_task(
            state,
            second["payload"]["task_id"],
            now=worker["ts"],
            lease_seconds=20,
        )
        self.assertEqual("workflow_budget_limit", explanation["code"])
        self.assertEqual("attempts", explanation["details"]["resource"])

        wall = self._create("wall-flow", "too late")
        bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": "short window",
                "max_wall_clock_seconds": 5,
            },
            correlation_id="wall-flow",
        )
        cursor.catch_up(DirectPMBus())
        wall_explanation = explain_task(
            state,
            wall["payload"]["task_id"],
            now=wall["ts"] + 6,
            lease_seconds=20,
        )
        self.assertEqual("workflow_budget_limit", wall_explanation["code"])
        self.assertEqual(
            "wall_clock_seconds", wall_explanation["details"]["resource"]
        )

    def test_agent_limit_crossing_assignment_is_fenced_and_reissued(self):
        worker = self._register(capacity=2)
        self._create("agent-race", "task")
        original = bus.append_event(
            "agent.policy_set",
            "human",
            {
                "agent_name": "alice",
                "source": "operator",
                "reason": "initial",
                "max_active_assignments": 2,
            },
        )
        state, cursor = self._projection()
        injected = False
        replacement = None

        def race(topic, payload, kwargs):
            nonlocal injected, replacement
            if topic == "task.assigned" and not injected:
                injected = True
                replacement = bus.append_event(
                    "agent.policy_set",
                    "human",
                    {
                        "agent_name": "alice",
                        "source": "operator",
                        "reason": "lower before publication",
                        "max_active_assignments": 1,
                    },
                )

        emitted = reconcile(
            state,
            DirectPMBus(race),
            now=worker["ts"],
            cursor=cursor,
        )
        assignments = [item for item in emitted if item["topic"] == "task.assigned"]
        self.assertEqual(2, len(assignments))
        self.assertEqual(original["id"], assignments[0]["payload"]["agent_policy_event_id"])
        self.assertEqual(replacement["id"], assignments[1]["payload"]["agent_policy_event_id"])
        self.assertEqual(2, assignments[1]["payload"]["attempt"])
        replayed = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        self.assertEqual(state.tasks[1].assignment_id, replayed.tasks[1].assignment_id)

    def test_agent_limit_is_visible_and_does_not_revoke_existing_work(self):
        worker = self._register(capacity=3)
        first = self._create("flow-a", "first")
        second = self._create("flow-b", "second")
        third = self._create("flow-c", "third")
        bus.append_event(
            "agent.policy_set",
            "human",
            {
                "agent_name": "alice",
                "source": "operator",
                "reason": "two expensive agent slots",
                "max_active_assignments": 2,
            },
        )
        state, cursor = self._projection()
        reconcile(state, DirectPMBus(), now=worker["ts"], cursor=cursor)
        self.assertEqual("assigned", state.tasks[first["payload"]["task_id"]].status)
        self.assertEqual("assigned", state.tasks[second["payload"]["task_id"]].status)
        policy = bus.append_event(
            "agent.policy_set",
            "human",
            {
                "agent_name": "alice",
                "source": "operator",
                "reason": "lower without revocation",
                "max_active_assignments": 1,
            },
        )
        cursor.catch_up(DirectPMBus())
        self.assertEqual("assigned", state.tasks[first["payload"]["task_id"]].status)
        self.assertEqual("assigned", state.tasks[second["payload"]["task_id"]].status)
        self.assertEqual("open", state.tasks[third["payload"]["task_id"]].status)
        explanation = explain_task(
            state,
            third["payload"]["task_id"],
            now=worker["ts"],
            lease_seconds=20,
        )
        self.assertEqual("agent_concurrency_limit", explanation["code"])
        view = worker_views(state, now=worker["ts"], lease_seconds=20)[0]
        self.assertEqual(3, view["capacity"])
        self.assertEqual(1, view["effective_capacity"])
        self.assertEqual(2, view["load"])
        self.assertEqual(policy["id"], view["policy"]["event_id"])

    def test_concurrency_patch_preserves_existing_budget_policy(self):
        self._register(capacity=1)
        self._create("patch-flow", "task")
        budget = self._budget_policy(
            "patch-flow", tokens=100, reserve_tokens=25
        )
        concurrency = bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": "change only concurrency",
                "max_active_assignments": 2,
            },
            correlation_id="patch-flow",
        )
        state = replay_events(bus.fetch_after(0, list(PM_TOPICS)))
        control = state.workflows["patch-flow"]
        self.assertEqual(concurrency["id"], control.policy_event_id)
        self.assertEqual(100, control.max_total_tokens)
        self.assertEqual(25, control.reserve_tokens_per_assignment)
        self.assertNotEqual(budget["id"], control.policy_event_id)

    def test_policy_and_usage_contracts_reject_unsafe_shapes(self):
        self._create("contract-flow", "task")
        with self.assertRaisesRegex(bus.EventValidationError, "changed together"):
            bus.append_event(
                "workflow.policy_set",
                "human",
                {
                    "source": "operator",
                    "reason": "incomplete",
                    "max_total_tokens": 100,
                },
                correlation_id="contract-flow",
            )
        with self.assertRaisesRegex(bus.EventValidationError, "producer"):
            bus.append_event(
                "workflow.usage_recorded",
                "alice",
                {
                    "task_id": 1,
                    "assignment_id": "task:1:attempt:1",
                    "worker_instance_id": "alice-1",
                    "invocation_id": "model-1",
                    "telemetry_event_id": 1,
                    "tokens": None,
                    "cost_usd": None,
                },
                caused_by=1,
                correlation_id="contract-flow",
            )

    def _register(self, *, capacity):
        return bus.append_event(
            "agent.registered",
            "alice",
            {
                "name": "alice",
                "instance_id": "alice-1",
                "capacity": capacity,
                "capabilities": [],
            },
        )

    def _create(self, correlation_id, title):
        return bus.append_event(
            "task.created", "human", {"title": title}, correlation_id=correlation_id
        )

    def _budget_policy(
        self,
        correlation_id,
        *,
        tokens,
        reserve_tokens,
        reason="budget",
    ):
        return bus.append_event(
            "workflow.policy_set",
            "human",
            {
                "source": "operator",
                "reason": reason,
                "max_total_tokens": tokens,
                "reserve_tokens_per_assignment": reserve_tokens,
                "max_total_cost_usd": 1.0,
                "reserve_cost_usd_per_assignment": 0.4,
            },
            correlation_id=correlation_id,
        )

    def _projection(self):
        state = PMState()
        cursor = OrderedProjectionCursor(state)
        cursor.consume(bus.fetch_after(0, list(PM_TOPICS)))
        return state, cursor

    def _report_usage(self, assignment, *, tokens, cost):
        context = AssignmentContext.from_event(assignment)
        sink = BusTelemetrySink(
            DirectWorkerBus(),
            producer=ProducerIdentity("test.worker", "alice-1", "0.10"),
        )
        started = sink.model_started(
            context,
            invocation_id=f"{context.assignment_id}:model:1",
            provider="test",
            model="test-model",
        )
        sink.model_completed(
            context,
            invocation_id=f"{context.assignment_id}:model:1",
            provider="test",
            model="test-model",
            duration_ms=10,
            usage={"total_tokens": tokens, "estimated_cost_usd": cost},
            caused_by=started["id"],
        )

    def _complete(self, assignment):
        bus.append_event(
            "task.completed",
            "alice",
            {
                "task_id": assignment["payload"]["task_id"],
                "assignment_id": assignment["payload"]["assignment_id"],
                "worker_instance_id": "alice-1",
                "summary": "done",
                "result": {},
            },
        )


class Phase3CLocalSurfaceTests(unittest.TestCase):
    def test_pm_environment_defaults_reject_invalid_values(self):
        with patch.dict(
            os.environ,
            {"LOGPLANE_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS": "4"},
            clear=True,
        ):
            self.assertEqual(4, _default_workflow_max_active_assignments())

        with patch.dict(os.environ, {"PHASE3C_LIMIT": "inf"}, clear=True):
            with self.assertRaisesRegex(SystemExit, "must be positive"):
                _optional_positive_env("PHASE3C_LIMIT", integer=False)

        with patch.dict(os.environ, {"PHASE3C_LIMIT": "0"}, clear=True):
            with self.assertRaisesRegex(SystemExit, "must be positive"):
                _optional_positive_env("PHASE3C_LIMIT", integer=True)

    def test_local_config_round_trip_and_legacy_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            path = initialize_local_config(directory)
            configured = LocalConfig.from_file(path)
            self.assertIsNone(configured.default_workflow_max_total_tokens)
            self.assertEqual(0, configured.default_workflow_reserve_tokens_per_assignment)
            value = json.loads(path.read_text(encoding="utf-8"))
            for field in list(value):
                if field.startswith("default_") and field != "default_workflow_max_active_assignments":
                    value.pop(field)
            path.write_text(json.dumps(value), encoding="utf-8")
            legacy = LocalConfig.from_file(path)
            with patch.dict(os.environ, {}, clear=True):
                legacy.apply_environment()
                self.assertEqual(
                    "unbounded", os.environ["LOGPLANE_DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS"]
                )
                self.assertEqual(
                    "0", os.environ["LOGPLANE_DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT"]
                )

    def test_client_and_cli_policy_surfaces_are_typed(self):
        class FakeClient(BusClient):
            def __init__(self):
                pass

            def publish(self, topic, payload, **kwargs):
                return {"id": 9, "topic": topic, "payload": payload, **kwargs}

        client = FakeClient()
        event = client.set_workflow_policy(
            "flow",
            max_total_tokens=100,
            reserve_tokens_per_assignment=25,
            reason="bound tokens",
        )
        self.assertEqual(100, event["payload"]["max_total_tokens"])
        with self.assertRaisesRegex(ValueError, "changed together"):
            client.set_workflow_policy(
                "flow", max_total_tokens=100, reason="unsafe"
            )

        with tempfile.TemporaryDirectory() as directory:
            config = initialize_local_config(directory)
            response = {
                "id": 12,
                "topic": "workflow.policy_set",
                "payload": {},
            }
            with patch("logplane.client.BusClient") as client_type:
                client_type.return_value.set_workflow_policy.return_value = response
                stdout = io.StringIO()
                code = logplane_cli.main(
                    [
                        "policy",
                        "flow",
                        "--max-tokens",
                        "100",
                        "--reserve-tokens-per-assignment",
                        "25",
                        "--reason",
                        "bound tokens",
                        "--config",
                        str(config),
                    ],
                    stdout=stdout,
                )
            self.assertEqual(0, code)
            client_type.return_value.set_workflow_policy.assert_called_once_with(
                "flow",
                reason="bound tokens",
                idempotency_key=None,
                max_total_tokens=100,
                reserve_tokens_per_assignment=25,
            )


if __name__ == "__main__":
    unittest.main()
