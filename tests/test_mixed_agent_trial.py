"""Offline acceptance checks for the real mixed-agent DAG example."""

import copy
import unittest

from examples.openclaw.mixed_trial import verify_handoff
from tests.test_pm_agent import assigned, created, event, registered


class MixedHandoffTests(unittest.TestCase):
    def setUp(self):
        self.expected = {"handoff_marker": "fresh-marker", "numbers": [17, 25, 9]}
        self.events = [
            registered(1, name="hermes", instance="h"),
            registered(2, name="openclaw", instance="o"),
            created(3, task_id=1, correlation_id="mixed"),
            created(4, task_id=2, correlation_id="mixed", depends_on=[1]),
            assigned(5, 1, worker="hermes", instance="h"),
            event(6, "task.started", "hermes", {"task_id": 1, "assignment_id": "task:1:attempt:1", "worker_instance_id": "h"}, caused_by=5),
            event(7, "task.completed", "hermes", {"task_id": 1, "assignment_id": "task:1:attempt:1", "worker_instance_id": "h",
                                                  "summary": "produced", "result": copy.deepcopy(self.expected)}, caused_by=5),
            assigned(8, 1, task_id=2, worker="openclaw", instance="o"),
            event(9, "task.started", "openclaw", {"task_id": 2, "assignment_id": "task:2:attempt:1", "worker_instance_id": "o"}, caused_by=8),
            event(10, "task.completed", "openclaw", {"task_id": 2, "assignment_id": "task:2:attempt:1", "worker_instance_id": "o",
                                                    "summary": "consumed", "result": {"handoff_marker": "fresh-marker", "total": 51,
                                                                                        "source_task_id": 1, "source_completion_event_id": 7}}, caused_by=8),
        ]
        for item in self.events:
            item["correlation_id"] = "mixed"
        self.events[3]["payload"]["context"] = {}
        self.events[7]["payload"].update(context={}, dependency_refs=[{"task_id": 1, "completion_event_id": 7}])
        self.deliveries = [{"task_id": 2, "dependencies": [{"task_id": 1, "completion_event_id": 7,
                                                         "summary": "produced", "result": copy.deepcopy(self.expected)}]}]

    def checks(self):
        return verify_handoff(self.events, 1, 2, self.expected, self.deliveries)

    def test_valid_handoff_replays_and_passes_all_checks(self):
        checks = self.checks()
        self.assertTrue(all(checks.values()), checks)

    def test_detects_manual_copy_and_incorrect_dependency_delivery(self):
        self.events[3]["payload"]["context"] = copy.deepcopy(self.expected)
        self.deliveries[0]["dependencies"][0]["result"] = {"numbers": [42]}
        checks = self.checks()
        self.assertFalse(checks["no_manual_result_copy"])
        self.assertFalse(checks["runtime_resolved_result"])

    def test_detects_wrong_result_and_wrong_event_reference(self):
        self.events[-1]["payload"]["result"]["total"] = 42
        self.events[7]["payload"]["dependency_refs"][0]["completion_event_id"] = 6
        checks = self.checks()
        self.assertFalse(checks["consumer_result_correct"])
        self.assertFalse(checks["assignment_uses_completion_reference"])

    def test_detects_early_assignment_and_lost_causal_edge(self):
        self.events[7]["id"] = 6
        self.events[-1]["caused_by"] = None
        checks = self.checks()
        self.assertFalse(checks["child_not_assigned_early"])
        self.assertFalse(checks["lifecycle_causes_preserved"])

    def test_extra_attempt_is_not_a_single_attempt_success(self):
        self.events.append(copy.deepcopy(self.events[7]))
        with self.assertRaisesRegex(ValueError, "exactly one task.assigned"):
            self.checks()


if __name__ == "__main__":
    unittest.main()
