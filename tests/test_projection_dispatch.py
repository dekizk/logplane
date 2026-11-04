"""Guards for apply_event's topic routing tables."""

import inspect
import unittest

from logplane import projection
from logplane.projection import PMState, apply_event
from logplane.topics import COORDINATION_TOPICS

TABLES = {
    "agent_and_usage": projection._AGENT_AND_USAGE_HANDLERS,
    "workflow": projection._WORKFLOW_HANDLERS,
    "task": projection._TASK_HANDLERS,
}
COMMON = ["state", "ev", "payload", "event_id", "timestamp"]
EXPECTED_PARAMETERS = {
    "agent_and_usage": COMMON,
    "workflow": COMMON + ["correlation_id", "control", "reason"],
    "task": COMMON + ["task_id", "task"],
}


class DispatchTableTests(unittest.TestCase):
    def test_tables_and_task_created_cover_exactly_the_coordination_topics(self):
        routed = {"task.created"}
        for table in TABLES.values():
            routed |= set(table)
        self.assertEqual(set(COORDINATION_TOPICS), routed)

    def test_tables_are_disjoint_and_task_created_is_routed_directly(self):
        seen = {}
        for name, table in TABLES.items():
            for topic in table:
                self.assertNotIn(topic, seen, f"{topic} is in both {seen.get(topic)} and {name}")
                seen[topic] = name
        self.assertNotIn("task.created", seen)

    def test_special_routing_cases(self):
        # Usage accounting is a workflow.* topic but must bypass the workflow
        # control checks, which would create a control record as a side effect.
        self.assertIn("workflow.usage_recorded", projection._AGENT_AND_USAGE_HANDLERS)
        self.assertNotIn("workflow.usage_recorded", projection._WORKFLOW_HANDLERS)
        for topic in projection._WORKFLOW_HANDLERS:
            self.assertTrue(topic.startswith("workflow."), topic)
        for topic in projection._TASK_HANDLERS:
            self.assertFalse(topic.startswith("workflow."), topic)

    def test_each_topic_runs_its_own_handler(self):
        for name, table in TABLES.items():
            for topic, handler in table.items():
                with self.subTest(table=name, topic=topic):
                    self.assertEqual("_apply_" + topic.replace(".", "_"), handler.__name__)

    def test_handler_signatures_match_their_table(self):
        # A mismatch would raise TypeError inside apply_event, which treats it
        # as a rejected event instead of failing loudly.
        for name, table in TABLES.items():
            for topic, handler in table.items():
                with self.subTest(table=name, topic=topic):
                    parameters = list(inspect.signature(handler).parameters)
                    self.assertEqual(EXPECTED_PARAMETERS[name], parameters)

    def test_unrouted_topics_are_rejected(self):
        for topic in ("unknown.topic", "workflow.unknown", "task.unknown"):
            with self.subTest(topic=topic):
                state = PMState()
                event = {"id": 1, "ts": 1.0, "topic": topic, "payload": {"task_id": 1}}
                self.assertFalse(apply_event(state, event))


if __name__ == "__main__":
    unittest.main()
