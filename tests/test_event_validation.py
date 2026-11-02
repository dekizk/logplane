"""Event contracts remain usable through both the extracted and legacy imports."""

import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from logplane import bus, event_validation


def validate(topic="task.created", actor="human", payload=None, **overrides):
    fields = dict(topic=topic, actor=actor, payload={"title": "demo"} if payload is None else payload,
                  caused_by=None, idempotency_key=None, schema_version=2)
    fields.update(overrides)
    return event_validation.validate_event(**fields)


class EventValidationTests(unittest.TestCase):
    def test_bus_keeps_the_existing_validation_entry_points(self):
        for name in ("validate_event", "EventValidationError", "_validate_correlation_id",
                     "_is_positive_int", "_is_nonnegative_int", "_is_nonnegative_number"):
            with self.subTest(name=name):
                self.assertIs(getattr(bus, name), getattr(event_validation, name))
        for name in ("CURRENT_SCHEMA_VERSION", "MAX_CORRELATION_ID_LENGTH"):
            self.assertEqual(getattr(bus, name), getattr(event_validation, name))
        self.assertTrue(issubclass(event_validation.EventValidationError, ValueError))

    def test_every_built_in_topic_has_exactly_one_payload_validator(self):
        # A topic added to KNOWN_TOPICS without a validator would otherwise be
        # accepted with only a task_id check.
        payload_topics = set(event_validation._PAYLOAD_VALIDATORS)
        task_topics = set(event_validation._TASK_VALIDATORS)
        self.assertEqual(set(), payload_topics & task_topics)
        self.assertEqual(set(event_validation.KNOWN_TOPICS) - set(event_validation.TELEMETRY_TOPICS),
                         payload_topics | task_topics)

    def test_valid_payloads_are_checked_without_normalizing_or_mutating_them(self):
        payload = {"title": " demo ", "depends_on": [1, 2], "context": {"nested": [1, 2]},
                   "retry_policy": {"max_retries": 0}}
        original = copy.deepcopy(payload)
        self.assertIsNone(validate(payload=payload))
        self.assertEqual(original, payload)
        # Assignment replay may carry an explicitly unbounded legacy retry policy.
        self.assertIsNone(validate("task.assigned", "pm", {
            "task_id": 3, "assignment_id": "task:3:attempt:1", "assignee": "alice",
            "worker_instance_id": "alice-1", "attempt": 1,
            "retry_policy": {"max_retries": None},
        }))

    def test_envelope_and_payload_error_messages_are_unchanged(self):
        cases = (
            ({"topic": " "}, "topic must be a non-empty string"),
            ({"actor": " "}, "actor must be a non-empty string"),
            ({"schema_version": True}, "schema_version must be a positive integer"),
            ({"schema_version": 1}, "unsupported schema_version 1 for known topic 'task.created'"),
            ({"caused_by": True}, "caused_by must be null or a positive event id"),
            ({"correlation_id": " flow "},
             "correlation_id must be null or a trimmed, non-empty string of at most 128 characters"),
            ({"payload": {}}, "payload.title must be a non-empty string"),
            ({"payload": {"title": "demo", "depends_on": [1, 1]}},
             "payload.depends_on must not contain duplicates"),
            ({"payload": {"title": "demo", "context": {"value": float("nan")}}},
             "payload.context must contain JSON-compatible values"),
            ({"payload": {"title": "demo", "context": {
                "value": "x" * event_validation.MAX_INLINE_CONTEXT_BYTES}}},
             f"payload.context must not exceed {event_validation.MAX_INLINE_CONTEXT_BYTES} encoded bytes"),
        )
        for fields, message in cases:
            with self.subTest(fields=fields):
                with self.assertRaises(bus.EventValidationError) as raised:
                    validate(**fields)
                self.assertEqual(message, str(raised.exception))

    def test_unknown_topics_keep_the_extension_contract(self):
        self.assertIsNone(validate("extension.note", payload={"arbitrary": [1, 2]}, schema_version=7))
        # Unknown payload contracts still require a valid common envelope.
        with self.assertRaisesRegex(bus.EventValidationError, "actor must be"):
            validate("extension.note", actor=" ", payload={}, schema_version=7)

    def test_policy_and_telemetry_validation_remain_strict(self):
        with self.assertRaises(bus.EventValidationError) as raised:
            validate("workflow.policy_set", payload={"source": "operator", "reason": "limit",
                     "max_total_tokens": 100}, correlation_id="flow")
        self.assertEqual("token budget and reservation must be changed together", str(raised.exception))

        producer = {"implementation": "example", "instance_id": "alice-1", "version": None}
        payload = {"task_id": 1, "assignment_id": "task:1:attempt:1",
                   "worker_instance_id": "alice-1", "invocation_id": "call-1",
                   "provider": "example", "model": "example", "duration_ms": 0}
        self.assertIsNone(validate("telemetry.model.completed", "alice", payload, producer=producer))
        with self.assertRaises(bus.EventValidationError) as raised:
            validate("telemetry.model.completed", "alice", payload)
        self.assertEqual("known telemetry events require producer identity", str(raised.exception))
        with self.assertRaises(bus.EventValidationError) as raised:
            validate("telemetry.model.completed", "alice", {**payload, "duration_ms": float("nan")},
                     producer=producer)
        self.assertEqual("payload.duration_ms must be a non-negative number", str(raised.exception))

    def test_import_does_not_start_or_configure_the_bus(self):
        source = Path(__file__).resolve().parents[1] / "src"
        with tempfile.TemporaryDirectory() as directory:
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith(("LOGPLANE_", "AGENT_BUS_"))}
            env.update(PYTHONPATH=str(source), PYTHONDONTWRITEBYTECODE="1",
                       LOGPLANE_TOKEN="", LOGPLANE_MAX_RETRIES="invalid",
                       AGENT_BUS_TOKEN="legacy-setting", LOGPLANE_DB_PATH=str(Path(directory) / "events.db"))
            result = subprocess.run([
                sys.executable, "-c",
                "import sys\n"
                "from logplane.event_validation import validate_event\n"
                "validate_event('task.created', 'human', {'title': 'demo'}, None, None, 2)\n"
                "assert 'logplane.bus' not in sys.modules\n"
                "assert 'fastapi' not in sys.modules\n",
            ], env=env, cwd=directory, capture_output=True, text=True, timeout=15)
            self.assertEqual(0, result.returncode, result.stdout + result.stderr)
            self.assertEqual([], list(Path(directory).iterdir()))


if __name__ == "__main__":
    unittest.main()
