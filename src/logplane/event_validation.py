"""Pure validation of event envelopes and built-in payload contracts.

This module performs no I/O and reads no deployment configuration. Database
reference checks and HTTP error translation remain in logplane.bus. Keep
validation order and error messages stable when changing these contracts.
"""

import json
import math
from typing import Callable, NamedTuple, Optional

from logplane.limits import (
    MAX_ARTIFACT_BYTES,
    MAX_TELEMETRY_ARTIFACT_REFS,
    MAX_TELEMETRY_OBJECT_BYTES,
    MAX_EXTERNAL_ID_LENGTH,
    MAX_INLINE_CONTEXT_BYTES,
    MAX_INLINE_RESULT_BYTES,
    MAX_TASK_DEPENDENCIES,
)
from logplane.scheduling import DEFAULT_TASK_PRIORITY, validate_task_priority
from logplane.topics import KNOWN_TOPICS, TELEMETRY_TOPICS

CURRENT_SCHEMA_VERSION = 2
MAX_CORRELATION_ID_LENGTH = 128
MAX_PRODUCER_FIELD_LENGTH = 128


class EventValidationError(ValueError):
    """A known event does not satisfy its versioned contract."""


def _is_positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_positive_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _is_nonnegative_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _require_string(payload: dict, field: str) -> None:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"payload.{field} must be a non-empty string")


def _require_task_id(payload: dict) -> None:
    if not _is_positive_int(payload.get("task_id")):
        raise EventValidationError("payload.task_id must be a positive integer")


def _validate_json_object(
    payload: dict,
    field: str,
    *,
    max_bytes: int,
) -> None:
    value = payload.get(field, {})
    if not isinstance(value, dict):
        raise EventValidationError(f"payload.{field} must be a JSON object")
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            f"payload.{field} must contain JSON-compatible values"
        ) from exc
    if len(encoded) > max_bytes:
        raise EventValidationError(
            f"payload.{field} must not exceed {max_bytes} encoded bytes"
        )


def _validate_decisions(payload: dict) -> None:
    decisions = payload.get("decisions", [])
    if not isinstance(decisions, list):
        raise EventValidationError("payload.decisions must be a JSON array")
    try:
        json.dumps(decisions, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise EventValidationError(
            "payload.decisions must contain JSON-compatible values"
        ) from exc
    expected_fields = {
        "event_id",
        "actor",
        "assignment_id",
        "decision_id",
        "decision",
    }
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict) or set(decision) != expected_fields:
            raise EventValidationError(
                f"payload.decisions[{index}] has an invalid shape"
            )
        if not _is_positive_int(decision["event_id"]):
            raise EventValidationError(
                f"payload.decisions[{index}].event_id must be a positive integer"
            )
        for field in ("actor", "assignment_id", "decision_id"):
            value = decision[field]
            if not isinstance(value, str) or not value.strip():
                raise EventValidationError(
                    f"payload.decisions[{index}].{field} must be a non-empty string"
                )


def _validate_task_dependencies(payload: dict) -> None:
    depends_on = payload.get("depends_on", [])
    if not isinstance(depends_on, list):
        raise EventValidationError("payload.depends_on must be a JSON array")
    if len(depends_on) > MAX_TASK_DEPENDENCIES:
        raise EventValidationError(
            f"payload.depends_on must contain at most {MAX_TASK_DEPENDENCIES} tasks"
        )
    if not all(_is_positive_int(task_id) for task_id in depends_on):
        raise EventValidationError(
            "payload.depends_on must contain only positive task ids"
        )
    if len(depends_on) != len(set(depends_on)):
        raise EventValidationError("payload.depends_on must not contain duplicates")


def _validate_task_scheduling(payload: dict) -> None:
    try:
        validate_task_priority(payload.get("priority", DEFAULT_TASK_PRIORITY))
    except ValueError as exc:
        raise EventValidationError(f"payload.{exc}") from exc
    not_before = payload.get("not_before")
    if not_before is not None and not _is_positive_number(not_before):
        raise EventValidationError(
            "payload.not_before must be a positive finite timestamp"
        )
    deadline_at = payload.get("deadline_at")
    if (
        not_before is not None
        and deadline_at is not None
        and _is_positive_number(deadline_at)
        and not_before >= deadline_at
    ):
        raise EventValidationError(
            "payload.not_before must be earlier than payload.deadline_at"
        )


def _validate_control_attempt(payload: dict) -> None:
    last_assignment_id = payload.get("last_assignment_id")
    if last_assignment_id is not None and (
        not isinstance(last_assignment_id, str) or not last_assignment_id.strip()
    ):
        raise EventValidationError(
            "payload.last_assignment_id must be null or a non-empty string"
        )
    if not _is_nonnegative_int(payload.get("attempts")):
        raise EventValidationError(
            "payload.attempts must be a non-negative integer"
        )


def _validate_dependency_refs(payload: dict) -> None:
    refs = payload.get("dependency_refs", [])
    if not isinstance(refs, list):
        raise EventValidationError("payload.dependency_refs must be a JSON array")
    if len(refs) > MAX_TASK_DEPENDENCIES:
        raise EventValidationError(
            "payload.dependency_refs must contain at most "
            f"{MAX_TASK_DEPENDENCIES} references"
        )
    seen: set[int] = set()
    for index, ref in enumerate(refs):
        if not isinstance(ref, dict) or set(ref) != {
            "task_id",
            "completion_event_id",
        }:
            raise EventValidationError(
                f"payload.dependency_refs[{index}] has an invalid shape"
            )
        if not _is_positive_int(ref["task_id"]):
            raise EventValidationError(
                f"payload.dependency_refs[{index}].task_id must be a positive integer"
            )
        if not _is_positive_int(ref["completion_event_id"]):
            raise EventValidationError(
                "payload.dependency_refs"
                f"[{index}].completion_event_id must be a positive integer"
            )
        if ref["task_id"] in seen:
            raise EventValidationError(
                "payload.dependency_refs must not contain duplicate task ids"
            )
        seen.add(ref["task_id"])


def _validate_capabilities(payload: dict) -> None:
    required = payload.get("required_capabilities", [])
    if not isinstance(required, list) or not all(
        isinstance(item, str) and item.strip() for item in required
    ):
        raise EventValidationError(
            "payload.required_capabilities must be a list of strings"
        )


def _validate_retry_policy(payload: dict, *, allow_null: bool = False) -> None:
    if "retry_policy" not in payload:
        return
    retry_policy = payload["retry_policy"]
    max_retries = (
        retry_policy.get("max_retries")
        if isinstance(retry_policy, dict)
        else None
    )
    if (
        not isinstance(retry_policy, dict)
        or set(retry_policy) != {"max_retries"}
        or (
            max_retries is None
            and not allow_null
        )
        or (
            max_retries is not None
            and not _is_nonnegative_int(max_retries)
        )
    ):
        nullable = "null or " if allow_null else ""
        raise EventValidationError(
            "payload.retry_policy must contain only a "
            f"{nullable}non-negative integer max_retries"
        )


def _validate_external_origin(payload: dict, *, required: bool = False) -> None:
    origin = payload.get("external_origin")
    if origin is None and not required:
        return
    if not isinstance(origin, dict) or set(origin) != {"system", "task_ref"}:
        raise EventValidationError(
            "payload.external_origin must contain only system and task_ref"
        )
    for field in ("system", "task_ref"):
        value = origin.get(field)
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > MAX_EXTERNAL_ID_LENGTH
        ):
            raise EventValidationError(
                f"payload.external_origin.{field} must be a non-empty string "
                f"of at most {MAX_EXTERNAL_ID_LENGTH} characters"
            )


def _validate_ownership(payload: dict, *, observed: bool = False) -> None:
    ownership = payload.get("ownership")
    if "ownership" not in payload and not observed:
        return
    if not isinstance(ownership, dict) or set(ownership) != {"mode", "owner"}:
        raise EventValidationError(
            "payload.ownership must contain only mode and owner"
        )
    pair = (ownership.get("mode"), ownership.get("owner"))
    allowed = (
        {("shadow", "external"), ("canary", "external")}
        if observed
        else {("controlled", "agent-bus"), ("canary", "agent-bus")}
    )
    if pair not in allowed:
        raise EventValidationError(
            "payload.ownership mode and owner are not valid for this topic"
        )
    if ownership.get("mode") == "canary":
        _validate_external_origin(payload, required=True)


def _validate_correlation_id(correlation_id: Optional[str]) -> None:
    if correlation_id is None:
        return
    if (
        not isinstance(correlation_id, str)
        or not correlation_id.strip()
        or correlation_id != correlation_id.strip()
        or len(correlation_id) > MAX_CORRELATION_ID_LENGTH
    ):
        raise EventValidationError(
            "correlation_id must be null or a trimmed, non-empty string "
            f"of at most {MAX_CORRELATION_ID_LENGTH} characters"
        )


def _validate_producer(producer: Optional[dict]) -> None:
    if producer is None:
        return
    if not isinstance(producer, dict) or set(producer) != {
        "implementation",
        "instance_id",
        "version",
    }:
        raise EventValidationError(
            "producer must contain only implementation, instance_id, and version"
        )
    for field in ("implementation", "instance_id"):
        value = producer.get(field)
        if (
            not isinstance(value, str)
            or not value.strip()
            or value != value.strip()
            or len(value) > MAX_PRODUCER_FIELD_LENGTH
        ):
            raise EventValidationError(
                f"producer.{field} must be a trimmed, non-empty string of at most "
                f"{MAX_PRODUCER_FIELD_LENGTH} characters"
            )
    version = producer.get("version")
    if version is not None and (
        not isinstance(version, str)
        or not version.strip()
        or version != version.strip()
        or len(version) > MAX_PRODUCER_FIELD_LENGTH
    ):
        raise EventValidationError(
            "producer.version must be null or a trimmed, non-empty string of at "
            f"most {MAX_PRODUCER_FIELD_LENGTH} characters"
        )


def _validate_artifact_refs(payload: dict) -> None:
    refs = payload.get("artifacts", [])
    if not isinstance(refs, list):
        raise EventValidationError("payload.artifacts must be a JSON array")
    if len(refs) > MAX_TELEMETRY_ARTIFACT_REFS:
        raise EventValidationError(
            "payload.artifacts must contain at most "
            f"{MAX_TELEMETRY_ARTIFACT_REFS} references"
        )
    for index, ref in enumerate(refs):
        if not isinstance(ref, dict) or set(ref) != {
            "sha256",
            "size_bytes",
            "media_type",
            "kind",
        }:
            raise EventValidationError(
                f"payload.artifacts[{index}] has an invalid shape"
            )
        digest = ref.get("sha256")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise EventValidationError(
                f"payload.artifacts[{index}].sha256 must be a lowercase SHA-256 digest"
            )
        if not _is_nonnegative_int(ref.get("size_bytes")):
            raise EventValidationError(
                f"payload.artifacts[{index}].size_bytes must be a non-negative integer"
            )
        if ref["size_bytes"] > MAX_ARTIFACT_BYTES:
            raise EventValidationError(
                f"payload.artifacts[{index}].size_bytes exceeds the artifact limit"
            )
        for field in ("media_type", "kind"):
            value = ref.get(field)
            if (
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
                or len(value) > MAX_PRODUCER_FIELD_LENGTH
            ):
                raise EventValidationError(
                    f"payload.artifacts[{index}].{field} must be a non-empty string"
                )


def _validate_telemetry(payload: dict, topic: str) -> None:
    common_fields = {
        "task_id",
        "assignment_id",
        "worker_instance_id",
        "attributes",
        "artifacts",
    }
    is_model = topic.startswith("telemetry.model.")
    identity_fields = (
        {"invocation_id", "provider", "model"}
        if is_model
        else {"tool_call_id", "tool_name"}
    )
    status_fields: set[str] = set()
    if topic.endswith(".completed"):
        status_fields.add("duration_ms")
        if is_model:
            status_fields.add("usage")
    elif topic.endswith(".failed"):
        status_fields.update({"duration_ms", "error_code", "retryable"})
        if is_model:
            status_fields.add("usage")
    allowed_fields = common_fields | identity_fields | status_fields
    if not is_model:
        allowed_fields.add("invocation_id")
    unexpected_fields = set(payload) - allowed_fields
    if unexpected_fields:
        names = ", ".join(sorted(unexpected_fields))
        raise EventValidationError(f"telemetry payload has unexpected fields: {names}")

    _require_task_id(payload)
    for field in ("assignment_id", "worker_instance_id"):
        _require_telemetry_string(payload, field)
    _validate_artifact_refs(payload)
    _validate_json_object(
        payload,
        "attributes",
        max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
    )

    if is_model:
        for field in ("invocation_id", "provider", "model"):
            _require_telemetry_string(payload, field)
    else:
        for field in ("tool_call_id", "tool_name"):
            _require_telemetry_string(payload, field)
        if "invocation_id" in payload:
            _require_telemetry_string(payload, "invocation_id")

    if topic.endswith(".started"):
        return
    duration_ms = payload.get("duration_ms")
    if (
        not isinstance(duration_ms, (int, float))
        or isinstance(duration_ms, bool)
        or not math.isfinite(duration_ms)
        or duration_ms < 0
    ):
        raise EventValidationError("payload.duration_ms must be a non-negative number")
    if topic.endswith(".completed"):
        if is_model:
            _validate_json_object(
                payload,
                "usage",
                max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
            )
        return
    _require_telemetry_string(payload, "error_code")
    if not isinstance(payload.get("retryable"), bool):
        raise EventValidationError("payload.retryable must be a boolean")
    if is_model:
        _validate_json_object(
            payload,
            "usage",
            max_bytes=MAX_TELEMETRY_OBJECT_BYTES,
        )


def _require_telemetry_string(payload: dict, field: str) -> None:
    _require_string(payload, field)
    value = payload[field]
    if value != value.strip() or len(value) > MAX_PRODUCER_FIELD_LENGTH:
        raise EventValidationError(
            f"payload.{field} must be trimmed and at most "
            f"{MAX_PRODUCER_FIELD_LENGTH} characters"
        )


class _Event(NamedTuple):
    """The fields a payload validator may need, besides the payload itself."""

    topic: str
    actor: str
    payload: dict
    caused_by: Optional[int]
    correlation_id: Optional[str]
    producer: Optional[dict]


def validate_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
    schema_version: int,
    correlation_id: Optional[str] = None,
    producer: Optional[dict] = None,
) -> None:
    """Validate common fields and v2 contracts for built-in event topics.

    Version 1 remains replayable for databases created by the prototype. New
    publishes default to v2. Unknown topics are deliberately allowed so the
    bus stays an extensible coordination substrate rather than a task-board API.
    Each built-in topic's payload rules live in one function in the tables
    below; task-scoped topics check ``task_id`` before their own rules.
    """
    _validate_envelope(topic, actor, schema_version, idempotency_key, caused_by,
                       correlation_id, producer)
    if topic not in KNOWN_TOPICS:
        return
    if schema_version != CURRENT_SCHEMA_VERSION:
        raise EventValidationError(
            f"unsupported schema_version {schema_version} for known topic {topic!r}"
        )
    if topic in TELEMETRY_TOPICS:
        if producer is None:
            raise EventValidationError("known telemetry events require producer identity")
        _validate_telemetry(payload, topic)
        return

    event = _Event(topic, actor, payload, caused_by, correlation_id, producer)
    validator = _PAYLOAD_VALIDATORS.get(topic)
    if validator is not None:
        validator(event)
        return
    _require_task_id(payload)
    validator = _TASK_VALIDATORS.get(topic)
    if validator is not None:
        validator(event)


def _validate_envelope(topic, actor, schema_version, idempotency_key, caused_by,
                       correlation_id, producer) -> None:
    if not isinstance(topic, str) or not topic.strip():
        raise EventValidationError("topic must be a non-empty string")
    if not isinstance(actor, str) or not actor.strip():
        raise EventValidationError("actor must be a non-empty string")
    if not _is_positive_int(schema_version):
        raise EventValidationError("schema_version must be a positive integer")
    if idempotency_key is not None and (
        not isinstance(idempotency_key, str) or not idempotency_key.strip()
    ):
        raise EventValidationError("idempotency_key must be null or a non-empty string")
    if caused_by is not None and not _is_positive_int(caused_by):
        raise EventValidationError("caused_by must be null or a positive event id")
    _validate_correlation_id(correlation_id)
    _validate_producer(producer)


# -- shared field rules -------------------------------------------------------

def _require_pm(event: _Event) -> None:
    if event.actor != "pm":
        raise EventValidationError(f"{event.topic} must be emitted by pm")


def _require_correlation(event: _Event) -> None:
    if not isinstance(event.correlation_id, str) or not event.correlation_id.strip():
        raise EventValidationError(f"{event.topic} requires a non-empty correlation_id")


def _require_positive_int_field(payload: dict, field: str) -> None:
    if not _is_positive_int(payload.get(field)):
        raise EventValidationError(f"payload.{field} must be a positive integer")


def _require_nonnegative_int_field(payload: dict, field: str, default: object = None) -> None:
    if not _is_nonnegative_int(payload.get(field, default)):
        raise EventValidationError(f"payload.{field} must be a non-negative integer")


def _optional_positive_int_field(payload: dict, field: str) -> None:
    value = payload.get(field)
    if value is not None and not _is_positive_int(value):
        raise EventValidationError(f"payload.{field} must be null or a positive integer")


def _optional_positive_number_field(payload: dict, field: str) -> None:
    value = payload.get(field)
    if value is not None and not _is_positive_number(value):
        raise EventValidationError(f"payload.{field} must be null or a positive number")


def _optional_nonempty_string(payload: dict, field: str) -> None:
    value = payload.get(field)
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise EventValidationError(f"payload.{field} must be null or a non-empty string")


def _optional_deadline(payload: dict) -> None:
    deadline_at = payload.get("deadline_at")
    if deadline_at is not None and not _is_positive_number(deadline_at):
        raise EventValidationError("payload.deadline_at must be a positive finite timestamp")


def _validate_policy_source(event: _Event, kind: str) -> None:
    source = event.payload.get("source")
    if source not in {"default", "operator"}:
        raise EventValidationError("payload.source must be default or operator")
    if source == "default" and event.actor != "pm":
        raise EventValidationError(f"default {kind} policy must be emitted by pm")
    if source == "operator" and event.actor == "pm":
        raise EventValidationError(f"operator {kind} policy must not be emitted by pm")


def _validate_budget_pair(payload: dict, kind: str, limit: str, reserve: str) -> None:
    fields = {limit, reserve}
    if fields.intersection(payload) and not fields.issubset(payload):
        raise EventValidationError(f"{kind} budget and reservation must be changed together")
    if payload.get(limit) is not None and not 0 < payload[reserve] <= payload[limit]:
        raise EventValidationError(
            f"finite {kind} budget requires a positive reservation no larger than the budget"
        )


# -- tasks entering the log ----------------------------------------------------

def _task_created(event: _Event) -> None:
    payload = event.payload
    if "task_id" in payload:
        _require_task_id(payload)
    _require_string(payload, "title")
    _validate_capabilities(payload)
    _validate_json_object(payload, "context", max_bytes=MAX_INLINE_CONTEXT_BYTES)
    _validate_retry_policy(payload)
    _validate_task_dependencies(payload)
    _validate_task_scheduling(payload)
    _validate_external_origin(payload)
    _validate_ownership(payload)
    supersedes_task_id = payload.get("supersedes_task_id")
    if supersedes_task_id is not None:
        _require_positive_int_field(payload, "supersedes_task_id")
        _require_string(payload, "supersession_reason")
        if supersedes_task_id in payload.get("depends_on", []):
            raise EventValidationError(
                "a replacement task cannot depend on the task it supersedes"
            )
    elif payload.get("supersession_reason") is not None:
        raise EventValidationError(
            "payload.supersession_reason requires payload.supersedes_task_id"
        )
    _optional_deadline(payload)


def _task_observed(event: _Event) -> None:
    payload = event.payload
    _require_string(payload, "title")
    _validate_capabilities(payload)
    _validate_json_object(payload, "context", max_bytes=MAX_INLINE_CONTEXT_BYTES)
    _validate_retry_policy(payload)
    _validate_task_scheduling(payload)
    _validate_external_origin(payload, required=True)
    _validate_ownership(payload, observed=True)
    _optional_deadline(payload)


# -- agents ---------------------------------------------------------------------

def _agent_presence(event: _Event) -> None:
    payload = event.payload
    _require_string(payload, "name")
    _require_string(payload, "instance_id")
    if payload["name"] != event.actor:
        raise EventValidationError("agent event actor must match payload.name")
    if event.topic == "agent.registered":
        if not _is_positive_int(payload.get("capacity", 1)):
            raise EventValidationError("payload.capacity must be a positive integer")
        capabilities = payload.get("capabilities", [])
        if not isinstance(capabilities, list) or not all(
            isinstance(item, str) and item.strip() for item in capabilities
        ):
            raise EventValidationError("payload.capabilities must be a list of strings")


def _agent_policy_set(event: _Event) -> None:
    if event.correlation_id is not None:
        raise EventValidationError("agent.policy_set must not have correlation_id")
    _require_string(event.payload, "agent_name")
    _validate_policy_source(event, "agent")
    _require_string(event.payload, "reason")
    _optional_positive_int_field(event.payload, "max_active_assignments")


# -- workflows ------------------------------------------------------------------

WORKFLOW_POLICY_FIELDS = frozenset({
    "max_active_assignments",
    "max_total_tokens",
    "max_total_cost_usd",
    "max_attempts",
    "max_wall_clock_seconds",
    "reserve_tokens_per_assignment",
    "reserve_cost_usd_per_assignment",
})


def _workflow_control_requested(event: _Event) -> None:
    _require_correlation(event)
    _require_string(event.payload, "reason")


def _workflow_policy_set(event: _Event) -> None:
    payload = event.payload
    _require_correlation(event)
    _validate_policy_source(event, "workflow")
    _require_string(payload, "reason")
    for field in ("max_active_assignments", "max_total_tokens", "max_attempts"):
        _optional_positive_int_field(payload, field)
    for field in ("max_total_cost_usd", "max_wall_clock_seconds"):
        _optional_positive_number_field(payload, field)
    if "reserve_tokens_per_assignment" in payload:
        _require_nonnegative_int_field(payload, "reserve_tokens_per_assignment")
    if "reserve_cost_usd_per_assignment" in payload and not _is_nonnegative_number(
        payload["reserve_cost_usd_per_assignment"]
    ):
        raise EventValidationError(
            "payload.reserve_cost_usd_per_assignment must be a non-negative number"
        )
    _validate_budget_pair(payload, "token", "max_total_tokens", "reserve_tokens_per_assignment")
    _validate_budget_pair(payload, "cost", "max_total_cost_usd", "reserve_cost_usd_per_assignment")
    if not WORKFLOW_POLICY_FIELDS.intersection(payload):
        raise EventValidationError("workflow.policy_set must change at least one policy field")


USAGE_RECORDED_FIELDS = frozenset({
    "task_id",
    "assignment_id",
    "worker_instance_id",
    "invocation_id",
    "telemetry_event_id",
    "tokens",
    "cost_usd",
})


def _usage_recorded(event: _Event) -> None:
    payload = event.payload
    if event.actor == "pm":
        raise EventValidationError("workflow.usage_recorded must be emitted by a worker")
    if event.producer is None:
        raise EventValidationError("workflow.usage_recorded requires producer identity")
    _require_correlation(event)
    if set(payload) != USAGE_RECORDED_FIELDS:
        raise EventValidationError("workflow.usage_recorded has an invalid payload shape")
    _require_task_id(payload)
    for field in ("assignment_id", "worker_instance_id", "invocation_id"):
        _require_telemetry_string(payload, field)
    _require_positive_int_field(payload, "telemetry_event_id")
    if event.caused_by != payload["telemetry_event_id"]:
        raise EventValidationError(
            "workflow.usage_recorded must be caused by its telemetry event"
        )
    tokens = payload.get("tokens")
    if tokens is not None and not _is_nonnegative_int(tokens):
        raise EventValidationError("payload.tokens must be null or a non-negative integer")
    cost = payload.get("cost_usd")
    if cost is not None and not _is_nonnegative_number(cost):
        raise EventValidationError("payload.cost_usd must be null or a non-negative number")


def _workflow_control_applied(event: _Event) -> None:
    payload = event.payload
    paused = event.topic == "workflow.paused"
    _require_pm(event)
    _require_correlation(event)
    _require_string(payload, "reason")
    _require_positive_int_field(
        payload, "pause_request_event_id" if paused else "resume_request_event_id"
    )
    if not paused:
        return
    interrupted = payload.get("interrupted_assignments")
    if not isinstance(interrupted, list):
        raise EventValidationError("payload.interrupted_assignments must be a JSON array")
    for index, item in enumerate(interrupted):
        if (
            not isinstance(item, dict)
            or set(item) != {"task_id", "assignment_id"}
            or not _is_positive_int(item.get("task_id"))
            or not isinstance(item.get("assignment_id"), str)
            or not item["assignment_id"].strip()
        ):
            raise EventValidationError(
                f"payload.interrupted_assignments[{index}] has an invalid shape"
            )


# -- assignment -----------------------------------------------------------------

def _task_assigned(event: _Event) -> None:
    payload = event.payload
    for field in ("assignment_id", "assignee", "worker_instance_id"):
        _require_string(payload, field)
    _require_positive_int_field(payload, "attempt")
    _optional_positive_int_field(payload, "workflow_policy_event_id")
    _optional_positive_int_field(payload, "agent_policy_event_id")
    _validate_budget_reservation(payload.get("budget_reservation"))
    _validate_fairness(payload.get("fairness"))
    _validate_capabilities(payload)
    _validate_json_object(payload, "context", max_bytes=MAX_INLINE_CONTEXT_BYTES)
    _validate_decisions(payload)
    _validate_dependency_refs(payload)
    _validate_retry_policy(payload, allow_null=True)
    _require_nonnegative_int_field(payload, "retryable_failures", default=0)
    _validate_external_origin(payload)
    _validate_ownership(payload)
    _optional_deadline(payload)


def _validate_budget_reservation(reservation: object) -> None:
    if reservation is None:
        return
    if not isinstance(reservation, dict) or set(reservation) != {
        "policy_event_id", "tokens", "cost_usd",
    }:
        raise EventValidationError("payload.budget_reservation has an invalid shape")
    if not _is_positive_int(reservation.get("policy_event_id")):
        raise EventValidationError("payload.budget_reservation.policy_event_id must be positive")
    if not _is_nonnegative_int(reservation.get("tokens")):
        raise EventValidationError("payload.budget_reservation.tokens must be non-negative")
    if not _is_nonnegative_number(reservation.get("cost_usd")):
        raise EventValidationError("payload.budget_reservation.cost_usd must be non-negative")


def _validate_fairness(fairness: object) -> None:
    if fairness is None:
        return
    if not isinstance(fairness, dict) or set(fairness) != {
        "policy", "previous_assignment_event_id",
    }:
        raise EventValidationError(
            "payload.fairness must contain policy and previous_assignment_event_id"
        )
    if fairness.get("policy") != "workflow_round_robin_v1":
        raise EventValidationError("payload.fairness.policy must be workflow_round_robin_v1")
    previous = fairness.get("previous_assignment_event_id")
    if previous is not None and not _is_positive_int(previous):
        raise EventValidationError(
            "payload.fairness.previous_assignment_event_id must be null or a positive integer"
        )


# -- operator control and PM decisions --------------------------------------------

def _reason_only(event: _Event) -> None:
    _require_string(event.payload, "reason")


def _task_paused(event: _Event) -> None:
    _require_pm(event)
    _require_positive_int_field(event.payload, "pause_request_event_id")
    _require_string(event.payload, "reason")
    _require_resume_status(event.payload)
    _validate_control_attempt(event.payload)


def _task_resumed(event: _Event) -> None:
    _require_pm(event)
    _require_positive_int_field(event.payload, "resume_request_event_id")
    _require_string(event.payload, "reason")
    _require_resume_status(event.payload)


def _require_resume_status(payload: dict) -> None:
    if payload.get("resume_status") not in {"open", "blocked"}:
        raise EventValidationError("payload.resume_status must be open or blocked")


def _task_superseded(event: _Event) -> None:
    _require_pm(event)
    _require_positive_int_field(event.payload, "replacement_task_id")
    _require_positive_int_field(event.payload, "replacement_created_event_id")
    _require_string(event.payload, "reason")
    _validate_control_attempt(event.payload)


def _task_cancelled(event: _Event) -> None:
    _require_pm(event)
    _require_positive_int_field(event.payload, "cancel_request_event_id")
    _require_string(event.payload, "reason")
    _optional_nonempty_string(event.payload, "last_assignment_id")
    _require_nonnegative_int_field(event.payload, "attempts")


def _task_deadline_exceeded(event: _Event) -> None:
    _require_pm(event)
    if not _is_positive_number(event.payload.get("deadline_at")):
        raise EventValidationError("payload.deadline_at must be a positive finite timestamp")
    _optional_nonempty_string(event.payload, "last_assignment_id")
    _require_nonnegative_int_field(event.payload, "attempts")


def _task_failed(event: _Event) -> None:
    payload = event.payload
    for field in ("reason_code", "reason", "last_assignment_id"):
        _require_string(payload, field)
    _require_positive_int_field(payload, "attempts")
    _require_nonnegative_int_field(payload, "retryable_failures")
    max_retries = payload.get("max_retries")
    if max_retries is not None and not _is_nonnegative_int(max_retries):
        raise EventValidationError("payload.max_retries must be null or a non-negative integer")


def _task_dependency_failed(event: _Event) -> None:
    _require_pm(event)
    _require_positive_int_field(event.payload, "dependency_task_id")
    _require_positive_int_field(event.payload, "dependency_event_id")
    _require_string(event.payload, "reason")


def _task_retry_requested(event: _Event) -> None:
    _require_positive_int_field(event.payload, "additional_retries")
    _require_string(event.payload, "reason")


# -- worker outcomes and decisions --------------------------------------------------

def _attempt_progress(event: _Event) -> None:
    payload = event.payload
    _require_string(payload, "assignment_id")
    _require_string(payload, "worker_instance_id")
    if event.topic == "task.blocked":
        _require_string(payload, "reason")
    if event.topic == "task.completed" and "result" in payload:
        _validate_json_object(payload, "result", max_bytes=MAX_INLINE_RESULT_BYTES)


def _attempt_failed(event: _Event) -> None:
    for field in ("assignment_id", "worker_instance_id", "failure_code", "reason"):
        _require_string(event.payload, field)
    if not isinstance(event.payload.get("retryable"), bool):
        raise EventValidationError("payload.retryable must be a boolean")


def _assignment_expired(event: _Event) -> None:
    for field in ("assignment_id", "assignee", "worker_instance_id", "reason"):
        _require_string(event.payload, field)


def _decision_needed(event: _Event) -> None:
    for field in ("assignment_id", "decision_id", "reason"):
        _require_string(event.payload, field)


def _decision_made(event: _Event) -> None:
    for field in ("assignment_id", "decision_id"):
        _require_string(event.payload, field)
    if "decision" not in event.payload:
        raise EventValidationError("payload.decision is required")
    try:
        json.dumps(event.payload["decision"], allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise EventValidationError("payload.decision must be JSON-compatible") from exc


Validator = Callable[[_Event], None]

# Topics whose payload is not tied to one existing task.
_PAYLOAD_VALIDATORS: dict[str, Validator] = {
    "task.created": _task_created,
    "integration.task_observed": _task_observed,
    "agent.registered": _agent_presence,
    "agent.heartbeat": _agent_presence,
    "agent.policy_set": _agent_policy_set,
    "workflow.pause_requested": _workflow_control_requested,
    "workflow.resume_requested": _workflow_control_requested,
    "workflow.policy_set": _workflow_policy_set,
    "workflow.usage_recorded": _usage_recorded,
    "workflow.paused": _workflow_control_applied,
    "workflow.resumed": _workflow_control_applied,
}

# Task-scoped topics; validate_event checks payload.task_id before these run.
_TASK_VALIDATORS: dict[str, Validator] = {
    "task.assigned": _task_assigned,
    "task.cancel_requested": _reason_only,
    "task.pause_requested": _reason_only,
    "task.resume_requested": _reason_only,
    "task.paused": _task_paused,
    "task.resumed": _task_resumed,
    "task.superseded": _task_superseded,
    "task.cancelled": _task_cancelled,
    "task.deadline_exceeded": _task_deadline_exceeded,
    "task.started": _attempt_progress,
    "task.completed": _attempt_progress,
    "task.blocked": _attempt_progress,
    "task.attempt_failed": _attempt_failed,
    "task.assignment_expired": _assignment_expired,
    "task.failed": _task_failed,
    "task.dependency_failed": _task_dependency_failed,
    "task.retry_requested": _task_retry_requested,
    "decision.needed": _decision_needed,
    "decision.made": _decision_made,
}
