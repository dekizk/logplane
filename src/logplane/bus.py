"""
logplane: an append-only event bus for local multi-agent orchestration.

Events are immutable SQLite rows. Consumers rebuild state by replaying the log,
then follow the same ordered stream over SSE. Versioned contracts protect known
orchestration events while unknown topics remain available to extensions.

Run: python -m uvicorn bus:app --port 8765
"""

import asyncio
import contextlib
import json
import math
import os
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from logplane.legacy import check_legacy_environment
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
from logplane.version import VERSION

DB_PATH = Path(os.environ.get("LOGPLANE_DB_PATH", Path.cwd() / "events.db"))
CURRENT_SCHEMA_VERSION = 2
MAX_CORRELATION_ID_LENGTH = 128
MAX_PRODUCER_FIELD_LENGTH = 128


def _read_default_max_retries() -> int:
    raw_value = os.environ.get("LOGPLANE_MAX_RETRIES", "2")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(
            "LOGPLANE_MAX_RETRIES must be a non-negative integer"
        ) from exc
    if value < 0:
        raise RuntimeError("LOGPLANE_MAX_RETRIES must be a non-negative integer")
    return value


DEFAULT_MAX_RETRIES = _read_default_max_retries()

# Refuse to start with pre-0.13 AGENT_BUS_* settings: an ignored
# AGENT_BUS_TOKEN would otherwise leave the bus unauthenticated.
check_legacy_environment()

# Optional perimeter auth: when LOGPLANE_TOKEN is set, every data route
# requires "Authorization: Bearer <token>". Actor strings are still
# self-reported — this authenticates clients, not identities.
def _read_api_token() -> Optional[str]:
    value = os.environ.get("LOGPLANE_TOKEN")
    if value is not None and not value.strip():
        # An empty value usually means an unexpanded variable or a typo. Treating
        # it as "no auth" would silently start an unauthenticated bus.
        raise RuntimeError(
            "LOGPLANE_TOKEN is set but empty; set a token, or unset it to run "
            "without authentication"
        )
    return value


API_TOKEN = _read_api_token()


async def require_token(request: Request) -> None:
    if not API_TOKEN:
        return
    # Compare bytes: compare_digest rejects non-ASCII str with TypeError, which
    # would turn a bad header into a 500. Starlette decodes headers as latin-1,
    # so encoding back to latin-1 recovers the bytes the client sent.
    supplied = request.headers.get("authorization", "").encode("latin-1", "replace")
    expected = f"Bearer {API_TOKEN}".encode("utf-8")
    if not secrets.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")

class EventValidationError(ValueError):
    """A known event does not satisfy its versioned contract."""


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for a different logical request."""


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


def _validate_query_correlation_id(correlation_id: Optional[str]) -> None:
    try:
        _validate_correlation_id(correlation_id)
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


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
    """
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

    if topic == "task.created":
        if "task_id" in payload:
            _require_task_id(payload)
        _require_string(payload, "title")
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_retry_policy(payload)
        _validate_task_dependencies(payload)
        _validate_task_scheduling(payload)
        _validate_external_origin(payload)
        _validate_ownership(payload)
        supersedes_task_id = payload.get("supersedes_task_id")
        supersession_reason = payload.get("supersession_reason")
        if supersedes_task_id is not None:
            if not _is_positive_int(supersedes_task_id):
                raise EventValidationError(
                    "payload.supersedes_task_id must be a positive integer"
                )
            _require_string(payload, "supersession_reason")
            if supersedes_task_id in payload.get("depends_on", []):
                raise EventValidationError(
                    "a replacement task cannot depend on the task it supersedes"
                )
        elif supersession_reason is not None:
            raise EventValidationError(
                "payload.supersession_reason requires payload.supersedes_task_id"
            )
        deadline_at = payload.get("deadline_at")
        if deadline_at is not None and not _is_positive_number(deadline_at):
            raise EventValidationError(
                "payload.deadline_at must be a positive finite timestamp"
            )
        return

    if topic == "integration.task_observed":
        _require_string(payload, "title")
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_retry_policy(payload)
        _validate_task_scheduling(payload)
        _validate_external_origin(payload, required=True)
        _validate_ownership(payload, observed=True)
        deadline_at = payload.get("deadline_at")
        if deadline_at is not None and not _is_positive_number(deadline_at):
            raise EventValidationError(
                "payload.deadline_at must be a positive finite timestamp"
            )
        return

    if topic in {"agent.registered", "agent.heartbeat"}:
        _require_string(payload, "name")
        _require_string(payload, "instance_id")
        if payload["name"] != actor:
            raise EventValidationError("agent event actor must match payload.name")
        if topic == "agent.registered":
            capacity = payload.get("capacity", 1)
            if not _is_positive_int(capacity):
                raise EventValidationError("payload.capacity must be a positive integer")
            capabilities = payload.get("capabilities", [])
            if not isinstance(capabilities, list) or not all(
                isinstance(item, str) and item.strip() for item in capabilities
            ):
                raise EventValidationError("payload.capabilities must be a list of strings")
        return

    if topic == "agent.policy_set":
        if correlation_id is not None:
            raise EventValidationError("agent.policy_set must not have correlation_id")
        _require_string(payload, "agent_name")
        source = payload.get("source")
        if source not in {"default", "operator"}:
            raise EventValidationError(
                "payload.source must be default or operator"
            )
        if source == "default" and actor != "pm":
            raise EventValidationError("default agent policy must be emitted by pm")
        if source == "operator" and actor == "pm":
            raise EventValidationError("operator agent policy must not be emitted by pm")
        _require_string(payload, "reason")
        limit = payload.get("max_active_assignments")
        if limit is not None and not _is_positive_int(limit):
            raise EventValidationError(
                "payload.max_active_assignments must be null or a positive integer"
            )
        return

    if topic in {"workflow.pause_requested", "workflow.resume_requested"}:
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise EventValidationError(
                f"{topic} requires a non-empty correlation_id"
            )
        _require_string(payload, "reason")
        return

    if topic == "workflow.policy_set":
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise EventValidationError(
                "workflow.policy_set requires a non-empty correlation_id"
            )
        source = payload.get("source")
        if source not in {"default", "operator"}:
            raise EventValidationError(
                "payload.source must be default or operator"
            )
        if source == "default" and actor != "pm":
            raise EventValidationError(
                "default workflow policy must be emitted by pm"
            )
        if source == "operator" and actor == "pm":
            raise EventValidationError(
                "operator workflow policy must not be emitted by pm"
            )
        _require_string(payload, "reason")
        limit = payload.get("max_active_assignments")
        if "max_active_assignments" in payload and (
            limit is not None and not _is_positive_int(limit)
        ):
            raise EventValidationError(
                "payload.max_active_assignments must be null or a positive integer"
            )
        for field in ("max_total_tokens", "max_attempts"):
            value = payload.get(field)
            if field in payload and value is not None and not _is_positive_int(value):
                raise EventValidationError(
                    f"payload.{field} must be null or a positive integer"
                )
        for field in ("max_total_cost_usd", "max_wall_clock_seconds"):
            value = payload.get(field)
            if field in payload and value is not None and not _is_positive_number(value):
                raise EventValidationError(
                    f"payload.{field} must be null or a positive number"
                )
        if "reserve_tokens_per_assignment" in payload and not _is_nonnegative_int(
            payload.get("reserve_tokens_per_assignment")
        ):
            raise EventValidationError(
                "payload.reserve_tokens_per_assignment must be a non-negative integer"
            )
        if "reserve_cost_usd_per_assignment" in payload and not _is_nonnegative_number(
            payload.get("reserve_cost_usd_per_assignment")
        ):
            raise EventValidationError(
                "payload.reserve_cost_usd_per_assignment must be a non-negative number"
            )
        token_fields = {
            "max_total_tokens",
            "reserve_tokens_per_assignment",
        }
        if token_fields.intersection(payload) and not token_fields.issubset(payload):
            raise EventValidationError(
                "token budget and reservation must be changed together"
            )
        if payload.get("max_total_tokens") is not None and not (
            0 < payload["reserve_tokens_per_assignment"]
            <= payload["max_total_tokens"]
        ):
            raise EventValidationError(
                "finite token budget requires a positive reservation no larger than the budget"
            )
        cost_fields = {
            "max_total_cost_usd",
            "reserve_cost_usd_per_assignment",
        }
        if cost_fields.intersection(payload) and not cost_fields.issubset(payload):
            raise EventValidationError(
                "cost budget and reservation must be changed together"
            )
        if payload.get("max_total_cost_usd") is not None and not (
            0 < payload["reserve_cost_usd_per_assignment"]
            <= payload["max_total_cost_usd"]
        ):
            raise EventValidationError(
                "finite cost budget requires a positive reservation no larger than the budget"
            )
        policy_fields = {
            "max_active_assignments",
            "max_total_tokens",
            "max_total_cost_usd",
            "max_attempts",
            "max_wall_clock_seconds",
            "reserve_tokens_per_assignment",
            "reserve_cost_usd_per_assignment",
        }
        if not policy_fields.intersection(payload):
            raise EventValidationError(
                "workflow.policy_set must change at least one policy field"
            )
        return

    if topic == "workflow.usage_recorded":
        if actor == "pm":
            raise EventValidationError("workflow.usage_recorded must be emitted by a worker")
        if producer is None:
            raise EventValidationError(
                "workflow.usage_recorded requires producer identity"
            )
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise EventValidationError(
                "workflow.usage_recorded requires a non-empty correlation_id"
            )
        expected_fields = {
            "task_id",
            "assignment_id",
            "worker_instance_id",
            "invocation_id",
            "telemetry_event_id",
            "tokens",
            "cost_usd",
        }
        if set(payload) != expected_fields:
            raise EventValidationError(
                "workflow.usage_recorded has an invalid payload shape"
            )
        _require_task_id(payload)
        for field in (
            "assignment_id",
            "worker_instance_id",
            "invocation_id",
        ):
            _require_telemetry_string(payload, field)
        telemetry_event_id = payload.get("telemetry_event_id")
        if not _is_positive_int(telemetry_event_id):
            raise EventValidationError(
                "payload.telemetry_event_id must be a positive integer"
            )
        if caused_by != telemetry_event_id:
            raise EventValidationError(
                "workflow.usage_recorded must be caused by its telemetry event"
            )
        tokens = payload.get("tokens")
        if tokens is not None and not _is_nonnegative_int(tokens):
            raise EventValidationError(
                "payload.tokens must be null or a non-negative integer"
            )
        cost = payload.get("cost_usd")
        if cost is not None and not _is_nonnegative_number(cost):
            raise EventValidationError(
                "payload.cost_usd must be null or a non-negative number"
            )
        return

    if topic in {"workflow.paused", "workflow.resumed"}:
        if actor != "pm":
            raise EventValidationError(f"{topic} must be emitted by pm")
        if not isinstance(correlation_id, str) or not correlation_id.strip():
            raise EventValidationError(
                f"{topic} requires a non-empty correlation_id"
            )
        _require_string(payload, "reason")
        request_field = (
            "pause_request_event_id"
            if topic == "workflow.paused"
            else "resume_request_event_id"
        )
        if not _is_positive_int(payload.get(request_field)):
            raise EventValidationError(
                f"payload.{request_field} must be a positive integer"
            )
        if topic == "workflow.paused":
            interrupted = payload.get("interrupted_assignments")
            if not isinstance(interrupted, list):
                raise EventValidationError(
                    "payload.interrupted_assignments must be a JSON array"
                )
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
        return

    _require_task_id(payload)

    if topic == "task.assigned":
        for field in ("assignment_id", "assignee", "worker_instance_id"):
            _require_string(payload, field)
        if not _is_positive_int(payload.get("attempt")):
            raise EventValidationError("payload.attempt must be a positive integer")
        policy_event_id = payload.get("workflow_policy_event_id")
        if policy_event_id is not None and not _is_positive_int(policy_event_id):
            raise EventValidationError(
                "payload.workflow_policy_event_id must be null or a positive integer"
            )
        agent_policy_event_id = payload.get("agent_policy_event_id")
        if agent_policy_event_id is not None and not _is_positive_int(
            agent_policy_event_id
        ):
            raise EventValidationError(
                "payload.agent_policy_event_id must be null or a positive integer"
            )
        reservation = payload.get("budget_reservation")
        if reservation is not None:
            if not isinstance(reservation, dict) or set(reservation) != {
                "policy_event_id",
                "tokens",
                "cost_usd",
            }:
                raise EventValidationError(
                    "payload.budget_reservation has an invalid shape"
                )
            if not _is_positive_int(reservation.get("policy_event_id")):
                raise EventValidationError(
                    "payload.budget_reservation.policy_event_id must be positive"
                )
            if not _is_nonnegative_int(reservation.get("tokens")):
                raise EventValidationError(
                    "payload.budget_reservation.tokens must be non-negative"
                )
            if not _is_nonnegative_number(reservation.get("cost_usd")):
                raise EventValidationError(
                    "payload.budget_reservation.cost_usd must be non-negative"
                )
        fairness = payload.get("fairness")
        if fairness is not None:
            if not isinstance(fairness, dict) or set(fairness) != {
                "policy",
                "previous_assignment_event_id",
            }:
                raise EventValidationError(
                    "payload.fairness must contain policy and previous_assignment_event_id"
                )
            if fairness.get("policy") != "workflow_round_robin_v1":
                raise EventValidationError(
                    "payload.fairness.policy must be workflow_round_robin_v1"
                )
            previous = fairness.get("previous_assignment_event_id")
            if previous is not None and not _is_positive_int(previous):
                raise EventValidationError(
                    "payload.fairness.previous_assignment_event_id must be null or a positive integer"
                )
        _validate_capabilities(payload)
        _validate_json_object(
            payload,
            "context",
            max_bytes=MAX_INLINE_CONTEXT_BYTES,
        )
        _validate_decisions(payload)
        _validate_dependency_refs(payload)
        _validate_retry_policy(payload, allow_null=True)
        if not _is_nonnegative_int(payload.get("retryable_failures", 0)):
            raise EventValidationError(
                "payload.retryable_failures must be a non-negative integer"
            )
        _validate_external_origin(payload)
        _validate_ownership(payload)
        deadline_at = payload.get("deadline_at")
        if deadline_at is not None and not _is_positive_number(deadline_at):
            raise EventValidationError(
                "payload.deadline_at must be a positive finite timestamp"
            )
    elif topic == "task.cancel_requested":
        _require_string(payload, "reason")
    elif topic in {"task.pause_requested", "task.resume_requested"}:
        _require_string(payload, "reason")
    elif topic == "task.paused":
        if actor != "pm":
            raise EventValidationError("task.paused must be emitted by pm")
        if not _is_positive_int(payload.get("pause_request_event_id")):
            raise EventValidationError(
                "payload.pause_request_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
        if payload.get("resume_status") not in {"open", "blocked"}:
            raise EventValidationError(
                "payload.resume_status must be open or blocked"
            )
        _validate_control_attempt(payload)
    elif topic == "task.resumed":
        if actor != "pm":
            raise EventValidationError("task.resumed must be emitted by pm")
        if not _is_positive_int(payload.get("resume_request_event_id")):
            raise EventValidationError(
                "payload.resume_request_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
        if payload.get("resume_status") not in {"open", "blocked"}:
            raise EventValidationError(
                "payload.resume_status must be open or blocked"
            )
    elif topic == "task.superseded":
        if actor != "pm":
            raise EventValidationError("task.superseded must be emitted by pm")
        if not _is_positive_int(payload.get("replacement_task_id")):
            raise EventValidationError(
                "payload.replacement_task_id must be a positive integer"
            )
        if not _is_positive_int(payload.get("replacement_created_event_id")):
            raise EventValidationError(
                "payload.replacement_created_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
        _validate_control_attempt(payload)
    elif topic == "task.cancelled":
        if actor != "pm":
            raise EventValidationError("task.cancelled must be emitted by pm")
        if not _is_positive_int(payload.get("cancel_request_event_id")):
            raise EventValidationError(
                "payload.cancel_request_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
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
    elif topic == "task.deadline_exceeded":
        if actor != "pm":
            raise EventValidationError(
                "task.deadline_exceeded must be emitted by pm"
            )
        if not _is_positive_number(payload.get("deadline_at")):
            raise EventValidationError(
                "payload.deadline_at must be a positive finite timestamp"
            )
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
    elif topic in {"task.started", "task.completed", "task.blocked"}:
        _require_string(payload, "assignment_id")
        _require_string(payload, "worker_instance_id")
        if topic == "task.blocked":
            _require_string(payload, "reason")
        if topic == "task.completed" and "result" in payload:
            _validate_json_object(
                payload,
                "result",
                max_bytes=MAX_INLINE_RESULT_BYTES,
            )
    elif topic == "task.attempt_failed":
        for field in (
            "assignment_id",
            "worker_instance_id",
            "failure_code",
            "reason",
        ):
            _require_string(payload, field)
        if not isinstance(payload.get("retryable"), bool):
            raise EventValidationError("payload.retryable must be a boolean")
    elif topic == "task.assignment_expired":
        for field in ("assignment_id", "assignee", "worker_instance_id", "reason"):
            _require_string(payload, field)
    elif topic == "task.failed":
        for field in ("reason_code", "reason", "last_assignment_id"):
            _require_string(payload, field)
        if not _is_positive_int(payload.get("attempts")):
            raise EventValidationError("payload.attempts must be a positive integer")
        if not _is_nonnegative_int(payload.get("retryable_failures")):
            raise EventValidationError(
                "payload.retryable_failures must be a non-negative integer"
            )
        max_retries = payload.get("max_retries")
        if max_retries is not None and not _is_nonnegative_int(max_retries):
            raise EventValidationError(
                "payload.max_retries must be null or a non-negative integer"
            )
    elif topic == "task.dependency_failed":
        if actor != "pm":
            raise EventValidationError("task.dependency_failed must be emitted by pm")
        if not _is_positive_int(payload.get("dependency_task_id")):
            raise EventValidationError(
                "payload.dependency_task_id must be a positive integer"
            )
        if not _is_positive_int(payload.get("dependency_event_id")):
            raise EventValidationError(
                "payload.dependency_event_id must be a positive integer"
            )
        _require_string(payload, "reason")
    elif topic == "task.retry_requested":
        if not _is_positive_int(payload.get("additional_retries")):
            raise EventValidationError(
                "payload.additional_retries must be a positive integer"
            )
        _require_string(payload, "reason")
    elif topic == "decision.needed":
        for field in ("assignment_id", "decision_id", "reason"):
            _require_string(payload, field)
    elif topic == "decision.made":
        for field in ("assignment_id", "decision_id"):
            _require_string(payload, field)
        if "decision" not in payload:
            raise EventValidationError("payload.decision is required")
        try:
            json.dumps(payload["decision"], allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise EventValidationError(
                "payload.decision must be JSON-compatible"
            ) from exc


# ---------------------------------------------------------------- storage


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextlib.contextmanager
def db():
    """Connection that both commits the transaction AND closes the handle.

    sqlite3's own `with conn:` only commits/rolls back — it does NOT close,
    so using `with _connect() as conn:` alone would leak a connection (and
    keep the WAL/-shm files open) on every call.
    """
    conn = _connect()
    try:
        with conn:  # transaction scope (commit/rollback)
            yield conn
    finally:
        conn.close()  # lifetime scope


def init_db() -> None:
    with db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        # Serialize schema/backfill work with publishers; no partially built
        # identity projection may become visible after a crash or concurrent init.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("CREATE TABLE IF NOT EXISTS bus_metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT OR IGNORE INTO bus_metadata VALUES ('database_id',?)", (uuid.uuid4().hex,))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        REAL    NOT NULL,
                topic     TEXT    NOT NULL,
                actor     TEXT    NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                idempotency_key TEXT,
                caused_by INTEGER,
                correlation_id TEXT,
                producer  TEXT,
                payload   TEXT    NOT NULL
            )
            """
        )
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
        if "idempotency_key" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN idempotency_key TEXT")
        if "schema_version" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
            )
        if "correlation_id" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN correlation_id TEXT")
        if "producer" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN producer TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic ON events(topic)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, id)")
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_identity'"
        ).fetchone()
        _create_task_identity(conn)
        if exists is None:
            _backfill_task_identity(conn)
        _advance_task_counter(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_correlation_id
            ON events(correlation_id, id)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_events_actor_idempotency
            ON events(actor, idempotency_key)
            WHERE idempotency_key IS NOT NULL
            """
        )
        # This is a rebuildable uniqueness projection over immutable adoption
        # events. Unlike ordinary idempotency keys, an external origin is
        # process/actor independent: two misconfigured bridge actor names must
        # not be able to create two orchestration owners for the same work.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS external_origin_claims (
                system   TEXT NOT NULL,
                task_ref TEXT NOT NULL,
                event_id INTEGER,
                PRIMARY KEY (system, task_ref)
            )
            """
        )
        for row in conn.execute(
            """
            SELECT id, payload FROM events
            WHERE topic IN ('task.created', 'integration.task_observed')
            ORDER BY id
            """
        ):
            try:
                origin = json.loads(row["payload"]).get("external_origin")
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
            if not isinstance(origin, dict):
                continue
            system = origin.get("system")
            task_ref = origin.get("task_ref")
            if isinstance(system, str) and isinstance(task_ref, str):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO external_origin_claims
                        (system, task_ref, event_id)
                    VALUES (?, ?, ?)
                    """,
                    (system, task_ref, row["id"]),
                )
        # One immutable task can name at most one direct replacement. This
        # rebuildable uniqueness projection makes that invariant atomic across
        # actors and concurrent publishers; later changes supersede the
        # replacement task rather than branching the old intent.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_supersession_claims (
                task_id  INTEGER PRIMARY KEY,
                event_id INTEGER
            )
            """
        )
        for row in conn.execute(
            "SELECT id, payload FROM events WHERE topic = 'task.created' ORDER BY id"
        ):
            try:
                supersedes_task_id = json.loads(row["payload"]).get(
                    "supersedes_task_id"
                )
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue
            if _is_positive_int(supersedes_task_id):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO task_supersession_claims
                        (task_id, event_id)
                    VALUES (?, ?)
                    """,
                    (supersedes_task_id, row["id"]),
                )


def _create_task_identity(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS task_identity ("
        "task_id INTEGER PRIMARY KEY, created_event_id INTEGER NOT NULL UNIQUE)"
    )


def _backfill_task_identity(conn: sqlite3.Connection) -> None:
    # First valid creation wins, matching historical task identity semantics.
    # This O(task history) scan is reserved for migration/explicit recovery.
    for row in conn.execute(
        "SELECT id, payload FROM events WHERE topic='task.created' ORDER BY id"
    ):
        try:
            payload = json.loads(row["payload"])
        except (json.JSONDecodeError, TypeError):
            continue
        task_id = payload.get("task_id") if isinstance(payload, dict) else None
        if _is_positive_int(task_id):
            conn.execute(
                "INSERT OR IGNORE INTO task_identity VALUES (?, ?)",
                (task_id, row["id"]),
            )


def _advance_task_counter(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO counters(name,value) VALUES ('task_id',"
        "(SELECT COALESCE(MAX(task_id),0) FROM task_identity)) "
        "ON CONFLICT(name) DO UPDATE SET value=MAX(value,excluded.value)"
    )


def rebuild_task_index() -> int:
    """Atomically replace the disposable identity index, never event history.

    Run against an initialized database. Writers wait for the transaction;
    readers retain their previous consistent view. Failures roll back the rebuild.
    """
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        _create_task_identity(conn)
        conn.execute("DELETE FROM task_identity")
        _backfill_task_identity(conn)
        _advance_task_counter(conn)
        return conn.execute("SELECT COUNT(*) FROM task_identity").fetchone()[0]


def next_task_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "UPDATE counters SET value = value + 1 WHERE name = 'task_id' RETURNING value"
    ).fetchone()
    if row is None:
        conn.execute("INSERT INTO counters (name, value) VALUES ('task_id', 1)")
        return 1
    return int(row["value"])


def row_to_dict(row: sqlite3.Row) -> dict:
    event = dict(row)
    event["payload"] = json.loads(event["payload"])
    if event.get("producer") is not None:
        event["producer"] = json.loads(event["producer"])
    event.setdefault("schema_version", 1)
    event.setdefault("correlation_id", None)
    event.setdefault("producer", None)
    return event


def _find_task_created(
    conn: sqlite3.Connection,
    task_id: int,
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT events.* FROM task_identity AS identity "
        "JOIN events ON events.id=identity.created_event_id "
        "WHERE identity.task_id=?", (task_id,),
    ).fetchone()


def _resolve_correlation_id(
    conn: sqlite3.Connection,
    *,
    topic: str,
    actor: str,
    caused_by: Optional[int],
    correlation_id: Optional[str],
    payload: dict,
    producer: Optional[dict],
) -> Optional[str]:
    """Resolve a new event's workflow identity inside its append transaction."""
    resolved = correlation_id
    if caused_by is not None:
        parent = conn.execute(
            "SELECT * FROM events WHERE id = ?",
            (caused_by,),
        ).fetchone()
        if parent is None:
            raise EventValidationError(f"caused_by event {caused_by} does not exist")
        parent_correlation_id = parent["correlation_id"]
        if (
            parent_correlation_id is not None
            and resolved is not None
            and resolved != parent_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the caused_by event"
            )
        resolved = parent_correlation_id or resolved

        if topic == "workflow.usage_recorded":
            try:
                parent_payload = json.loads(parent["payload"])
                parent_producer = (
                    json.loads(parent["producer"])
                    if parent["producer"] is not None
                    else None
                )
            except (json.JSONDecodeError, TypeError) as exc:
                raise EventValidationError(
                    "usage accounting parent is malformed"
                ) from exc
            if parent["topic"] not in {
                "telemetry.model.completed",
                "telemetry.model.failed",
            }:
                raise EventValidationError(
                    "workflow.usage_recorded must follow terminal model telemetry"
                )
            for field in (
                "task_id",
                "assignment_id",
                "worker_instance_id",
                "invocation_id",
            ):
                if payload.get(field) != parent_payload.get(field):
                    raise EventValidationError(
                        f"usage accounting {field} conflicts with telemetry"
                    )
            if parent["actor"] != actor or parent_producer != producer:
                raise EventValidationError(
                    "usage accounting identity conflicts with telemetry"
                )
            usage = parent_payload.get("usage")
            usage = usage if isinstance(usage, dict) else {}
            expected_tokens = usage.get("total_tokens")
            if not _is_nonnegative_int(expected_tokens):
                input_tokens = usage.get("input_tokens")
                output_tokens = usage.get("output_tokens")
                expected_tokens = (
                    input_tokens + output_tokens
                    if _is_nonnegative_int(input_tokens)
                    and _is_nonnegative_int(output_tokens)
                    else None
                )
            expected_cost = usage.get(
                "cost_usd", usage.get("estimated_cost_usd")
            )
            if not _is_nonnegative_number(expected_cost):
                expected_cost = None
            if (
                payload.get("tokens") != expected_tokens
                or payload.get("cost_usd") != expected_cost
            ):
                raise EventValidationError(
                    "usage accounting values conflict with telemetry"
                )

    if topic == "task.created":
        supersedes_task_id = payload.get("supersedes_task_id")
        if supersedes_task_id is not None:
            superseded = _find_task_created(conn, supersedes_task_id)
            if superseded is None:
                raise EventValidationError(
                    f"task {supersedes_task_id} does not exist"
                )
            superseded_correlation_id = superseded["correlation_id"]
            if superseded_correlation_id is None:
                raise EventValidationError(
                    f"task {supersedes_task_id} has no workflow identity"
                )
            if resolved is not None and resolved != superseded_correlation_id:
                raise EventValidationError(
                    "correlation_id conflicts with the superseded task"
                )
            resolved = superseded_correlation_id
        for dependency_task_id in payload.get("depends_on", []):
            dependency = _find_task_created(conn, dependency_task_id)
            if dependency is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} does not exist"
                )
            dependency_correlation_id = dependency["correlation_id"]
            if dependency_correlation_id is None:
                raise EventValidationError(
                    f"dependency task {dependency_task_id} has no workflow identity"
                )
            if resolved is not None and resolved != dependency_correlation_id:
                raise EventValidationError(
                    "all dependencies must belong to the same correlation_id"
                )
            resolved = dependency_correlation_id

    if topic in {
        "task.cancel_requested",
        "task.pause_requested",
        "task.resume_requested",
    }:
        task = _find_task_created(conn, payload["task_id"])
        if task is None:
            raise EventValidationError(
                f"task {payload['task_id']} does not exist"
            )
        task_correlation_id = task["correlation_id"]
        if (
            task_correlation_id is not None
            and resolved is not None
            and resolved != task_correlation_id
        ):
            raise EventValidationError(
                "correlation_id conflicts with the target task"
            )
        resolved = task_correlation_id or resolved

    if topic.startswith("workflow."):
        if resolved is None:
            raise EventValidationError(f"{topic} requires correlation_id")
        exists = conn.execute(
            "SELECT 1 FROM events WHERE topic = 'task.created' AND correlation_id = ? LIMIT 1",
            (resolved,),
        ).fetchone()
        if exists is None:
            raise EventValidationError(
                f"workflow {resolved!r} does not exist"
            )

    if resolved is not None:
        return resolved
    if topic == "task.created":
        return uuid.uuid4().hex
    return None


def _assert_idempotent_match(
    row: sqlite3.Row,
    *,
    topic: str,
    payload: dict,
    caused_by: Optional[int],
    schema_version: int,
    correlation_id: Optional[str],
    producer: Optional[dict],
) -> None:
    existing = row_to_dict(row)
    stored_payload = existing["payload"]
    if topic == "task.created":
        stored_payload = dict(stored_payload)
        if "task_id" not in payload:
            # task_id is a server-generated response field, not part of the
            # caller's logical idempotent request.
            stored_payload.pop("task_id", None)
        if "retry_policy" not in payload:
            # The server materializes its current default into new task events.
            # Omitting it remains the same logical request on a retry.
            stored_payload.pop("retry_policy", None)
        if "ownership" not in payload:
            # Controlled ownership is a server-materialized default.
            stored_payload.pop("ownership", None)
        if "depends_on" not in payload:
            stored_payload.pop("depends_on", None)
        if "priority" not in payload:
            # Normal priority is a server-materialized scheduling default.
            stored_payload.pop("priority", None)
    # Omitting correlation_id delegates generation/inheritance to the server.
    # On a retry, the stored value is therefore the effective requested value.
    effective_correlation_id = (
        existing["correlation_id"] if correlation_id is None else correlation_id
    )
    requested = (
        topic,
        payload,
        caused_by,
        schema_version,
        effective_correlation_id,
        producer,
    )
    stored = (
        existing["topic"],
        stored_payload,
        existing["caused_by"],
        existing["schema_version"],
        existing["correlation_id"],
        existing["producer"],
    )
    if stored != requested:
        raise IdempotencyConflict(
            "idempotency key is already attached to a different event"
        )


def _recover_concurrent_idempotent_claim(
    conn: sqlite3.Connection,
    *,
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int],
    idempotency_key: Optional[str],
    schema_version: int,
    correlation_id: Optional[str],
    producer: Optional[dict],
) -> Optional[dict]:
    """Recover when a claim race lost to the same logical request."""
    if idempotency_key is None:
        return None
    # Release the failed write transaction before reading the winning event.
    conn.rollback()
    row = conn.execute(
        "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
        (actor, idempotency_key),
    ).fetchone()
    if row is None:
        return None
    _assert_idempotent_match(
        row,
        topic=topic,
        payload=payload,
        caused_by=caused_by,
        schema_version=schema_version,
        correlation_id=correlation_id,
        producer=producer,
    )
    return row_to_dict(row)


def append_event(
    topic: str,
    actor: str,
    payload: dict,
    caused_by: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    schema_version: int = CURRENT_SCHEMA_VERSION,
    correlation_id: Optional[str] = None,
    producer: Optional[dict] = None,
) -> dict:
    validate_event(
        topic,
        actor,
        payload,
        caused_by,
        idempotency_key,
        schema_version,
        correlation_id,
        producer,
    )
    requested_payload = dict(payload)
    with db() as conn:
        # Identity checks and all derived claims must observe the same serialized
        # write transaction as append (including concurrent explicit task IDs).
        conn.execute("BEGIN IMMEDIATE")
        if idempotency_key is not None:
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is not None:
                _assert_idempotent_match(
                    row,
                    topic=topic,
                    payload=requested_payload,
                    caused_by=caused_by,
                    schema_version=schema_version,
                    correlation_id=correlation_id,
                    producer=producer,
                )
                return row_to_dict(row)

        resolved_correlation_id = _resolve_correlation_id(
            conn,
            topic=topic,
            actor=actor,
            caused_by=caused_by,
            correlation_id=correlation_id,
            payload=requested_payload,
            producer=producer,
        )
        payload = dict(requested_payload)
        origin_claim = None
        supersession_claim = None
        if topic in {"task.created", "integration.task_observed"}:
            origin = payload.get("external_origin")
            if isinstance(origin, dict):
                origin_claim = (origin["system"], origin["task_ref"])
                try:
                    conn.execute(
                        """
                        INSERT INTO external_origin_claims
                            (system, task_ref, event_id)
                        VALUES (?, ?, NULL)
                        """,
                        origin_claim,
                    )
                except sqlite3.IntegrityError as exc:
                    recovered = _recover_concurrent_idempotent_claim(
                        conn,
                        topic=topic,
                        actor=actor,
                        payload=requested_payload,
                        caused_by=caused_by,
                        idempotency_key=idempotency_key,
                        schema_version=schema_version,
                        correlation_id=correlation_id,
                        producer=producer,
                    )
                    if recovered is not None:
                        return recovered
                    existing = conn.execute(
                        """
                        SELECT event_id FROM external_origin_claims
                        WHERE system = ? AND task_ref = ?
                        """,
                        origin_claim,
                    ).fetchone()
                    event_label = (
                        f" at event #{existing['event_id']}"
                        if existing is not None and existing["event_id"] is not None
                        else ""
                    )
                    raise IdempotencyConflict(
                        "external origin "
                        f"{origin_claim[0]!r}/{origin_claim[1]!r} is already claimed"
                        f"{event_label}; refusing possible dual ownership"
                    ) from exc
        if topic == "task.created":
            supersedes_task_id = payload.get("supersedes_task_id")
            if supersedes_task_id is not None:
                supersession_claim = supersedes_task_id
                try:
                    conn.execute(
                        """
                        INSERT INTO task_supersession_claims (task_id, event_id)
                        VALUES (?, NULL)
                        """,
                        (supersession_claim,),
                    )
                except sqlite3.IntegrityError as exc:
                    recovered = _recover_concurrent_idempotent_claim(
                        conn,
                        topic=topic,
                        actor=actor,
                        payload=requested_payload,
                        caused_by=caused_by,
                        idempotency_key=idempotency_key,
                        schema_version=schema_version,
                        correlation_id=correlation_id,
                        producer=producer,
                    )
                    if recovered is not None:
                        return recovered
                    existing = conn.execute(
                        """
                        SELECT event_id FROM task_supersession_claims
                        WHERE task_id = ?
                        """,
                        (supersession_claim,),
                    ).fetchone()
                    event_label = (
                        f" at event #{existing['event_id']}"
                        if existing is not None and existing["event_id"] is not None
                        else ""
                    )
                    raise EventValidationError(
                        f"task {supersession_claim} already has a replacement"
                        f"{event_label}"
                    ) from exc
            payload.setdefault(
                "retry_policy",
                {"max_retries": DEFAULT_MAX_RETRIES},
            )
            payload.setdefault(
                "ownership",
                {"mode": "controlled", "owner": "agent-bus"},
            )
            payload.setdefault("depends_on", [])
            payload.setdefault("priority", DEFAULT_TASK_PRIORITY)
            if "task_id" not in payload:
                payload["task_id"] = next_task_id(conn)
            elif _is_positive_int(payload["task_id"]):
                if _find_task_created(conn, payload["task_id"]) is not None:
                    raise EventValidationError(
                        f"task_id {payload['task_id']} already exists"
                    )
                conn.execute(
                    "UPDATE counters SET value = max(value, ?) WHERE name = 'task_id'",
                    (payload["task_id"],),
                )
        if topic == "task.retry_requested":
            task_id = payload["task_id"]
            replacement = conn.execute(
                "SELECT event_id FROM task_supersession_claims WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if replacement is not None:
                event_label = (
                    f" at event #{replacement['event_id']}"
                    if replacement["event_id"] is not None
                    else ""
                )
                raise EventValidationError(
                    f"task {task_id} has already been superseded{event_label} "
                    "and cannot be retried"
                )

        try:
            cur = conn.execute(
                """
                INSERT INTO events
                    (ts, topic, actor, schema_version, idempotency_key, caused_by,
                     correlation_id, producer, payload)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    time.time(),
                    topic,
                    actor,
                    schema_version,
                    idempotency_key,
                    caused_by,
                    resolved_correlation_id,
                    json.dumps(producer, sort_keys=True, separators=(",", ":"))
                    if producer is not None
                    else None,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )
        except sqlite3.IntegrityError:
            if idempotency_key is None:
                raise
            # Undo any counter increment performed before a racing idempotent
            # insert lost the unique-index race.
            conn.rollback()
            row = conn.execute(
                "SELECT * FROM events WHERE actor = ? AND idempotency_key = ?",
                (actor, idempotency_key),
            ).fetchone()
            if row is None:
                raise
            _assert_idempotent_match(
                row,
                topic=topic,
                payload=requested_payload,
                caused_by=caused_by,
                schema_version=schema_version,
                correlation_id=correlation_id,
                producer=producer,
            )
            return row_to_dict(row)
        row = conn.execute("SELECT * FROM events WHERE id = ?", (cur.lastrowid,)).fetchone()
        if topic == "task.created":
            conn.execute(
                "INSERT INTO task_identity(task_id,created_event_id) VALUES (?,?)",
                (payload["task_id"], cur.lastrowid),
            )
        if origin_claim is not None:
            conn.execute(
                """
                UPDATE external_origin_claims SET event_id = ?
                WHERE system = ? AND task_ref = ?
                """,
                (cur.lastrowid, *origin_claim),
            )
        if supersession_claim is not None:
            conn.execute(
                """
                UPDATE task_supersession_claims SET event_id = ?
                WHERE task_id = ?
                """,
                (cur.lastrowid, supersession_claim),
            )
    return row_to_dict(row)


def fetch_after(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 1000,
    correlation_id: Optional[str] = None,
) -> list[dict]:
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        return [row_to_dict(r) for r in conn.execute(q, args).fetchall()]


def fetch_event(event_id: int) -> Optional[dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    return row_to_dict(row) if row is not None else None


def fetch_stream_window(
    after_id: int,
    topics: Optional[list[str]],
    limit: int = 500,
    correlation_id: Optional[str] = None,
) -> tuple[list[dict], int, bool]:
    """Read a bounded matching-id window without decoding unrelated events."""
    q = "SELECT * FROM events WHERE id > ?"
    args: list = [after_id]
    if topics:
        q += f" AND topic IN ({','.join('?' * len(topics))})"
        args += topics
    if correlation_id is not None:
        q += " AND correlation_id = ?"
        args.append(correlation_id)
    q += " ORDER BY id LIMIT ?"
    args.append(limit)
    with db() as conn:
        rows = conn.execute(q, args).fetchall()
    events = [row_to_dict(row) for row in rows]
    scanned_to = events[-1]["id"] if events else after_id
    return events, scanned_to, len(events) == limit


# ---------------------------------------------------------------- app

new_event = asyncio.Condition()
event_generation = 0

# Open event streams wait for new events in short slices so they notice a
# departed client or a server shutdown within about a second, while idle
# keepalives keep their original 15-second cadence.
STREAM_POLL_SECONDS = 1.0
STREAM_KEEPALIVE_SECONDS = 15.0
_shutting_down = False


def begin_shutdown() -> None:
    """Ask open event streams to finish so the server can stop promptly.

    Called by `logplane serve` when it receives a stop signal. Clients see
    the stream end and reconnect with their normal backoff.
    """
    global _shutting_down
    _shutting_down = True


def _stream_should_stop_now() -> bool:
    return _shutting_down


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(init_db)
    yield


app = FastAPI(title="logplane", version=VERSION, lifespan=lifespan)


class PublishRequest(BaseModel):
    topic: str = Field(..., examples=["task.created"])
    actor: str = Field(..., examples=["pm"])
    payload: dict = Field(default_factory=dict)
    caused_by: Optional[int] = None
    idempotency_key: Optional[str] = None
    schema_version: int = CURRENT_SCHEMA_VERSION
    correlation_id: Optional[str] = None
    producer: Optional[dict] = None


@app.post("/events", dependencies=[Depends(require_token)])
async def publish(req: PublishRequest) -> dict:
    global event_generation
    try:
        event = await run_in_threadpool(
            append_event,
            req.topic,
            req.actor,
            req.payload,
            req.caused_by,
            req.idempotency_key,
            req.schema_version,
            req.correlation_id,
            req.producer,
        )
    except EventValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    async with new_event:
        event_generation += 1
        new_event.notify_all()
    return event


@app.get("/events", dependencies=[Depends(require_token)])
async def query(
    response: Response,
    after_id: int = 0,
    topics: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10_000),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
) -> list[dict]:
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None
    events = await run_in_threadpool(
        fetch_after, after_id, topic_list, limit, correlation_id
    )
    # A full page means more events may exist; callers that want everything
    # should paginate (see BusClient.query_all) instead of trusting one page.
    response.headers["X-Page-Full"] = "1" if len(events) == limit else "0"
    return events


@app.get("/events/stream", dependencies=[Depends(require_token)])
async def stream(
    request: Request,
    from_id: int = 0,
    topics: Optional[str] = Query(None),
    correlation_id: Optional[str] = Query(
        None, min_length=1, max_length=MAX_CORRELATION_ID_LENGTH
    ),
):
    """Replay history after from_id in bounded windows, then follow live."""
    _validate_query_correlation_id(correlation_id)
    topic_list = [topic.strip() for topic in topics.split(",") if topic.strip()] if topics else None

    async def gen():
        last_id = from_id
        observed_generation = event_generation
        while True:
            if _stream_should_stop_now() or await request.is_disconnected():
                return
            events, scanned_to, full_window = await run_in_threadpool(
                fetch_stream_window, last_id, topic_list, 500, correlation_id
            )
            last_id = scanned_to
            for event in events:
                yield f"id: {event['id']}\ndata: {json.dumps(event)}\n\n"
            if full_window:
                continue
            # Wait for the next event without polling the database, checking
            # for a departed client or shutdown each slice. After a full
            # keepalive interval with no event, send a keepalive and re-check
            # history, exactly as the single 15-second wait used to.
            waited = 0.0
            while True:
                if _stream_should_stop_now() or await request.is_disconnected():
                    return
                try:
                    async with new_event:
                        if event_generation == observed_generation:
                            await asyncio.wait_for(
                                new_event.wait(), timeout=STREAM_POLL_SECONDS
                            )
                        observed_generation = event_generation
                    break
                except asyncio.TimeoutError:
                    waited += STREAM_POLL_SECONDS
                    if waited >= STREAM_KEEPALIVE_SECONDS:
                        yield ": keepalive\n\n"
                        break

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/events/{event_id}", dependencies=[Depends(require_token)])
async def get_event(event_id: int) -> dict:
    if event_id <= 0:
        raise HTTPException(status_code=422, detail="event_id must be positive")
    event = await run_in_threadpool(fetch_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


@app.get("/health")
async def health(response: Response) -> dict:
    # Local launcher readiness, not an authentication credential. Only the bus
    # child receives this per-launch marker; ordinary `serve` needs no marker.
    startup_id = os.environ.get("LOGPLANE_STARTUP_ID")
    if startup_id:
        response.headers["X-Logplane-Startup-Id"] = startup_id
    def identity():
        with db() as conn:
            row = conn.execute("SELECT value, (SELECT COALESCE(MAX(id), 0) FROM events) "
                               "FROM bus_metadata WHERE name='database_id'").fetchone()
            return {"database_id": row[0], "last_event_id": row[1]}
    return {"ok": True, "schema_version": CURRENT_SCHEMA_VERSION,
            **await run_in_threadpool(identity)}
