"""Standalone adapter conformance checks for integration authors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from logplane.executor_protocol import CURRENT_PROTOCOL_VERSION, outcome_message, parse_outcome_message
from logplane.executors import (
    AssignmentContext,
    Blocked,
    Completed,
    OUTCOME_TYPES,
    PermanentFailure,
    RetryableFailure,
    json_size,
    outcome_from_dict,
)
from logplane.limits import MAX_INLINE_RESULT_BYTES


@dataclass(frozen=True)
class ConformanceCheck:
    name: str
    ok: bool
    detail: str
    required: bool = True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "ok": self.ok,
            "required": self.required,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ConformanceReport:
    checks: tuple[ConformanceCheck, ...]
    outcome_type: Optional[str] = None

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks if check.required)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "outcome_type": self.outcome_type,
            "checks": [check.to_dict() for check in self.checks],
        }


def probe_assignment() -> AssignmentContext:
    """A probe requesting no external effects; adapters must honor that request."""
    return AssignmentContext(
        correlation_id="logplane-conformance",
        task_id=1,
        assignment_id="conformance:task:1:attempt:1",
        assignment_event_id=1,
        attempt=1,
        goal="Validate the adapter contract without external side effects",
        context={
            # Adapters check this key to skip side effects during probes.
            "agent_bus_conformance_probe": True,
            "instructions": "Return a typed outcome; do not perform external side effects.",
        },
        required_capabilities=(),
        max_retries=0,
        retryable_failures=0,
        assignee="adapter-check",
        worker_instance_id="adapter-check-instance",
    )


def check_executor(executor, *, close: bool = True) -> ConformanceReport:
    """Synchronously validate one probe; no timeout or sandbox is provided.

    The CLI loads targets in a separate, timed process. Library callers own
    their executor's execution/cleanup bounds and side-effect protections.
    """
    checks = []
    execute = getattr(executor, "execute", None)
    checks.append(
        ConformanceCheck(
            "execute hook",
            callable(execute),
            "adapter exposes execute(assignment)",
        )
    )
    if not callable(execute):
        return ConformanceReport(tuple(checks))

    outcome = None
    try:
        outcome = execute(probe_assignment())
        checks.append(
            ConformanceCheck(
                "typed outcome",
                isinstance(outcome, OUTCOME_TYPES),
                (
                    f"returned {outcome.__class__.__name__}"
                    if isinstance(outcome, OUTCOME_TYPES)
                    else f"returned unsupported {type(outcome).__name__}"
                ),
            )
        )
        if isinstance(outcome, OUTCOME_TYPES):
            payload = _outcome_payload(outcome)
            decoded = outcome_from_dict(
                parse_outcome_message(
                    outcome_message(payload, CURRENT_PROTOCOL_VERSION),
                    CURRENT_PROTOCOL_VERSION,
                )
            )
            checks.append(
                ConformanceCheck(
                    "versioned round trip",
                    decoded == outcome,
                    "outcome survives protocol-v1 serialization",
                )
            )
            size = json_size(outcome.result) if isinstance(outcome, Completed) else 0
            checks.append(
                ConformanceCheck(
                    "inline result limit",
                    size <= MAX_INLINE_RESULT_BYTES,
                    f"encoded result is {size} bytes; limit is {MAX_INLINE_RESULT_BYTES}",
                )
            )
    except Exception as exc:
        checks.append(
            ConformanceCheck(
                "probe execution",
                False,
                f"probe raised {exc.__class__.__name__}: {exc}",
            )
        )
    finally:
        if close:
            close_hook = getattr(executor, "close", None)
            if callable(close_hook):
                try:
                    close_hook()
                except Exception as exc:
                    checks.append(
                        ConformanceCheck(
                            "cleanup hook",
                            False,
                            f"close raised {exc.__class__.__name__}: {exc}",
                        )
                    )

    cancel = getattr(executor, "cancel", None)
    checks.append(
        ConformanceCheck(
            "cancellation hook",
            callable(cancel),
            (
                "adapter exposes cooperative cancellation"
                if callable(cancel)
                else "no cancellation hook; runtime still fences late output, but work may continue locally"
            ),
            required=False,
        )
    )
    checks.append(
        ConformanceCheck(
            "side-effect contract",
            True,
            "manual invariant: assignment_id identifies one delivery attempt; irreversible external operations must use assignment.effect_id(operation_name)",
            required=False,
        )
    )
    return ConformanceReport(
        tuple(checks),
        outcome.__class__.__name__ if isinstance(outcome, OUTCOME_TYPES) else None,
    )


def _outcome_payload(outcome) -> dict:
    if isinstance(outcome, Completed):
        return {
            "status": "completed",
            "summary": outcome.summary,
            "result": dict(outcome.result),
        }
    if isinstance(outcome, Blocked):
        return {"status": "blocked", "reason": outcome.reason}
    if isinstance(outcome, RetryableFailure):
        return {
            "status": "retryable_failure",
            "code": outcome.code,
            "reason": outcome.reason,
            **(
                {"retry_after_seconds": outcome.retry_after_seconds}
                if outcome.retry_after_seconds is not None
                else {}
            ),
        }
    if isinstance(outcome, PermanentFailure):
        return {
            "status": "permanent_failure",
            "code": outcome.code,
            "reason": outcome.reason,
        }
    raise TypeError("unsupported outcome")
