"""Crash-safe project-manager reconciliation agent.

The PM derives state with the shared pure projection, then reconciles missing
effects using stable idempotency keys.
"""

import fcntl
import json
import math
import os
import stat
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional

from logplane.client import BusClient
from logplane import pm_lock
from logplane.legacy import check_legacy_environment
from logplane.projection import (
    ACTIVE_TASK_STATUSES,
    DEPENDENCY_TERMINAL_STATUSES,
    PMState,
    PROJECTION_TOPICS,
    TASK_TERMINAL_STATUSES,
    TaskRecord,
    WorkerRecord,
    apply_event,
    dependency_terminal_event_id,
    retry_budget_exhausted,
)

BUS_URL = os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765")
PM_TOPICS = PROJECTION_TOPICS


@dataclass
class OrderedProjectionCursor:
    """Consume each persisted coordination event once in event-ID order."""

    state: PMState
    last_event_id: int = 0

    def consume(self, events: Iterable[dict]) -> list[tuple[dict, bool]]:
        consumed: list[tuple[dict, bool]] = []
        for event in events:
            event_id = event.get("id")
            if (
                not isinstance(event_id, int)
                or isinstance(event_id, bool)
                or event_id <= self.last_event_id
            ):
                continue
            applied = apply_event(self.state, event)
            self.last_event_id = event_id
            consumed.append((event, applied))
        return consumed

    def catch_up(self, bus: BusClient) -> list[tuple[dict, bool]]:
        events = bus.query_all(
            after_id=self.last_event_id,
            topics=list(PM_TOPICS),
        )
        return self.consume(events)


def _lock_path() -> Path:
    return pm_lock.lock_path(BUS_URL)


LOCK_PATH = _lock_path()
WORKER_LEASE_SECONDS = float(os.environ.get("LOGPLANE_WORKER_LEASE_SECONDS", "20"))
_UNSET_POLICY_DEFAULT = object()
_UNSET_AGENT_POLICY_DEFAULT = object()


def _default_workflow_max_active_assignments() -> Optional[int]:
    raw = os.environ.get("LOGPLANE_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS")
    if raw is None or raw.strip().lower() in {"", "none", "null", "unbounded"}:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(
            "[pm] LOGPLANE_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS must be "
            "a positive integer or 'unbounded'"
        ) from exc
    if value <= 0:
        raise SystemExit(
            "[pm] LOGPLANE_DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS must be "
            "a positive integer or 'unbounded'"
        )
    return value


DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS = (
    _default_workflow_max_active_assignments()
)


def _optional_positive_env(name: str, *, integer: bool) -> Optional[float | int]:
    raw = os.environ.get(name)
    if raw is None or raw.strip().lower() in {"", "none", "null", "unbounded"}:
        return None
    try:
        value = int(raw) if integer else float(raw)
    except ValueError as exc:
        raise SystemExit(f"[pm] {name} must be positive or 'unbounded'") from exc
    if (not integer and not math.isfinite(value)) or value <= 0:
        raise SystemExit(f"[pm] {name} must be positive or 'unbounded'")
    return value


def _nonnegative_env(name: str, *, integer: bool) -> float | int:
    raw = os.environ.get(name, "0")
    try:
        value = int(raw) if integer else float(raw)
    except ValueError as exc:
        raise SystemExit(f"[pm] {name} must be non-negative") from exc
    if (not integer and not math.isfinite(value)) or value < 0:
        raise SystemExit(f"[pm] {name} must be non-negative")
    return value


DEFAULT_AGENT_MAX_ACTIVE_ASSIGNMENTS = _optional_positive_env(
    "LOGPLANE_DEFAULT_AGENT_MAX_ACTIVE_ASSIGNMENTS", integer=True
)
DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS = _optional_positive_env(
    "LOGPLANE_DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS", integer=True
)
DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD = _optional_positive_env(
    "LOGPLANE_DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD", integer=False
)
DEFAULT_WORKFLOW_MAX_ATTEMPTS = _optional_positive_env(
    "LOGPLANE_DEFAULT_WORKFLOW_MAX_ATTEMPTS", integer=True
)
DEFAULT_WORKFLOW_MAX_WALL_CLOCK_SECONDS = _optional_positive_env(
    "LOGPLANE_DEFAULT_WORKFLOW_MAX_WALL_CLOCK_SECONDS", integer=False
)
DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT = _nonnegative_env(
    "LOGPLANE_DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT", integer=True
)
DEFAULT_WORKFLOW_RESERVE_COST_USD_PER_ASSIGNMENT = _nonnegative_env(
    "LOGPLANE_DEFAULT_WORKFLOW_RESERVE_COST_USD_PER_ASSIGNMENT", integer=False
)
if DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS is not None and not (
    0 < DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT
    <= DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS
):
    raise SystemExit(
        "[pm] a finite default token budget requires a positive reservation "
        "no larger than the budget"
    )
if DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD is not None and not (
    0 < DEFAULT_WORKFLOW_RESERVE_COST_USD_PER_ASSIGNMENT
    <= DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD
):
    raise SystemExit(
        "[pm] a finite default cost budget requires a positive reservation "
        "no larger than the budget"
    )


@contextmanager
def single_pm_lock():
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise SystemExit("[pm] this platform cannot safely open the PM lock")
    flags |= nofollow

    fd: Optional[int] = None
    try:
        fd = os.open(LOCK_PATH, flags, 0o600)
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise PermissionError("PM lock must be a regular file owned by this user")
        os.fchmod(fd, 0o600)
    except OSError as exc:
        if fd is not None:
            os.close(fd)
        raise SystemExit(f"[pm] cannot safely open PM lock: {exc}") from exc

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise SystemExit("[pm] another PM is already running")

    # The lock protects the path, not just this open file. If the file was
    # removed or replaced between open and flock, another PM could lock the new
    # file at the same path, so require that the path still names this file.
    try:
        current = os.stat(LOCK_PATH, follow_symlinks=False)
    except FileNotFoundError:
        current = None
    if current is None or (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
        os.close(fd)
        raise SystemExit("[pm] PM lock file was replaced while locking; start the PM again")

    try:
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        yield
    finally:
        os.close(fd)


def plan_next_emission(
    state: PMState,
    now: float,
    lease_seconds: float = WORKER_LEASE_SECONDS,
    default_workflow_max_active_assignments: object = _UNSET_POLICY_DEFAULT,
    default_agent_max_active_assignments: object = _UNSET_AGENT_POLICY_DEFAULT,
    default_workflow_max_total_tokens: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_total_cost_usd: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_attempts: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_wall_clock_seconds: object = _UNSET_POLICY_DEFAULT,
    default_workflow_reserve_tokens_per_assignment: object = _UNSET_POLICY_DEFAULT,
    default_workflow_reserve_cost_usd_per_assignment: object = _UNSET_POLICY_DEFAULT,
) -> Optional[dict]:
    """Return the next deterministic effect needed to reconcile derived state."""
    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status != "cancellation_requested"
            or task.cancel_request_event_id is None
        ):
            continue
        return {
            "topic": "task.cancelled",
            "payload": {
                "task_id": task.task_id,
                "cancel_request_event_id": task.cancel_request_event_id,
                "reason": task.cancel_reason or "cancellation requested",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.cancel_request_event_id,
            "idempotency_key": (
                f"cancelled:task:{task.task_id}:"
                f"request:{task.cancel_request_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status != "supersession_requested"
            or task.supersession_request_event_id is None
            or task.superseded_by_task_id is None
        ):
            continue
        return {
            "topic": "task.superseded",
            "payload": {
                "task_id": task.task_id,
                "replacement_task_id": task.superseded_by_task_id,
                "replacement_created_event_id": task.supersession_request_event_id,
                "reason": task.supersession_reason or "task intent was replaced",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.supersession_request_event_id,
            "idempotency_key": (
                f"superseded:task:{task.task_id}:"
                f"replacement:{task.superseded_by_task_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if (
            task.status in TASK_TERMINAL_STATUSES
            or task.status == "cancellation_requested"
            or task.deadline_at is None
            or now < task.deadline_at
        ):
            continue
        return {
            "topic": "task.deadline_exceeded",
            "payload": {
                "task_id": task.task_id,
                "deadline_at": task.deadline_at,
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
            },
            "caused_by": task.created_event_id,
            "idempotency_key": (
                f"deadline-exceeded:task:{task.task_id}:"
                f"created:{task.created_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "pause_requested" or task.pause_request_event_id is None:
            continue
        return {
            "topic": "task.paused",
            "payload": {
                "task_id": task.task_id,
                "pause_request_event_id": task.pause_request_event_id,
                "reason": task.pause_reason or "pause requested",
                "last_assignment_id": (
                    task.assignment_id
                    if task.assignment_id is not None
                    else task.last_assignment_id
                ),
                "attempts": task.attempt,
                "resume_status": task.paused_from_status,
            },
            "caused_by": task.pause_request_event_id,
            "idempotency_key": (
                f"paused:task:{task.task_id}:request:{task.pause_request_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if task.status != "resume_requested" or task.resume_request_event_id is None:
            continue
        return {
            "topic": "task.resumed",
            "payload": {
                "task_id": task.task_id,
                "resume_request_event_id": task.resume_request_event_id,
                "reason": task.resume_reason or "resume requested",
                "resume_status": task.paused_from_status,
            },
            "caused_by": task.resume_request_event_id,
            "idempotency_key": (
                f"resumed:task:{task.task_id}:request:{task.resume_request_event_id}"
            ),
        }

    for correlation_id in sorted(state.workflows):
        control = state.workflows[correlation_id]
        if control.status == "pause_requested" and control.pause_request_event_id is not None:
            interrupted = [
                {"task_id": task_id, "assignment_id": assignment_id}
                for task_id, assignment_id in control.pause_assignment_snapshot
            ]
            return {
                "topic": "workflow.paused",
                "payload": {
                    "pause_request_event_id": control.pause_request_event_id,
                    "reason": control.pause_reason or "workflow pause requested",
                    "interrupted_assignments": interrupted,
                },
                "caused_by": control.pause_request_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"paused:workflow:{correlation_id}:"
                    f"request:{control.pause_request_event_id}"
                ),
            }
        if control.status == "resume_requested" and control.resume_request_event_id is not None:
            return {
                "topic": "workflow.resumed",
                "payload": {
                    "resume_request_event_id": control.resume_request_event_id,
                    "reason": control.resume_reason or "workflow resume requested",
                },
                "caused_by": control.resume_request_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"resumed:workflow:{correlation_id}:"
                    f"request:{control.resume_request_event_id}"
                ),
            }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if state.workflow_is_paused(task.correlation_id):
            continue
        if task.status not in ACTIVE_TASK_STATUSES or task.assignment_id is None:
            continue
        worker = state.workers.get(task.assignee or "")
        if worker is None:
            reason = "worker is no longer registered"
        elif worker.instance_id != task.worker_instance_id:
            reason = "worker process was replaced"
        elif now - worker.last_seen > lease_seconds:
            reason = "worker lease expired"
        else:
            continue
        return {
            "topic": "task.assignment_expired",
            "payload": {
                "task_id": task.task_id,
                "assignment_id": task.assignment_id,
                "assignee": task.assignee,
                "worker_instance_id": task.worker_instance_id,
                "reason": reason,
            },
            "caused_by": task.assignment_event_id,
            "idempotency_key": f"expire:{task.assignment_id}",
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if state.workflow_is_paused(task.correlation_id):
            continue
        if task.status != "open" or task.assignment_id is not None:
            continue
        for dependency_task_id in task.depends_on:
            dependency = state.tasks[dependency_task_id]
            if dependency.status not in DEPENDENCY_TERMINAL_STATUSES:
                continue
            dependency_event_id = dependency_terminal_event_id(dependency)
            if dependency_event_id is None:
                continue
            return {
                "topic": "task.dependency_failed",
                "payload": {
                    "task_id": task.task_id,
                    "dependency_task_id": dependency_task_id,
                    "dependency_event_id": dependency_event_id,
                    "reason": (
                        f"dependency task {dependency_task_id} ended in "
                        f"{dependency.status}"
                    ),
                },
                "caused_by": dependency_event_id,
                "idempotency_key": (
                    f"dependency-failed:task:{task.task_id}:"
                    f"dependency:{dependency_task_id}:event:{dependency_event_id}"
                ),
            }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if state.workflow_is_paused(task.correlation_id):
            continue
        if (
            task.status != "open"
            or task.last_failure_event_id is None
            or not (
                task.permanent_failure_pending
                or retry_budget_exhausted(task)
            )
        ):
            continue
        if task.permanent_failure_pending:
            reason_code = task.last_failure_code or "permanent_attempt_failure"
            reason = (
                task.last_failure_reason
                or "worker reported a permanent failure"
            )
        else:
            reason_code = "retry_budget_exhausted"
            reason = (
                f"retry budget exhausted after {task.retryable_failures} "
                "retryable failures"
            )
        return {
            "topic": "task.failed",
            "payload": {
                "task_id": task.task_id,
                "reason_code": reason_code,
                "reason": reason,
                "last_assignment_id": task.last_assignment_id,
                "attempts": task.attempt,
                "retryable_failures": task.retryable_failures,
                "max_retries": task.max_retries,
            },
            "caused_by": task.last_failure_event_id,
            "idempotency_key": (
                f"failed:task:{task.task_id}:failure:{task.last_failure_event_id}"
            ),
        }

    for task_id in sorted(state.tasks):
        task = state.tasks[task_id]
        if state.workflow_is_paused(task.correlation_id):
            continue
        if task.status == "blocked" and not task.decision_needed:
            return {
                "topic": "decision.needed",
                "payload": {
                    "task_id": task.task_id,
                    "assignment_id": task.assignment_id,
                    "decision_id": task.decision_id,
                    "reason": task.block_reason or "worker requires human input",
                },
                "caused_by": task.block_event_id,
                "idempotency_key": f"decision-needed:{task.assignment_id}",
            }

    if default_workflow_max_active_assignments is not _UNSET_POLICY_DEFAULT:
        workflows_needing_policy = set()
        for task in state.tasks.values():
            if (
                task.correlation_id is None
                or task.ownership_owner != "agent-bus"
                or task.status in TASK_TERMINAL_STATUSES
            ):
                continue
            control = state.workflow_control(task.correlation_id)
            if control is not None and control.policy_event_id is not None:
                continue
            workflows_needing_policy.add(task.correlation_id)
        policy_candidates = {
            correlation_id: min(
                (
                    task
                    for task in state.tasks.values()
                    if task.correlation_id == correlation_id
                ),
                key=lambda task: task.created_event_id or task.task_id,
            )
            for correlation_id in workflows_needing_policy
        }
        if policy_candidates:
            correlation_id, first_task = min(
                policy_candidates.items(),
                key=lambda item: (
                    item[1].created_event_id or item[1].task_id,
                    item[0],
                ),
            )
            limit = default_workflow_max_active_assignments
            if limit is not None and (
                not isinstance(limit, int)
                or isinstance(limit, bool)
                or limit <= 0
            ):
                raise ValueError(
                    "default workflow max active assignments must be null or positive"
                )
            return {
                "topic": "workflow.policy_set",
                "payload": {
                    "source": "default",
                    "reason": "materialized deployment default",
                    "max_active_assignments": limit,
                    **(
                        {"max_total_tokens": default_workflow_max_total_tokens}
                        if default_workflow_max_total_tokens
                        is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                    **(
                        {
                            "max_total_cost_usd": (
                                default_workflow_max_total_cost_usd
                            )
                        }
                        if default_workflow_max_total_cost_usd
                        is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                    **(
                        {"max_attempts": default_workflow_max_attempts}
                        if default_workflow_max_attempts is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                    **(
                        {
                            "max_wall_clock_seconds": (
                                default_workflow_max_wall_clock_seconds
                            )
                        }
                        if default_workflow_max_wall_clock_seconds
                        is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                    **(
                        {
                            "reserve_tokens_per_assignment": (
                                default_workflow_reserve_tokens_per_assignment
                            )
                        }
                        if default_workflow_reserve_tokens_per_assignment
                        is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                    **(
                        {
                            "reserve_cost_usd_per_assignment": (
                                default_workflow_reserve_cost_usd_per_assignment
                            )
                        }
                        if default_workflow_reserve_cost_usd_per_assignment
                        is not _UNSET_POLICY_DEFAULT
                        else {}
                    ),
                },
                "caused_by": first_task.created_event_id,
                "correlation_id": correlation_id,
                "idempotency_key": (
                    f"policy-default:workflow:{correlation_id}:"
                    f"task:{first_task.task_id}:created:{first_task.created_event_id}"
                ),
            }

    if default_agent_max_active_assignments is not _UNSET_AGENT_POLICY_DEFAULT:
        workers_needing_policy = [
            worker
            for worker in state.workers.values()
            if state.agent_policy(worker.name) is None
        ]
        if workers_needing_policy:
            worker = min(
                workers_needing_policy,
                key=lambda item: (item.registered_event_id, item.name),
            )
            limit = default_agent_max_active_assignments
            if limit is not None and (
                not isinstance(limit, int)
                or isinstance(limit, bool)
                or limit <= 0
            ):
                raise ValueError(
                    "default agent max active assignments must be null or positive"
                )
            return {
                "topic": "agent.policy_set",
                "payload": {
                    "agent_name": worker.name,
                    "source": "default",
                    "reason": "materialized deployment default",
                    "max_active_assignments": limit,
                },
                "caused_by": worker.registered_event_id,
                "idempotency_key": (
                    f"policy-default:agent:{worker.name}:"
                    f"registered:{worker.registered_event_id}"
                ),
            }

    assignment_candidate = state.next_assignment_candidate(now, lease_seconds)
    if assignment_candidate is not None:
        task, worker = assignment_candidate
        attempt = task.last_assignment_attempt + 1
        assignment_id = f"task:{task.task_id}:attempt:{attempt}"
        return {
            "topic": "task.assigned",
            "payload": {
                "task_id": task.task_id,
                "assignment_id": assignment_id,
                "attempt": attempt,
                "assignee": worker.name,
                "worker_instance_id": worker.instance_id,
                "title": task.title,
                "goal": task.title,
                "context": task.context,
                "decisions": json.loads(json.dumps(task.decisions)),
                "dependency_refs": [
                    {
                        "task_id": dependency_task_id,
                        "completion_event_id": state.tasks[
                            dependency_task_id
                        ].completion_event_id,
                    }
                    for dependency_task_id in task.depends_on
                ],
                "required_capabilities": sorted(task.required_capabilities),
                "retry_policy": {"max_retries": task.max_retries},
                "retryable_failures": task.retryable_failures,
                "fairness": {
                    "policy": "workflow_round_robin_v1",
                    "previous_assignment_event_id": (
                        state.last_scheduling_assignment_event_id
                    ),
                },
                **(
                    {
                        "agent_policy_event_id": state.agent_policy(
                            worker.name
                        ).policy_event_id
                    }
                    if state.agent_policy(worker.name) is not None
                    else {}
                ),
                **(
                    {
                        "budget_reservation": {
                            "policy_event_id": state.workflow_control(
                                task.correlation_id
                            ).policy_event_id,
                            "tokens": state.workflow_control(
                                task.correlation_id
                            ).reserve_tokens_per_assignment,
                            "cost_usd": state.workflow_control(
                                task.correlation_id
                            ).reserve_cost_usd_per_assignment,
                        }
                    }
                    if state.workflow_control(task.correlation_id) is not None
                    and state.workflow_control(task.correlation_id).policy_event_id
                    is not None
                    else {}
                ),
                **(
                    {
                        "workflow_policy_event_id": state.workflow_control(
                            task.correlation_id
                        ).policy_event_id
                    }
                    if state.workflow_control(task.correlation_id) is not None
                    and state.workflow_control(task.correlation_id).policy_event_id
                    is not None
                    else {}
                ),
                **(
                    {"deadline_at": task.deadline_at}
                    if task.deadline_at is not None
                    else {}
                ),
                "ownership": {
                    "mode": task.ownership_mode,
                    "owner": task.ownership_owner,
                },
                **(
                    {"external_origin": task.external_origin}
                    if task.external_origin is not None
                    else {}
                ),
            },
            "caused_by": (
                max(
                    state.tasks[dependency_task_id].completion_event_id
                    for dependency_task_id in task.depends_on
                )
                if task.depends_on and task.attempt == 0
                else task.open_event_id
            ),
            "idempotency_key": f"assign:{assignment_id}",
        }
    return None


def reconcile(
    state: PMState,
    bus: BusClient,
    *,
    now: Optional[float] = None,
    lease_seconds: float = WORKER_LEASE_SECONDS,
    clock: Callable[[], float] = time.time,
    cursor: Optional[OrderedProjectionCursor] = None,
    default_workflow_max_active_assignments: object = _UNSET_POLICY_DEFAULT,
    default_agent_max_active_assignments: object = _UNSET_AGENT_POLICY_DEFAULT,
    default_workflow_max_total_tokens: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_total_cost_usd: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_attempts: object = _UNSET_POLICY_DEFAULT,
    default_workflow_max_wall_clock_seconds: object = _UNSET_POLICY_DEFAULT,
    default_workflow_reserve_tokens_per_assignment: object = _UNSET_POLICY_DEFAULT,
    default_workflow_reserve_cost_usd_per_assignment: object = _UNSET_POLICY_DEFAULT,
) -> list[dict]:
    """Publish effects until stable, consuming persisted order when available."""
    emitted: list[dict] = []
    for _ in range(10_000):
        current_time = now if now is not None else clock()
        planned = plan_next_emission(
            state,
            current_time,
            lease_seconds,
            default_workflow_max_active_assignments,
            default_agent_max_active_assignments,
            default_workflow_max_total_tokens,
            default_workflow_max_total_cost_usd,
            default_workflow_max_attempts,
            default_workflow_max_wall_clock_seconds,
            default_workflow_reserve_tokens_per_assignment,
            default_workflow_reserve_cost_usd_per_assignment,
        )
        if planned is None:
            return emitted
        if (
            cursor is not None
            and planned["topic"] == "task.assignment_expired"
            and planned["payload"].get("reason") == "worker lease expired"
            and cursor.catch_up(bus)
        ):
            # The PM applies events one at a time. When it falls behind the
            # log (a busy host, a burst of events), a heartbeat the worker
            # already sent may still be unread, and expiring now would retry
            # work a live worker is still doing. Read to the head of the log
            # and plan again; expire only if the lease is still stale.
            continue
        publish_options = {
            "caused_by": planned.get("caused_by"),
            "idempotency_key": planned["idempotency_key"],
        }
        if "correlation_id" in planned:
            publish_options["correlation_id"] = planned["correlation_id"]
        sent = bus.publish(
            planned["topic"],
            planned["payload"],
            **publish_options,
        )
        if cursor is None:
            if not apply_event(state, sent):
                raise RuntimeError(
                    f"PM emitted {sent.get('topic')}#{sent.get('id')} but could not apply it"
                )
            applied = True
        else:
            consumed = cursor.catch_up(bus)
            sent_id = sent.get("id")
            if not isinstance(sent_id, int) or cursor.last_event_id < sent_id:
                raise RuntimeError(
                    f"PM could not catch up through emitted event #{sent_id}"
                )
            applied = any(
                event.get("id") == sent_id and event_applied
                for event, event_applied in consumed
            )
        emitted.append(sent)
        print(
            (
                f"[pm] reconciled -> {sent['topic']}#{sent['id']} {sent['payload']}"
                if applied
                else f"[pm] stale plan -> {sent['topic']}#{sent['id']}"
            ),
            flush=True,
        )
    raise RuntimeError("reconciliation did not converge")


def initial_cursor(bus: BusClient, snapshot_path: Optional[Path] = None) -> OrderedProjectionCursor:
    if snapshot_path is None:
        cursor = OrderedProjectionCursor(PMState())
        cursor.consume(bus.query_all(after_id=0, topics=list(PM_TOPICS)))
        return cursor
    import sqlite3
    from logplane.projection_store import event_anchor, load_projection, save_projection

    try:
        result = load_projection(snapshot_path)
    except sqlite3.Error as exc:
        raise ValueError("could not replay the local snapshot database; start the upgraded bus "
                         "with the same --config, or omit --snapshot") from exc
    if bus.database_identity() != result.database_id:
        raise ValueError("local snapshot database does not match the HTTP bus; check --config")
    if result.last_event_id and event_anchor(bus.get_event(result.last_event_id)) != result.anchor:
        raise ValueError("local snapshot event anchor does not match the HTTP bus; check --config")
    # Refresh only from this pinned replay, before reconcile mutates the state.
    # A cache write failure must not prevent a successfully replayed PM starting.
    try:
        saved = save_projection(snapshot_path, result)
        if not saved:
            print("[pm] snapshot too large; using replayed state without saving", flush=True)
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(f"[pm] snapshot not saved: {exc}", flush=True)
    print(f"[pm] {result.cache_note}; replayed {result.replayed_events} coordination events", flush=True)
    return OrderedProjectionCursor(result.state, result.last_event_id)


def main(snapshot_path: Optional[Path] = None):
    check_legacy_environment()
    with single_pm_lock():
        bus = BusClient(BUS_URL, actor="pm")
        cursor = initial_cursor(bus, snapshot_path)
        state = cursor.state
        print(f"[pm] replayed log up to #{cursor.last_event_id}, then reconciling...", flush=True)

        policy_defaults = {
            "default_workflow_max_active_assignments": (
                DEFAULT_WORKFLOW_MAX_ACTIVE_ASSIGNMENTS
            ),
            "default_agent_max_active_assignments": (
                DEFAULT_AGENT_MAX_ACTIVE_ASSIGNMENTS
            ),
            "default_workflow_max_total_tokens": (
                DEFAULT_WORKFLOW_MAX_TOTAL_TOKENS
            ),
            "default_workflow_max_total_cost_usd": (
                DEFAULT_WORKFLOW_MAX_TOTAL_COST_USD
            ),
            "default_workflow_max_attempts": DEFAULT_WORKFLOW_MAX_ATTEMPTS,
            "default_workflow_max_wall_clock_seconds": (
                DEFAULT_WORKFLOW_MAX_WALL_CLOCK_SECONDS
            ),
            "default_workflow_reserve_tokens_per_assignment": (
                DEFAULT_WORKFLOW_RESERVE_TOKENS_PER_ASSIGNMENT
            ),
            "default_workflow_reserve_cost_usd_per_assignment": (
                DEFAULT_WORKFLOW_RESERVE_COST_USD_PER_ASSIGNMENT
            ),
        }

        # This closes the prototype's crash window: state replay is followed by
        # deterministic effect reconciliation before waiting for another event.
        reconcile(
            state,
            bus,
            cursor=cursor,
            **policy_defaults,
        )

        for event in bus.subscribe(
            from_id=cursor.last_event_id,
            topics=list(PM_TOPICS),
            on_idle=lambda: reconcile(
                state,
                bus,
                cursor=cursor,
                **policy_defaults,
            ),
        ):
            cursor.consume([event])
            reconcile(
                state,
                bus,
                cursor=cursor,
                **policy_defaults,
            )


if __name__ == "__main__":
    sys.exit(main())
