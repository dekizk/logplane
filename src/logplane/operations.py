"""Deterministic, read-only operational views over logplane history."""

from __future__ import annotations

import math
from collections import Counter
from typing import Iterable, Optional

from logplane.projection import (
    ACTIVE_TASK_STATUSES,
    DEPENDENCY_TERMINAL_STATUSES,
    TASK_TERMINAL_STATUSES,
    CoordinationProjection,
    TaskRecord,
    replay_events,
    retry_budget_exhausted,
)


class ProjectionLookupError(LookupError):
    """A requested task or workflow is absent from the replayed history."""


def build_projection(events: Iterable[dict]) -> CoordinationProjection:
    """Rebuild the coordination projection from scratch."""
    return replay_events(events)


def retries_remaining(task: TaskRecord) -> Optional[int]:
    """Return remaining automatic retries, or None for a legacy unbounded task."""
    if task.max_retries is None:
        return None
    return max(0, task.max_retries - task.retryable_failures)


def operator_next_action(task: TaskRecord, *, now=None) -> list[str]:
    """Copyable command templates, never automatically executed."""
    base = ['logplane']
    if (now is not None and task.deadline_at is not None and now >= task.deadline_at
            and task.status not in TASK_TERMINAL_STATUSES):
        return base + ['explain', str(task.task_id)]
    if task.status == 'failed' and task.failed_event_id:
        return base + ['retry', str(task.task_id), '--failed-event', str(task.failed_event_id),
                       '--additional-retries', '1', '--reason', 'REPLACE_WITH_REASON']
    if task.status == 'blocked' and task.decision_needed and task.decision_event_id:
        return base + ['decide', str(task.task_id), '--decision-event', str(task.decision_event_id),
                       '--value', '"REPLACE_WITH_ANSWER"']
    if task.status == 'paused':
        return base + ['resume', 'task', str(task.task_id), '--reason', 'REPLACE_WITH_REASON']
    return base + ['explain' if task.status not in TASK_TERMINAL_STATUSES else 'task', str(task.task_id)]


def discovery_view(state, *, kind, now, lease_seconds, limit=50, after_task_id=0,
                   after_workflow='', status=None, correlation_id=None):
    """Paginate current projected state; pagination does not mutate history."""
    if not 1 <= limit <= 200:
        raise ValueError('listing limit must be between 1 and 200')
    if kind == 'workflows':
        identities = sorted({task.correlation_id for task in state.tasks.values()
                             if task.correlation_id is not None})
        rows = []
        for identity in identities:
            if identity <= after_workflow:
                continue
            view = workflow_view(state, identity, now=now, lease_seconds=lease_seconds)
            if status and view['status'] != status:
                continue
            rows.append({key: view[key] for key in ('correlation_id', 'status', 'task_count', 'status_counts')})
            if len(rows) > limit:
                break
        return {'workflows': rows[:limit], 'next_after_workflow': rows[limit-1]['correlation_id'] if len(rows) > limit else None,
                'uncorrelated_task_count': sum(t.correlation_id is None for t in state.tasks.values())}
    candidates = [task for task in sorted(state.tasks.values(), key=lambda task: task.task_id)
                  if task.task_id > after_task_id
                  and (not status or task.status == status)
                  and (correlation_id is None or task.correlation_id == correlation_id)
                  and (kind != 'decisions' or (task.status == 'blocked' and task.decision_needed))]
    rows = []
    for task in candidates[:limit]:
        view = task_view(state, task.task_id, now=now, lease_seconds=lease_seconds)
        row = {key: view[key] for key in ('task_id', 'title', 'correlation_id', 'status', 'effective_status', 'status_event_id')}
        row.update(explanation=view['explanation'], next_action=operator_next_action(task, now=now))
        if kind == 'decisions':
            row.update(decision_id=task.decision_id, decision_event_id=task.decision_event_id,
                       assignment_id=task.assignment_id, reason=task.block_reason,
                       response_allowed=task.deadline_at is None or now < task.deadline_at)
            if not row['response_allowed']:
                row['next_action'] = ['logplane', 'explain', str(task.task_id)]
        rows.append(row)
    return {kind: rows, 'next_after_task_id': rows[-1]['task_id'] if len(candidates) > limit else None}


def worker_views(
    state: CoordinationProjection,
    *,
    now: float,
    lease_seconds: float,
) -> list[dict]:
    """Describe the latest registered process for every worker name."""
    views = []
    for name in sorted(state.workers):
        worker = state.workers[name]
        lease_age = max(0.0, now - worker.last_seen)
        load = state.worker_load(worker)
        policy = state.agent_policy(worker.name)
        effective_capacity = state.agent_limit(worker)
        views.append(
            {
                "name": worker.name,
                "instance_id": worker.instance_id,
                "status": "healthy" if lease_age <= lease_seconds else "stale",
                "lease_age_seconds": round(lease_age, 3),
                "lease_seconds": lease_seconds,
                "capacity": worker.capacity,
                "effective_capacity": effective_capacity,
                "load": load,
                "available_slots": max(0, effective_capacity - load),
                "policy": (
                    {
                        "event_id": policy.policy_event_id,
                        "source": policy.source,
                        "reason": policy.reason,
                        "max_active_assignments": policy.max_active_assignments,
                    }
                    if policy is not None
                    else {"status": "unmaterialized"}
                ),
                "capabilities": sorted(worker.capabilities),
                "last_event_id": worker.last_event_id,
            }
        )
    return views


def explain_task(
    state: CoordinationProjection,
    task_id: int,
    *,
    now: float,
    lease_seconds: float,
) -> dict:
    """Explain the concrete condition currently governing one task."""
    task = _get_task(state, task_id)
    evidence = [task.status_event_id]

    if task.status == "completed":
        return _explanation(
            "completed",
            f"Task completed successfully at event #{task.completion_event_id}.",
            evidence,
        )
    if task.status == "failed":
        reason = task.terminal_failure_reason or "the recorded terminal failure"
        code = task.terminal_failure_code or "task_failed"
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "failed",
            f"Task is terminal after {code}: {reason}",
            evidence,
        )
    if task.status == "dependency_failed":
        evidence.append(task.dependency_failure_event_id)
        return _explanation(
            "dependency_failed",
            task.dependency_failure_reason
            or f"Dependency task {task.dependency_failure_task_id} ended terminally.",
            evidence,
            dependency_task_id=task.dependency_failure_task_id,
        )
    if task.status == "cancelled":
        evidence.append(task.cancel_request_event_id)
        return _explanation(
            "cancelled",
            f"Task was cancelled: {task.cancel_reason or 'no reason recorded'}",
            evidence,
        )
    if task.status == "deadline_exceeded":
        evidence.append(task.created_event_id)
        return _explanation(
            "deadline_exceeded",
            f"Task deadline {task.deadline_at} was reached before completion.",
            evidence,
        )
    if task.status == "superseded":
        evidence.extend(
            [task.supersession_request_event_id, task.superseded_event_id]
        )
        return _explanation(
            "superseded",
            f"Task intent was replaced by task {task.superseded_by_task_id}: "
            f"{task.supersession_reason or 'no reason recorded'}",
            evidence,
            replacement_task_id=task.superseded_by_task_id,
        )
    if task.status == "supersession_requested":
        evidence.append(task.supersession_request_event_id)
        return _explanation(
            "supersession_pending",
            f"Replacement task {task.superseded_by_task_id} is recorded; the PM has not yet terminalized this task.",
            evidence,
            replacement_task_id=task.superseded_by_task_id,
        )
    if task.status == "cancellation_requested":
        evidence.append(task.cancel_request_event_id)
        return _explanation(
            "cancellation_pending",
            "Cancellation is recorded and is waiting for PM reconciliation.",
            evidence,
        )
    if task.deadline_at is not None and now >= task.deadline_at:
        evidence.append(task.created_event_id)
        control_detail = None
        if task.status in {"pause_requested", "paused", "resume_requested"}:
            control_detail = task.status
        else:
            workflow_control = state.workflow_control(task.correlation_id)
            if workflow_control is not None and workflow_control.status != "active":
                control_detail = f"workflow_{workflow_control.status}"
                evidence.append(workflow_control.last_event_id)
        return _explanation(
            "deadline_reconciliation_pending",
            "The task deadline has passed and the PM has not yet recorded task.deadline_exceeded.",
            evidence,
            control_status=control_detail,
        )
    if task.status == "pause_requested":
        evidence.append(task.pause_request_event_id)
        if task.resume_request_event_id is not None:
            evidence.append(task.resume_request_event_id)
            return _explanation(
                "resume_queued",
                "Resume is queued while the PM finishes recording the pause ownership fence.",
                evidence,
            )
        return _explanation(
            "pause_pending",
            "Pause is recorded and is waiting for PM acknowledgement.",
            evidence,
        )
    if task.status == "paused":
        evidence.extend([task.pause_request_event_id, task.paused_event_id])
        return _explanation(
            "paused",
            f"Task is paused: {task.pause_reason or 'no reason recorded'}",
            evidence,
            resume_status=task.paused_from_status,
        )
    if task.status == "resume_requested":
        evidence.append(task.resume_request_event_id)
        return _explanation(
            "resume_pending",
            "Resume is recorded and is waiting for PM acknowledgement.",
            evidence,
        )

    workflow_control = state.workflow_control(task.correlation_id)
    if workflow_control is not None and workflow_control.status != "active":
        evidence.append(workflow_control.last_event_id)
        if (
            workflow_control.status == "pause_requested"
            and workflow_control.resume_request_event_id is not None
        ):
            return _explanation(
                "workflow_resume_queued",
                f"Workflow {task.correlation_id} will resume after the PM records its pause ownership fence.",
                evidence,
                workflow_status=workflow_control.status,
            )
        return _explanation(
            f"workflow_{workflow_control.status}",
            f"Workflow {task.correlation_id} is {workflow_control.status.replace('_', ' ')}; this task cannot advance.",
            evidence,
            workflow_status=workflow_control.status,
        )
    if task.status == "blocked":
        evidence.extend([task.block_event_id, task.decision_event_id])
        if task.decision_needed:
            return _explanation(
                "human_decision_required",
                task.block_reason or "The worker requires human input.",
                evidence,
                decision_id=task.decision_id,
            )
        return _explanation(
            "decision_request_pending",
            "The worker is blocked and the PM has not yet recorded decision.needed.",
            evidence,
            decision_id=task.decision_id,
        )
    if task.status in ACTIVE_TASK_STATUSES:
        evidence.append(task.assignment_event_id)
        evidence.append(task.assignment_policy_event_id)
        evidence.append(task.assignment_previous_event_id)
        evidence.append(task.assignment_agent_policy_event_id)
        worker = state.workers.get(task.assignee or "")
        if worker is None:
            return _explanation(
                "active_worker_missing",
                f"Attempt {task.assignment_id} is assigned, but worker {task.assignee!r} is no longer registered.",
                evidence,
            )
        evidence.append(worker.last_event_id)
        if worker.instance_id != task.worker_instance_id:
            return _explanation(
                "active_worker_replaced",
                f"Attempt {task.assignment_id} belongs to replaced process {task.worker_instance_id}; current process is {worker.instance_id}.",
                evidence,
            )
        lease_age = max(0.0, now - worker.last_seen)
        if lease_age > lease_seconds:
            return _explanation(
                "active_lease_expired",
                f"Attempt {task.assignment_id} has a stale worker lease ({lease_age:.1f}s old; limit {lease_seconds:.1f}s).",
                evidence,
                lease_age_seconds=round(lease_age, 3),
            )
        verb = "running on" if task.status == "started" else "assigned to"
        return _explanation(
            "active_lease_healthy",
            f"Attempt {task.assignment_id} is {verb} {task.assignee} with a healthy lease.",
            evidence,
            lease_age_seconds=round(lease_age, 3),
            workflow_policy_event_id=task.assignment_policy_event_id,
            fairness_policy=task.assignment_fairness_policy,
            previous_assignment_event_id=task.assignment_previous_event_id,
            agent_policy_event_id=task.assignment_agent_policy_event_id,
            budget_reservation={
                "tokens": task.assignment_reserved_tokens,
                "cost_usd": task.assignment_reserved_cost_usd,
            },
        )

    if task.status != "open":
        return _explanation(
            "unknown_state",
            f"Task is in unrecognized derived state {task.status!r}.",
            evidence,
        )

    if task.last_failure_event_id is not None and task.permanent_failure_pending:
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "permanent_failure_reconciliation_pending",
            f"The last attempt failed permanently ({task.last_failure_code}); the PM has not yet recorded task.failed.",
            evidence,
        )
    if (
        task.last_failure_event_id is not None
        and task.max_retries is not None
        and task.retryable_failures > task.max_retries
    ):
        evidence.append(task.last_failure_event_id)
        return _explanation(
            "retry_exhaustion_reconciliation_pending",
            "The automatic retry budget is exhausted and the PM has not yet recorded task.failed.",
            evidence,
        )

    terminal_dependencies = []
    incomplete_dependencies = []
    for dependency_task_id in task.depends_on:
        dependency = state.tasks[dependency_task_id]
        if dependency.status in DEPENDENCY_TERMINAL_STATUSES:
            terminal_dependencies.append(dependency_task_id)
        elif dependency.status != "completed":
            incomplete_dependencies.append(dependency_task_id)
        evidence.append(dependency.status_event_id)
    if terminal_dependencies:
        return _explanation(
            "dependency_failure_reconciliation_pending",
            "A prerequisite ended terminally; the PM has not yet propagated task.dependency_failed.",
            evidence,
            dependency_task_ids=terminal_dependencies,
        )
    if incomplete_dependencies:
        labels = ", ".join(str(value) for value in incomplete_dependencies)
        return _explanation(
            "dependencies_incomplete",
            f"Waiting for prerequisite task(s) {labels} to complete.",
            evidence,
            dependency_task_ids=incomplete_dependencies,
        )

    if task.not_before is not None and now < task.not_before:
        evidence.append(task.created_event_id)
        return _explanation(
            "not_before_pending",
            f"Task is deliberately delayed until Unix timestamp {task.not_before}.",
            evidence,
            priority=task.priority,
            not_before=task.not_before,
            seconds_remaining=round(task.not_before - now, 3),
        )

    if (
        task.retry_not_before is not None
        and now < task.retry_not_before
        and not retry_budget_exhausted(task)
    ):
        evidence.append(task.last_failure_event_id)
        remaining = round(task.retry_not_before - now, 3)
        return _explanation(
            "retry_delay_pending",
            f"Waiting {_duration(remaining)} before the next attempt: the last attempt "
            f"failed with {task.last_failure_code} and asked for a delay.",
            evidence,
            failure_code=task.last_failure_code,
            failure_reason=task.last_failure_reason,
            retry_not_before=task.retry_not_before,
            seconds_remaining=remaining,
        )

    workflow_policy = state.workflow_control(task.correlation_id)
    if (
        workflow_policy is not None
        and workflow_policy.policy_event_id is not None
        and state.workflow_limit_reached(task.correlation_id)
    ):
        active_tasks = sorted(
            state.workflow_active_tasks(task.correlation_id),
            key=state.task_schedule_key,
        )
        evidence.append(workflow_policy.policy_event_id)
        evidence.extend(item.assignment_event_id for item in active_tasks)
        return _explanation(
            "workflow_concurrency_limit",
            f"Workflow {task.correlation_id} is using all "
            f"{workflow_policy.max_active_assignments} permitted active assignment(s).",
            evidence,
            policy_event_id=workflow_policy.policy_event_id,
            max_active_assignments=workflow_policy.max_active_assignments,
            active_task_ids=[item.task_id for item in active_tasks],
        )

    budget_block = state.workflow_budget_block(task.correlation_id, now)
    if budget_block is not None:
        evidence.append(budget_block.get("policy_event_id"))
        usage = state.workflow_budget_usage(task.correlation_id)
        evidence.extend(
            record.assignment_event_id
            for record in state.workflow_accounting(task.correlation_id)
        )
        return _explanation(
            "workflow_budget_limit",
            f"Workflow {task.correlation_id} cannot reserve another assignment: "
            f"{budget_block['resource']} budget is exhausted.",
            evidence,
            **budget_block,
            accounting=usage,
        )

    active = state.active_workers(now, lease_seconds)
    if not active:
        evidence.extend(worker.last_event_id for worker in state.workers.values())
        return _explanation(
            "no_active_workers",
            "No worker currently has a healthy lease.",
            evidence,
        )
    capable = [
        worker
        for worker in active
        if task.required_capabilities.issubset(worker.capabilities)
    ]
    evidence.extend(worker.last_event_id for worker in active)
    if not capable:
        required = ", ".join(sorted(task.required_capabilities)) or "none"
        return _explanation(
            "capabilities_unavailable",
            f"No healthy worker provides all required capabilities: {required}.",
            evidence,
            required_capabilities=sorted(task.required_capabilities),
        )
    available = [worker for worker in capable if not state.agent_limit_reached(worker)]
    if not available:
        policy_limited = [
            worker
            for worker in capable
            if state.agent_policy(worker.name) is not None
            and state.agent_policy(worker.name).max_active_assignments is not None
            and state.agent_limit(worker) < worker.capacity
            and state.agent_limit_reached(worker)
        ]
        if policy_limited:
            evidence.extend(
                state.agent_policy(worker.name).policy_event_id
                for worker in policy_limited
            )
            return _explanation(
                "agent_concurrency_limit",
                "All capable agents are at their persisted concurrency limit.",
                evidence,
                agents=[
                    {
                        "name": worker.name,
                        "load": state.worker_load(worker),
                        "limit": state.agent_limit(worker),
                        "policy_event_id": state.agent_policy(
                            worker.name
                        ).policy_event_id,
                    }
                    for worker in policy_limited
                ],
            )
        capable_worker_names = {worker.name for worker in capable}
        occupying = sorted(
            (
                active_task
                for active_task in state.tasks.values()
                if active_task.status in ACTIVE_TASK_STATUSES
                and active_task.assignee in capable_worker_names
            ),
            key=state.task_schedule_key,
        )
        evidence.extend(active_task.assignment_event_id for active_task in occupying)
        return _explanation(
            "workers_at_capacity",
            "All healthy workers with the required capabilities are at capacity"
            + (
                "; active task(s): "
                + ", ".join(
                    f"{item.task_id} ({item.priority})" for item in occupying
                )
                if occupying
                else ""
            )
            + ".",
            evidence,
            active_tasks=[
                {"task_id": item.task_id, "priority": item.priority}
                for item in occupying
            ],
        )

    selected = state.next_assignment_candidate(now, lease_seconds)
    if selected is not None and selected[0].task_id != task.task_id:
        preceding = selected[0]
        evidence.append(preceding.created_event_id)
        evidence.append(state.last_scheduling_assignment_event_id)
        if preceding.correlation_id != task.correlation_id:
            return _explanation(
                "ready_after_fair_workflow",
                f"Task is eligible; workflow {preceding.correlation_id} has the "
                "next round-robin assignment turn.",
                evidence,
                priority=task.priority,
                selected_workflow_correlation_id=preceding.correlation_id,
                selected_task_id=preceding.task_id,
                previous_assignment_event_id=(
                    state.last_scheduling_assignment_event_id
                ),
                fairness_policy="workflow_round_robin_v1",
            )
        if preceding.priority != task.priority:
            code = "ready_after_higher_priority"
            ordering = f"higher priority {preceding.priority}"
        else:
            code = "ready_after_earlier_task"
            ordering = "earlier immutable creation order"
        return _explanation(
            code,
            f"Task is eligible; PM considers task {preceding.task_id} first by "
            f"{ordering}.",
            evidence,
            priority=task.priority,
            preceding_task_id=preceding.task_id,
            preceding_priority=preceding.priority,
        )
    return _explanation(
        "ready_for_assignment",
        "All dependencies and policies are satisfied; the task is waiting for PM assignment reconciliation.",
        evidence,
        priority=task.priority,
    )


def task_view(
    state: CoordinationProjection,
    task_id: int,
    *,
    now: float,
    lease_seconds: float,
) -> dict:
    """Build a JSON-safe operational view of one task."""
    task = _get_task(state, task_id)
    explanation = explain_task(
        state,
        task_id,
        now=now,
        lease_seconds=lease_seconds,
    )
    dependencies = []
    for dependency_task_id in task.depends_on:
        dependency = state.tasks[dependency_task_id]
        dependencies.append(
            {
                "task_id": dependency_task_id,
                "title": dependency.title,
                "status": dependency.status,
                "satisfied": dependency.status == "completed",
                "status_event_id": dependency.status_event_id,
            }
        )
    workflow_control = state.workflow_control(task.correlation_id)
    effective_status = task.status
    if (
        task.status not in TASK_TERMINAL_STATUSES
        and workflow_control is not None
        and workflow_control.status != "active"
    ):
        effective_status = f"workflow_{workflow_control.status}"
    return {
        "task_id": task.task_id,
        "title": task.title,
        "next_action": operator_next_action(task, now=now),
        "correlation_id": task.correlation_id,
        "status": task.status,
        "effective_status": effective_status,
        "status_event_id": task.status_event_id,
        "created_event_id": task.created_event_id,
        "attempt": task.attempt,
        "assignment_id": task.assignment_id,
        "assignment_event_id": task.assignment_event_id,
        "assignment_active": task.status in ACTIVE_TASK_STATUSES,
        "assignee": task.assignee,
        "worker_instance_id": task.worker_instance_id,
        "required_capabilities": sorted(task.required_capabilities),
        "priority": task.priority,
        "not_before": task.not_before,
        "retry_not_before": task.retry_not_before,
        "workflow_policy": _workflow_policy_view(workflow_control),
        "assignment_policy_event_id": task.assignment_policy_event_id,
        "assignment_agent_policy_event_id": task.assignment_agent_policy_event_id,
        "budget_reservation": {
            "tokens": task.assignment_reserved_tokens,
            "cost_usd": task.assignment_reserved_cost_usd,
        },
        "assignment_fairness": {
            "policy": task.assignment_fairness_policy,
            "previous_assignment_event_id": task.assignment_previous_event_id,
        },
        "dependencies": dependencies,
        "retry_policy": {
            "max_retries": task.max_retries,
            "retryable_failures": task.retryable_failures,
            "remaining": retries_remaining(task),
        },
        "deadline_at": task.deadline_at,
        "decision": {
            "needed": task.decision_needed,
            "decision_id": task.decision_id,
            "event_id": task.decision_event_id,
            "reason": task.block_reason,
        },
        "ownership": {
            "mode": task.ownership_mode,
            "owner": task.ownership_owner,
        },
        "completion_summary": task.completion_summary,
        "control": {
            "pause_request_event_id": task.pause_request_event_id,
            "pause_reason": task.pause_reason,
            "paused_event_id": task.paused_event_id,
            "resume_request_event_id": task.resume_request_event_id,
            "resume_reason": task.resume_reason,
            "resumed_event_id": task.resumed_event_id,
            "supersedes_task_id": task.supersedes_task_id,
            "superseded_by_task_id": task.superseded_by_task_id,
            "supersession_reason": task.supersession_reason,
            "superseded_event_id": task.superseded_event_id,
        },
        "explanation": explanation,
    }


def workflow_view(
    state: CoordinationProjection,
    correlation_id: str,
    *,
    now: float,
    lease_seconds: float,
    telemetry_events: Iterable[dict] = (),
) -> dict:
    """Summarize one correlated DAG plus its separate telemetry stream."""
    tasks = [
        task
        for task in state.tasks.values()
        if task.correlation_id == correlation_id
    ]
    if not tasks:
        raise ProjectionLookupError(f"workflow {correlation_id!r} was not found")
    tasks.sort(key=lambda task: task.task_id)
    views = [
        task_view(state, task.task_id, now=now, lease_seconds=lease_seconds)
        for task in tasks
    ]
    counts = Counter(task.status for task in tasks)
    statuses = {task.status for task in tasks}
    control = state.workflow_control(correlation_id)
    if control is not None and control.status != "active":
        status = control.status
    elif statuses == {"completed"}:
        status = "completed"
    elif statuses.issubset(TASK_TERMINAL_STATUSES):
        failure_statuses = {"failed", "dependency_failed", "deadline_exceeded"}
        status = (
            "ended_with_failures"
            if statuses.intersection(failure_statuses)
            else "ended_by_control"
        )
    elif statuses.intersection(
        {"blocked", "cancellation_requested", "pause_requested", "paused", "resume_requested", "supersession_requested"}
    ):
        status = "needs_attention"
    elif statuses.intersection(ACTIVE_TASK_STATUSES):
        status = "running"
    else:
        status = "waiting"
    edges = [
        {"from_task_id": dependency_id, "to_task_id": task.task_id}
        for task in tasks
        for dependency_id in task.depends_on
    ]
    supersessions = [
        {
            "from_task_id": task.supersedes_task_id,
            "to_task_id": task.task_id,
            "reason": task.supersession_reason,
        }
        for task in tasks
        if task.supersedes_task_id is not None
    ]
    relevant_telemetry = [
        event
        for event in telemetry_events
        if event.get("correlation_id") == correlation_id
    ]
    return {
        "correlation_id": correlation_id,
        "status": status,
        "task_count": len(tasks),
        "status_counts": dict(sorted(counts.items())),
        "tasks": views,
        "edges": edges,
        "supersessions": supersessions,
        "control": (
            {
                "status": control.status,
                "pause_request_event_id": control.pause_request_event_id,
                "pause_reason": control.pause_reason,
                "paused_event_id": control.paused_event_id,
                "resume_request_event_id": control.resume_request_event_id,
                "resume_reason": control.resume_reason,
                "resumed_event_id": control.resumed_event_id,
                "last_event_id": control.last_event_id,
            }
            if control is not None
            else {"status": "active"}
        ),
        "policy": _workflow_policy_view(control),
        "scheduling": {
            "policy": "workflow_round_robin_v1",
            "last_assignment_event_id": state.last_scheduling_assignment_event_id,
        },
        "budget": {
            "policy_event_id": (
                control.policy_event_id if control is not None else None
            ),
            **state.workflow_budget_usage(correlation_id),
            "blocked": state.workflow_budget_block(correlation_id, now),
        },
        "telemetry": summarize_telemetry(relevant_telemetry),
        "event_ids": sorted(
            {
                event_id
                for task in tasks
                for event_id in (task.created_event_id, task.status_event_id)
                if event_id is not None
            }
            | {
                event_id
                for event_id in (
                    control.policy_event_id if control is not None else None,
                    control.last_event_id if control is not None else None,
                    state.last_scheduling_assignment_event_id,
                )
                if event_id is not None
            }
            | {
                usage[0]
                for record in state.workflow_accounting(correlation_id)
                for usage in record.usage.values()
            }
        ),
    }


def workflow_mermaid(value: dict) -> str:
    """Render a derived workflow view as a read-only Mermaid flowchart."""
    tasks = value.get("tasks")
    edges = value.get("edges")
    supersessions = value.get("supersessions", [])
    if (
        not isinstance(tasks, list)
        or not isinstance(edges, list)
        or not isinstance(supersessions, list)
    ):
        raise ValueError("workflow view must contain task and edge arrays")
    lines = ["flowchart LR"]
    known = set()
    for task in tasks:
        if not isinstance(task, dict):
            raise ValueError("workflow task view must be an object")
        task_id = task.get("task_id")
        if not isinstance(task_id, int) or isinstance(task_id, bool) or task_id <= 0:
            raise ValueError("workflow task id must be a positive integer")
        known.add(task_id)
        title = _mermaid_text(str(task.get("title", "Task")))
        status = _mermaid_text(
            str(task.get("effective_status", task.get("status", "unknown")))
        )
        lines.append(f'  T{task_id}["Task {task_id}: {title}<br/>{status}"]')
    for edge in edges:
        if not isinstance(edge, dict):
            raise ValueError("workflow edge must be an object")
        source = edge.get("from_task_id")
        target = edge.get("to_task_id")
        if source not in known or target not in known:
            raise ValueError("workflow edge references an unknown task")
        lines.append(f"  T{source} --> T{target}")
    for link in supersessions:
        if not isinstance(link, dict):
            raise ValueError("workflow supersession view must be an object")
        source = link.get("from_task_id")
        target = link.get("to_task_id")
        if source not in known or target not in known:
            raise ValueError("workflow supersession references an unknown task")
        lines.append(f"  T{source} -. superseded by .-> T{target}")
    return "\n".join(lines)


def _mermaid_text(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", " ")
    )


def summarize_telemetry(events: Iterable[dict]) -> dict:
    """Aggregate bounded usage once per terminal model/tool span."""
    model_started: dict[tuple[object, object], dict] = {}
    model_terminal: dict[tuple[object, object], dict] = {}
    tool_started: dict[tuple[object, object], dict] = {}
    tool_terminal: dict[tuple[object, object], dict] = {}
    for event in events:
        if not isinstance(event, dict):
            continue
        topic = event.get("topic")
        payload = event.get("payload")
        if not isinstance(topic, str) or not isinstance(payload, dict):
            continue
        if topic.startswith("telemetry.model."):
            key = (payload.get("assignment_id"), payload.get("invocation_id"))
            target = model_started if topic.endswith(".started") else model_terminal
        elif topic.startswith("telemetry.tool."):
            key = (payload.get("assignment_id"), payload.get("tool_call_id"))
            target = tool_started if topic.endswith(".started") else tool_terminal
        else:
            continue
        previous = target.get(key)
        if previous is None or _event_id(event) > _event_id(previous):
            target[key] = event

    usage_totals = {
        "input_tokens": 0.0,
        "output_tokens": 0.0,
        "total_tokens": 0.0,
        "cost_usd": 0.0,
        "duration_ms": 0.0,
    }
    usage_samples = 0
    cost_samples = 0
    by_model: dict[tuple[object, object], dict] = {}
    for event in model_terminal.values():
        payload = event["payload"]
        usage = payload.get("usage")
        usage = usage if isinstance(usage, dict) else {}
        input_tokens = _number(usage.get("input_tokens")) or 0.0
        output_tokens = _number(usage.get("output_tokens")) or 0.0
        total_tokens = _number(usage.get("total_tokens"))
        if total_tokens is None:
            total_tokens = input_tokens + output_tokens
        cost = _first_number(
            usage,
            "reported_cost_usd",
            "estimated_cost_usd",
            "cost_usd",
        )
        duration = _number(payload.get("duration_ms")) or 0.0
        usage_totals["input_tokens"] += input_tokens
        usage_totals["output_tokens"] += output_tokens
        usage_totals["total_tokens"] += total_tokens
        usage_totals["duration_ms"] += duration
        if usage:
            usage_samples += 1
        if cost is not None:
            usage_totals["cost_usd"] += cost
            cost_samples += 1
        model_key = (payload.get("provider"), payload.get("model"))
        group = by_model.setdefault(
            model_key,
            {
                "provider": payload.get("provider"),
                "model": payload.get("model"),
                "invocations": 0,
                "total_tokens": 0.0,
                "cost_usd": 0.0,
            },
        )
        group["invocations"] += 1
        group["total_tokens"] += total_tokens
        if cost is not None:
            group["cost_usd"] += cost

    terminal_model_topics = Counter(
        event.get("topic") for event in model_terminal.values()
    )
    terminal_tool_topics = Counter(
        event.get("topic") for event in tool_terminal.values()
    )
    return {
        "model": {
            "started": len(model_started),
            "completed": terminal_model_topics["telemetry.model.completed"],
            "failed": terminal_model_topics["telemetry.model.failed"],
            "open": len(set(model_started) - set(model_terminal)),
        },
        "tool": {
            "started": len(tool_started),
            "completed": terminal_tool_topics["telemetry.tool.completed"],
            "failed": terminal_tool_topics["telemetry.tool.failed"],
            "open": len(set(tool_started) - set(tool_terminal)),
        },
        "usage": {
            key: _clean_number(value) for key, value in usage_totals.items()
        },
        "usage_samples": usage_samples,
        "cost_samples": cost_samples,
        "by_model": [
            {
                **group,
                "total_tokens": _clean_number(group["total_tokens"]),
                "cost_usd": _clean_number(group["cost_usd"]),
            }
            for _, group in sorted(
                by_model.items(), key=lambda item: (str(item[0][0]), str(item[0][1]))
            )
        ],
        "event_ids": sorted(
            _event_id(event)
            for event in [
                *model_started.values(),
                *model_terminal.values(),
                *tool_started.values(),
                *tool_terminal.values(),
            ]
            if _event_id(event) > 0
        ),
    }


def _get_task(state: CoordinationProjection, task_id: int) -> TaskRecord:
    task = state.tasks.get(task_id)
    if task is None:
        raise ProjectionLookupError(f"task {task_id} was not found")
    return task


def _workflow_policy_view(control) -> dict:
    if control is None or control.policy_event_id is None:
        return {"status": "unmaterialized"}
    return {
        "status": "active",
        "event_id": control.policy_event_id,
        "actor": control.policy_actor,
        "source": control.policy_source,
        "reason": control.policy_reason,
        "max_active_assignments": control.max_active_assignments,
        "max_total_tokens": control.max_total_tokens,
        "max_total_cost_usd": control.max_total_cost_usd,
        "max_attempts": control.max_attempts,
        "max_wall_clock_seconds": control.max_wall_clock_seconds,
        "reserve_tokens_per_assignment": control.reserve_tokens_per_assignment,
        "reserve_cost_usd_per_assignment": (
            control.reserve_cost_usd_per_assignment
        ),
    }


def _explanation(
    code: str,
    summary: str,
    event_ids: Iterable[Optional[int]],
    **details: object,
) -> dict:
    return {
        "code": code,
        "summary": summary,
        "event_ids": sorted({value for value in event_ids if isinstance(value, int)}),
        "details": details,
    }


def _duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    if seconds < 90:
        return f"{seconds} seconds"
    minutes = round(seconds / 60)
    if minutes < 90:
        return f"about {minutes} minutes"
    return f"about {round(seconds / 3600, 1):g} hours"


def _event_id(event: dict) -> int:
    value = event.get("id")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _number(value: object) -> Optional[float]:
    if (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    ):
        return float(value)
    return None


def _first_number(mapping: dict, *keys: str) -> Optional[float]:
    for key in keys:
        value = _number(mapping.get(key))
        if value is not None:
            return value
    return None


def _clean_number(value: float) -> int | float:
    return int(value) if value.is_integer() else round(value, 9)
