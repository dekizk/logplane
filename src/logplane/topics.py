"""Canonical topic groups shared by the bus and its consumers."""

COORDINATION_TOPICS = frozenset(
    {
        "agent.registered",
        "agent.heartbeat",
        "agent.stopping",
        "agent.stopped",
        "agent.policy_set",
        "task.created",
        "task.assigned",
        "task.started",
        "task.completed",
        "task.blocked",
        "task.attempt_failed",
        "task.assignment_expired",
        "task.failed",
        "task.dependency_failed",
        "task.retry_requested",
        "task.cancel_requested",
        "task.cancelled",
        "task.deadline_exceeded",
        "task.pause_requested",
        "task.paused",
        "task.resume_requested",
        "task.resumed",
        "task.superseded",
        "workflow.pause_requested",
        "workflow.paused",
        "workflow.policy_set",
        "workflow.usage_recorded",
        "workflow.resume_requested",
        "workflow.resumed",
        "decision.needed",
        "decision.made",
    }
)

INTEGRATION_TOPICS = frozenset(
    {
        "integration.task_observed",
    }
)

TELEMETRY_TOPICS = frozenset(
    {
        "telemetry.model.started",
        "telemetry.model.completed",
        "telemetry.model.failed",
        "telemetry.tool.started",
        "telemetry.tool.completed",
        "telemetry.tool.failed",
    }
)

# In-attempt approval of individual tool calls. Deliberately outside the
# coordination topics: the PM never replays these, an approval never changes
# task ownership, and the running attempt keeps its assignment while it waits.
APPROVAL_TOPICS = frozenset(
    {
        "approval.requested",
        "approval.resolved",
        "approval.rule_applied",
        "approval.rules_set",
    }
)

KNOWN_TOPICS = (
    COORDINATION_TOPICS | INTEGRATION_TOPICS | TELEMETRY_TOPICS | APPROVAL_TOPICS
)
