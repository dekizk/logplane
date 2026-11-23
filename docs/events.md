# Event contracts and the HTTP API

The event envelope, built-in topics, validation, size limits, and how to read, follow, and publish events over HTTP. `topics.py` and `validate_event()` in `bus.py` remain the authoritative sources.

## Versioned event contracts

Every stored event uses the same envelope:

| Field | Contract |
|---|---|
| `id` | Positive, server-assigned SQLite id used for audit references and resume cursors |
| `ts` | Server-assigned Unix timestamp |
| `topic` | Non-empty routing name; built-in names are grouped in `topics.py` |
| `actor` | Non-empty, self-reported publisher name |
| `schema_version` | Positive contract version; new built-in events require v2 |
| `idempotency_key` | Optional logical publication identity, unique per `(actor, key)` |
| `caused_by` | Optional existing parent event id for direct causality and correlation inheritance |
| `correlation_id` | Optional workflow identity; generated for a root task and shared across its DAG |
| `producer` | Required on built-in telemetry; identifies implementation, process, and optional version |
| `payload` | Topic-specific JSON object |

New built-in events use top-level `schema_version: 2` by default. Unknown
topics remain permitted so applications can extend the bus without changing
its core, but they are not automatically coordination topics and receive no
built-in payload validation.

Validation and recovery deliberately operate at three different layers:

1. `bus.py` structurally validates known v2 envelopes and payloads before
   append and rejects contract violations with HTTP 422.
2. `projection.py` remains total during replay: irrelevant, duplicate, stale,
   malformed, or invalid-transition rows are ignored instead of preventing
   later valid history from being derived.
3. `runtime.py` resolves referenced dependency completions and constructs an
   executable `AssignmentContext`. If an assignment cannot be interpreted
   safely, the worker stops so lease expiry can recover it rather than
   continuing to heartbeat around work it skipped.

Append-time structural validation is therefore not a proof that arbitrary
events published by a client claiming to be `pm` are executable. For example,
the runtime still requires a usable assignment goal or title. Correct local
operation assumes the real single PM is the publisher of PM lifecycle events.

Existing databases are migrated automatically. Historical rows are marked as
schema v1 and remain replayable with legacy assignment identities derived from
their event ids. Existing events retain `NULL` correlation IDs; the migration
does not invent workflow relationships that were never recorded. v0.6 also
adds a nullable top-level `producer` column; historical events retain `NULL`.

### Additive fields and historical rows

Fields added after schema v2 are optional on old rows, so history written by
earlier releases still replays:

- New `task.created` events must contain a non-empty `payload.title` (since
  v0.4). Historical titleless events remain replayable and receive a
  deterministic fallback title in the PM projection.
- `payload.depends_on` on `task.created` and `payload.dependency_refs` on PM
  assignments (since v0.5). Their absence keeps single-task behavior.
- `payload.deadline_at` on task creation and assignments, plus the
  cancellation and deadline topics (since v0.7). Tasks without a deadline keep
  their previous behavior.
- `payload.priority` and `payload.not_before` on task creation (since v0.10).
  The server materializes omitted priority as `normal`, and historical rows
  receive the same replay default. These fields are PM scheduling policy and
  are not delegated to executors.
- Task and workflow pause request/acknowledgement pairs and immutable task
  supersession (since v0.10). Workflow control events require a top-level
  `correlation_id`; task controls inherit it from their target.
- `workflow.policy_set` and the optional `workflow_policy_event_id` on new
  assignments (since v0.10). Historical assignments without it remain
  replayable when no workflow policy preceded them.
- The optional `fairness` evidence object on new assignments, identifying the
  `workflow_round_robin_v1` policy and the preceding accepted assignment event
  (since v0.10). Historical assignments without it remain replayable.
- `payload.lease_policy` on `task.created` (after v0.14.1), an object with a
  non-negative integer `max_expiries`, materialized by the server when
  omitted. Tasks without it keep the earlier rule that a lease expiry consumes
  a retry.
- The optional `payload.retry_after_seconds` on a retryable
  `task.attempt_failed` (after v0.14.1): a positive number of at most 604,800
  that delays the next assignment until that long after the event. Failures
  without it retry immediately, as before.
- `agent.policy_set`, compact `workflow.usage_recorded` accounting, and
  optional agent-policy and budget-reservation evidence on new assignments
  (since v0.10). Workflow policy patches may add token, cost, attempt, and
  wall-clock limits. Historical assignments without these fields remain
  replayable and uncharged unless their history supplies accounting evidence.

Core v2 topics:

| Topic | Emitted by | Purpose |
|---|---|---|
| `agent.registered` | worker | Announces a process instance, capabilities, and capacity |
| `agent.heartbeat` | worker | Renews that process instance's lease |
| `task.created` | human/agent | Requests a logical outcome with immutable dependency and scheduling policy |
| `task.assigned` | PM | Creates a numbered execution attempt with decisions and completion references |
| `task.started` | worker | Confirms the active attempt began |
| `task.completed` | worker | Completes the active attempt and logical task |
| `task.blocked` | worker | Pauses the attempt for human input |
| `task.attempt_failed` | worker | Records a retryable or permanent execution failure |
| `task.assignment_expired` | PM | Records loss or replacement of the assigned worker |
| `task.failed` | PM | Terminates a task after policy exhaustion or permanent failure |
| `task.dependency_failed` | PM | Terminates downstream work whose prerequisite failed |
| `task.retry_requested` | human/agent | Extends policy and reopens the latest failed task when its intent has not been superseded |
| `task.cancel_requested` | human/agent | Requests terminal cancellation of an existing task |
| `task.cancelled` | PM | Records crash-safe terminal cancellation and its last attempt |
| `task.deadline_exceeded` | PM | Terminates a task after its persisted absolute deadline |
| `task.pause_requested` | human/agent | Requests a non-terminal pause of one task |
| `task.paused` | PM | Acknowledges a crash-safe task pause and fences its last attempt |
| `task.resume_requested` | human/agent | Requests that one paused task become eligible again |
| `task.resumed` | PM | Restores the task's pre-pause open or blocked state |
| `task.superseded` | PM | Terminates old intent after a replacement task is created |
| `workflow.pause_requested` | human/agent | Requests a correlation-scoped orchestration pause |
| `workflow.paused` | PM | Acknowledges the request-time assignment snapshot; replay interrupts only assignments that still match |
| `workflow.policy_set` | PM/operator | Materializes or changes immutable workflow concurrency and budget policy |
| `agent.policy_set` | PM/operator | Materializes or changes immutable concurrency policy for one logical agent |
| `workflow.usage_recorded` | telemetry sink | Carries compact validated model usage into workflow accounting |
| `workflow.resume_requested` | human/agent | Requests that a paused workflow continue |
| `workflow.resumed` | PM | Re-enables ordinary reconciliation for the workflow |
| `decision.needed` | PM | Requests one human decision for a blocked attempt |
| `decision.made` | human | Records a response that is carried into the next attempt |

Integration topics are validated separately and deliberately excluded from PM
replay:

| Topic | Emitted by | Purpose |
|---|---|---|
| `integration.task_observed` | bridge | Records externally owned shadow or unselected canary work |

Approval topics record [tool-call approvals](operations.md#tool-call-approvals).
They are validated and deliberately excluded from PM replay: an approval never
changes task state or ownership.

| Topic | Emitted by | Purpose |
|---|---|---|
| `approval.requested` | worker | Asks a human before one tool call of the worker's current attempt |
| `approval.resolved` | human | Allows or denies one request; never the requesting worker, at most once |
| `approval.rule_applied` | worker | Records that a standing rule settled a call, and which rule |
| `approval.rules_set` | human | Replaces one workflow's standing allow and deny rules |

Telemetry topics are also validated and deliberately excluded from PM replay:

| Topic | Purpose |
|---|---|
| `telemetry.model.started` | Records the start of one stable model invocation |
| `telemetry.model.completed` | Records duration, bounded usage metadata, and optional artifact refs |
| `telemetry.model.failed` | Records a typed retryable/permanent invocation failure |
| `telemetry.tool.started` | Records one tool-call start and optional parent invocation |
| `telemetry.tool.completed` | Records tool duration and optional artifact refs |
| `telemetry.tool.failed` | Records a typed retryable/permanent tool failure |

`topics.py` is the canonical source for all four groups. Tests assert that the
PM uses exactly the coordination group and never consumes integration,
approval or telemetry topics. `validate_event()` in `bus.py` is the canonical built-in
payload validator; `tests/test_bus.py` records its compatibility and rejection
behavior. Keep those sources authoritative instead of treating this compact
README table as a separately maintained exhaustive schema.

### Shared contract limits

| Data | Limit |
|---|---|
| Task context | 16 KiB encoded JSON |
| Final structured result | 16 KiB encoded JSON |
| Resolved dependency input | 32 KiB aggregate encoded JSON |
| Direct task dependencies or assignment references | 128 |
| Telemetry `attributes` or model `usage` object | 8 KiB encoded JSON each |
| Artifact references on one telemetry event | 16 |
| One content-addressed artifact | 16 MiB |
| Approval summary shown to a human | 500 characters (the whole input is digested) |
| Standing approval rules per workflow | 100, each pattern at most 200 characters |
| Correlation, producer, telemetry identity, and external-origin fields | 128 characters where applicable |

These are coordination safety boundaries, not a transport for large model
content. Artifact references in telemetry do not yet make oversized DAG
results transparently resolvable by downstream executors.

## Reading and following the log

History queries are bounded; use `after_id` to paginate:

```sh
curl 'http://127.0.0.1:8765/events?after_id=0&limit=1000'
curl 'http://127.0.0.1:8765/events?topics=task.assigned,task.completed'
curl 'http://127.0.0.1:8765/events?correlation_id=release-2026-07'
curl 'http://127.0.0.1:8765/events?topics=telemetry.model.completed&correlation_id=release-2026-07'
curl 'http://127.0.0.1:8765/events/42'
```

A response with header `X-Page-Full: 1` filled its page, so more events may
exist — request the next page with `after_id` set to the last event id
received (`BusClient.query_all` does this automatically).

Follow history and live events over SSE:

```sh
curl -N 'http://127.0.0.1:8765/events/stream?from_id=0'
curl -N 'http://127.0.0.1:8765/events/stream?correlation_id=release-2026-07'
```

SSE history applies topic and correlation filters in SQLite before payloads
are decoded. High-volume extension topics therefore do not inflate PM replay
or force filtered consumers to deserialize unrelated events.
`BusClient.query()`, `query_all()`, and `subscribe()` expose the same filters.

## Publishing raw events

The CLI publishes these events for you (see [operating logplane](operations.md)).
Use them directly when building your own publisher or integration. Python
publishers can call `BusClient` helpers such as `cancel_task(task_id, reason=...)`.

A task with required capabilities and inline context:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Run the Python test suite",
    "context": {"repository": "logplane", "suite": "integration"},
    "required_capabilities": ["python", "testing"]
  }
}
```

A task with an explicit retry policy:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Run integration tests",
    "retry_policy": {"max_retries": 2}
  }
}
```

Cancel a task:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.cancel_requested",
    "actor":"human",
    "idempotency_key":"cancel:task:1",
    "payload":{"task_id":1,"reason":"superseded by a new request"}
  }'
```

Answer a pending decision (find the `decision.needed` event first):

```sh
curl 'http://127.0.0.1:8765/events?topics=decision.needed'
```

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"decision.made",
    "actor":"human",
    "caused_by":14,
    "idempotency_key":"decision:14",
    "payload":{
      "task_id":2,
      "assignment_id":"task:2:attempt:1",
      "decision_id":"decision:task:2:attempt:1",
      "decision":"SQLite"
    }
  }'
```

Retry a failed task, as a child of its latest `task.failed` event:

```sh
curl -X POST http://127.0.0.1:8765/events \
  -H 'content-type: application/json' \
  -d '{
    "topic":"task.retry_requested",
    "actor":"human",
    "caused_by":21,
    "idempotency_key":"retry:task:2:failed:21",
    "payload":{
      "task_id":2,
      "additional_retries":1,
      "reason":"The dependency is healthy again"
    }
  }'
```
