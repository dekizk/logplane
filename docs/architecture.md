# How logplane works

The model behind logplane: the event log, identities, lifecycle, components, invariants, crash recovery, and trust boundaries. For day-to-day commands see [operating logplane](operations.md); for connecting agents see [integrating agents](integration.md); for schemas and the HTTP API see [event contracts](events.md).

## The event log is the source of truth

The bus does not store a mutable kanban card as truth. Every action is an
immutable SQLite event. The project manager (PM) derives current state by
replaying those events, then reconciles any effects that are missing. Processes
can therefore restart without relying on hidden in-memory ownership state.

Existing agents remain bounded executors while logplane retains ownership, recovery, and immutable history.

## Identity model

Four identities keep workflows, retries, and replays unambiguous:

- `correlation_id` identifies one wider workflow or user goal across tasks.
- `task_id` identifies the logical outcome requested by a human or agent.
- `assignment_id` identifies one execution attempt for that task.
- `instance_id` identifies one running worker process.

Executors also receive a derived `effect_scope`, which remains stable across
attempts for the same logical task. Python adapters can call
`assignment.effect_id("operation-name")` to obtain a deterministic key for one
irreversible external operation, which the system performing that operation
must enforce as an idempotency key. This is deliberately distinct from
`assignment_id`: the latter identifies a delivery attempt and changes on retry.

A correlation may contain several tasks, and each task may have several
attempts. `caused_by` remains the direct parent event; it is not overloaded as
the workflow identifier.

A task may have several attempts but only its current attempt may change task
state. The worker runtime consumes each addressed delivery once, rejects an
already-expired assignment before execution, and suppresses results after known
ownership loss. A lifecycle claim can still race with an expiry while an HTTP
publication is already in flight; if it reaches the immutable log, the PM
projection rejects that claim as stale.

Lifecycle events are claims. Only the replay projection determines whether a
claim was accepted for the active assignment; rejected claims remain visible
for audit rather than being deleted from history.

The normal lifecycle is:

```text
task.created
    -> task.assigned (attempt 1)
    -> task.started
    -> task.completed
```

Tasks may also form a directed acyclic graph (DAG). A dependent remains
unassigned until every declared upstream task has completed:

```text
task.created (A) -> task.completed (A)
                                  -> task.assigned (B, depends_on A)
task.failed (A)                   -> task.dependency_failed (B)
```

Recovery and human-input paths are explicit events:

```text
task.assigned -> task.assignment_expired -> task.assigned (attempt 2)
task.started  -> task.attempt_failed -> task.assigned (next attempt)
              -> task.failed -> task.retry_requested
              -> task.assigned (next monotonic attempt)
task.started  -> task.blocked -> decision.needed -> decision.made
              -> task.assigned (next attempt)
task.created/assigned/started/blocked -> task.cancel_requested -> task.cancelled
task.created/assigned/started/blocked -> task.deadline_exceeded
task.open/assigned/started/blocked -> task.pause_requested -> task.paused
task.paused -> task.resume_requested -> task.resumed -> task.assigned
task.created (replacement, supersedes old) -> task.superseded (old)
workflow.pause_requested -> workflow.paused -> workflow.resume_requested
                         -> workflow.resumed
```

## Components

Source files live in the `logplane` package under `src/logplane/`; the last
two rows are runtime data in the directory where you run logplane.

| File | Role |
|---|---|
| `bus.py` | FastAPI, SQLite event storage, transactional reference checks, queries, and SSE streaming |
| `event_validation.py` | Pure event-envelope and payload validation; no database or HTTP dependencies |
| `client.py` | Publish, bounded history reads, reconnecting subscriptions, durable offsets |
| `projection.py` | Pure coordination reducer shared by the PM and operator tools |
| `pm_agent.py` | Deterministic post-replay reconciliation and single-PM runtime |
| `operations.py` | Task explanations, worker health, DAG views, and telemetry totals |
| `observer.py` | GET-only history and SSE client with no offsets or local state |
| `scheduling.py` | Shared priority classes and deterministic immutable ordering key |
| `cli.py` | Human-friendly `logplane` operations command with JSON output |
| `executors.py` | Immutable assignment/outcome contract and Python/subprocess adapters |
| `__init__.py`, `adapters.py`, `protocol.py` | Stable public integration imports for application and adapter authors |
| `executor_protocol.py` | Strict, versioned CLI and HTTP assignment/outcome envelopes |
| `integration.py` | Python, configured CLI, and guarded HTTP adapter wrappers |
| `conformance.py` | Synchronous library adapter contract probe |
| `adapter_check.py` | Internal timed process boundary for CLI adapter probes |
| `local_config.py` | Validated loopback-only local onboarding configuration |
| `runtime.py` | Leased, concurrent worker runtime with ownership-loss cancellation |
| `adoption.py` | Controlled, shadow, and deterministic canary integration helpers |
| `telemetry.py` | Optional model/tool telemetry sink, lifecycle helpers, and producer identity |
| `artifacts.py` | Atomic, content-addressed local storage for opt-in captured content |
| `topics.py` | Canonical coordination, integration, and telemetry topic groups |
| `worker.py` | Demo executor wired through the reusable runtime |
| `demo.py` | Scripted `logplane demo` scenarios with real processes in a throwaway directory |
| `.logplane/events.db` | Append-only event log and task-id counter (path set in `logplane.local.json`) |
| `.offsets/` | Optional durable resume points for stable consumer identities |

`bus.py` delegates envelope and payload checks to `event_validation.py` and
retains checks that need stored history, such as referenced events and
idempotency claims. HTTP error translation also stays in the bus. Existing
`bus.validate_event` and `bus.EventValidationError` imports remain available.

## Architecture and invariants

The bus, PM, workers, and operator views have deliberately separate roles:

1. publishers append intent and outcomes to the event log;
2. `projection.py` derives current coordination state through a pure replay;
3. the PM reconciles any missing assignment, recovery, decision, or terminal
   event using stable idempotency keys;
4. workers execute only assignments addressed to their current process
   instance; and
5. read-only tools rebuild views from HTTP history without becoming another
   scheduler or source of truth.

The design depends on these invariants:

- **Immutable history.** Accepted events are appended, never edited. A
  correction or new request is another event.
- **Replayable effects.** PM and worker lifecycle publications use logical
  idempotency keys so replay after a crash does not duplicate an effect.
- **Current ownership only.** A task may have several attempts, but only its
  current assignment may advance it. Late output is suppressed or ignored.
- **Immutable DAG intent.** Dependency edges exist only on `task.created`.
  New intent means new tasks, not edited edges or mutable board state.
- **Bounded coordination data.** Context, results, decisions, and resolved
  dependencies remain compact. Large content belongs in external artifacts.
- **Raw telemetry is separate from coordination.** The PM does not replay model
  or tool topics. Compact `workflow.usage_recorded` coordination events derived
  from model usage do affect budget admission; telemetry cannot directly grant
  ownership or complete a task.
- **Local trust boundary.** The current bus assumes one trusted PM on one host;
  actor strings are self-reported even when perimeter authentication is on.
- **External effects remain the adapter's responsibility.** See the
  [adapter safety contract](integration.md#adapter-safety-contract).

## Crash recovery guarantees

This section explains how processes recover by replay. For operator
procedures (artifact audits, verified backup and restore, rebuilding the task
index, and PM snapshots), see [local recovery operations](../RECOVERY.md).
Retention auditing is read-only; restore always uses a fresh directory.

### Lifecycle recovery behavior

Standard worker admission reads coordination history lazily in pages of at most
1,000 events, stopping at the delivered assignment's exact event ID. It validates
ordering, event shape, filtering, and the persisted assignment before executing.
Transport errors or an unavailable target stop the worker so lease recovery can
proceed. Filtered ID gaps are normal and are not treated as missing events.
This bounds temporary history memory, not retained task/accounting state.
Embedded clients exposing only `query_all` remain supported but retain their
previous full-list memory use; implement the `BusClient.iter_events` contract
to receive bounded paging. No snapshot trust or orchestration rules change.

### PM

At startup the PM:

1. reads coordination topics in bounded, server-filtered pages;
2. rebuilds workers, tasks, dependency edges, attempts, and decisions through a total reducer;
3. reconciles missing effects before waiting for another event;
4. publishes effects with stable logical idempotency keys.

This closes the crash window where the prototype could record `task.created`,
an upstream completion, or `task.blocked` but fail before publishing the
corresponding ready assignment, dependency failure, or decision request.

### Workers

Workers publish lifecycle events using keys derived from `assignment_id`:

```text
started:{assignment_id}
blocked:{assignment_id}
completed:{assignment_id}
attempt-failed:{assignment_id}
```

A new worker instance subscribes from its own registration event — assignments
addressed to it cannot exist earlier — so startup cost does not grow with log
history. `BusClient` retains the latest event id in memory across SSE
reconnections. A malformed SSE frame raises the stable `BusProtocolError` and
terminates that subscription at the last confirmed event instead of silently
skipping data or advancing its resume position. A replacement process
registers a new `instance_id`, so demo
workers neither resume the previous process's assignments nor create durable
offset files. Stable consumers that do need cross-process resume can use
`BusClient.load_offset()` and `save_offset()`; those files are replaced
atomically and only move forward. After all pre-v0.2.1 workers have stopped,
their old per-instance files may be removed once; new demo workers do not
replenish them.

The reusable runtime also follows cancellation requests, cancellation/deadline
terminal events, assignment expiry, terminal failure, and replacement
registration. It rejects already-expired and already-consumed assignment
deliveries, cancels adapters only during their execution phase, serializes
adapter cancellation with cleanup, and suppresses lifecycle output once that
instance no longer owns an attempt.

External tool calls, payments, deployments, and file mutations need their own
retry-stable effect key; see the
[adapter safety contract](integration.md#adapter-safety-contract).

### Worker leases

Each worker start gets a new random `instance_id` and publishes heartbeats. The
PM expires an active attempt when the registered process is replaced or its
lease becomes stale, then creates the next attempt when a capable worker is
available.

The PM also reconciles on SSE keepalives, so lease expiry does not depend on a
new task or heartbeat arriving after a worker dies.

Defaults can be changed with:

```sh
LOGPLANE_HEARTBEAT_SECONDS=5
LOGPLANE_WORKER_LEASE_SECONDS=20
LOGPLANE_MAX_RETRIES=2
LOGPLANE_URL=http://127.0.0.1:8765
```

Lease decisions are emitted into the log as `task.assignment_expired`, so task
history remains replayable and auditable. Assignment expiry and explicit
retryable worker failure share the same bounded recovery policy.

## Trust model

The bus is designed for local, single-user orchestration. Two boundaries to
understand before exposing it any wider:

- **Perimeter auth is opt-in.** Set `LOGPLANE_TOKEN` in the bus's environment
  and every data route requires an `Authorization: Bearer <token>` header;
  `BusClient` picks the same variable up automatically (or accepts `token=`).
  `/health` stays open. Without the variable, anyone who can reach the port can
  publish.
- **Actors are self-reported.** The token authenticates *clients*, not
  identities: any authorized client may publish under any actor string,
  including `pm`. Per-actor authentication would require signed events or
  per-actor credentials, which is out of scope for a local bus.

The PM's single-instance lock is per user, per machine, per bus URL (a lock
file in the user's runtime directory keyed by a hash of `LOGPLANE_URL`), so
two checkouts on one machine exclude each other. The lock file is opened
without following symlinks, verified as owned by the current user, and made
user-only. It is not distributed leadership: PMs on different machines or
under different OS users are not excluded and must not coordinate the same bus.

## Scope and next boundaries

logplane is a trusted, single-process, single-host runtime. Actor names are
asserted by clients, not authenticated identities. The PM lock and SSE wake-up
condition are process-local mechanisms; do not run the FastAPI app with
multiple uvicorn workers. Before distributing the bus, add authenticated actor
identity and replace local exclusivity/notification with shared infrastructure.

Future product sequencing, adoption goals, and release boundaries live only in
[ROADMAP.md](../ROADMAP.md). New operational views and controls must remain
projections and commands over the event log—not a return to a mutable board as
the source of truth.
