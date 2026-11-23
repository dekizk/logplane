# Integrating agents

Connecting existing agents as workers, the adapter safety contract, telemetry, and adopting logplane alongside an existing system. Copyable examples live in [`examples/`](../examples/README.md); they come from the source checkout (or the source distribution), not from installing the wheel alone.

## Integrating an existing agent

Application code should import the supported integration surface from
`logplane`. Flat modules remain available for compatibility, but are internal
building blocks rather than the preferred long-term import path. The public
package includes assignment/outcome types, `WorkerRuntime`, Python/CLI/HTTP
adapters, adoption helpers, telemetry hooks, and artifact-reference validation.

Executors receive one immutable `AssignmentContext` and return exactly one of
`Completed`, `Blocked`, `RetryableFailure`, or `PermanentFailure`. The runtime
owns registration, heartbeats, subscriptions, bounded concurrency, lifecycle
idempotency, and stale-result suppression.

`AssignmentContext.decisions` is a chronological, immutable tuple of accepted
human-decision records. Each record identifies the `decision.made` event, its
actor, the blocked assignment and decision IDs, and the JSON decision value.
Executors must consult these records before returning `Blocked`: later accepted
decisions supersede conflicting earlier decisions or original context, and a
clear decision satisfies a corresponding null or missing context value. The
bus preserves this precedence rule without mutating the original task event.
Historical assignments created before v0.4.1 parse this field as an empty
tuple.

`AssignmentContext.dependencies` is an immutable tuple of resolved upstream
completions in the task's declared `depends_on` order. Each entry contains the
upstream `task_id`, immutable `completion_event_id`, summary, and structured
result. Executors consume this field directly; publishers do not copy upstream
results into downstream context.

An existing Python agent with a `run()` method can be wrapped without teaching
it about the bus:

```python
from logplane import BusClient, Completed, PythonAgentAdapter, WorkerRuntime

class ExistingAgent:
    def run(self, assignment):
        result = do_existing_work(
            goal=assignment.goal,
            context=assignment.context,
            idempotency_key=assignment.effect_id("do-existing-work"),
        )
        return Completed("agent finished", {"result_ref": result.ref})

bus = BusClient("http://127.0.0.1:8765", actor="existing-agent")
WorkerRuntime(
    bus,
    name="existing-agent",
    executor=PythonAgentAdapter(ExistingAgent()),
    capacity=2,
    capabilities=["python"],
).run()
```

Before connecting it to a live bus, run the contract probe:

```sh
logplane adapter check --python-target my_package.agent:ExistingAgent
# For a deliberately slower trusted probe:
logplane adapter check --python-target my_package.agent:ExistingAgent --timeout 60
```

The target may be a zero-argument class, an object with `run(assignment)`, or a
callable in an installed package or a module beneath the command's current
working directory. It must recognize
`assignment.context["agent_bus_conformance_probe"]`
and return a typed outcome without performing external side effects. The check
does not require a running bus or PM. A complete copyable implementation lives
in `examples/python_agent/`.

Each check runs in a fresh process with a 30-second total timeout (`--timeout` to change it). **This is not a sandbox or a side-effect guarantee:** use trusted targets, and an OS sandbox when stronger containment is needed.

<details>
<summary>Timeout, exit status, and cleanup details</summary>

The CLI runs each check in a fresh process with a **30-second total timeout**
by default. `--timeout` must be positive and finite and covers target import,
construction, execution, cleanup, and interpreter exit. A timeout produces a
failed probe (exit 4); invalid configuration or an unimportable target remains
exit 2. Ctrl-C or SIGTERM stops the check group and returns 130. Group termination
may add up to roughly 1.25 seconds of cleanup after the deadline under normal OS
conditions, plus at most a one-second process-state check on a macOS permission
race. That check tolerates the error only after the leader has exited and no
non-zombie group members remain. Live members or unavailable/invalid process
inspection still produce a cleanup error, not a passing probe. Normal results
retain the existing human/JSON format; adapter
stdout/stderr is discarded, and the result channel is limited to 64 KiB so
chatter cannot corrupt `--json` or fill a capture buffer.

On success, failure, or handled interruption, the CLI stops remaining processes
in its check's process group. **This is not a sandbox or a side-effect guarantee.**
The adapter retains the user's file/network permissions. Detached processes,
remote requests, and effects already performed may survive. Killing the CLI
with SIGKILL or losing the host bypasses its cleanup. Use trusted targets that
recognize the probe flag; use an OS sandbox when stronger containment is needed.
No cancellation protocol is sent to remote agents by this timeout.

</details>

The public library helper `logplane.check_executor(executor)` remains
synchronous and untimed for already-created objects. Library callers must own
execution/cleanup bounds themselves; arbitrary live objects are not transferred
to the CLI subprocess.

`capacity` is enforced by a bounded execution pool, so the worker never runs
more assignments concurrently than it advertises. Unexpected Python
exceptions default to permanent failure; applications may explicitly configure
them as retryable.

When a failure has a known recovery time, say so, and Logplane holds the retry
until then instead of retrying at once:

```python
return RetryableFailure("usage_limit", "limit resets at 14:50",
                        retry_after_seconds=2400)
```

CLI and HTTP agents put the same optional `retry_after_seconds` in a
`retryable_failure` outcome.

### Subprocess agents

The configured CLI adapter uses protocol v1. It sends one strict envelope on
stdin and expects one envelope on stdout:

```json
{"protocol":{"name":"agent-bus.executor","version":1,"supported_versions":[1]},"assignment":{}}
{"protocol":{"name":"agent-bus.executor","version":1},"outcome":{"status":"completed","summary":"done","result":{}}}
```

`version` is the selected wire format. `supported_versions` advertises the
sender's capabilities and must include that selected version; receivers ignore
additional future version numbers they do not yet implement rather than
rejecting an otherwise valid v1 message. Automatic version selection or
downgrade is not supported.

Use a checked-in, credential-free configuration to describe the process:

```json
{
  "schema_version": 1,
  "worker": {"name": "my-cli", "capabilities": ["review"], "capacity": 1},
  "adapter": {
    "type": "cli",
    "command": ["my-agent", "--json"],
    "protocol_version": 1,
    "timeout_seconds": 300
  }
}
```

```sh
logplane adapter check --config adapter.json
logplane adapter run adapter.json
```

Exit code `75` maps to a retryable process failure; other non-zero exits map to
permanent failure. Input, stdout, and stderr are size-limited. Timeouts are
retryable. When assignment ownership is lost or the runtime stops, a running
subprocess is terminated and its result is suppressed. Shell execution is not
used.

In-process Python threads cannot be force-killed safely. Such agents should
pass `assignment.effect_id(...)` as an idempotency key that the receiving system
enforces. If
the wrapped object exposes `cancel(assignment_id)` or `close()`,
`PythonAgentAdapter` forwards those lifecycle calls.

The legacy unversioned `SubprocessExecutor` input remains readable for existing
v0.4 integrations. New configuration files accept only protocol v1, so later
wire changes can be negotiated rather than silently misparsed. See
`examples/cli_agent/` for the smallest working implementation and
`examples/hermes/` for a real agent integration.

### HTTP agents

Use an HTTP adapter when the agent cannot run in the same process or as a child
process. It uses the same protocol-v1 envelope and sends `assignment_id` as the
attempt-scoped `Idempotency-Key` and `X-Agent-Bus-Assignment-Id` headers. It
also sends the retry-stable `X-Agent-Bus-Effect-Scope` header; the HTTP agent
must combine that scope with a stable operation name for irreversible
downstream effects. Loopback HTTP works by default. A remote endpoint must
explicitly set `allow_remote: true`, use HTTPS, and name a populated secret
environment variable with `token_env`; a credential is never stored in the
config file.

```json
{
  "schema_version": 1,
  "worker": {"name": "local-http-agent", "capabilities": ["http"]},
  "adapter": {
    "type": "http",
    "endpoint": "http://127.0.0.1:9000/execute",
    "cancellation_endpoint": "http://127.0.0.1:9000/cancel",
    "protocol_version": 1,
    "timeout_seconds": 300
  }
}
```

HTTP `408`, `425`, `429`, transport errors, and `5xx` responses are retryable;
other `4xx` responses and malformed outcomes are permanent attempt failures.
A `Retry-After` header given in seconds on a retryable status delays the next
attempt by that long; the date form is ignored.
Cancellation delivery is best-effort. The runtime's ownership fence remains
authoritative and suppresses late output even if a remote agent cannot stop.
The copyable loopback implementation in `examples/http_agent/` includes a
credential-free adapter configuration, standalone conformance check, visible
retry/effect observations, cooperative cancellation, automated regression
coverage, and its recorded live trial evidence.

### Adapter safety contract

- Treat `assignment_id` as one delivery-attempt identity. Use
  `assignment.effect_id("stable-operation-name")` in Python, or combine the
  protocol's `effect_scope` with a stable operation name, for each file write,
  API call, deployment, payment, or other irreversible logical effect that
  must not repeat when the task is retried. logplane supplies the stable key;
  the system that performs the effect must enforce it atomically, for example
  an API's idempotency key or a unique database constraint. A separate "check,
  then write" in the agent is not enough, because a lease can expire while the
  old attempt is still running, so two attempts of one task can overlap.
- Check `deadline_at` before starting and cooperate with
  `cancel(assignment_id)` where possible. Never publish a late result yourself.
- Return only `Completed`, `Blocked`, `RetryableFailure`, or
  `PermanentFailure`; do not invent mutable status outside the event log.
- Keep inline result data small and return immutable artifact references for
  larger content. Prompts and tool output stay out of SQLite by default.
- Do not schedule follow-up tasks inside the adapter. DAG creation and task
  ownership belong to the publisher and PM.

The bus can make orchestration effects idempotent; it cannot make an arbitrary
external side effect atomic. `logplane demo approval` and
`logplane demo audit-triage` show effect keys preventing repeated refunds and
submissions across retries.

### Asking before a tool call

An executor can put individual tool calls behind a
[tool-call approval](operations.md#tool-call-approvals) while its attempt keeps
running:

```python
from logplane.approvals import ApprovalGate

class ExistingAgent:
    def __init__(self, bus):
        self.bus = bus  # the worker's own BusClient

    def run(self, assignment):
        gate = ApprovalGate.for_assignment(self.bus, assignment, wait_seconds=1800)
        answer = gate.check("Bash", {"command": "git push origin main"})
        if not answer.allowed:
            return Blocked(f"push was not approved: {answer.reason}")
        ...
```

`check` blocks until a standing rule or a human settles the call, and returns a
denial instead of raising when the bus is unreachable or nobody answers in
time. Pass the agent's own tool-call id as `call_id` when it has one, so a
repeated check of the same call is idempotent.

Agents that run as a separate program use a hook instead.
`logplane gate claude-hook` is a Claude Code `PreToolUse` command hook: the
worker passes the attempt in `LOGPLANE_GATE_*` variables
(`logplane.gate.gate_environment`), and the hook prints the permission decision.
The [Claude Code example](../examples/claude_code/README.md) wires it up behind
`--gated-tools`. `ApprovalGate` and the hook are new in this release and are
not yet part of the stable `logplane` import surface.

## Telemetry and artifacts

Raw telemetry is observational and is never consumed by the PM. Model and tool
lifecycles use their own canonical topic group, so coordination replay remains
small even when an executor emits many telemetry events. For budget accounting,
a model terminal event also produces one compact, validated
`workflow.usage_recorded` coordination event containing only normalized token
and cost totals plus identity references; prompts, outputs, and tool details do
not enter PM replay. Each raw telemetry event
retains `correlation_id`, `task_id`, `assignment_id`, worker process identity,
and a stable invocation or tool-call id. The top-level `producer` envelope
identifies the implementation and process that emitted it; provider and model
identity remain invocation fields.

Executors can use the framework-neutral sink without depending on PM internals:

```python
from telemetry import BusTelemetrySink, ProducerIdentity

telemetry = BusTelemetrySink(
    bus,
    producer=ProducerIdentity(
        implementation="my-agent-adapter",
        instance_id=worker_instance_id,
        version="1.0.0",
    ),
)
```

The sink supports `model_started/completed/failed` and
`tool_started/completed/failed`. Started and terminal events use deterministic
idempotency keys. A terminal event is caused by its start event when available,
and all events retain the assignment workflow correlation. Telemetry publishing
must remain best-effort from an executor: an observability outage must not alter
task ownership or outcome.

Prompts, outputs, tool arguments, credentials, and transcripts are never added
to telemetry metadata automatically. Content capture is opt-in and requires an
`ArtifactStore`; events then contain only `{sha256, size_bytes, media_type,
kind}` references. Files are written atomically, deduplicated by SHA-256, made
user-only, and verified on reads. One artifact is limited to 16 MiB.

The store performs no automatic deletion. A digest can be shared by several
immutable events, so retention may delete only blobs proven unreferenced by the
event log. Treat the artifact directory as sensitive local data: use a private
directory, redact before capture, never capture credentials, and apply your own
backup and retention policy. `artifacts/` is ignored by Git.

## Safe adoption modes

`AdoptionBridge` records a stable decision for work originating in another
system:

```python
from logplane import AdoptionBridge, AdoptionMode, CanarySelector, ExternalOrigin

event = AdoptionBridge(bus).adopt(
    origin=ExternalOrigin("legacy-system", "ticket-42"),
    title="Handle ticket 42",
    mode=AdoptionMode.CANARY,
    selector=CanarySelector(10, namespace="first-rollout"),
    context={"priority": "high"},
    deadline_at=2000000000.0,
)
```

- **Controlled:** emits `task.created`; logplane owns execution.
- **Shadow:** emits `integration.task_observed`; the external system remains
  owner and logplane does not create an executable task.
- **Canary:** a stable hash selects a configured percentage. Selected work is
  bus-owned; unselected work is recorded as externally owned.

The bridge uses the same idempotency identity for either ownership result, and
the bus transactionally claims each `(external system, task reference)` across
all bridge actor names. A later attempt to reinterpret the origin—or a second
misconfigured bridge actor trying to claim it—is rejected with a dual-ownership
conflict instead of producing two tasks. The claim table is a rebuildable
uniqueness projection; the immutable adoption event remains truth.

The external system must honour the recorded decision: bus-owned work must be
removed from its own execution queue before an agent may execute it. Shadow
mode must never invoke a second side-effecting agent. Canary percentages and
`include_refs` affect only external origins that have not yet been adopted;
there is deliberately no ownership-transfer operation for an existing origin.

Controlled and canary tasks may include `deadline_at`; adapters receive the
persisted value through `AssignmentContext` and should treat the PM's terminal
deadline event as the ownership boundary.

### Safe rollout recipe

1. **Shadow:** publish observations from the external source and compare the
   derived view with its current workflow. logplane performs no work.
2. **Canary:** select only new origins, durably record the ownership decision,
   and remove selected work from the external executor before allowing the bus
   worker to run it.
3. **Controlled:** stop the old source from enqueueing new work, drain or leave
   existing externally owned origins alone, then create new bus-owned tasks.

For a database or queue you do not control transactionally with logplane, use
an outbox. In one external transaction, update the source task and insert an
outbox row containing a stable `system`, `task_ref`, title, and payload. A
bridge process publishes that row through `AdoptionBridge` with a stable actor,
then marks the outbox row delivered only after the event is accepted. Retrying
the row is safe; changing its recorded owner is rejected. Never dequeue work
for execution merely because the bridge attempted a network call—wait for the
durable ownership decision.

### Integration troubleshooting

- **409 / dual ownership:** the external origin was already observed or
  controlled. Inspect that event; do not change actor names or keys to bypass
  the decision.
- **Adapter check performs real work:** teach the adapter to recognize the
  conformance marker before any side effect.
- **Malformed CLI output:** emit exactly one protocol-v1 JSON envelope on
  stdout; send diagnostics to stderr and stay within the configured byte cap.
- **Timeout or repeated retry:** ensure the agent's own timeout is shorter than
  the adapter deadline and that irreversible work uses a retry-stable effect
  identity rather than the changing assignment attempt.
- **Remote HTTP rejected:** use HTTPS, set `allow_remote`, and put the bearer
  token in the environment variable named by `token_env`.
- **Task remains open:** run `logplane explain TASK_ID`; capability mismatch,
  absent workers, dependencies, PM state, and lease health are distinguished.
