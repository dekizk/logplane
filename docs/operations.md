# Operating logplane

Submitting, inspecting, and steering work from the CLI: finding work, human decisions, retries, cancellation, pausing, workflows and dependencies, scheduling, concurrency, fairness, and budgets. Every command here is a view over, or an event appended to, the immutable log. See [how logplane works](architecture.md) for the underlying model.

## Connecting to the bus

Every command finds the bus the same way: an explicit `--url` first, then
`logplane.local.json` in the current directory, then `LOGPLANE_URL`, and
finally the loopback default. Intervention commands also accept `--config`. If
the local file and the environment disagree, the CLI stops with both values
instead of silently connecting to the wrong bus; unset `LOGPLANE_URL` or pass
`--url` deliberately. `LOGPLANE_TOKEN` and `LOGPLANE_WORKER_LEASE_SECONDS`
remain environment defaults, with `--token` and `--lease-seconds` available for
one invocation. Persistent warnings from `logplane doctor` usually mean the
PM, worker, or bus is not using the same URL.

`logplane up` runs the bus and the PM as two child processes from one
command, writes their logs to `logs/` beside the database (`.logplane/logs/`
by default), echoes the PM's reconciliation lines unless `--quiet` is given,
and stops the PM and then the bus on Ctrl-C or `SIGTERM`. If either process
exits, `up` stops the other and exits non-zero, so a bus never keeps running
without its coordinator. It refuses to start if a PM already holds the lock
for that bus.

`logplane doctor` reports the coordinator alongside the bus and workers. For a
loopback bus it checks the PM's lock file on this machine: `running` with the
PM's process ID, `not running`, or, for a non-local bus, `unknown`. When tasks
are waiting for reconciliation and no PM is running, the warning says so
directly.

## Submitting work

When you submit a task, the server assigns its `task_id` atomically. The PM assigns a live worker and emits
an `assignment_id`; the worker includes that assignment and its process
`instance_id` in every subsequent lifecycle event. The server also generates a
`correlation_id` for the root task and propagates it through the resulting
event chain. New tasks store their retry policy in the event itself.
`logplane submit` defaults to no automatic retries unless `--max-retries` is
supplied; direct event publishers retain the server default. New tasks also
materialize immutable `normal` priority unless the publisher selects another
class. Before assigning the first task in a new workflow, the PM records the
configured workflow-concurrency default as `workflow.policy_set`; the generated
local configuration currently defaults to four active assignments per
workflow.

Workers advertise capabilities and capacity, and a task can require
capabilities and carry inline context into every assignment:

```sh
logplane demo-worker coder --capacity 2 --capability python --capability testing
logplane submit "Run the Python test suite" \
  --capability python --capability testing \
  --context '{"repository": "logplane", "suite": "integration"}'
```

Raw publishers set the same `required_capabilities` and `context` fields on
`task.created` ([examples](events.md#publishing-raw-events)). Configured
adapters declare capabilities in their `worker` block.

Context and each final structured result are limited to 16 KiB of encoded JSON.
Resolved dependency input is limited to 32 KiB in aggregate.
They are coordination data, not storage for prompts, transcripts, or large
artifacts. logplane keeps that content out of SQLite by default and can store
explicitly opted-in captures in a separate content-addressed artifact store.

## Finding and inspecting work

```sh
logplane tasks
logplane tasks --status failed --limit 20
logplane tasks --workflow YOUR_CORRELATION_ID
logplane workflows
logplane decisions
```

Lists show IDs, projected state, and a next-command template. Replace any
`REPLACE_WITH_...` placeholders before executing it. `--json` returns structured
rows plus `observed_through_id`; follow `next_after_task_id` with
`--after-task-id`, or `next_after_workflow` with `--after-workflow`, preserving
your filters. The default is 50 rows, maximum 200. Task `--status` filters raw
task state; `effective_status` additionally exposes workflow pauses. Workflows
are ordered by correlation ID; tasks/decisions by task ID. Legacy uncorrelated
tasks remain in `tasks` and are counted separately by `workflows`.

Each command replays a fixed event prefix in bounded HTTP pages. No consumer
offsets or mutable task database are created. The retained projection still
grows with history; a small output limit does not make replay constant-cost.
Separate listing calls may observe newer state, so pagination is not a frozen
multi-command snapshot. These commands require the matching bus version's
`/health.last_event_id`; upgrade bus and CLI together.

The inspection commands below read the public HTTP/SSE API only. They never publish events,
create consumer offsets, cache a mutable projection, or participate in
scheduling:

```sh
logplane doctor
logplane workers
logplane task 1
logplane explain 1
logplane workflow YOUR_CORRELATION_ID
logplane tail YOUR_CORRELATION_ID
```

`task` shows current state, attempt ownership, dependencies, retry allowance,
deadline, and the event ids supporting its explanation. `workflow` renders all
tasks and dependency edges for one correlation and totals model tokens, cost,
duration, failures, and still-open spans from the separate telemetry stream.
When usage or cost was not reported, the CLI says so instead of displaying a
misleading zero.

All finite commands accept `--json` before or after the command for scripts:

```sh
logplane task 1 --json
logplane --json workflow YOUR_CORRELATION_ID
```

`doctor` reports bus/schema health, replay counts, worker lease health, safely ignored stale
events, and tasks that appear to be waiting for PM reconciliation.

## Cancel, retry, and decide

Use the real IDs returned by the views in these templates:

```sh
logplane cancel TASK_ID --reason 'No longer needed'
logplane retry TASK_ID --failed-event FAILURE_EVENT_ID --additional-retries 1 --reason 'Underlying issue fixed'
logplane decide TASK_ID --decision-event DECISION_EVENT_ID --value '"staging"'
# A decision can also be an object:
logplane decide TASK_ID --decision-event DECISION_EVENT_ID --value '{"release_target":"staging"}'
```

Retry targets the exact `task.failed` event; it grants the specified number of
new assignment opportunities, not an unlimited reset. Decide targets the exact
`decision.needed` event shown by `decisions`; the CLI carries its assignment and
decision identity automatically. This prevents stale commands from responding
to a newer failure/question. Cancellation targets the logical task, not just one
attempt. `task` and `explain` also display next-action templates.

All three commands publish existing intent topics as `human`. They precheck
current replayed state, then verify acceptance by replaying through the recorded
event. A concurrent state change can still make an appended claim inapplicable:
it remains in immutable history and is reported as `accepted: false` (exit 4),
not successful control. An accepted cancel is initially a cancellation request;
PM acknowledgement and eventual execution results must be checked separately.
An accepted retry/decision reopens work; it does not mean the worker completed it.

Commands derive stable idempotency keys from action/task and, for retry/decide,
the explicit source event. Repeating the **identical** command returns the old
event without reapplying it, even after the task moves on. Changed intent with
the same key is refused. `--idempotency-key` is available for deliberately new
intent; do not change it merely to bypass a stale-target warning. A rejected
cancel that later becomes relevant again needs an explicitly new key after
inspection. On a transport error, retry the same command/key because the first
publication may already be recorded. Task/workflow lookup absence returns 3; invalid
or stale prechecks return 2. JSON output includes the event, acceptance,
duplicate status, observed cursor, task state, and follow-up command.

No command provides per-user authorization or exactly-once external side
effects.

## Human decisions

When a worker emits `task.blocked`, the PM deterministically emits one
`decision.needed` for that assignment. `logplane decisions` lists pending
questions with a ready-to-edit `logplane decide` command; the
[raw-event equivalent](events.md#publishing-raw-events) is also available.
`logplane demo approval` shows the whole flow.

The PM accepts the response only for the current pending decision, records it
in its replay projection, reopens the logical task, and creates a new assignment
attempt. The assignment contains a chronological `decisions` array, so a
stateless or replacement executor can use the human input without mutating the
original task context. The old attempt can no longer complete the task.

Keep decisions compact coordination data. Prompts, transcripts, and large
artifacts belong in the separate content-addressed store and should appear in
telemetry only as compact references.

## Tool-call approvals

A human decision (above) ends the attempt and starts a new one with the answer.
A tool-call approval is the lighter form: a running attempt asks before one
tool call, keeps its assignment while it waits, and continues with the answer.
The task stays `started` throughout, and nothing is retried.

```sh
logplane approvals                          # what is waiting, with next-step commands
logplane approve REQUEST_EVENT_ID           # let that one call run
logplane deny REQUEST_EVENT_ID --reason "no force pushes"
logplane approvals --all --workflow CORRELATION_ID   # include answered and expired
```

`REQUEST_EVENT_ID` is the `approval.requested` event shown by `approvals`. A
request is answered at most once. A request whose attempt has since ended is
listed as `expired`; answering it reaches nobody.

Standing rules settle routine calls without you. They belong to one workflow,
and each change replaces that workflow's whole rule set:

```sh
logplane approval-rules CORRELATION_ID                # show the current rules
logplane approval-rules CORRELATION_ID \
  --allow Read --allow 'Bash:git status*' --deny 'Bash:git push*' \
  --reason "overnight run"
logplane approval-rules CORRELATION_ID --clear --reason "back to asking"
```

A rule is `TOOL` or `TOOL:INPUT_PATTERN`, with shell-style wildcards. The input
is one line: the command for Bash, the path for file tools, the URL for
WebFetch. Deny rules are checked first, then allow rules; a call that no rule
matches goes to a human. Calls settled by a rule are recorded as
`approval.rule_applied`, so the log shows which rule let each one through.

What to know before relying on it:

- **Rules match text; they are not a sandbox.** `Bash:git status*` says nothing
  about what `git` does on your machine, and an agent can spell one command
  many ways. Allow rules are therefore literal: the pattern must match the
  whole command, and never matches one that contains shell control characters
  (`;`, `&`, `|`, backticks, `$(`, `${`, redirection, newlines). A spelling
  the pattern does not cover, such as `git -C /repo status`, goes to a human;
  add `--allow 'Bash:git -C * status'` if you want it too. A bare
  `--allow Bash` allows every command, compound ones included.
- **Deny rules read more broadly, on a best-effort basis.** A Bash deny pattern
  is tried against the whole command, each part of a compound command, and
  each part with git's global options removed, so `Bash:git push*` also denies
  `git -C /repo push` and `cd /repo && git push`. It still reads text, not a
  shell: a script or alias that pushes is not caught. Treat a deny rule as a
  convenience, and keep credentials the agent should not use out of its reach.
- **Unanswered means denied.** The gate denies when nobody answers within its
  wait, when the bus is unreachable, or when it cannot read the call. The wait
  is set by the worker (`--approval-wait` on the Claude Code worker).
- **Expired requests cannot be answered.** `approve` and `deny` refuse a
  request whose attempt has already ended, and record nothing.
- **An agent's own name cannot approve.** The bus refuses an approval, or a
  `logplane decide` answer, recorded under the requesting worker's actor, and
  ignores rules that worker set. Actor names are still self-reported
  ([trust model](architecture.md#trust-model)), so this stops an agent from
  approving itself by mistake or through its own client; it does not stop a
  process that deliberately publishes under another name.
- **Approvals never change ownership.** The PM does not replay approval events.
  Cancelling, pausing or expiring the attempt still works as usual and ends
  the wait.

[Integrating agents](integration.md#asking-before-a-tool-call) shows how a
worker asks.

## Bounded retries and terminal failure

Every new `task.created` records a replayable retry policy:

```sh
logplane submit "Run integration tests" --max-retries 2
```

Raw publishers set `payload.retry_policy`, for example `{"max_retries": 2}`.

`max_retries` counts retries after the initial assignment. A value of `0`
allows only the initial attempt. The server materializes
`LOGPLANE_MAX_RETRIES` (default `2`) when the caller omits the policy, so a
later process configuration change cannot alter replayed task behavior.
Historical tasks that predate this field remain unlimited rather than being
silently assigned a new policy.

A retryable `task.attempt_failed` consumes one retry. A permanent attempt
failure does not spin: the PM immediately reconciles it to terminal
`task.failed`. Blocking for a human decision does not consume the retry
budget.

Losing a worker is not the task's failure, so a lease expiry has its own
allowance. Every new `task.created` records
`payload.lease_policy.max_expiries`, materialized from
`LOGPLANE_MAX_LEASE_EXPIRIES` (default `3`) when omitted: the task is
reassigned after each lost worker until it has lost more than that many, and
then ends `task.failed` with `lease_expiry_limit_exhausted`. A task that kills
its worker every time therefore still stops. `logplane task` shows the count
as "lost workers 1 of 3 allowed". Tasks created before this policy existed
keep the old rule, where a lease expiry consumes a retry. `logplane retry`
reopens a task that ran out of either allowance, and its
`--additional-retries N` then allows exactly N further assignments in total,
whether each ends in a failure or a lost worker.

Stopping a worker on purpose is not losing one. A worker stopped with Ctrl-C
or `SIGTERM` that has work running first publishes `agent.stopping`, so the
coordinator sends it nothing more. It cancels its running attempts and waits
for its executors to return, still heartbeating so that the work is not
handed to another worker while it may still be running here. Then it
publishes `agent.stopped`. The coordinator hands the work back the moment it
reads that, without waiting for the lease to run out, and does not count it:
the next attempt can start on another worker within a second, and "lost
workers" and the retry budget are untouched. `logplane workers` shows the
worker as `stopping` and then `stopped`, and it is given no new work until a
new process registers under the name. Starting the worker again straight away
is safe: the hand-back is remembered against the work the old process held,
not against the name. An assignment to the old process that is recorded only
after its replacement registered is handed back at once and spends no
allowance either.

The executor and its adapter's `cancel()` and `close()` hooks share one
10-second grace period. An executor or hook that ignores cancellation for
longer does not keep the worker looking alive: the worker stops heartbeating
and publishes `agent.stopped` anyway, and the work moves on. The old call may
then still be running alongside the new attempt, and the process cannot exit
until it returns.

Code that embeds `WorkerRuntime` can call `runtime.stop()` from any thread for
the same orderly stop. It returns immediately: cancelling the executors, the
wait for them and the announcements happen in the background, so the work is
handed back even if the event stream is quiet. Executors may therefore still
be running when `stop()` returns; `run()` returning is what says they have
stopped or been given up on.

A worker that stops itself over a failure, including an unexpected
event-stream error, announces the stop as `requested: false` if the bus is
reachable. Its work is also handed back at once,
but an attempt it had already started counts as a lost worker; one it had not
started never does. If the announcements cannot be delivered, or the worker
is killed outright (`SIGKILL`, a crash, a power cut), nothing is announced
and the lease rules above apply as before.

The coordinator also avoids expiring leases it cannot judge. Old heartbeats
mean it was not watching, not that workers died, in three cases: when it
starts, when its event stream reconnects after any break, and when it has not
looked at the log for longer than a lease (a suspended laptop). It then waits
one lease period for workers to be heard from before expiring any lease for
silence alone; a worker whose process was replaced is still expired at once.
A reconnect does not extend the grace an earlier reconnect began, or start one
within a lease period of that one ending, so a stream that drops every few
seconds still leaves windows in which a silent worker is expired. That is not
a guarantee: the coordinator only checks leases when it gets to look at the
log, so if every look comes after a break longer than a lease, each one starts
a new grace and a silent worker stays assigned until the connection holds for
a lease period. A suspension shorter than a lease with no reconnect
gets no grace, so a worker whose last heartbeat was already a few seconds old
can still be expired after one.

**Stopping a disconnected worker.** The coordinator's lease is not a
worker shutdown timer. By default a worker keeps running its assignment
during a bus outage, subject to its existing deadline. If the executor
finishes, the worker retries publishing the same outcome with the same key;
it does not rerun the executor. A revocation observed on reconnect still
takes precedence. Stopping `logplane up` stops its bus and coordinator, not
workers started in other terminals.

For an unattended worker, optionally set a separate disconnect timeout in
the **worker's** environment. For example, to tolerate a 30-second outage
but not retry publication indefinitely:

```sh
LOGPLANE_WORKER_DISCONNECT_TIMEOUT_SECONDS=120 logplane demo-worker alice
```

The same setting applies to `logplane adapter run` and workers built with
`WorkerRuntime`; an explicit positive `disconnect_timeout_seconds` constructor
argument overrides the environment. Unset means disabled. After registration,
the first failed heartbeat or retryable lifecycle write starts the timer.
Repeated failures do not extend it. A successful heartbeat or lifecycle write
clears it; an idle event stream alone does not start it. Detection starts
when a write reports failure, not at the instant the network goes down.
A write the bus *refuses* (a 4xx answer, such as a bad token) is not an
outage and does not use the timer: the worker stops at once, whether the
refused write was a heartbeat or a lifecycle event, and with or without a
timeout set.
The timeout must be at least twice the worker's heartbeat interval (10
seconds with the default 5-second heartbeat); a shorter value is refused at
start-up, because the timer would run out before a retry was even sent. That
minimum is a floor, not a safe setting. The timer runs until a write is
*confirmed*, so a retry that succeeds but is answered slowly (a write may take
up to 10 seconds to time out) can still arrive too late. Allow for the
heartbeat interval, that 10 seconds, and the longest outage you want a running
assignment to survive.

On timeout the worker stops accepting assignments and retrying writes,
requests cancellation of running executors, and logs unconfirmed assignment
IDs and phases locally. It does **not** declare those tasks failed on the bus.
In-flight writes may already have committed: check `logplane task` after
recovery. Unpublished results are held in memory, not spooled to disk.
Shutdown is cooperative: an in-process executor that ignores cancellation
can keep the process alive; the standard stream reader may also take up to
its 30-second idle read timeout to notice shutdown. Use retry-stable effect
IDs for irreversible external operations.

A worker stopped this way does not come back by itself, and that includes an
idle worker with no assignment. So run an unattended worker under a
supervisor that restarts it on failure (launchd `KeepAlive` with
`SuccessfulExit` false, systemd `Restart=on-failure`, or a shell loop).
`logplane demo-worker`, `logplane adapter run` and the example workers report
why they stopped in their exit status:

| Status | The worker stopped because |
| --- | --- |
| 0 | it was asked to: Ctrl-C, or a replacement instance with the same name registered |
| 75 | it could not reach the bus: the disconnect timeout ran out, or it could not read the log to check an incoming assignment |
| 1 | of another failure: the bus refused a heartbeat or lifecycle write (a 4xx answer), an assignment could not be resolved safely, or an executor ignored cancellation after an [attempt timeout](#cancellation-and-deadlines) |

The second cause of 75 does not wait for the timeout and applies whether or
not one is set: a worker that cannot verify an incoming assignment stops at
once instead of running it (see
[agent limits and workflow budgets](#agent-limits-and-workflow-budgets)).
A restarted worker registers as a new instance; anything the old one left
unconfirmed is expired by the coordinator and reassigned within the task's
lost-worker allowance. Code that embeds `WorkerRuntime` reads
`runtime.exit_code` (or `runtime.stopped_by_disconnect`) after `run()`
returns.

A retry normally starts as soon as a capable worker is free. A worker can
instead ask for a wait, by reporting `retry_after_seconds` with a retryable
failure: the task then stays `open` and is not reassigned until that long
after the failure event. This is for failures with a known recovery time,
such as a provider's usage limit or an HTTP `Retry-After`. `logplane task`
and `explain` show the wait as `retry_delay_pending` with the time remaining.
The wait is at most seven days, the retry still consumes one retry, and a
task whose retry budget is already spent fails at once instead of waiting.
Deadlines, cancellation and pauses apply to a waiting task as usual.

To revive a failed task, record a retry against its latest `task.failed` event
(the [raw event](events.md#publishing-raw-events) is a `task.retry_requested`
caused by that failure):

```sh
logplane retry 2 --failed-event 21 --additional-retries 1 \
  --reason "The dependency is healthy again"
```

This records exactly the requested number of new assignment opportunities; it
never resets `attempt`. After retry exhaustion that extends the previous
budget. After a permanent failure it does not silently restore the unused
automatic budget. The next assignment therefore has a new, monotonically
increasing `assignment_id`, and late output from any failed attempt remains
unable to complete the task.

## Cancellation and deadlines

Cancellation and deadlines are durable controls without mutable task state.
Cancellation is a command event followed by a PM-derived terminal event:

```sh
logplane cancel 1 --reason "superseded by a new request"
```

Python publishers can call `BusClient.cancel_task(task_id, reason=...)`, which
uses the same stable task-scoped idempotency key. The server verifies that the
task exists and inherits its `correlation_id`. The PM then emits exactly one
`task.cancelled`, including the last assignment identity when the task was
active. A request recorded after a task is already terminal is retained in the
log but does not rewrite its outcome.

An optional absolute Unix timestamp can bound the whole logical task:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Finish before the release cutoff",
    "deadline_at": 2000000000.0
  }
}
```

The deadline is persisted on `task.created`, copied into every assignment, and
exposed as `AssignmentContext.deadline_at`. When it is reached, the PM emits one
`task.deadline_exceeded` instead of assigning or retrying the task. Cancellation
and deadline termination do not consume retry budget. They also terminate
blocked work without creating another human-decision request.

Event order is authoritative: a completion already recorded before a
cancellation request remains completed; once the cancellation request is
accepted by the reducer, later attempt output is stale. Worker outcomes whose
event timestamp is at or after the persisted deadline are likewise ignored by
the PM projection. Executor timeouts and worker lease expiry remain separate
retryable attempt failures—they are not task deadlines.

The runtime arms a local timer from the persisted assignment deadline, and also
follows cancellation, pause, workflow-pause, and supersession control events.
It terminates
subprocess adapters, invokes cooperative `cancel(assignment_id)` on in-process
adapters that provide it, and suppresses late lifecycle output. This stops
local execution at the cutoff even if PM reconciliation is temporarily down;
the PM remains responsible for recording the authoritative terminal event.
Python threads cannot be force-killed, so side effects must still be
idempotent.

**Limiting one attempt.** A deadline bounds the whole task, and a worker's
heartbeat says only that the worker process is alive, not that the agent is
getting anywhere. So an executor with no time limit of its own, stuck on a
call that never answers, holds its task and its worker indefinitely. A
worker can be given a limit on each attempt:

```sh
logplane demo-worker alice --attempt-timeout 600
```

or `WorkerRuntime(..., attempt_timeout_seconds=600)` in code. It is off by
default. When an attempt has been executing for that long, the worker calls
the executor's `cancel(assignment_id)` and reports the attempt as
`task.attempt_failed` with code `attempt_timeout`, a retryable failure, so the
task is retried within its retry budget like any other failed attempt. The
worker carries on with its next assignment.

- The limit is wall-clock time from the moment execution starts, and it
  includes any time the agent spends waiting for a person. Do not set it on a
  worker whose agent waits for tool-call approvals.
- The Claude Code, Codex, Hermes and OpenClaw example workers and the CLI and
  HTTP adapters already limit each attempt themselves (`--timeout`,
  `timeout_seconds`); the Claude Code worker's limit does not count approval
  waits. This setting is for executors that have no limit: in-process Python
  agents and your own `Executor` classes.
- An executor has 10 seconds to return after being cancelled. If it does not
  (Python code blocked in a call cannot be interrupted from outside), the
  worker retires: the slot that call still occupies cannot be reused, and a
  worker that kept heartbeating would be handed assignments it could never
  start. From that moment it accepts no work, stops renewing its lease, and
  publishes `agent.stopping` so the retry is not sent back to it. It tries for
  up to 10 seconds to report the timeout, then announces that it has stopped
  and ends with exit status 1. If none of those messages gets through, the
  lease running out frees its tasks as for any lost worker. The process cannot
  exit until the stuck call returns, so a supervisor may need to kill it. Give
  in-process agents a `cancel` that works, or their own timeouts, to avoid
  this.

Cancelled or deadline-exceeded prerequisites propagate
`task.dependency_failed` to downstream DAG tasks. Terminal tasks are not
revived; create a new task to record new intent.

## Pause, resume, and supersession

Pause, resume, and supersession are immutable commands followed by
PM-derived acknowledgements. A task pause immediately fences its active
attempt, then `task.paused` records the stable paused state. Resuming records a
new command and acknowledgement; if execution had been interrupted, the next
assignment uses the next attempt number without charging a retryable failure.
A blocked task resumes to its existing human-decision state rather than losing
the question it was waiting on. If resume races the pause acknowledgement, the
request is queued: the PM records the ownership fence first, then immediately
acknowledges the resume. The PM consumes its own effects through the same
ordered event cursor as every other command, so live state and fresh replay
cannot observe those events in different orders.

```sh
logplane pause task 12 --reason "hold during maintenance"
logplane resume task 12 --reason "maintenance complete"
logplane pause workflow 01abc... --reason "freeze this rollout"
logplane resume workflow 01abc... --reason "continue rollout"
```

A workflow pause is a correlation-scoped gate. It revokes active assignments,
prevents new assignments and ordinary PM effects, and also gates tasks created
while the workflow is paused. Cancellation and persisted task deadlines remain
authoritative during a pause. The pause request captures an immutable snapshot
of assignment ownership. Its acknowledgement always names that request-time
snapshot, but revokes only assignments that are still active and still match;
an intervening cancellation, task pause, supersession, or deadline keeps its
stronger state. Resuming the workflow reopens only the tasks actually
interrupted by that acknowledgement, with monotonic attempt identities, while
leaving individually paused or terminal tasks untouched. A stale assignment
that crosses a pause boundary is retained as audit evidence but never becomes
executable ownership; its delivery number is not reused after resume. Workers
also remember task and workflow pause gates so such a delivery cannot reach the
adapter before the PM rejects it.

Supersession records changed intent by creating a replacement task. It never
edits the old task or its dependency edges:

```sh
logplane supersede 12 "Deploy the revised release" \
  --reason "release requirements changed" \
  --priority high
```

The replacement `task.created` carries `supersedes_task_id`. If the old task is
still live, it is immediately fenced and the PM records terminal
`task.superseded`; an already terminal task keeps its original outcome while
retaining the replacement link. Existing DAG edges are not silently
redirected. A task that depended on a live superseded task ends through the
normal dependency-failure path, so changed downstream intent must also be
represented by new tasks. Each old task may have only one direct replacement,
while a later revision can supersede that replacement to form an auditable
chain.

Once a task has a replacement, the old intent cannot be revived with
`task.retry_requested`, even when its original terminal outcome was failure.
Retry or supersede the replacement instead. This prevents both the obsolete
and replacement intents from becoming executable at the same time.

The command helpers accept optional idempotency keys. Supply the same key when
retrying an operator command after an uncertain HTTP response. Without one,
the CLI generates a fresh command identity so a task can be paused and resumed
more than once over its lifetime.

## Workflow correlation

`correlation_id` is a top-level event-envelope field. A root `task.created`
receives a random correlation automatically when the caller omits one. A new
event with `caused_by` inherits its parent's correlation inside the same
append transaction, so PM agents and workers do not need to copy the value
manually.

Callers may provide a correlation explicitly to group several root tasks under
one wider goal:

```json
{
  "topic": "task.created",
  "actor": "human",
  "correlation_id": "release-2026-07",
  "payload": {"title": "Run integration tests"}
}
```

New events must reference an existing `caused_by` event. If both a child and
its parent have correlations, they must match. `task_id` still identifies only
one task; it is never used as a workflow identifier.

## DAG orchestration

A task declares dependency edges when it is created:

```json
{
  "topic": "task.created",
  "actor": "human",
  "payload": {
    "title": "Plan fixes from the audit",
    "depends_on": [4],
    "required_capabilities": ["hermes"]
  }
}
```

From the command line, `logplane submit "Plan fixes from the audit"
--depends-on 4 --capability hermes` publishes the same event.

Every dependency must already exist, appear only once, and belong to the same
`correlation_id`. Because new nodes may point only to existing nodes, the bus
cannot accept a cycle. A dependent with no explicit correlation inherits the
workflow identity of its dependencies. Fan-in is capped at 128 direct
dependencies per task. These rules support chains, fan-in, and fan-out while
keeping the graph reconstructible from immutable `task.created` events; there
is no mutable DAG table or board.

The PM derives readiness during replay. It assigns a task only after every
dependency has a valid `task.completed` event. The persisted `task.assigned`
contains compact `dependency_refs` pairs (`task_id` and `completion_event_id`),
not copied results. The worker retrieves those immutable completion events via
`GET /events/{event_id}`, verifies their task and workflow identities, and then
constructs `AssignmentContext.dependencies` for the executor. This keeps each
result stored once in the coordination log. A transient lookup failure is
retried three times before the worker stops. Malformed references and 4xx
responses stop it immediately; continuing to heartbeat after silently skipping
an owned assignment would otherwise leave that task assigned forever. Stopping
allows the lease and normal reassignment path to recover safely.

If an upstream task reaches `task.failed`, `task.dependency_failed`,
`task.cancelled`, or `task.deadline_exceeded`, the PM
emits a deterministic `task.dependency_failed` for each affected downstream
task and cascades that state through the graph without assigning doomed work.
Terminal propagation is deliberately conservative: reviving an
upstream task does not silently revive already failed descendants. Recreate
those descendants as new tasks after the upstream recovery so the new intent
and causal history are explicit.

## Scheduling policy

Scheduling policy lives on `task.created`, not in a mutable queue.
Every task has one of four priority classes:

```text
low < normal < high < urgent
```

When several tasks are eligible for currently available workers, the PM
considers higher priority first, then the lower `task.created` event id, then
`task_id` as the final deterministic tie-breaker. Priority affects the next
assignment only: it never interrupts an active attempt, bypasses dependencies,
revives terminal work, exceeds worker capacity, or changes capability matching.
Changing intent means creating or superseding a task rather than editing its
priority in place. Within a workflow, a sustained stream of higher-priority
eligible work can delay lower-priority siblings. Across workflows, the
round-robin policy described below prevents one workflow from monopolizing all
new assignment turns.

`not_before` is an optional absolute Unix timestamp. A future task remains
visible and explainable but cannot be assigned until that timestamp. If both
`not_before` and `deadline_at` are present, `not_before` must be earlier than
the deadline. Historical tasks replay as immediately eligible `normal` work.
Eligibility is checked against the PM clock; assignment may follow on the next
event or idle reconciliation after the boundary rather than at sub-second
timer precision.

```sh
logplane submit "Run the release checks" \
  --priority high \
  --not-before 2000000000
```

`logplane task`, `workflow`, and `explain` show the immutable policy. Before
reconciliation, explanations identify a higher-priority or earlier-created
eligible task considered first; after assignment consumes capacity, they name
the active tasks occupying compatible workers. These answers are rebuilt from
the event log and current worker leases—there is no hidden queue table.

## Workflow concurrency policy

Workflow-specific concurrency policy lives in the same event log as the work
it governs. The PM materializes
`default_workflow_max_active_assignments` from `logplane.local.json` before a
workflow's first assignment. Older configuration files without this optional
field materialize an unbounded default, preserving their previous behavior.
Restarting with a different default does not alter a workflow that already has
a policy event.

An operator can append a later policy event without editing history:

```sh
logplane policy "$CORRELATION_ID" \
  --max-active-assignments 2 \
  --reason "bound this workflow's fan-out"

# Explicitly return the workflow to unbounded concurrency:
logplane policy "$CORRELATION_ID" \
  --unbounded \
  --reason "dedicated capacity is now available"
```

The latest accepted policy event takes effect for subsequent assignments. A
lower limit never cancels work already assigned or running; the PM waits until
active ownership falls below the new limit before assigning more. Every new
`task.assigned` records `workflow_policy_event_id`, so inspection can identify
the exact event that authorized it; the framework-neutral `AssignmentContext`
also exposes that identity to adapters. Before invoking the executor, workers
replay coordination history through the assignment using the same reducer as
the PM. Only an accepted assignment can execute. Stale plans remain audit
evidence and are reissued with a fresh attempt identity when eligible.

Workflow policy is orchestration intent. Deployment safety caps such as process
limits, memory limits, and network restrictions remain external operational
controls and may be stricter.

## Workflow fairness

The PM schedules ready workflows with a strict round-robin rule. A
workflow gets one assignment turn, then the next ready workflow gets the next
turn. Task priority and immutable creation order still decide which task is
chosen *within* that workflow. This prevents one large or urgent workflow from
monopolizing every available assignment while retaining useful priority
semantics inside a single body of work.

There is no mutable scheduler queue or private fairness counter. The cursor is
the latest accepted `task.assigned` event, and workflow order is derived from
each workflow's first `task.created` event. Every new assignment records:

```json
{
  "fairness": {
    "policy": "workflow_round_robin_v1",
    "previous_assignment_event_id": 42
  }
}
```

The first policy-aware assignment records `null` as its predecessor. If
another accepted assignment crosses a PM plan before publication, the stale
plan cannot advance the cursor or become executable ownership. Reconciliation
consumes that delivery identity and reissues the task with a fresh attempt and
the current predecessor. Fresh replay therefore reconstructs the same cursor
and next workflow without a scheduler side database. Historical assignments
without `fairness` remain replayable and seed the cursor in event order;
historical uncorrelated tasks retain their original global priority ordering.

`logplane task`, `logplane explain`, and `logplane workflow` expose the
policy and predecessor event, so an operator can distinguish capacity,
workflow-concurrency, priority, and fairness waits. This is assignment fairness,
not runtime time-slicing: it does not preempt active work or revoke a long
running task.

## Agent limits and workflow budgets

Agent policy separates an agent's advertised worker capacity from the
operator's persisted concurrency policy. The effective capacity is the lower
of the two. A later decrease does not revoke work already running; it prevents
new assignments until active work falls below the new limit:

```sh
logplane agent-policy hermes \
  --max-active-assignments 1 \
  --reason "canary one assignment at a time"
```

Workflow policy also supports token, cost, attempt, and elapsed wall-clock
budgets (`logplane demo budget` shows a cost cap in action). Token and cost limits use an explicit per-assignment reservation so
the PM does not admit several concurrent attempts against the same remaining
budget:

```sh
logplane policy "$CORRELATION_ID" \
  --max-tokens 100000 \
  --reserve-tokens-per-assignment 20000 \
  --max-cost 5 \
  --reserve-cost-per-assignment 1 \
  --max-attempts 10 \
  --max-wall-clock 3600 \
  --reason "bounded integration trial"
```

Each accepted `task.assigned` is the reservation record. It names the workflow
and agent policy events that governed admission. While active, an attempt is
charged at least its reservation. Once it ends, fully reported usage replaces
the reservation with actual usage; if usage is absent, the reservation remains
as the conservative charge. Actual usage above a reservation is authoritative
and may block subsequent assignments. A later policy change is a new event,
not an edit, and only governs later admission decisions.

Use `--unbounded-tokens --reserve-tokens-per-assignment 0` or
`--unbounded-cost --reserve-cost-per-assignment 0` to remove a budget
explicitly. A limit and its reservation must be changed together. Attempt
budgets count accepted assignments. Wall-clock budgets begin at the first
`task.created` timestamp in the workflow. These are orchestration admission
controls, not hard provider billing cutoffs: an in-flight model call can exceed
its reservation, and late valid usage is still reconciled into the log.

`logplane task`, `logplane workflow`, `logplane workers`, and
`logplane explain` expose effective limits, charges, missing reports,
reservations, blocking resources, and the relevant policy/accounting event
IDs. Deployment defaults are materialized once as policy events. Restarting
the PM with different defaults therefore cannot silently change an existing
workflow or logical agent.

Worker admission includes usage recorded between planning and publication,
wall-clock budget boundaries, and policies that predate worker registration.
An obsolete default policy arriving after an operator policy is ignored by
both PM and worker. The worker caches its replay cursor and reads the unseen
coordination suffix on later assignments. Raw telemetry is excluded. If this
history cannot be read or the assignment is missing from it, the worker stops
so heartbeats cease and lease recovery can proceed. A policy change after an
accepted assignment does not retroactively revoke that work.

The first admission requires a full coordination replay. This adds per-worker
memory and read costs; disposable snapshots and large-log benchmarks remain
part of the scale work in `ROADMAP.md`.

Embedded runtimes that supply a finite event iterable still need a bus client
with `query_all()` returning persisted coordination history in event-ID order.
The standard `BusClient` provides this automatically.
