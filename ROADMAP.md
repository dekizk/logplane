# logplane roadmap

## Product direction

`logplane` should become an agent-agnostic control plane that people can run
alongside agents they already use. Adopting it should not require rewriting an
agent, replacing its model provider, or moving the agent's internal logic into
logplane.

The product is intended to be a realistic alternative to kanban-style agent
monitoring. A board represents work through mutable cards that humans or agents
must keep synchronized. logplane instead records immutable facts about work,
derives current state by replaying those facts, and coordinates the next safe
action. Any dashboard or CLI is a view over that history—not another source of
truth.

| Board-style monitoring | logplane |
|---|---|
| Mutable cards describe current state | Immutable events record what happened |
| Status can drift from real execution | State is derived from execution history |
| Humans or agents move cards | Workers emit lifecycle outcomes |
| Retries and ownership are often implicit | Attempts, leases, and ownership are explicit |
| Dependencies are visual conventions | DAG readiness is enforced by the coordinator |
| Recovery depends on manually fixing state | Replay and reconciliation recover missing effects |
| Usually tied to one workflow product | Framework-neutral contracts connect existing agents |

The goal is not to build a better task board. The goal is to make a task board
unnecessary as the orchestration authority while still giving operators a
clear, approachable view of their agents.

## Product principles

Every version should preserve these constraints:

1. **Agent agnostic.** Any Python agent, CLI agent, or external workflow should
   be able to integrate through a small stable boundary.
2. **Adopt incrementally.** Shadow, canary, and controlled modes let a team
   observe first and transfer ownership only when ready.
3. **Easy first success.** A new user should be able to install logplane,
   connect one existing agent, submit one task, and inspect its state in under
   ten minutes.
4. **One orchestration owner.** Integration must prevent an external scheduler
   and logplane from executing the same task simultaneously.
5. **The event log is truth.** Operational views, indexes, snapshots, and UIs
   are disposable projections that can be rebuilt.
6. **Local-first and safe by default.** The smallest useful deployment should
   remain easy to understand, run, back up, and remove.
7. **Bounded agents.** Agents execute assignments; they do not silently become
   a second project manager, scheduler, or source of task state.
8. **Progressive authority.** Tool access, content capture, and external side
   effects remain explicit opt-ins.
9. **Explainable decisions.** Assignment, waiting, retry, cancellation, and
   terminal states should always be traceable to recorded events and policy.
10. **Evidence-driven scope.** Real integrations and failure trials should
    determine which orchestration features are built next.

## Current implementation — v0.14.0

The project already provides the core mechanics needed for a local event-driven
agent control plane:

- append-only SQLite events with schema validation and idempotent publishing;
- workflow correlation, causal links, task identity, attempts, and worker
  process identity;
- crash-safe PM replay and deterministic reconciliation;
- worker registration, capabilities, capacity, heartbeats, leases, and stale
  attempt rejection;
- bounded retry policy, permanent failure, human retry, blocking, and human
  decisions;
- framework-neutral executor outcomes with in-process and subprocess adapters;
- shadow, deterministic canary, and controlled adoption modes;
- automatic DAG readiness, dependency result references, and terminal failure
  propagation;
- separate model/tool telemetry and content-addressed artifact storage;
- durable cancellation, task-wide deadlines, local adapter revocation, and DAG
  propagation for cancelled or expired work;
- a real Hermes adapter and live evidence covering execution, human decisions,
  recovery, DAGs, telemetry, cancellation, and deadlines.
- a shared replay-only coordination projection and GET-only observer client;
- human and JSON task, workflow, worker, explanation, health, and tail views;
- event-traced waiting reasons plus workflow token, cost, duration, and span
  summaries from the separate telemetry stream.
- a stable `logplane` integration package, protocol-v1 CLI/HTTP envelopes,
  configuration-driven adapters, and a standalone conformance command;
- loopback-only local onboarding commands, minimal Python/CLI examples, a
  guarded HTTP bridge, safe rollout/outbox guidance, and Mermaid DAG export;
- cross-actor external-origin claims that make accidental dual ownership a
  transactionally rejected conflict rather than a documentation convention;
- immutable scheduling priority, delayed eligibility, task/workflow pause and
  resume, and task supersession;
- persisted workflow and agent concurrency policy, deterministic workflow
  fairness, and accounted token, cost, attempt, and wall-clock budgets;
- shared PM/runtime assignment admission from coordination history, including
  intervening usage and obsolete default-policy rejection;
- a rebuildable task identity index, opt-in PM replay snapshots, bounded-page
  worker admission, read-only artifact audits, and verified backup/restore;
- scripted `logplane demo` scenarios, CLI DAG submission, scriptable demo
  worker outcomes, and prompt bus shutdown with open event streams.

v0.9's integration work and its CLI/HTTP trials are complete and committed.
v0.10's scheduling and management policy shipped, and v0.11.0 (2026-09-13)
added local scale and recovery hardening. v0.12.0 (2026-10-20) released the
pre-v1.0 readiness checkpoints below together with the demonstration work; see
the remaining v1.0 gates. v0.13.0 renamed the project from agent-bus to
logplane and moved the code to a `src/` layout, with no contract changes;
v0.13.1 and v0.13.2 fixed the PyPI page and hardened migration, the demo
effect receivers, budget wording, and release validation.
Evidence is recorded in the CLI, HTTP, and Hermes example trial notes; the
Hermes notes also record credential-free coordination trials.

## Completed release step — v0.8

- [x] install the repository into a clean disposable environment and verify
  the console entry point;
- [x] use the CLI against a live two-task Hermes DAG containing telemetry;
- [x] inspect `doctor`, `workers`, `task`, `explain`, `workflow`, `tail`, and at
  least one `--json` response;
- [x] confirm the human views answer the trial questions without raw event JSON;
- [x] record exact output, event ids, friction, and any explanation mismatch in
  `examples/hermes/TRIAL_NOTES.md` before committing and pushing v0.8.

## v0.8 — approachable operations and read-only visibility

This release candidate makes the existing control plane understandable without
requiring users to read raw event rows.

### Operator projection

Add a read-only projection that can answer:

- What state is this task or workflow in?
- Why is it waiting?
- Which worker owns the current attempt, and is its lease healthy?
- Which dependencies are incomplete or terminal?
- How many retries remain?
- Is the task blocked on a person, cancellation, or a deadline?
- Which event caused the current state?
- How much model usage and reported cost belongs to the workflow?

The projection must be derived entirely from the event log and rebuildable from
scratch.

### Human-friendly CLI

Implemented commands include:

```text
logplane doctor
logplane workers
logplane task TASK_ID
logplane workflow CORRELATION_ID
logplane explain TASK_ID
logplane tail CORRELATION_ID
```

Output should default to concise human-readable summaries, with `--json` for
automation. `explain` should state the concrete reason a task cannot advance,
not merely repeat its status.

### Installation and first-run experience

v0.8 establishes an editable-install console command for operator visibility.
The fuller published-package path remains the target:

```text
pip install logplane
logplane init
logplane doctor
logplane serve
```

At v0.8, publishing to PyPI plus the `init` and `serve` convenience commands
remained v0.9 onboarding work. v0.9 now implements and package-tests those
commands; publishing the reviewed release to PyPI remains a release action.
Before v0.9, a checkout used `python -m pip install -e .`,
`python -m uvicorn bus:app`, `python -m pm_agent`, and `python -m worker`.
Configuration, errors, and the read-only command surface are designed together
so a user need not understand SQLite, SSE, reducer internals, or four identity
fields before completing the first example.

### v0.8 acceptance criteria

- One command explains every current task state.
- A two-task workflow can be understood without inspecting raw event JSON.
- Every displayed value is traceable to event ids.
- Restarting or deleting the projection does not change orchestration truth.
- The quick start is tested from a clean environment.
- A newcomer can connect the demo executor and inspect a completed workflow in
  under ten minutes.

## v0.9 — integration SDK and onboarding

Once the system is easy to observe, make it easy to attach to arbitrary agents.
This is intentionally earlier than advanced scheduling policy because broad
integration is central to the product, not an optional final layer.

### Stable public integration surface

- Package and document `AssignmentContext`, executor outcomes, runtime hooks,
  adoption modes, telemetry, and artifact references as public APIs.
- Stabilize the subprocess JSON protocol and version negotiation.
- Provide a generic wrapper for Python objects exposing `run()`.
- Provide a configuration-driven wrapper for existing CLI agents.
- Define an HTTP/webhook bridge for agents that cannot run in the same process.
- Provide explicit cancellation, deadline, idempotency, and external side-effect
  requirements for adapter authors.

### Integration kit

- `logplane adapter check` conformance tests;
- a minimal Python-agent example;
- a minimal CLI-agent example;
- the Hermes example as a realistic reference integration;
- copyable shadow, canary, and controlled rollout recipes;
- an external outbox pattern for safely adopting tasks from another system;
- troubleshooting guidance for ownership, credentials, timeouts, and malformed
  outcomes.

### v0.9 acceptance criteria

- An existing Python agent can be connected without changing its core logic.
- An existing JSON-capable CLI agent can be connected through configuration.
- Adapters can prove conformance without running the full repository test suite.
- Shadow-to-canary-to-controlled adoption has one documented safe path.
- Dual ownership is mechanically difficult and prominently diagnosed.

### v0.9 implementation checklist

- [x] package the stable Python assignment, outcome, runtime, adoption,
  telemetry, and artifact-reference surface;
- [x] add a strict versioned subprocess/HTTP protocol while retaining legacy
  subprocess compatibility;
- [x] provide generic Python, configured CLI, and guarded HTTP wrappers;
- [x] add `logplane adapter check` without requiring a running bus or PM;
- [x] add `init`, `serve`, `pm`, `demo-worker`, `submit`, and configured adapter
  run commands for an approachable local first success;
- [x] provide minimal Python and CLI examples while retaining Hermes as the
  realistic reference integration;
- [x] document cancellation, deadlines, idempotency, external effects,
  shadow/canary/controlled rollout, an outbox, and troubleshooting;
- [x] enforce cross-actor external-origin uniqueness to reject accidental
  dual ownership;
- [x] provide a read-only Mermaid workflow export;
- [x] add protocol, adapter, HTTP guard, onboarding, packaging, and ownership
  regression tests;
- [x] record a clean-environment install and one live non-Hermes adapter trial
  before declaring the release complete.
- [x] reject expired or replayed deliveries before executor effects and make
  cancellation/cleanup phase-safe;
- [x] distinguish attempt identity from retry-stable external-effect identity;
- [x] accept forward capability advertisements while selecting only supported
  protocol v1;
- [x] validate directly constructed public adapter configs and expose stable
  malformed-stream/cleanup failures;
- [x] diagnose conflicting local-config and environment bus URLs instead of
  silently choosing one.

Live v0.9 evidence now covers both the configured CLI path and the guarded HTTP
path. The HTTP trial exercised normal completion, HTTP-503 retry with a stable
logical-effect identity, and cooperative cancellation without a late lifecycle
event; see `examples/http_agent/TRIAL_NOTES.md`.

## v0.10 — scheduling and management policy

Add richer controls only after visibility and integration trials show they are
needed:

- immutable priority classes rather than manually ordered cards;
- fair scheduling between workflows or tenants;
- per-workflow and per-agent concurrency limits;
- token, cost, attempt, or wall-clock budgets;
- `not_before` scheduling;
- crash-safe pause and resume commands;
- explicit task supersession;
- deterministic explanations for every scheduling choice.

Pause/resume should use the same ownership discipline as cancellation but keep
the logical task eligible for a later attempt. Priority must not turn into an
unexplained mutable queue position.

If trials show that pause/resume and scheduling fairness are independently
complex, split this work into separate versions rather than forcing both into a
large release.

### v0.10 implementation checklist

Phase 1 — deterministic scheduling foundation:

- [x] add immutable `low`, `normal`, `high`, and `urgent` priority classes;
- [x] add optional absolute `not_before` eligibility;
- [x] default historical and newly omitted priority to immediately eligible
  `normal` work;
- [x] select eligible work by priority, creation event, then task id without
  bypassing dependencies, deadlines, capabilities, or capacity;
- [x] expose scheduling policy and deterministic selection explanations in
  task/workflow views and the CLI;
- [x] support scheduling policy through controlled/canary adoption;
- [x] cover validation, replay, delayed eligibility, priority ordering, and
  explanation behavior with regressions.

Phase 2 — operator controls:

- [x] add crash-safe task/workflow pause and resume;
- [x] add explicit immutable task supersession;
- [x] fence late output from paused or superseded attempts;
- [x] explain every control transition from event evidence.

Phase 2 correctness hardening:

- [x] consume concurrent commands and PM effects through one ordered cursor;
- [x] queue resume commands that arrive before pause acknowledgement;
- [x] make workflow-pause acknowledgements stable from the request-time
  assignment snapshot while preserving stronger intervening task controls;
- [x] fence stale assignments and expiries at workflow pause boundaries;
- [x] recover identical concurrent supersession and adoption requests by
  idempotency key;
- [x] prevent retry from reviving intent that already has a replacement;
- [x] align deadline explanations with PM transition precedence;
- [x] cover live-versus-replay equality and control interleavings with isolated
  regressions;
- [x] repeat the live executor pause/resume trial against the hardened runtime.

Phase 2 keeps control state in the log. Task pause/resume uses request and PM
acknowledgement events; workflow pause is a correlation-scoped scheduling and
ownership gate. Supersession creates a replacement `task.created` event and a
PM-derived terminal event for the old task rather than editing old intent.
Unit evidence covers replay, duplicate acknowledgements, concurrent commands,
crash windows, blocked-task preservation, tasks created under a workflow pause,
hard deadlines, dependency propagation, stale PM effects, and late worker
output. The original Hermes phase 2 trials and an isolated ordered-cursor live
worker repeat passed. The Phase 3 sections below record the policy, fairness,
and accounting work that subsequently completed v0.10.

Phase 3A — persisted workflow policy and concurrency:

- [x] add immutable workflow policy events with deployment-configured defaults;
- [x] materialize defaults before first assignment without changing existing
  workflows when deployment configuration changes;
- [x] add explicit append-only policy changes and unbounded policy;
- [x] add per-workflow concurrency limits without revoking active work;
- [x] identify the governing policy event on every policy-aware assignment;
- [x] fence assignments that cross a policy change in both PM and worker
  projections, then safely reissue them;
- [x] expose policy and concurrency-limit explanations in task/workflow views;
- [x] cover validation, idempotency, policy races, lowering, replay, and restart
  behavior with deterministic regressions;
- [x] pass a live multi-workflow policy and concurrency trial.

Phase 3B — deterministic workflow fairness:

- [x] derive a workflow round-robin cursor from accepted assignment events;
- [x] retain priority and immutable creation order within each workflow;
- [x] record the fairness policy and preceding assignment on every new
  assignment;
- [x] fence and deterministically reissue plans that cross a newer assignment;
- [x] preserve replay of historical assignments and uncorrelated task ordering;
- [x] expose fairness waits and event evidence through operator views;
- [x] cover cross-workflow priority, changing eligibility, stale publication,
  executor round trips, and fresh replay with regressions;
- [x] pass an isolated live multi-workflow fairness and restart trial.

Phase 3C — agent limits and accounted budgets:

- [x] add per-agent concurrency policy beyond worker-advertised capacity;
- [x] define token and cost reservation, reconciliation, and missing-usage
  behavior before enforcing those budgets;
- [x] add token, cost, attempt, and wall-clock budgets as immutable workflow
  policy;
- [x] materialize deployment defaults once so restart configuration changes do
  not silently alter existing workflows or agents;
- [x] identify the governing policy/reservation events in enforcement decisions;
- [x] keep raw model telemetry outside PM replay while deriving compact,
  validated workflow accounting events;
- [x] explain agent limits and budget decisions without hidden scheduler state;
- [x] cover policy races, replay, validation, missing usage, over-reservation,
  and configuration compatibility with deterministic regressions;
- [x] pass an isolated live usage, missing-usage, policy-change, and restart
  trial.

Phase 3C admission hardening:

- [x] reproduce late usage crossing assignment publication and obsolete
  default-policy races;
- [x] use the shared reducer to authorize execution through each assignment's
  event position, including history before worker registration;
- [x] preserve later policy changes as prospective and skip obsolete defaults
  consistently for both agent and workflow policies;
- [x] stop safely when admission history is unavailable;
- [x] cover budget extension recovery, wall-clock publication boundaries,
  replay, and incremental history reads with regressions;
- [x] repeat budget and both policy races over live HTTP/SSE with a running
  worker, then record the evidence.

The v0.10 implementation and hardening checklists are complete, with automated
regressions and isolated live HTTP/SSE evidence. A sustained real-agent soak
remains useful release evidence beyond these correctness trials.

## v0.11 — local scale, retention, and recovery hardening

**Complete for the bounded local scope — 2026-09-13.** Version `0.11.0`
includes phases 1–4 below. Longer-term validation remains open as cross-cutting
work, not a claim of production certification. See [release notes](RELEASE_NOTES.md).

### Phase 1 — measured, rebuildable task lookup (implemented and verified)

- [x] Add an isolated, credential-free benchmark for late/missing task lookup,
  dependency-bearing append, coordination replay, and replay peak memory.
- [x] Record the scan-based baseline before changing storage.
- [x] Maintain a task identity index in the same transaction as event append;
  backfill existing databases without changing events or task identities.
- [x] Provide an atomic local rebuild command. A missing index must be recreated
  at startup; a failed rebuild must leave the previous index intact.
- [x] Test migration, idempotency, concurrent duplicate creation, rollback,
  rebuild equivalence, and indexed query plans.
- [x] Compare before/after measurements and run the full regression suite.

Rechecked 2026-09-13: 246 tests passed, including real process loss during
rebuild and concurrent rebuild/publish checks. See
[measurements and reproduction instructions](benchmarks/RESULTS.md).

Acceptance: task lookup uses primary-key searches instead of parsing task history;
deleting/rebuilding the derived index preserves event bytes and lookup results;
event, counter, and index writes commit or roll back together. Benchmarks report
measurements, not machine-independent timing assertions. This phase does not
claim faster PM or worker admission replay: worker-specific admission benchmarks,
incremental replay, snapshots, and broader DAG/worker workloads belong to phase 2.
Retention and backup/corruption/soak work follow separately. Completing phase 1
does not complete v0.11.

### Phase 2 — replay measurements and opt-in local PM snapshots (implemented and verified)

- [x] Benchmark streamed coordination replay and actual worker admission:
  initial history, incremental history, memory, and rows read, with mixed
  telemetry, multiple workers, and wide/deep task dependencies.
- [x] Add a versioned, checksummed, JSON-only coordination snapshot containing
  the complete reducer state and an exact persisted event cursor.
- [x] Bind snapshots to a database identity, reducer implementation fingerprint,
  and event anchor; missing, incompatible, or damaged caches trigger full replay.
- [x] Replay the suffix from a consistent SQLite read transaction; atomically
  replace the disposable cache without modifying event history.
- [x] Provide local build/delete commands and opt-in PM startup integration;
  verify that the HTTP bus is the same database before using local state.
- [x] Test full-replay equivalence (including subsequent reconciliation), stale
  caches, wrong identity/version, corruption, interruption, and deletion/rebuild.
- [x] Record benchmark evidence, run regression tests, and recheck this list.

Rechecked 2026-09-13: 258 tests passed (one existing dependency deprecation
warning). Wheel build/import checks include the new snapshot module. Package
metadata now identifies this unfinished iteration as `0.11.0.dev0`; v0.11 as a
whole is not yet complete. See [replay evidence](benchmarks/REPLAY_RESULTS.md).
The subsequent phase 3 completed streaming initial worker admission history
without changing independent validation; its incremental read path remains
small. The live local PM restart/snapshot smoke check passed on 2026-09-13:
five demo-worker tasks, valid/deleted/corrupted cache recovery, pending workflow
pause/resume, and zero duplicate assignments/completions. Evidence is recorded
in [replay results](benchmarks/REPLAY_RESULTS.md); broader load/soak and the
remaining v0.11 work are not claimed complete.

Design boundary: snapshots are local performance caches, not event contracts or
external inputs. No pickle or executable serialization. They contain task context
and must receive the same protection as the database. Snapshot loading is opt-in
for the PM; worker admission keeps its independent exact-prefix replay. Worker
snapshot transport is deferred until measurements justify extending that trust
surface. No automatic retention, event deletion, or distributed checkpointing.

### Phase 3 — bounded worker admission (implemented and verified)

- [x] Add a lazy, bounded-page client history iterator with an exact upper cursor.
- [x] Use it for standard worker admission without changing independent validation.
- [x] Reject malformed, unordered, oversized, and incorrectly filtered pages;
  preserve fail-closed behavior when the target assignment is unavailable.
- [x] Test page boundaries, concurrent publication, transport failure, and
  acceptance equivalence with full replay.
- [x] Repeat the memory benchmark and a live worker-restart trial; record evidence.
- [x] Run regressions and recheck this checklist.

Rechecked 2026-09-13: 264 tests passed (one existing dependency warning).
Initial admission peak traced allocations fell from about 115.5 MB to 4.5 MB
on the 201,020-event fixture. A replacement demo worker traversed more than
one page of history and completed four further tasks without duplicates.
See [Phase 3 evidence](benchmarks/REPLAY_RESULTS.md#phase-3--bounded-worker-admission-2026-09-13).

Retained reducer state still grows with tasks/accounting; only the temporary
history list is bounded. Legacy duck-typed clients exposing only `query_all`
remain compatible but cannot receive the memory improvement.

### Phase 4 — conservative retention audit and recovery bundles (implemented and verified)

- [x] Audit artifact reachability across all event payloads, including nested
  custom-event references; verify referenced bytes and report aged orphans.
- [x] Keep retention read-only: no safe cross-process artifact publication fence
  exists yet, so an unreferenced blob is a candidate, not permission to delete.
- [x] Back up SQLite with its backup API, validate database integrity, and include
  every referenced artifact from explicitly selected stores.
- [x] Produce private, checksummed bundles with a completion marker; incomplete
  or corrupted bundles must never be accepted for restore.
- [x] Restore only into a new directory; verify database/artifact bytes and
  rebuild disposable projections without editing original history.
- [x] Test missing/corrupt artifacts, malformed history, symlinks, destination
  collisions, incomplete bundles, and restored replay equivalence.
- [x] Run a disposable recovery drill, document limitations, and recheck tests.

Rechecked 2026-09-13: 276 tests passed (one existing dependency warning), plus
wheel build/import checks. The CLI recovery drill preserved event-derived state
and referenced bytes, rejected corrupt backups and existing destinations, and
never deleted the reported orphan. SQLite bundles are standalone; unexpected
WAL/journal sidecars are rejected. See [recovery operations and evidence](RECOVERY.md).

No automatic deletion, event archival, secure erasure, or repair of corrupted
authoritative history is included. Stop artifact publishers for a full backup;
SQLite copying is transactionally consistent but filesystem blobs cannot be
snapshotted atomically with the log. Long-running soak evidence remains separate.

The focused post-restore live trial passed on 2026-09-13: completed work was not
rerun, interrupted work recovered via lease expiry on a fresh worker, and its
dependent task completed afterward. Original events/artifact bytes were preserved.
See [live recovery evidence](RECOVERY.md#post-restore-live-execution--2026-09-13).

Remaining v0.11 validation, without widening the release into new platform features:

### First sustained local soak — 30-minute run

- [x] Keep one local bus running with two demo workers and repeated five-task
  root/fan-out/fan-in DAGs for at least 30 minutes of workload generation.
- [x] Repeatedly kill an active worker and observe lease expiry and replacement;
  restart the PM using snapshots during ongoing work.
- [x] Exercise workflow pause/resume and cancellation across repeated cycles.
- [x] Compare live state with full replay after each settled cycle; reject duplicate
  completions or assignments whose dependency references are not earlier completions.
- [x] Record events, failure counts, progress, and process RSS samples.
- [x] Drain tasks, verify artifacts, and finish with backup/restore equivalence.
- [x] Inspect the final evidence and mark pass/fail explicitly.

**Passed 2026-09-13:** 179 DAG cycles over 1,807.58 seconds, 939 tasks,
89 worker lease expiries, 59 PM restarts, and 180 replay comparisons.
See [sustained soak evidence](RECOVERY.md#sustained-local-soak--2026-09-13).

This is a bounded demo workload with no model calls. It is the first sustained
soak, not week-long or genuine-agent production evidence. The runner owns and
stops its isolated processes; an unexpected process exit, inconsistent state,
stalled cycle, or process exceeding 512 MiB RSS stops the run with diagnostics.

- gather longer-running soak, crash-injection, and repeated race evidence;
- review practical scale limits with genuine multi-worker/DAG workloads;
- review the completed checklist and operational documentation before promotion.

### Release review checkpoint — 2026-09-13

- [x] Commit the bounded soak evidence (`c839cbb`).
- [x] Re-run the full suite: 276 passed, one existing dependency warning.
- [x] Run a real Hermes two-task DAG with worker loss, lease-expiry recovery,
  downstream result propagation, usage inspection, and artifact verification.
- [x] Review phases 1–4 and document validation limits; correct stale phase-2
  wording that still described the implemented phase 3 as future work.
- [x] Make the final release decision and promote development package metadata.

The focused Hermes trial passed; see
[trial evidence](examples/hermes/TRIAL_NOTES.md#v011-live-hermes-dag-recovery--2026-09-13).
It confirms two real completions after one interrupted attempt, not sustained
real-agent operation. No implementation blocker was found in this checkpoint's
tests and focused trial; this is not a new exhaustive code/security audit.
Week-long genuine-agent operation, broader live race/scale evidence, and the
cross-cutting trials below remain unclaimed. The approved release is `0.11.0`
for this bounded local scope; automatic deletion and archival segments remain
deliberately out of scope. Historical phase checkpoints above describe their
development-time status, not the final release status.

Automatic artifact deletion and archival log segments are consciously deferred:
they require additional ownership/publication guarantees, not just an age flag.
The current retention promise is safe reachability reporting, not automatic GC.

## v0.13 — the logplane name and a src/ layout

- [x] rename the distribution, package, command, environment variables, and
  local config to logplane, with clear failures for leftover agent-bus
  settings;
- [x] move every module into `src/logplane/`;
- [x] keep stored and wire identifiers (`agent-bus` owner, effect salts,
  protocol name, HTTP headers, probe key, backup kinds) unchanged;
- [x] publish `logplane` to PyPI through trusted publishing.

## v0.12 — demonstrations and operator ergonomics

**Released 2026-10-20.** Makes the existing control plane easy to show and to
try, without new coordination semantics. See [release notes](RELEASE_NOTES.md).

- [x] `logplane demo` with `recovery`, `approval`, `budget`, and
  `audit-triage` scenarios using real processes in a throwaway directory, with
  stand-in model usage so token and cost accounting is visible;
- [x] `submit --depends-on` and `demo-worker` outcome flags so every demo path
  is reachable from the CLI;
- [x] prompt `serve` shutdown with open event streams, and a documented exit
  status for startup failure;
- [x] README introduction with one-command demos and an audit-triage guide,
  with reference material split into `docs/`;
- [x] record the README demo animations for `recovery`, `approval`, and
  `budget` (`scripts/record_demo.sh`).

The demos also make the remaining *unaided newcomer trial* gate a short ask:
a newcomer can install, run `logplane demo recovery`, then inspect and
intervene with the normal commands.

## Pre-v1.0 readiness — implementation checkpoints

These checkpoints close the audit's support and usability gaps without adding
new scheduling semantics. No new release number is assigned by this checklist.
The released v0.11.0 scope remains unchanged; this work is not a v1.0 declaration.

### Checkpoint 1 — compatibility and reproducible release gates

- [x] Define public SDK/event/adapter/config/CLI boundaries, upgrade/rollback
  guidance, deprecation intent, and explicit platform limitations in
  [COMPATIBILITY.md](COMPATIBILITY.md).
- [x] Capture public exports and representative executor messages from the
  released v0.11.0 wheel; test continued imports, fields/effect identity,
  message parsing, and representative CLI JSON/exit statuses.
- [x] Configure read-only CI for macOS/Linux, Python 3.10/3.13/3.14, a separate
  direct-dependency-floor lane, regressions, sdist-to-wheel builds, and isolated
  installed-CLI smoke checks. Retain resolved dependencies and diagnostics.
- [x] Add a reusable credential-free installed-wheel check with import-origin
  validation, bounded waits, owned-process cleanup, and live demo execution.
- [x] Correct telemetry/accounting and quick-start verification overclaims;
  add source acquisition, supported-platform, and terminal/shutdown guidance.
- [x] Complete local sdist/wheel and fresh-install validation; recheck the list.
- [x] Observe the GitHub matrix passing after an approved commit/push. All six
  lanes passed for `ac6a73d`; see the remote validation evidence below.

Locally rechecked 2026-09-13: 284 tests passed (one existing dependency warning).
The source archive contains the runner, fixtures, development requirements,
and examples; the wheel built from that archive passed a fresh-venv live demo
on macOS ARM64 / CPython 3.13.5. Installed-module origins were all inside the
new venv. Doctor reported one healthy worker, no pending reconciliation, and
no warnings. Trial-owned processes and the disposable environment were cleaned
up; diagnostics remain in
`outputs/agent-bus-trials/readiness-checkpoint1-final` in the Codex task workspace.
No source runtime semantics, version, release tag, or live user state changed.
Linux, other Python versions, and the direct-dependency-floor lane await CI.

### Checkpoint 2 — bounded adapter checks

- [x] Bound CLI target import/construction, probe execution, and cleanup with
  actionable timeout errors; document the synchronous library helper separately.
- [x] Test hangs and child-process cleanup. Do not describe timeout isolation
  as a security sandbox or a proof of side-effect safety.
- [x] Recheck the full suite and installed-wheel success/timeout/live-workflow
  probes; record the validation evidence before closing this checkpoint.

The implemented CLI default is 30 seconds, configurable with `--timeout`.
Target loading and probing occur in a fresh process group; handled interruption
and normal completion also clean up same-group children. Library
`check_executor()` remains synchronous/untimed. Detached processes, remote
effects, and parent SIGKILL/host loss are explicitly outside this cleanup
guarantee. No new Windows support or sandbox claim is introduced.

Locally rechecked 2026-09-13: 298 tests passed, with the same existing dependency
warning. Regressions cover blocking config reads, import/constructor/execute/
close/exit hangs, abrupt process exit, large/noisy/malformed results, same-group
child cleanup, SIGINT/SIGTERM, and normal Python/CLI/loopback-HTTP adapters.
The sdist-built wheel passed an isolated installation, normal and timed-out CLI
probes, and a live demo task. Evidence:
`outputs/agent-bus-trials/readiness-checkpoint2-verified` in the Codex task
workspace. Temporary runtime processes/environments were cleaned up. These
are macOS/CPython 3.13 local results; the remote platform matrix remains pending.

### Checkpoint 3 — everyday operator actions

- [x] Add read-only task/workflow discovery and pending human-decision views.
- [x] Add cancel/retry/decision commands over existing intent events, preserving
  validation, causal links, idempotency, and pending-versus-acknowledged status.
- [x] Test stale targets and control races, then trial the packaged intervention
  workflow and give waiting/failed states a concrete next action.

`tasks`, `workflows`, and `decisions` replay a fixed event prefix in bounded
pages; the resulting projection still grows with history. `cancel`, `retry`,
and `decide` publish existing events, not mutable board state. Retry and human
response require the exact failure/question event ID. Identical command retries
reuse stable intent keys, and ordered replay verifies acceptance after publish;
a recorded request is not a promise of completion or a PM acknowledgement.

Locally rechecked 2026-09-13: 314 tests passed, with one existing dependency
deprecation warning. Regressions cover pagination, changed intent, stale replies,
newer questions, and completion/cancellation/supersession/deadline publication
races. The sdist-built wheel passed a fresh-install live trial: discover a
blocked task, answer it and verify the answer reaches execution, revive a
failed task, and cancel unassigned work. All three repeated commands returned
their original events without appending another intent; stale replies/retries
with new keys were refused. Evidence:
`outputs/agent-bus-trials/readiness-checkpoint3-final` in the Codex task workspace.
This was a credential-free reference-executor trial, not a new Hermes trial or
an unaided newcomer trial. Temporary processes/environments were cleaned up.
The remote platform matrix, commit/push, and remaining release gates are pending;
no version bump or release claim is implied.

### First remote readiness run — 2026-09-13

Checkpoint commit `644c043` was pushed and tested by
[GitHub run 34752538760](https://github.com/dekizk/agent-bus/actions/runs/34752538760).
Five lanes passed their regressions, sdist/wheel build, and installed live
operator trial: Ubuntu Python 3.10 (latest and direct dependency floors),
3.13, and 3.14; macOS Python 3.14. macOS Python 3.13 failed the configured CLI
timeout cleanup test when the second group signal raised `PermissionError`.
The matrix gate remains open until a subsequent complete run passes.

The follow-up handles Darwin's zombie-only process-group race with a bounded
process-state check, rather than ignoring permission errors. The leader must
have exited and all remaining group members must be zombies (or absent).
Live members, failed/timed-out inspection, and malformed output remain errors.
This does not expand the process-group or external-side-effect guarantees.
Apple's [kernel implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sig.c)
filters zombie group members before computing the signal result; the initial
CI traceback is consistent with that race, but did not capture process states.
Local validation: 317 tests passed (one existing dependency warning), including
deterministic denial cases at both TERM and KILL, zombie/absent groups, live
members, and unavailable/malformed/timed-out inspection. A fresh sdist-built
wheel passed the adapter success/timeout probes and live operator workflow.
Evidence: `outputs/agent-bus-trials/readiness-darwin-cleanup-fix` in the Codex
task workspace.

Remote recheck: fix commit `ac6a73d` passed all six lanes in
[GitHub run 34753501956](https://github.com/dekizk/agent-bus/actions/runs/34753501956):
Ubuntu Python 3.10 (latest and direct dependency floors), 3.13, and 3.14;
macOS Python 3.13 and 3.14. Every lane passed regressions, the sdist/wheel
build, and the fresh-installed adapter and live operator workflow checks.
Downloaded reports are retained in
`outputs/agent-bus-trials/readiness-ci-34753501956` in the Codex task workspace.
This closes the remote matrix gate for these checkpoints. Earlier pending
statements above are historical checkpoint observations; this result supersedes
them. No new version, native Windows support, or v1.0 completion is claimed.

### Checkpoint 4 — released upgrade baseline

- [x] Capture an actual v0.11.0 database using the pinned released bus/PM,
  with source hashes and repeatable, synthetic fixture generation.
- [x] Preserve raw event values, database identity, idempotency and task IDs
  through initialization; verify snapshot fallback and index reconstruction.
- [x] Continue the released DAG, human decision, retry, pause/resume,
  pending cancellation, lease-expiry and deadline paths under current code.
- [x] Verify persisted policy/missing-usage reservations survive changed process
  defaults, and explicit new policy events can admit more work.
- [x] Compare twelve released CLI cases, all four outcome forms, assignment
  messages/effect IDs and cancellation messages without freezing human wording.
- [x] Verify the source archive and fresh-installed wheel, then record results.

Scope is v0.11.0 to current development; this checkpoint introduces no migration
or new release. Fixtures contain eight tasks and 26 events, plus an older
snapshot with a replay suffix. The SQL dump comes from SQLite, not hand-written
schema guesses; raw database/WAL file compatibility is not implied. Old snapshots
may be discarded safely instead of preserving private implementation formats.

Locally rechecked 2026-09-13: 324 tests passed (one existing dependency warning).
A second capture from the pinned release reproduced the SQL and expected JSON
byte-for-byte. The source archive included the fixtures and standalone tests;
its runner verified a fresh-installed wheel outside the checkout, including all
seven released-upgrade tests, bounded adapter probes, and the live operator
workflow. Evidence: `outputs/agent-bus-trials/readiness-released-upgrade` in the
Codex task workspace. Disposable test databases/processes were cleaned up; no
live user bus data was touched.

Remote recheck: checkpoint `74603fe` was reviewed, committed and pushed;
[GitHub run 34754905683](https://github.com/dekizk/agent-bus/actions/runs/34754905683)
passed all six configurations: Ubuntu Python 3.10 (latest and direct dependency
floors), 3.13 and 3.14; macOS Python 3.13 and 3.14. Every lane passed the full
regression suite, source/wheel build, and clean-installed upgrade plus live
operator checks. Reports are retained in
`outputs/agent-bus-trials/readiness-ci-34754905683` in the Codex task workspace.
Checkpoint 4 is complete for its stated v0.11.0 baseline; the support-window
decision and remaining v1.0 gates below are not implied complete.

### Owner-approved license and upgrade scope — 2026-09-13

- [x] Select MIT licensing with `deki` as copyright holder; credit GPT Sol,
  Fable 5.1, and GPT-6 Astra separately in development acknowledgements.
- [x] Add license/author metadata and include the license in distributions.
- [x] Select v0.11.0 as the oldest supported direct-upgrade source for v1.0;
  do not imply coverage of older or untested intervening releases.
- [x] Verify distribution contents, run regressions and recheck this list.

GitHub source installations remain the trial path. PyPI publication and the
v1.0 release itself are not authorized or implied by these decisions.

Locally rechecked: 324 tests passed (one existing dependency warning). The
source archive contains LICENSE and CONTRIBUTORS.md; the installed wheel
declares MIT, lists deki as author, and includes a byte-identical copyright/
license notice. Its upgrade and live operator checks also passed. Evidence:
`outputs/agent-bus-trials/readiness-mit-license` in the Codex task workspace.
Build tooling now requires setuptools 77.0.3+ for standard license metadata;
runtime requirements and the version are unchanged. These local changes await
review/commit and remote CI; no release tag or package publication was changed.

### Remaining v1.0 gates

- [x] Obtain the owner's upgrade-scope and license decisions, recorded above.
- [ ] Rerun the v0.11.0 baseline against the final v1.0 release candidate;
  add fixtures before promising any further direct-upgrade source versions.
- [ ] Record an unaided newcomer install/connect/inspect/intervene trial.
- [ ] Verify another genuine agent integration through the public contract.
- [ ] Gather longer genuine-agent evidence with an agreed workload, authority,
  duration, and spend cap; keep missing usage and external effects explicit.
- [ ] Recheck the release requirements below before promoting to v1.0.

## v1.0 — stable local agent control plane

v1.0 should make a clear, supportable promise: one local event-driven control
plane can reliably coordinate heterogeneous agents without a mutable board as
its source of truth.

Release requirements:

- stable event-envelope, executor, adapter, and CLI contracts;
- a documented compatibility and deprecation policy;
- tested database upgrades and migrations;
- packaged CLI and service entry points;
- configuration validation and useful diagnostics;
- documented backup and recovery guarantees;
- multiple live agent integrations;
- fault-injection and long-running reliability evidence;
- clear trust, security, and deployment boundaries;
- complete quick-start, integration, operations, and troubleshooting guides.

## Post-v1.0 — distributed operation

Distribution should come after local semantics and user experience are stable:

- authenticated producer and actor identities;
- per-actor authorization;
- shared PM leadership and leader election;
- multi-process-safe notifications;
- multi-host workers and leases;
- shared artifact storage;
- PostgreSQL or another durable shared event store;
- clock-skew handling, distributed tracing, and high availability.

This phase must preserve the same replayable semantics rather than replacing
them with hidden broker or scheduler state.

## Cross-cutting trials

Continue running these alongside feature development:

- telemetry under hard worker loss and lease-expiry recovery;
- week-long operation with at least one genuine existing-agent workflow;
- wide-DAG and large-result pressure tests;
- repeated completion/cancellation/deadline races;
- artifact growth and retention measurements;
- shadow and canary adoption from a real external task source;
- clean-environment onboarding by someone unfamiliar with the codebase.

Record concrete evidence and friction in the relevant integration trial notes.

## Usability release gate

Every version should answer these questions before it is declared complete:

- Can a new user discover and install it?
- Can they connect an existing agent instead of rewriting it?
- Can they understand current work without reading raw events?
- Can they tell exactly why a task is waiting or terminal?
- Can they adopt it without creating two owners for one task?
- Are failure messages actionable?
- Are examples safe, disposable, and copyable?
- Does every operational view remain a projection over immutable history?

If a feature is powerful but makes those answers worse, it is not ready.

## Features deliberately not implied by this roadmap

- A mutable kanban database as a second source of truth
- Editable dependency edges — new intent means new tasks, not modified history
- Agents editing their own status cards
- Agent-specific orchestration logic in the core
- A second scheduler hidden inside an adapter
- Full prompts, outputs, or tool payloads stored inline in SQLite by default
- Telemetry events participating in PM coordination replay
- Distributed complexity before local operation is dependable and approachable
