# Release notes

## Unreleased

- **Event validation moves out of the bus and into one function per topic.**
  Envelope and payload rules now live in `event_validation.py`, with no
  database or HTTP dependencies; reference checks against stored history and
  HTTP error handling stay in `bus.py`. The 559-line `validate_event` is now a
  short dispatcher over per-topic validators, and a test fails if a built-in
  topic has no validator. Accepted payloads, validation order, and error
  messages are unchanged: an old-versus-new comparison over 68,251 recorded and
  mutated events found no difference. `logplane.bus` still exposes
  `validate_event` and `EventValidationError`; limits and topic constants
  should be imported from `logplane.limits` and `logplane.topics`.

## v0.14.1 — 2026-10-26

Hardening from an external review of v0.14.0. No event, executor-wire,
database, or backup format changes.

- **Startup checks identify this launch's bus.** `logplane up` gives its bus
  child a fresh readiness marker and checks the health-response header before
  launching the coordinator. Another listener taking the port after the early
  check can no longer be mistaken for the new bus while its child is starting.
- **Source review stops when Git filtering fails.** Missing Git, timeouts,
  ownership errors, and broken checkouts no longer silently disable ignore
  rules. Only a confirmed non-checkout uses directory filtering alone.
- **External-send consent is explicit.** The Hermes code-review preview now
  defaults to **no**, so pressing Enter declines where it used to approve.
  Piped/redirected or non-interactive runs stop unless `--yes` is passed;
  automatic answers in simulated demos do not authorize paid source uploads.
- **Shared worker and demo setup.** Both demo-worker entry points use the same
  flag validation and runtime wiring. Scripted demos and Hermes fleet examples
  share directory setup, cleanup, and reopening instructions. The standalone
  worker's environment-only launch remains supported.

## v0.14.0 — 2026-10-24

Easier first runs. No event, executor-wire, database, or backup format
changes.

### Changes

- **`logplane up` runs the bus and the coordinator together.** The quick
  start drops from four terminals to three: `logplane up`, a worker, and a
  terminal for commands. `up` starts the bus, waits until it is healthy,
  starts the PM, writes both logs to `.logplane/logs/`, and shows the PM's
  reconciliation lines. Ctrl-C or `SIGTERM` stops the PM and then the bus; if
  either exits on its own, `up` stops the other and exits with status 1. It
  refuses to start when a PM is already running for that bus. `serve` and `pm`
  are unchanged for running the two separately.
- **`logplane up` refuses a port something else is already answering on**,
  rather than starting the coordinator against whatever is there, and a reply
  only counts as the new bus if the child process is still running.
- **The code review example previews what it will send and skips what it
  should not.** Hidden, ignored (`.gitignore`), dependency, build, lock, and
  secret-looking files are never sent to the model, and the run lists every
  chunk and asks before the first paid call.
- **`logplane doctor` says whether a coordinator is running.** For a local bus
  it checks the PM's lock on this machine and reports `running` with its
  process ID, or `not running` with the command to start one. Before, a task
  with no PM to assign it just stayed `open`. The JSON output gains a `pm`
  object (`status`, `detail`); existing fields are unchanged.

## v0.13.3 — 2026-10-23

Fixes for bugs found when the [code review example](examples/hermes/README.md#review-a-codebase-with-a-fleet-of-reviewers)
reviewed logplane's own source with real models. No event, executor-wire,
database, or backup format changes.

### Fixes

- **An empty `LOGPLANE_TOKEN` no longer means "no auth".** A set but empty or
  blank value, usually an unexpanded variable, now stops the bus with a clear
  message instead of starting it unauthenticated. Unset the variable to run
  without a token.
- **A non-ASCII `Authorization` header gets a 401, not a 500.** The token
  comparison now works on bytes.
- **Changing a reducer's default argument values invalidates PM snapshots.**
  The snapshot fingerprint hashed function code but not default values, so a
  changed default could replay an old snapshot under new behavior. Existing
  snapshots are discarded once after upgrading and rebuilt from the log.
- **Snapshots containing NaN or Infinity are rejected** as corrupt, matching
  the encoder, which never writes them.
- **The PM lock checks that its file was not replaced while locking.** If the
  lock file is deleted and recreated between opening and locking it, the PM
  now stops instead of holding a lock another PM could also take.
- **Telemetry and usage idempotency keys include the assignment.** An adapter
  that reuses invocation or tool-call ids across attempts (numbering calls
  from 1 each time, say) no longer has a later attempt's telemetry and usage
  silently dropped as duplicates. Keys for events already in the log are
  unchanged.

`BusClient.pause_task` and `resume_task` keep a unique default key per call,
because a task can be paused and resumed more than once; their docstrings now
say to pass `idempotency_key` when retrying the same request.

## v0.13.2 — 2026-10-22

Hardening from an external review. No event, executor-wire, database, or
backup format changes.

### Fixes

- **No silent fallback to another bus during migration.** When only
  `agent-bus.local.json` exists, read commands and `cancel`, `retry`, and
  `decide` now stop with the rename message instead of falling back to
  `LOGPLANE_URL` or the default port, which could reach a different bus. An
  explicit `--url` still works. `logplane init` refuses to create a fresh
  config next to an old one, which would have started an empty history.
- **Demo effects are enforced by the receiver.** The approval and
  audit-triage demos used to check their ledger and then write to it as two
  steps, so two overlapping attempts of one task could both perform the
  effect. Their simulated payments system and contest platform now accept each
  `effect_id` once in a single atomic SQLite write (`refunds.db`,
  `contest-inbox.db`), and a new test runs overlapping attempts.
- **Wording matches the guarantees.** `effect_id()` gives a stable key; the
  system you call must enforce it as an idempotency key. The docs no longer
  suggest that checking before writing is equivalent. Budgets are described as
  admission control rather than a billing cutoff, and every demo says that its
  agents' outputs, token counts, and costs are simulated.
- **No false lease expiry when the PM falls behind.** The PM applies events one
  at a time and checks leases against the clock. On a busy host it could read
  an old heartbeat, expire a worker that was still alive, and hand its task to
  another attempt while the first kept running. Before expiring a lease, the
  PM now reads to the head of the log and expires only if the lease is still
  stale. This surfaced as an intermittent CI failure in the audit-triage demo.
- **Releases are validated before upload.** The publishing workflow now runs
  the regression suite and the installed-wheel check against the exact files it
  uploads, on Linux and macOS and with minimum dependencies, and publishes only
  if all of them pass.

## v0.13.1 — 2026-10-21

Fixes the project page on PyPI. No code changes.

- README images and links use absolute GitHub URLs, so the demo animations
  and documentation links work on pypi.org as well as on GitHub. A test keeps
  relative links out of the README.
- The PyPI description now shows the `pip install logplane` instructions;
  v0.13.0's still said to install from a clone.

## v0.13.0 — 2026-10-21

agent-bus is now **logplane**, and the code moves to a `src/` layout. This is
a clean break in names only: no event, executor-wire, database, or backup
format changes, and existing history replays unchanged.

### Changes

- The PyPI distribution, import package, and command are `logplane`.
  `pip install logplane`, `import logplane`, and
  `logplane demo recovery` replace their agent-bus equivalents.
- All implementation modules live in `src/logplane/` and import as
  `logplane.<module>` (for example `logplane.bus`, `logplane.cli`). The flat
  top-level modules and the `agent_bus` package are removed, with no
  compatibility shims.
- `python -m logplane` runs the CLI.
- Environment variables use the `LOGPLANE_` prefix (`LOGPLANE_TOKEN`,
  `LOGPLANE_URL`, `LOGPLANE_DB_PATH`, and the rest). `logplane init` writes
  `logplane.local.json` and keeps runtime data in `.logplane/`.
- Leftover `AGENT_BUS_*` environment variables stop the CLI, bus, PM, and demo
  worker with a message naming each replacement, rather than being ignored. An
  ignored `AGENT_BUS_TOKEN` would otherwise start an unauthenticated bus.
- When `logplane.local.json` is missing but `agent-bus.local.json` exists, the
  error says to rename the file.

### Unchanged on purpose

These identifiers are stored in event history or shared with external
adapters, so they keep their original spelling:

- the task ownership owner `agent-bus`;
- the effect-key salts behind `assignment.effect_scope` and `effect_id()`, so
  a retried task never repeats an external effect that already happened;
- the executor protocol name `agent-bus.executor` and the
  `X-Agent-Bus-Assignment-Id`, `X-Agent-Bus-Effect-Scope`, and
  `X-Agent-Bus-Protocol-Version` HTTP headers;
- the `agent_bus_conformance_probe` context key that adapters check to skip
  side effects during `adapter check`;
- the `agent-bus-backup` and `agent-bus-restore` bundle kinds, so earlier
  backups still verify and restore.

### Migrating from agent-bus

1. Stop the bus, PM, and workers.
2. Reinstall under the new name: `python -m pip uninstall agent-bus`, then
   `python -m pip install logplane` (or `python -m pip install -e .` in an
   updated checkout).
3. Rename `agent-bus.local.json` to `logplane.local.json`. Its
   `database_path` can keep pointing at `.agent-bus/events.db`; to tidy up,
   move that directory to `.logplane/` and update the path.
4. Rename environment variables from `AGENT_BUS_*` to `LOGPLANE_*` and unset
   the old ones, including in shell profiles, service files, and CI.
5. Replace `agent-bus` with `logplane` in scripts that run the command, and
   `agent_bus` with `logplane` in Python imports. Code that imported flat
   modules such as `bus` or `client` should use the public `logplane`
   package, or `logplane.bus` and similar for internals.
6. Start the bus, PM, and workers again and check `logplane doctor`.

External CLI and HTTP agents need no changes.

## v0.12.0 — 2026-10-20

Demonstrations and everyday operator ergonomics. No event, executor,
configuration, or database changes.

### Changes

- `agent-bus demo <scenario>` plays a scripted, narrated scenario against a
  real bus, PM, and workers started as separate processes in a fresh
  directory on a free port. It never touches existing history or a running
  bus, and crashes are genuine `SIGKILL`s. Scenarios: `recovery` (worker and
  PM killed mid-task), `approval` (an agent asks before an irreversible tool
  call and makes it at most once), `budget` (a workflow cost cap stops new
  work, and raising it resumes), and `audit-triage` (fan-out reviewers, a
  crashed reviewer, a human severity call, and exactly-once submission via
  `assignment.effect_id`). Stand-in agents report model usage through the
  normal telemetry sink, so every demo shows token and cost accounting, and
  the live event tail shows usage and policy events. Options: `--pace`, `--yes`, `--dir`,
  `--keep-running`, and `--read-pauses`, which holds each narration long
  enough to read for recordings and talks. The history is kept for
  inspection afterwards.
- `agent-bus submit --depends-on TASK_ID` (repeatable) creates DAG edges
  without composing raw events.
- `agent-bus demo-worker` gains `--block`, `--fail`, `--fail-permanently`,
  and `--delay`, matching the standalone `worker.py`.
- `agent-bus serve` stops promptly. Event streams now notice a departed
  client or a stop signal within about a second instead of up to 15 seconds,
  so Ctrl-C no longer waits, or hangs until a second Ctrl-C, while the PM or
  workers are connected. Idle keepalives and history re-reads keep their
  15-second cadence, and clients reconnect with their normal backoff.
- `agent-bus serve` now exits with status 2 and a short message when it
  cannot start, for example when the port is in use. It previously exited
  with uvicorn's status (1 or 3, depending on the uvicorn version); 3
  contradicted the documented "not found" meaning.
- The README is now a short introduction: an animation of
  `agent-bus demo recovery` (with `approval` and `budget` animations one click
  away), the one-command demos, install, a first task, a
  command cheat sheet, and links. Reference material moved, largely verbatim,
  to `docs/architecture.md`, `docs/operations.md`, `docs/integration.md`,
  `docs/events.md`, and `RECOVERY.md`, rewritten as current behaviour rather
  than per-version history. `scripts/record_demo.sh` re-records the animation
  with asciinema and agg.
  [`examples/audit_triage/`](examples/audit_triage/README.md) explains the
  audit pipeline and how to replace its stand-in agents with real ones.

### Validation

- 358 regression tests passed on the latest dependencies and on the declared
  minimums (one existing dependency deprecation warning). They include live
  runs of all four demo scenarios and a real `agent-bus serve` stopped with
  Ctrl-C while a client stream is open.
- Release wheel built from a clean checkout and verified by
  `scripts/check_release.py`.
- GitHub CI matrix passed for the merged demo work (PR #1).
- Live trials: all four demos on Linux and `audit-triage` on macOS, with
  every process log free of tracebacks; the bus stopped and restarted under a
  connected PM and worker, both of which resumed and completed new work.

### Upgrade and operating limits

Follow the normal [upgrade steps](COMPATIBILITY.md#upgrade-downgrade-and-deployment-versions).
Nothing is migrated, and the v0.11.0 released-database baseline continues to
pass against this release. Editable installs must be reinstalled
(`python -m pip install -e .`) so the new `demo` module is importable.

Demo agents are deterministic stand-ins with canned results; they show
coordination behaviour, not model quality. Demos use POSIX signals. The
v0.11.0 operating limits below still apply.

## v0.11.0 — 2026-09-13

Local scale and recovery hardening, preserving immutable event history and
framework-neutral agent integration.

### Changes

- Transactionally maintained, rebuildable task identity index avoids scanning
  task history for identity and dependency lookup.
- Opt-in local PM snapshots accelerate startup. Checksums, database identity,
  event anchors, and reducer fingerprints protect cache validity; invalid
  caches fall back to replay without changing authoritative history.
- Bounded-page worker admission reduces temporary history memory while
  retaining independent exact-prefix validation.
- Read-only artifact reachability/integrity audits and verified backup/restore
  bundles provide conservative local recovery tooling.

### Validation

- 276 regression tests passed (one existing dependency deprecation warning).
- Release wheel built and installed into a separate environment outside the
  checkout; version, CLI initialization, recovery/snapshot help, and core module
  import origins verified. Dependencies were reused from the host environment;
  this is not a fresh-machine dependency-resolution test.
- Benchmarks cover task lookup, replay, and worker admission memory;
  measurements are machine/workload specific, not performance guarantees.
- Live snapshot/restart and post-restore DAG checks passed.
- A 30-minute demo soak processed 939 tasks, recovered from 89 worker kills
  and 59 PM restarts, and passed 180 replay comparisons plus backup/restore.
- A real Hermes DAG recovered after worker loss and propagated its upstream
  result correctly. Missing usage for the interrupted attempt remained explicit.

Evidence and reproduction details: [recovery](RECOVERY.md),
[lookup benchmarks](benchmarks/RESULTS.md),
[replay benchmarks](benchmarks/REPLAY_RESULTS.md), and
[Hermes trial notes](examples/hermes/TRIAL_NOTES.md).

### Upgrade and operating limits

Stop writers and artifact publishers and preserve a backup before upgrading.
The task identity index is derived and backfilled without rewriting events.
PM snapshots remain opt-in (`agent-bus pm --snapshot`); they are disposable
caches, not portable authoritative history. See [recovery operations](RECOVERY.md)
for backup verification and restoring into a new directory.

This is a trusted local deployment release, not distributed coordination or
production certification. Week-long genuine-agent operation and broader live
race/scale tests remain open. Retained reducer state still grows with history.
There is no automatic artifact deletion, archival segmentation, secure erasure,
or repair of corrupted authoritative events. Artifact publication is not atomic
with database backup: quiesce publishers for a complete bundle. External effects
still require their own idempotency safeguards. Hard worker loss can leave
model spans open and usage unknown; reported cost is then incomplete.
