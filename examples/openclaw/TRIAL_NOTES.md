# OpenClaw trial notes

## 2026-11-04 — one tool-free Logplane assignment

Result: **the real OpenClaw task completed successfully on one attempt**.
This was not yet the mixed Hermes–OpenClaw DAG trial.

Setup:

- OpenClaw `2026.9.6 (eb377ac)`, installed with its own Node 24 runtime.
- `agent exec`, explicit config, ephemeral session, all tools denied, plugins disabled.
- Model `openai/gpt-6-luna`, high thinking; custom provider name `nousportal`.
- Hermes' loopback-only subscription proxy provided Nous Portal authentication.
  No Hermes agent turn ran; OpenClaw performed the assignment.
- Separate live HTTP bus and PM; real WorkerRuntime registration, heartbeat,
  capability matching, and completion; capacity one and zero task retries.

Workflow: `49547528e37f42a094eafcefe5363a7f`; task 1,
assignment `task:1:attempt:1`, worker instance
`d74f27eb49a445828e891faca258753c`.

| Event | Topic | Caused by |
|---|---|---|
| 3 | task.created | none |
| 5 | task.assigned | 3 |
| 6 | task.started | 5 |
| 12 | task.completed | 5 |

Completion summary: `OpenClaw worker test passed`.
Result: `{"value":42}`. Full replay independently yields `completed`.
Start-to-completion time from log timestamps: approximately 10.31 seconds.

OpenClaw reported one assistant turn and **2,348 total tokens**:
input 3, output 38, cache read 0, cache write 2,307; reasoning tokens 12
were separately reported and are not added again to the total. Reported cost:
**$0.000307675 USD**, marked `provider-billed` in its usage metadata. These are
agent-reported figures, not an independently reconciled invoice or Logplane
telemetry/accounting events.

### Trial-runner correction

The first runner exited with a failed verification after the successful task:
it required `toolSummary.calls == 0`, but OpenClaw omitted that optional field.
The original `summary.json` retains `passed: false`; it has not been rewritten
to hide this. The corrected verifier accepts an absent optional count without
calling it a measured zero, and still rejects a positive count. The pinned
OpenClaw config was validated before the model call and denied every tool.

Rechecking the saved event log with the corrected verifier passed. **No second
model invocation was made.** A regression test covers omitted, zero, and
positive tool counts, plus an incorrect task result. The corrected runner has
not yet had a second end-to-end paid run; its verifier was exercised on this
live evidence offline.

The evidence is kept locally, outside the repository. It contains the disposable database, `events.json`, original `summary.json`,
`verification.json`, and local service logs. The trial's services and the
temporary subscription proxy were stopped afterward.

### What remained unproven after the first smoke test

- A real dependency handoff from Hermes to OpenClaw.
- Live cancellation, worker-crash recovery, and multiple sustained assignments
  with this adapter (timeout/cancellation have credential-free process tests).
- OpenClaw usage emission into Logplane telemetry and budget accounting.
- Tool-enabled file editing or external effects; those require explicit new
  authority and a separate integration trial.

### Implementation checklist rechecked

- [x] Optional example adapter and worker launcher; no changes to core modules.
- [x] Ten credential-free adapter/verifier tests, including timeout,
  cancellation, oversized output, malformed outcomes, and missing tool metadata.
- [x] One real Nous-backed OpenClaw task through the live bus and PM.
- [x] Correct result and terminal state verified by replay; evidence retained.
- [x] Temporary services stopped and their listening ports confirmed closed.
- [x] Full regression suite: **417 passed**, one existing dependency warning.
- [x] Pinned Ruff and core-module mypy checks pass.

Work is on `feat/openclaw-worker-trial`; committing and pushing are separate steps.

## 2026-11-04 — real Hermes -> OpenClaw dependency handoff

Result: **passed all 13 live acceptance checks**, first attempt for each task.
Both agents used GPT-6 Luna/high through Nous Portal. Hermes ran its own agent
turn directly; OpenClaw used the subscription proxy only for inference/auth.
No core Logplane modules changed.

Workflow: `4b658dfb261d4b43b6d94cc81533f82a`.
Both tasks were declared before execution: task 1 at event 1, task 2 at event 3
with immutable `depends_on: [1]`. OpenClaw registered first, and the operator
projection explicitly reported `dependencies_incomplete`: "Waiting for
prerequisite task(s) 1 to complete." Task 2 had no assignment and attempt zero.

| Step | Task | Event | Caused by |
|---|---|---|---|
| Hermes assigned | 1 | 8 | 1 |
| Hermes started | 1 | 9 | 8 |
| Hermes completed | 1 | 24 | 8 |
| OpenClaw assigned | 2 | 25 | 24 |
| OpenClaw started | 2 | 26 | 25 |
| OpenClaw completed | 2 | 43 | 25 |

Hermes returned the fresh marker `32e954cb30e644a6ac4cfa6f20b4c5aa`
and numbers `[17, 25, 9]`. OpenClaw returned:

```json
{
  "handoff_marker": "32e954cb30e644a6ac4cfa6f20b4c5aa",
  "total": 51,
  "source_task_id": 1,
  "source_completion_event_id": 24
}
```

Task 2's creation and assignment contexts were both empty. Its assignment
contained only the dependency reference `{task_id: 1, completion_event_id: 24}`;
the captured input at the executor boundary contained the correctly resolved
result and summary. No operator copied the producer result into the child.
The two tasks share one correlation, went to the intended agents, and replay
as completed. There were no retries, expiries, or failure events.

Start-to-completion times from event timestamps: Hermes approximately 14.24 s;
OpenClaw approximately 16.33 s. End-to-end from task 1 creation to task 2
completion: approximately 31.24 s.

### Usage and limits of this evidence

| Agent | Reported total tokens | Cost USD | Cost provenance |
|---|---|---|---|
| Hermes | 1,528 | 0.000221675 | estimated, `provider_models_api` |
| OpenClaw | 2,527 | 0.000345050 | reported, `provider-billed` |

Combined reported/estimated cost: **$0.000566725**; this is not an independently
reconciled invoice. Hermes reported one API call and OpenClaw one assistant
turn. Cache and reasoning fields use different accounting conventions and were
not added again to the reported totals. OpenClaw again omitted its optional
tool count; no measured zero is claimed.

This example did not attach telemetry sinks: usage is in `summary.json`, not
Logplane's telemetry/accounting stream. The workflow correctly reports two
missing cost/token reports. Its zero accounted charges are not actual zero
model cost. Budget enforcement is therefore not a claim of this trial.

Hermes had safe mode and only the clarification toolset; OpenClaw had all
tools denied and plugins disabled. No file-editing or shell tool authority was
granted. The two WorkerRuntime instances ran in separate threads in the trial
process, with separate agent subprocesses; independent worker process recovery
was not exercised.

The evidence is kept locally, outside the repository. It includes events, actual executor inputs (`deliveries.json`), summary with all
13 check results, waiting/final operator views, and local service logs. The
temporary bus, PM, workers, and subscription proxy were stopped afterward.

### Next boundary

Cross-framework result handoff is now demonstrated. Real cancellation/recovery,
OpenClaw telemetry/accounting, and tool-enabled editing remain separate trials.
The next useful functional test is a bounded real workload; do not infer a
production integration guarantee from this small, successful DAG.

### Mixed-trial checklist rechecked

- [x] Submit both tasks before execution and observe the consumer waiting.
- [x] Run real Hermes and OpenClaw assignments with narrow tool authority.
- [x] Verify referenced result delivery, correct output, causal links, and replay.
- [x] Save results and usage with their provenance and limitations.
- [x] Stop temporary services and confirm both listening ports are closed.
- [x] Five new offline handoff-verifier tests; full suite **422 passed** with
  one existing dependency warning; Ruff and configured mypy checks pass.

Still on `feat/openclaw-worker-trial`; not committed or pushed by this trial.

## 2026-11-04 — interruption trial: a crashed worker and a frozen one

Result: **passed all 13 checks**. Both agents used GPT-6 Luna/high through
Nous Portal; every worker ran as its own process; lease 10 s, heartbeat 2.5 s.
No core Logplane modules changed.

| Time | Event | What happened |
|---|---|---|
| +0.7 s | #8, #9 | Hermes assigned and started task 1 |
| +9.3 s | #19, #20 | Hermes completed; task 2 attempt 1 assigned to `openclaw-a` |
| +11.6 s | — | `openclaw-a` killed (`kill -9`) with its 1 OpenClaw process |
| +20.9 s | #28 | Attempt 1 expired: worker lease expired |
| +21.1 s | #31 | Attempt 2 assigned to `openclaw-b` |
| +23.4 s | — | `openclaw-b` frozen (`SIGSTOP`); its OpenClaw process kept running |
| +33.6 s | #38 | Attempt 2 expired: worker lease expired |
| +34.0 s | #41 | Attempt 3 assigned to `openclaw-c` |
| +42.4 s | #49 | `openclaw-c` completed; this is the accepted result |
| +42.6 s | — | `openclaw-b` resumed; its adapter returned a **completed** result |
| +47.8 s | #57 | Probe: attempt 2's completion sent as `openclaw-b` |

The frozen worker held a genuine late result. Its OpenClaw call finished while
the worker was frozen, and the adapter returned a correct completed outcome 9.04
seconds after Logplane had expired that attempt. The worker runtime discarded
it and published nothing. (In stand-in runs the runtime sometimes cancels the
adapter first instead; both are correct.) The only attempt-2 completion in the
log is the labelled probe, #57, which replay ignored. `logplane doctor`
reported 1 stale worker (the killed one) and "1 historical event(s) were safely
ignored as stale or invalid".

All three attempts received the same resolved dependency, from completion
event #19, with an empty context. The accepted result was:

```json
{
  "handoff_marker": "df0f2cc4416548dcaaae62f7cb8c6f63",
  "total": 51,
  "source_task_id": 1,
  "source_completion_event_id": 19
}
```

The task view shows 3 attempts, 2 retryable failures (the two expiries), 0
retries remaining, and the last attempt on `openclaw-c`. There were no other
failure events, no tracebacks in any service log, and every agent workspace was
empty afterwards.

Usage: Hermes reported 1,447 input and 75 output tokens, estimated at
$0.0002183. OpenClaw's usage was not captured in this run: the worker record
did not yet save OpenClaw's report. It does now, under `openclaw_report` in
`records/`. Three OpenClaw calls were made; the first was cut off by the crash.

The evidence is kept locally, outside the repository: events, summary with all
13 check results, each worker's adapter record, the task and doctor views, and
service logs.

### What this shows, and what it does not

- Shown: lease expiry after a real crash and a real hang; retry to a different
  worker; dependency results surviving retries; a real late result discarded
  by the worker runtime; a stale report in the log ignored by replay.
- Not shown: restarting the coordinator itself during the trial (covered by
  `logplane demo recovery` with stand-in workers); OpenClaw usage in Logplane
  telemetry and budget accounting; tool-enabled editing.
