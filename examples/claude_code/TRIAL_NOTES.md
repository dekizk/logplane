# Claude Code trial notes

## 2026-11-20 — tool-call approvals after the fixes, `--auth login --load-user-config`

Result: **passed every check.** Waiting for an answer no longer used up the
agent's time, an unanswered call was denied and Claude stopped cleanly, a deny
rule stopped `git push` with no prompt, and an expired request could not be
answered.

Setup: commit `6109b11`, Claude Code through the user's Claude login with
`--load-user-config`, `--gated-tools Bash,Write,Edit --timeout 300
--approval-wait 900 --max-turns 15`, a fresh scratch repository with no remote,
tasks submitted with `--max-retries 2`. Rules on each workflow: allow
`Bash:git status*` and `Bash:git -C * status`, deny `Bash:git push*`.

| Check | Task | Result |
|---|---|---|
| A wait longer than `--timeout` does not end the attempt | 19 | Write request #6191 approved after 428 s, past the 300 s timeout; the same attempt continued |
| A standing allow rule settles a call | 19, 20 | `git status` ran with no request |
| An unanswered call is denied | 19 | `git add` (#6412) denied after 900 s; Claude did not retry it, reported `blocked`, and listed every call accurately |
| An expired request cannot be answered | 17, 19 | `logplane approve 4712` and `logplane approve 6412` refused with "has expired"; nothing recorded |
| Human approvals continue the same attempt | 20 | `cat`, `git add`, `git commit` approved one by one; commit `f868c02 gate trial` landed |
| A deny rule stops a call with no prompt | 20 | `git push origin main` denied by `Bash:git push*` (rules event #6185); Claude reported `blocked` and asked whether to allow the push |

Every task ended on attempt 1. Claude treated a denial as final each time and
named the rule or timeout that stopped it.

Friction: `logplane approvals --workflow "$CID"` with `$CID` unset in that
shell listed every workflow instead of failing; an empty `--workflow` is now an
error. Answering promptly is still on the operator: with requests arriving one
at a time, a `while` loop over `logplane approvals` was the easiest way to
watch.

### Not yet shown

- `--gated-tools` with `--auth api-key` (`--bare`): whether a hook passed with
  `--settings` runs in bare mode.
- An unattended overnight run relying on standing rules and `--approval-wait`.

## 2026-11-17 — tool-call approvals, `--auth login --load-user-config`

Result: **the approval gate worked against Claude Code.** The hook loaded,
human approvals were honoured under `dontAsk`, a standing allow rule let a call
through without a prompt, and the same attempt continued after each answer.
The rule denial was not exercised live.

Setup: Claude Code through the user's Claude login with `--load-user-config`
(the gate is a hook, and safe mode does not run hooks),
`--gated-tools Bash,Write,Edit --timeout 1800 --max-turns 15`, a scratch git
repository with no remote, live bus and coordinator. Workflow rules:
allow `Bash:git status*`, deny `Bash:git push*`. Task: write `NOTES.md`, then
`git status`, `git add`, `git commit` and `git push origin main` as separate
calls.

Attempt 1 (task 18):

| Call | Outcome |
|---|---|
| Write `NOTES.md` | waited (request #4939), approved, ran |
| `git status` | allowed by the standing rule, no request |
| `git add NOTES.md` | waited (request #5269), approved |
| `git commit` with a two-line message | waited (request #5756): the newline keeps it from matching any allow rule |

The attempt then ended as `claude_timeout`: the operator answered slowly, and
time spent waiting counted against the 30-minute timeout. With zero retries the
task failed; the approval sent just afterwards reached nobody but was still
recorded as "Approved".

Attempt 2 (after `logplane retry`): Claude phrased the commands as
`git -C /Users/.../gate-trial status` and `git -C ... push origin main`. Neither
rule matched that spelling, so every call went to the operator, including the
push, which was approved by hand and failed only because the repository has no
`origin`. The commit landed (`eadb443 gate trial`), and Claude reported
`blocked`, asking whether to add a remote, with an accurate list of its calls.

What this changed:

- Waiting for an answer no longer counts against `--timeout`
  (`--approval-wait` bounds one call's wait instead).
- `logplane approve` / `deny` now refuse a request whose attempt has ended.
- Bash deny rules now see through git's global options and compound commands,
  so `Bash:git push*` denies `git -C repo push`. Allow rules stay literal.

### Not yet shown

- The same run with the three changes above, including a live rule denial.
- `--gated-tools` with `--auth api-key` (`--bare`): whether a hook passed with
  `--settings` runs in bare mode.
- An unattended run that relies on standing rules and `--approval-wait`.

## 2026-11-09 — one task, `--auth login`

Result: **passed all 4 checks** on one attempt: one attempt, the expected
result, completed on replay, and usage in workflow accounting.

Setup: Claude Code 2.1.319 through the user's Claude login (`claude auth
status` reported `claude.ai`), `--safe-mode`, no tools, default model, separate
live bus and coordinator, zero task retries.

| Event | Topic | Detail |
|---|---|---|
| 3 | task.created | |
| 5, 6 | task.assigned, task.started | `claude`, attempt 1 |
| 7 | telemetry.model.started | provider `claude-code-login` |
| 9 | telemetry.model.completed | model `claude-opus-5-5` |
| 11 | workflow.usage_recorded | 3,608 tokens, $0.030188 |
| 12 | task.completed | `{"value": 42}`, "Claude Code worker test passed" |

Assignment to completion took about 3.7 seconds. Claude Code reported 2 turns,
1.9 seconds of API time and no permission denials.

What the run's own init message confirmed:

- **Credential source `none`**, as required for a Claude login. Before the run,
  `claude auth status` reported `claude.ai`.
- **Tools: `StructuredOutput` only.** `--tools ""` removes every built-in
  tool but keeps the structured-output tool, and the outcome came from it
  (`outcome_source: structured_output`), not from the text fallback.
- No MCP servers, permission mode `dontAsk`.

Usage reached Logplane's workflow budget: the workflow view shows 3,608
tokens and $0.030188 with no missing token or cost reports. Tokens are input 2,
output 111 and cache creation 3,495 (the system prompt). The cost is Claude
Code's estimate at API prices; under a subscription login it is not what you
are charged.

No errors in the service logs; the working directory was empty afterwards.

### Not yet shown

- `--auth api-key`: covered by stand-in tests only; no live run yet.
- `--read-only-tools` on a real folder, and a multi-agent workflow with Claude.
- A workflow cost cap stopping Claude work in a live run (the accounting it
  relies on is shown above).
