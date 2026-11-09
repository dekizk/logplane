# Claude Code trial notes

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
