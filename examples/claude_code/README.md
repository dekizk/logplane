# Claude Code: a Logplane worker

This optional example runs one non-interactive Claude Code turn (`claude -p`)
per assignment. Logplane owns registration, heartbeats, leases, retries and
completion. Claude Code owns the agent turn. Nothing here is imported by
Logplane's core.

Each assignment starts a fresh Claude Code process that exits when it's done.
It does not share context or memory with any interactive Claude session.

## Choose how Claude Code authenticates

You must pick one with `--auth`; there is no default, because the two are
billed differently.

| | `--auth login` | `--auth api-key` |
|---|---|---|
| Uses | your normal Claude Code login, for example a subscription | `ANTHROPIC_API_KEY`, billed through the Claude API |
| Runs | `claude -p` | `claude --bare -p` |
| Loads | your **user-level** Claude Code settings only, such as `~/.claude` hooks, plugins and `CLAUDE.md`; project and local settings from the working directory do not load | nothing from your machine: no hooks, plugins, skills, MCP servers, auto memory or `CLAUDE.md` |
| Best for | trying it with what you already pay for | the same behaviour on every machine, CI, sharing a setup |

In login mode, the worker removes `ANTHROPIC_API_KEY` from Claude Code's
environment, so an exported key cannot silently switch a run to API billing.
In api-key mode the worker refuses to start without the key. Set it in your own
shell; it is never written to evidence or logs.

## What Claude may do

By default, nothing but answer: built-in tools are off (`--tools ""`), MCP tools
and slash commands are disabled, anything that would need approval is denied
(`--permission-mode dontAsk`), and the session is not saved. With
`--read-only-tools`, Claude may also use Read, Grep and Glob in the working
directory, which suits reviews. No mode here lets it edit files or run commands.

Each call is bounded by a worker timeout, a turn limit (`--max-turns`, default
4), and Claude Code's own spending stop (`--max-budget-usd`, default $0.50).
Timeouts and cancellation terminate the whole process group. Charges already
incurred cannot be undone.

Results come back through Claude Code's structured output (`--json-schema`), so
the outcome is validated rather than parsed out of free text. If a run finishes
without structured output, the adapter accepts a reply that is exactly the
outcome JSON, and records which path it used as `outcome_source`.

## Try one task

From the repository root, with the development install from the root README:

```sh
.venv/bin/python -m examples.claude_code.live_smoke --yes --auth login
# or, with ANTHROPIC_API_KEY set in your shell:
.venv/bin/python -m examples.claude_code.live_smoke --yes --auth api-key
```

`--yes` approves one paid call. The script starts a separate bus and
coordinator, runs a Claude Code worker, submits one task returning
`{"value": 42}`, and checks four things: one attempt, the expected result, the
replayed state, and that the call's tokens and cost reached Logplane's
workflow accounting. It prints the evidence directory. Add `--model sonnet` (or
another alias or name) to choose a model, and `--claude-command PATH` if
`claude` is not on your PATH.

## Connect to your own bus

```sh
.venv/bin/python -m examples.claude_code.run_worker --auth login \
  --working-directory ~/src/my-project --read-only-tools
```

Then send it work:

```sh
logplane submit "Summarise how authentication works in this project" --capability claude
```

Useful options: `--name`, `--capability` (repeatable, default `claude`),
`--model`, `--effort`, `--timeout`, `--max-turns`, `--max-budget-usd`,
`--capacity`, and `--record FILE` to keep a JSONL record of each assignment and
outcome.

## Budgets

The worker reports each call's tokens and Claude Code's cost estimate to
Logplane, so a workflow cost cap covers Claude workers too:

```sh
logplane policy WORKFLOW_ID --max-cost 2.00 \
  --reserve-cost-per-assignment 0.50 --reason "Claude budget"
```

Reserving the same amount as `--max-budget-usd` means a single call can't take
the workflow far past its cap. The cost is Claude Code's client-side estimate
at API prices: it can differ from your bill, and under a subscription login it
is not what you are charged. Use `--no-telemetry` to stop reporting usage.

## Tests and status

`tests/test_claude_code_example.py` covers both auth modes, the locked-down
flags, prompt delivery on stdin, every outcome and failure mapping, timeout,
cancellation, the output limit, usage reporting, and an end-to-end run through
a live bus, all with a stand-in `claude` and no credentials.

See [TRIAL_NOTES.md](TRIAL_NOTES.md) for live runs.
