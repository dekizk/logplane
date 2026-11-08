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
| Runs | `claude --safe-mode -p` | `claude --bare -p` |
| Loads | your user-level permission settings only. `--safe-mode` keeps hooks, plugins, skills, `CLAUDE.md`, MCP servers and auto memory off; project and local settings from the working directory never load | nothing from your machine: no settings, hooks, plugins, skills, MCP servers, auto memory or `CLAUDE.md` |
| Best for | trying it with what you already pay for | the same behaviour on every machine, CI, sharing a setup |

**Login mode makes sure it is really your login.** Before accepting any work,
the worker:

1. removes every variable that would switch authentication or provider from
   Claude Code's environment: all `ANTHROPIC_*` (API keys, tokens, gateways and
   base URLs, profiles, workload identity federation) and the Bedrock, Google
   Cloud and Foundry switches (`CLAUDE_CODE_USE_*`). It prints the names it
   removed, never the values. `CLAUDE_CODE_OAUTH_TOKEN`, a Claude login token,
   is kept;
2. runs `claude auth status` with that environment and refuses to start unless
   it reports a Claude login (`claude.ai` or `oauth_token`). An `apiKeyHelper`
   or credential set in your Claude Code settings is caught here.

`--load-user-config` turns your customizations back on (no `--safe-mode`). Your
hooks then run, and **hooks can run commands**, whatever tools Claude itself
has. Use it only if you want that.

**Api-key mode** refuses to start without `ANTHROPIC_API_KEY`. Set it in your
own shell; it is never written to evidence or logs. Bare mode reads nothing
else from your machine, so the key is what it uses.

Organization-managed settings (managed policy) apply in every mode, including
policy-configured hooks; this adapter cannot switch those off.

## What Claude may do

By default, nothing but answer: built-in tools are off (`--tools ""`), MCP tools
and slash commands are disabled, anything that would need approval is denied
(`--permission-mode dontAsk`), and the session is not saved. Claude is never
given tools to edit files or run commands. (In login mode with
`--load-user-config`, your own hooks can still run commands, as above.)

With `--read-only-tools`, Claude may also use Read, Grep and Glob, which suits
reviews. The boundary is Claude Code's permission system, not the adapter:
reads inside the working directory need no approval, and reads elsewhere would,
so `dontAsk` denies them. Two things could widen that, and the worker refuses
to start if it finds either:

- in login mode, user-level permission rules that grant more:
  `permissions.additionalDirectories`, or `Read`, `Grep` or `Glob` allow rules
  in your Claude Code `settings.json`;
- in either mode, symlinks inside the working directory that point outside it.

Managed policy can still grant more, and the check covers only your user
settings file. For material that must not leave a folder, use `--auth api-key`
on a copy of just that folder.

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

`tests/test_claude_code_example.py` covers both auth modes, removal of every
authentication and provider switch, the `claude auth status` check, safe mode
and its opt-out, the read boundary checks, the locked-down flags, prompt
delivery on stdin, every outcome and failure mapping, timeout, cancellation of
a running process, the output limit, usage reporting, and an end-to-end run
through a live bus, all with a stand-in `claude` and no credentials.

See [TRIAL_NOTES.md](TRIAL_NOTES.md) for live runs.
