# Contest hunt trial notes

Findings stay out of these notes: the contest's terms make them confidential.
Only counts, timings and failures are recorded.

## 2026-11-22 — Precept pilot, area A02, three runs

Setup: `feat/contest-hunt` after the approval-gate merge, `--skip-map --areas A02`,
workers through the user's own logins (Claude Code with a Max plan, Codex with
ChatGPT, Hermes with Nous). Estimated costs below are each tool's own estimate
at API prices, not what a subscription is charged.

| Run | Hunters | Result |
|---|---|---|
| 1 | astra (Codex `gpt-6.2-astra`), opus, glm | Codex hunt and triage refused by OpenAI's cyber safeguards with 0 tokens; opus 0 candidates ($1.96 est.), glm 3; triage failed, so the report was untriaged |
| 2 | fable, astra with `daybreak`, opus, glm | Codex exited 1 before any event: Daybreak is unavailable for `gpt-6.2-astra`, but the worker had discarded stderr; fable 0 ($7.07 est.), opus 0, glm 1; triage failed |
| 3 | fable, astra (`gpt-5.6-sol` with `daybreak`), opus, glm | All four hunts completed with 0 candidates, so triage did not run |

Run 3 in detail:

| Hunter | Minutes | Turns used / cap | Estimated cost |
|---|---:|---:|---:|
| opus (`opus`) | 3.2 | 23 / 120 | $1.51 |
| astra (Codex `gpt-5.6-sol`, Daybreak) | 3.8 | 1 turn | not reported |
| glm (Hermes, Nous) | 6.1 | — | $0.10 |
| fable (`claude-fable-5-2`) | 11.0 | 31 / 150 | $6.15 |

No lease expiries, crashes, stale results or retries in any run. Every hunt
finished far inside its timeout and turn cap, so the limits did not shape the
result.

What the runs changed:

- The Codex worker passes `-c daybreak=true` when a model sets
  `"daybreak": true`, and checks the CLI version first.
- A refusal by OpenAI's cyber safeguards is reported as `codex_safety_refusal`.
- Codex's own error output is kept, so an early exit says why.

### Not yet shown

- A triage pass over real candidates.
- The map stage and a full run across all ten seed areas.
- A way to rerun triage alone after it fails.
